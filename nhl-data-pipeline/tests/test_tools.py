"""Tests for assistant/tools.py — tool definitions and dispatch."""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player
from assistant.tools import TOOLS, SessionContext, dispatch_tool


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database with minimal test data."""
    init_db(db_path)
    conn = get_db(db_path)

    # Players
    upsert_player(conn, {
        "id": 8478402, "full_name": "Connor McDavid",
        "first_name": "Connor", "last_name": "McDavid",
        "team_abbrev": "EDM", "position": "C",
    })
    upsert_player(conn, {
        "id": 8471675, "full_name": "Sidney Crosby",
        "first_name": "Sidney", "last_name": "Crosby",
        "team_abbrev": "PIT", "position": "C",
    })
    upsert_player(conn, {
        "id": 8477424, "full_name": "Juuse Saros",
        "first_name": "Juuse", "last_name": "Saros",
        "team_abbrev": "NSH", "position": "G",
    })
    upsert_player(conn, {
        "id": 8479318, "full_name": "Leon Draisaitl",
        "first_name": "Leon", "last_name": "Draisaitl",
        "team_abbrev": "EDM", "position": "C",
    })

    # Season totals
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8478402, NULL, '20252026', 1, 30, 40, 70, 150, 75, 200, 15, 10, 72000)"
    )
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8471675, NULL, '20252026', 1, 20, 30, 50, 75, 45, 150, 10, 8, 60000)"
    )
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8479318, NULL, '20252026', 1, 25, 35, 60, 100, 50, 180, 12, 6, 66000)"
    )

    # Per-game rows
    for i in range(15):
        gd = f"2025-10-{(10 + i):02d}"
        conn.execute(
            f"INSERT INTO skater_stats "
            f"(player_id, game_date, season, is_season_total, goals, assists, points, "
            f"hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8478402, '{gd}', '20252026', 0, 2, 3, 5, 10, 5, 14, 1, 0, 1200)"
        )
        conn.execute(
            f"INSERT INTO skater_stats "
            f"(player_id, game_date, season, is_season_total, goals, assists, points, "
            f"hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8471675, '{gd}', '20252026', 0, 1, 2, 3, 5, 3, 10, 1, 0, 1100)"
        )
        conn.execute(
            f"INSERT INTO skater_stats "
            f"(player_id, game_date, season, is_season_total, goals, assists, points, "
            f"hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8479318, '{gd}', '20252026', 0, 2, 2, 4, 7, 4, 12, 1, 0, 1150)"
        )

    # Goalie
    conn.execute(
        "INSERT INTO goalie_stats "
        "(player_id, game_date, season, is_season_total, "
        "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
        "VALUES (8477424, NULL, '20252026', 1, 20, 10, 5, 3, 1500, 80, 1580, 108000)"
    )
    for i in range(10):
        gd = f"2025-10-{(10 + i):02d}"
        w = 1 if i % 3 != 2 else 0
        conn.execute(
            f"INSERT INTO goalie_stats "
            f"(player_id, game_date, season, is_season_total, "
            f"wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            f"VALUES (8477424, '{gd}', '20252026', 0, {w}, {1 - w}, 0, 0, 30, 2, 32, 3600)"
        )

    # Fantasy setup
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team1', 'lg1', 'My Team', 'MT')"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Connor McDavid', 'C', 'active', 12500000)"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Juuse Saros', 'G', 'active', 5000000)"
    )
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team1', 1, 50, 20, 100, 5000.5, 4200.0, 'W3', 70, 71.4)"
    )

    # Injuries
    conn.execute(
        "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
        "VALUES (8471675, 'rotowire', 'Upper Body', 'Day-to-Day', '2026-02-18')"
    )

    # News
    conn.execute(
        "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
        "VALUES ('news001', 8478402, 'McDavid: Hat Trick', 'Scored three goals.', '2026-02-18')"
    )

    # Team games
    conn.execute(
        "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
        "VALUES ('EDM', '20252026', '2026-02-25', 'CGY', 'home')"
    )

    # Second fantasy team (opponent for trade tests)
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team2', 'lg1', 'Other Team', 'OT')"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team2', 'Sidney Crosby', 'C', 'active', 8700000)"
    )
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team2', 5, 35, 35, 70, 4000.0, 4100.0, 'L2', 70, 57.1)"
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def ctx(db: sqlite3.Connection) -> SessionContext:
    return SessionContext(conn=db, team_id="team1", season="20252026")


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------


class TestToolDefinitions:
    """Tests for the TOOLS list structure."""

    def test_correct_tool_count(self) -> None:
        assert len(TOOLS) == 14

    def test_all_tools_have_required_fields(self) -> None:
        for tool in TOOLS:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            assert tool["input_schema"]["type"] == "object"

    def test_tool_names_unique(self) -> None:
        names = [t["name"] for t in TOOLS]
        assert len(names) == len(set(names))

    def test_expected_tool_names(self) -> None:
        names = {t["name"] for t in TOOLS}
        expected = {
            "get_my_roster", "get_roster_analysis", "search_free_agents",
            "get_player_stats", "compare_players", "get_player_trends",
            "get_news_briefing", "get_schedule_analysis",
            "get_league_standings", "get_injuries",
            "get_trade_targets", "get_roster_moves",
            "get_team_roster", "suggest_trades",
        }
        assert names == expected


# ---------------------------------------------------------------------------
# SessionContext
# ---------------------------------------------------------------------------


class TestSessionContext:
    """Tests for SessionContext dataclass."""

    def test_creation(self, db: sqlite3.Connection) -> None:
        ctx = SessionContext(conn=db, team_id="t1", season="20252026")
        assert ctx.team_id == "t1"
        assert ctx.season == "20252026"


# ---------------------------------------------------------------------------
# dispatch_tool — happy path
# ---------------------------------------------------------------------------


class TestDispatchTool:
    """Tests for dispatch_tool function."""

    def test_get_my_roster(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_my_roster", {}, ctx)
        assert "Connor McDavid" in result

    def test_get_my_roster_sort_by(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_my_roster", {"sort_by": "fantasy_points"}, ctx)
        assert isinstance(result, str)

    def test_get_roster_analysis(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_roster_analysis", {}, ctx)
        assert "Roster Analysis" in result
        assert "Position Breakdown" in result

    def test_search_free_agents(self, ctx: SessionContext) -> None:
        result = dispatch_tool("search_free_agents", {"min_games": 1}, ctx)
        assert "Leon Draisaitl" in result  # not rostered on any team

    def test_search_free_agents_with_filters(self, ctx: SessionContext) -> None:
        result = dispatch_tool("search_free_agents", {
            "position": "C", "min_games": 1, "limit": 5,
        }, ctx)
        assert isinstance(result, str)

    def test_get_player_stats(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_player_stats", {"player_name": "Connor McDavid"}, ctx)
        assert "Connor McDavid" in result
        assert "EDM" in result

    def test_get_player_stats_not_found(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_player_stats", {"player_name": "Wayne Gretzky"}, ctx)
        assert "not found" in result.lower()

    def test_compare_players(self, ctx: SessionContext) -> None:
        result = dispatch_tool("compare_players", {
            "player_names": ["Connor McDavid", "Sidney Crosby"],
        }, ctx)
        assert "Connor McDavid" in result
        assert "Sidney Crosby" in result

    def test_compare_players_all_unknown(self, ctx: SessionContext) -> None:
        result = dispatch_tool("compare_players", {
            "player_names": ["Wayne Gretzky", "Bobby Orr"],
        }, ctx)
        assert "could not find" in result.lower()

    def test_compare_players_partial_unknown(self, ctx: SessionContext) -> None:
        result = dispatch_tool("compare_players", {
            "player_names": ["Connor McDavid", "Wayne Gretzky"],
        }, ctx)
        assert "Connor McDavid" in result
        assert "Could not find" in result

    def test_get_player_trends(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_player_trends", {"player_name": "Connor McDavid"}, ctx)
        assert "Trends" in result

    def test_get_player_trends_not_found(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_player_trends", {"player_name": "Nobody"}, ctx)
        assert "not found" in result.lower()

    def test_get_news_briefing_player(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_news_briefing", {"player_name": "Connor McDavid"}, ctx)
        assert "Hat Trick" in result

    def test_get_news_briefing_roster(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_news_briefing", {}, ctx)
        assert isinstance(result, str)

    def test_get_schedule_analysis(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_schedule_analysis", {
            "team_or_player": "EDM", "days_ahead": 30,
        }, ctx)
        assert "EDM" in result

    def test_get_schedule_analysis_not_found(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_schedule_analysis", {
            "team_or_player": "Nobody",
        }, ctx)
        assert "could not find" in result.lower()

    def test_get_league_standings(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_league_standings", {}, ctx)
        assert "My Team" in result

    def test_get_injuries_my_roster(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_injuries", {"scope": "my_roster"}, ctx)
        assert isinstance(result, str)

    def test_get_injuries_all(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_injuries", {"scope": "all"}, ctx)
        assert "Sidney Crosby" in result

    def test_get_injuries_team(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_injuries", {"scope": "team", "team": "PIT"}, ctx)
        assert "Sidney Crosby" in result

    def test_get_injuries_team_no_abbrev(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_injuries", {"scope": "team"}, ctx)
        assert "specify a team" in result.lower()

    def test_get_trade_targets(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_trade_targets", {}, ctx)
        assert isinstance(result, str)

    def test_get_roster_moves(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_roster_moves", {}, ctx)
        assert "RECOMMENDED" in result

    def test_get_team_roster(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_team_roster", {"team_name": "Other Team"}, ctx)
        assert "Sidney Crosby" in result

    def test_get_team_roster_not_found(self, ctx: SessionContext) -> None:
        result = dispatch_tool("get_team_roster", {"team_name": "Nonexistent"}, ctx)
        assert "not found" in result.lower()

    def test_suggest_trades(self, ctx: SessionContext) -> None:
        result = dispatch_tool("suggest_trades", {"opponent_team_name": "Other Team"}, ctx)
        assert isinstance(result, str)

    def test_suggest_trades_not_found(self, ctx: SessionContext) -> None:
        result = dispatch_tool("suggest_trades", {"opponent_team_name": "Nonexistent"}, ctx)
        assert "not found" in result.lower()


# ---------------------------------------------------------------------------
# dispatch_tool — error and edge cases
# ---------------------------------------------------------------------------


class TestDispatchToolEdgeCases:
    """Tests for dispatch_tool error handling."""

    def test_unknown_tool(self, ctx: SessionContext) -> None:
        result = dispatch_tool("nonexistent_tool", {}, ctx)
        assert "Unknown tool" in result

    def test_exception_caught(self, ctx: SessionContext) -> None:
        """dispatch_tool catches exceptions and returns error string."""
        # get_player_stats requires player_name key
        result = dispatch_tool("get_player_stats", {}, ctx)
        assert "Error executing" in result

    def test_returns_string(self, ctx: SessionContext) -> None:
        """All dispatch results are strings."""
        for tool in TOOLS:
            result = dispatch_tool(tool["name"], {}, ctx)
            assert isinstance(result, str), f"{tool['name']} returned {type(result)}"
