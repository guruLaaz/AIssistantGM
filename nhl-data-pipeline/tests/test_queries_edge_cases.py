"""Edge case tests for assistant/queries.py.

Covers boundary values, empty inputs, None values, and unusual data patterns
not covered by the main test_queries.py.
"""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player
from assistant.queries import (
    get_my_roster,
    get_roster_analysis,
    search_free_agents,
    get_player_stats,
    compare_players,
    get_player_trends,
    get_recent_news,
    get_schedule_analysis,
    get_league_standings,
    get_injuries,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def empty_db(db_path: Path) -> sqlite3.Connection:
    """Initialized DB with no data."""
    init_db(db_path)
    return get_db(db_path)


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """DB with minimal data for edge case tests."""
    init_db(db_path)
    conn = get_db(db_path)

    # Two players, one with minimal data
    upsert_player(conn, {
        "id": 1001, "full_name": "Test Skater",
        "first_name": "Test", "last_name": "Skater",
        "team_abbrev": "TOR", "position": "C",
    })
    upsert_player(conn, {
        "id": 1002, "full_name": "Zero Stats",
        "first_name": "Zero", "last_name": "Stats",
        "team_abbrev": "MTL", "position": "LW",
    })
    upsert_player(conn, {
        "id": 1003, "full_name": "Test Goalie",
        "first_name": "Test", "last_name": "Goalie",
        "team_abbrev": "TOR", "position": "G",
    })
    upsert_player(conn, {
        "id": 1004, "full_name": "O'Brien-Smith Jr.",
        "first_name": "O'Brien", "last_name": "Smith Jr.",
        "team_abbrev": "BOS", "position": "D",
    })

    # Season totals for Test Skater
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (1001, NULL, '20252026', 1, 10, 15, 25, 30, 20, 80, 5, 4, 36000)"
    )

    # Season totals with all zeros for Zero Stats
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (1002, NULL, '20252026', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
    )

    # Special char player with season totals
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (1004, NULL, '20252026', 1, 5, 10, 15, 40, 80, 60, -3, 20, 30000)"
    )

    # Per-game rows: only 3 games for Test Skater
    for i, gd in enumerate(["2025-10-10", "2025-10-12", "2025-10-14"]):
        conn.execute(
            f"INSERT INTO skater_stats "
            f"(player_id, game_date, season, is_season_total, goals, assists, points, "
            f"hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (1001, '{gd}', '20252026', 0, 3, 5, 8, 10, 7, 26, 2, 2, 1200)"
        )

    # Only 1 game for Zero Stats
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (1002, '2025-10-10', '20252026', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)"
    )

    # 3 games for special char player
    for gd in ["2025-10-10", "2025-10-12", "2025-10-14"]:
        conn.execute(
            f"INSERT INTO skater_stats "
            f"(player_id, game_date, season, is_season_total, goals, assists, points, "
            f"hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (1004, '{gd}', '20252026', 0, 2, 3, 5, 13, 27, 20, -1, 7, 1000)"
        )

    # Goalie season totals
    conn.execute(
        "INSERT INTO goalie_stats "
        "(player_id, game_date, season, is_season_total, "
        "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
        "VALUES (1003, NULL, '20252026', 1, 0, 0, 0, 0, 0, 0, 0, 0)"
    )

    # Fantasy teams
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team1', 'lg1', 'My Team', 'MT')"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Test Skater', 'C', 'active', 5000000)"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Test Goalie', 'G', 'active', 3000000)"
    )

    # Line deployment for free agent skaters (required by deployment filter)
    conn.execute(
        "INSERT INTO line_combinations "
        "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
        "VALUES (1002, 'MTL', 'Zero Stats', 'LW', 3, NULL, datetime('now'))"
    )
    conn.execute(
        "INSERT INTO line_combinations "
        "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
        "VALUES (1004, 'BOS', 'O''Brien-Smith Jr.', 'D', 2, 2, datetime('now'))"
    )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Empty DB edge cases
# ---------------------------------------------------------------------------


class TestEmptyDB:
    """Tests against a completely empty database."""

    def test_get_my_roster_empty_db(self, empty_db: sqlite3.Connection) -> None:
        assert get_my_roster(empty_db, "team1", "20252026") == []

    def test_get_roster_analysis_empty_db(self, empty_db: sqlite3.Connection) -> None:
        result = get_roster_analysis(empty_db, "team1", "20252026")
        assert result["position_counts"] == {"F": 0, "D": 0, "G": 0}
        assert result["bottom_performers"] == []
        assert result["injured_players"] == []

    def test_search_free_agents_empty_db(self, empty_db: sqlite3.Connection) -> None:
        assert search_free_agents(empty_db, "20252026") == []

    def test_get_league_standings_empty_db(self, empty_db: sqlite3.Connection) -> None:
        assert get_league_standings(empty_db) == []

    def test_get_injuries_empty_db(self, empty_db: sqlite3.Connection) -> None:
        assert get_injuries(empty_db, scope="all") == []

    def test_get_recent_news_empty_db(self, empty_db: sqlite3.Connection) -> None:
        assert get_recent_news(empty_db) == []


# ---------------------------------------------------------------------------
# Boundary values
# ---------------------------------------------------------------------------


class TestBoundaryValues:
    """Tests for boundary and edge values."""

    def test_search_free_agents_min_games_zero(self, db: sqlite3.Connection) -> None:
        """min_games=0 returns players with any number of games."""
        fa = search_free_agents(db, "20252026", min_games=0)
        assert len(fa) >= 1

    def test_search_free_agents_limit_one(self, db: sqlite3.Connection) -> None:
        """limit=1 returns at most 1 result."""
        fa = search_free_agents(db, "20252026", min_games=0, limit=1)
        assert len(fa) <= 1

    def test_search_free_agents_limit_zero(self, db: sqlite3.Connection) -> None:
        """limit=0 returns empty list."""
        fa = search_free_agents(db, "20252026", min_games=0, limit=0)
        assert fa == []

    def test_get_player_stats_recent_games_zero(self, db: sqlite3.Connection) -> None:
        """recent_games=0 returns empty game log."""
        result = get_player_stats(db, "Test Skater", "20252026", recent_games=0)
        assert result is not None
        assert result["game_log"] == []

    def test_get_player_stats_recent_games_exceeds_total(self, db: sqlite3.Connection) -> None:
        """recent_games > actual games returns all available games."""
        result = get_player_stats(db, "Test Skater", "20252026", recent_games=100)
        assert result is not None
        assert len(result["game_log"]) == 3  # only 3 games exist

    def test_get_recent_news_limit_zero(self, db: sqlite3.Connection) -> None:
        """limit=0 returns empty."""
        news = get_recent_news(db, limit=0)
        assert news == []

    def test_schedule_analysis_days_ahead_zero(self, db: sqlite3.Connection) -> None:
        """days_ahead=0 returns no future games."""
        # Add a game in the future
        db.execute(
            "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
            "VALUES ('TOR', '20252026', '2026-03-01', 'MTL', 'home')"
        )
        db.commit()
        result = get_schedule_analysis(db, "TOR", "20252026", days_ahead=0)
        assert result is not None
        assert result["game_count"] == 0


# ---------------------------------------------------------------------------
# Special characters and player names
# ---------------------------------------------------------------------------


class TestSpecialCharacters:
    """Tests for special characters in player names."""

    def test_player_with_apostrophe_and_hyphen(self, db: sqlite3.Connection) -> None:
        """Player with special chars in name resolves correctly."""
        result = get_player_stats(db, "O'Brien-Smith", "20252026")
        assert result is not None
        assert result["player"]["full_name"] == "O'Brien-Smith Jr."

    def test_compare_with_special_chars(self, db: sqlite3.Connection) -> None:
        """compare_players handles special character names."""
        result = compare_players(db, ["O'Brien-Smith", "Test Skater"], "20252026")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Zero and missing stats
# ---------------------------------------------------------------------------


class TestZeroStats:
    """Tests for players with zero or missing statistics."""

    def test_player_with_zero_season_stats(self, db: sqlite3.Connection) -> None:
        """Player with all zeros still returns valid stats."""
        result = get_player_stats(db, "Zero Stats", "20252026")
        assert result is not None
        assert result["season_stats"]["goals"] == 0
        assert result["season_stats"]["fantasy_points"] == 0.0
        assert result["season_stats"]["fpts_per_game"] == 0.0

    def test_goalie_with_zero_toi(self, db: sqlite3.Connection) -> None:
        """Goalie with 0 TOI doesn't divide by zero for GAA."""
        result = get_player_stats(db, "Test Goalie", "20252026")
        assert result is not None
        assert result["season_stats"].get("gaa", 0.0) == 0.0

    def test_goalie_with_zero_shots_against(self, db: sqlite3.Connection) -> None:
        """Goalie with 0 shots_against doesn't divide by zero for SV%."""
        result = get_player_stats(db, "Test Goalie", "20252026")
        assert result is not None
        assert result["season_stats"].get("sv_pct", 0.0) == 0.0


# ---------------------------------------------------------------------------
# Wrong season
# ---------------------------------------------------------------------------


class TestWrongSeason:
    """Tests for queries with a season that has no data."""

    def test_roster_wrong_season(self, db: sqlite3.Connection) -> None:
        """Roster with wrong season returns players but 0 stats."""
        roster = get_my_roster(db, "team1", "20242025")
        # Players still show up from roster slots, but with no stats
        for p in roster:
            assert p["games_played"] == 0
            assert p["fantasy_points"] == 0.0

    def test_player_stats_wrong_season(self, db: sqlite3.Connection) -> None:
        """Player stats with wrong season returns empty stats."""
        result = get_player_stats(db, "Test Skater", "20242025")
        assert result is not None
        assert result["season_stats"] == {}

    def test_trends_wrong_season(self, db: sqlite3.Connection) -> None:
        """Trends with wrong season returns neutral with empty windows."""
        result = get_player_trends(db, "Test Skater", "20242025")
        assert result is not None
        assert result["trend"] == "neutral"


# ---------------------------------------------------------------------------
# Unresolvable roster players
# ---------------------------------------------------------------------------


class TestUnresolvableRosterPlayers:
    """Tests for roster slots with names that don't match any NHL player."""

    def test_unresolvable_player_on_roster(self, db: sqlite3.Connection) -> None:
        """Roster slot with unresolvable name returns partial entry."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Totally Fake Player', 'LW', 'active', 1000000)"
        )
        db.commit()
        roster = get_my_roster(db, "team1", "20252026")
        fake = next(p for p in roster if p["player_name"] == "Totally Fake Player")
        assert fake["nhl_id"] is None
        assert fake["games_played"] == 0

    def test_empty_name_slot_skipped(self, db: sqlite3.Connection) -> None:
        """Roster slot with empty player_name is skipped."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', '', 'C', 'active', 0)"
        )
        db.commit()
        roster = get_my_roster(db, "team1", "20252026")
        names = [p["player_name"] for p in roster]
        assert "" not in names

    def test_null_name_slot_skipped(self, db: sqlite3.Connection) -> None:
        """Roster slot with NULL player_name is skipped."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', NULL, 'C', 'active', 0)"
        )
        db.commit()
        roster = get_my_roster(db, "team1", "20252026")
        names = [p["player_name"] for p in roster]
        assert None not in names


# ---------------------------------------------------------------------------
# compare_players edge cases
# ---------------------------------------------------------------------------


class TestComparePlayersEdgeCases:
    """Edge cases for compare_players."""

    def test_empty_list(self, db: sqlite3.Connection) -> None:
        result = compare_players(db, [], "20252026")
        assert result == []

    def test_all_unknown(self, db: sqlite3.Connection) -> None:
        result = compare_players(db, ["Nobody", "Also Nobody"], "20252026")
        assert result == []

    def test_duplicate_names(self, db: sqlite3.Connection) -> None:
        """Same player name twice returns two entries."""
        result = compare_players(db, ["Test Skater", "Test Skater"], "20252026")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# get_player_trends edge cases
# ---------------------------------------------------------------------------


class TestTrendsEdgeCases:
    """Edge cases for get_player_trends."""

    def test_player_with_fewer_than_7_games(self, db: sqlite3.Connection) -> None:
        """Player with 3 games: windows capped correctly."""
        result = get_player_trends(db, "Test Skater", "20252026")
        assert result is not None
        assert result["windows"]["last_7"]["games"] == 3
        assert result["windows"]["last_14"]["games"] == 3
        assert result["windows"]["season"]["games"] == 3

    def test_player_with_1_game(self, db: sqlite3.Connection) -> None:
        """Player with 1 game: all windows have 1 game."""
        result = get_player_trends(db, "Zero Stats", "20252026")
        assert result is not None
        assert result["windows"]["last_7"]["games"] == 1

    def test_zero_fpg_trend_is_neutral(self, db: sqlite3.Connection) -> None:
        """Player with 0 FP/G season average: trend is neutral (no division by zero)."""
        result = get_player_trends(db, "Zero Stats", "20252026")
        assert result is not None
        assert result["trend"] == "neutral"


# ---------------------------------------------------------------------------
# Schedule edge cases
# ---------------------------------------------------------------------------


class TestScheduleEdgeCases:
    """Edge cases for get_schedule_analysis."""

    def test_lowercase_team_abbrev_not_detected(self, db: sqlite3.Connection) -> None:
        """Lowercase input treated as player name, not team abbrev."""
        result = get_schedule_analysis(db, "tor", "20252026")
        # "tor" is not uppercase and not 3-letter uppercase, so treated as player name
        # No player named "tor" -> returns None
        assert result is None

    def test_two_letter_code_treated_as_team(self, db: sqlite3.Connection) -> None:
        """2-letter uppercase input treated as team abbreviation (len <= 3)."""
        result = get_schedule_analysis(db, "TO", "20252026")
        # "TO" is <= 3 chars and uppercase, so treated as team abbrev
        assert result is not None
        assert result["team"] == "TO"
        assert result["game_count"] == 0

    def test_no_games_in_window(self, db: sqlite3.Connection) -> None:
        """Team with no games in the window returns 0 count."""
        result = get_schedule_analysis(db, "TOR", "20252026", days_ahead=1)
        assert result is not None
        assert result["game_count"] == 0
        assert result["back_to_backs"] == []


# ---------------------------------------------------------------------------
# Injuries edge cases
# ---------------------------------------------------------------------------


class TestInjuriesEdgeCases:
    """Edge cases for get_injuries."""

    def test_invalid_scope_returns_empty(self, db: sqlite3.Connection) -> None:
        """Invalid scope falls through to my_roster with no team_id."""
        result = get_injuries(db, scope="invalid_scope")
        assert result == []

    def test_team_scope_nonexistent_team(self, db: sqlite3.Connection) -> None:
        """Team scope with non-existent abbreviation returns empty."""
        result = get_injuries(db, scope="team", team_id="ZZZ")
        assert result == []


# ---------------------------------------------------------------------------
# search_free_agents edge cases
# ---------------------------------------------------------------------------


class TestSearchFreeAgentsEdgeCases:
    """Edge cases for search_free_agents."""

    def test_sort_by_fantasy_points(self, db: sqlite3.Connection) -> None:
        """sort_by='fantasy_points' works without error."""
        fa = search_free_agents(db, "20252026", sort_by="fantasy_points", min_games=0)
        assert isinstance(fa, list)

    def test_position_filter_goalie(self, db: sqlite3.Connection) -> None:
        """Position filter G returns only goalies."""
        fa = search_free_agents(db, "20252026", position="G", min_games=0)
        for p in fa:
            assert p["position"] == "G"

    def test_position_filter_forward(self, db: sqlite3.Connection) -> None:
        """Position filter F returns C, LW, RW."""
        fa = search_free_agents(db, "20252026", position="F", min_games=0)
        for p in fa:
            assert p["position"] in ("C", "LW", "RW", "F")

    def test_all_players_rostered(self, db: sqlite3.Connection) -> None:
        """When all players are rostered, no free agents returned."""
        # Add all remaining players to a roster (use parameterized query for names with quotes)
        for name in ["Zero Stats", "O'Brien-Smith Jr.", "Test Goalie"]:
            db.execute(
                "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
                "VALUES (?, ?, 'C', 'active', 0)",
                ("team1", name),
            )
        db.commit()
        fa = search_free_agents(db, "20252026", min_games=0)
        assert fa == []
