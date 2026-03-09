"""Tests for assistant/queries.py — data query layer."""

import sqlite3
from datetime import date, timedelta
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
    get_nhl_standings,
    get_injuries,
    get_trade_candidates,
    get_drop_candidates,
    get_pickup_recommendations,
    suggest_trades,
    _get_skater_season_stats,
    _get_goalie_season_stats,
    _get_recent_pp_toi,
    _is_goalie,
    _position_group,
    _get_fantasy_gp,
)
from config.fantasy_constants import FORWARD_PLAYABLE_TOI_PER_GAME


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database with test data."""
    init_db(db_path)
    conn = get_db(db_path)

    # --- Players ---
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
        "id": 8480069, "full_name": "Cale Makar",
        "first_name": "Cale", "last_name": "Makar",
        "team_abbrev": "COL", "position": "D",
    })
    upsert_player(conn, {
        "id": 8477424, "full_name": "Juuse Saros",
        "first_name": "Juuse", "last_name": "Saros",
        "team_abbrev": "NSH", "position": "G",
    })
    # Free agent (not on any roster)
    upsert_player(conn, {
        "id": 8477934, "full_name": "Leon Draisaitl",
        "first_name": "Leon", "last_name": "Draisaitl",
        "team_abbrev": "EDM", "position": "C",
    })

    # --- Skater stats: season totals (with correct hits/blocks from API) ---
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
        "VALUES (8480069, NULL, '20252026', 1, 15, 45, 60, 45, 120, 180, 20, 5, 65000)"
    )
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8477934, NULL, '20252026', 1, 25, 35, 60, 105, 60, 170, 12, 6, 62000)"
    )

    # --- Skater per-game rows (with real hits/blocks) ---
    game_dates = [f"2025-10-{d:02d}" for d in range(10, 25)]

    for i, gd in enumerate(game_dates):
        # McDavid: 2G, 3A per game, 10 hits, 5 blocks
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8478402, '{gd}', '20252026', 0, 2, 3, 5, 10, 5, 14, 1, 0, 1200)"
        )
        # Crosby: 1G, 2A per game, 5 hits, 3 blocks
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8471675, '{gd}', '20252026', 0, 1, 2, 3, 5, 3, 10, 1, 0, 1100)"
        )
        # Makar: 1G, 3A per game, 3 hits, 8 blocks
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8480069, '{gd}', '20252026', 0, 1, 3, 4, 3, 8, 12, 2, 0, 1300)"
        )
        # Draisaitl: 2G, 2A per game, 7 hits, 4 blocks
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8477934, '{gd}', '20252026', 0, 2, 2, 4, 7, 4, 11, 1, 0, 1150)"
        )

    # --- Goalie stats ---
    conn.execute(
        "INSERT INTO goalie_stats "
        "(player_id, game_date, season, is_season_total, "
        "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
        "VALUES (8477424, NULL, '20252026', 1, 20, 10, 5, 3, 1500, 80, 1580, 108000)"
    )
    for i, gd in enumerate(game_dates[:10]):
        w = 1 if i % 3 != 2 else 0
        l = 0 if w else 1
        so = 1 if i == 0 else 0
        conn.execute(
            "INSERT INTO goalie_stats "
            "(player_id, game_date, season, is_season_total, "
            "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            f"VALUES (8477424, '{gd}', '20252026', 0, "
            f"{w}, {l}, 0, {so}, 30, 2, 32, 3600)"
        )

    # --- Fantasy teams and roster ---
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team1', 'lg1', 'My Team', 'MT')"
    )
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team2', 'lg1', 'Other Team', 'OT')"
    )

    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Connor McDavid', 'C', 'active', 12500000)"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Sidney Crosby', 'C', 'active', 8700000)"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Cale Makar', 'D', 'active', 9000000)"
    )
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team1', 'Juuse Saros', 'G', 'active', 5000000)"
    )

    # --- Fantasy standings ---
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team1', 1, 50, 20, 100, 5000.5, 4200.0, 'W3', 70, 71.4)"
    )
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team2', 2, 45, 25, 90, 4800.0, 4300.0, 'L1', 70, 68.6)"
    )

    # --- Injuries ---
    conn.execute(
        "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
        "VALUES (8471675, 'rotowire', 'Upper Body', 'Day-to-Day', '2026-02-18')"
    )

    # --- News ---
    conn.execute(
        "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
        "VALUES ('news001', 8478402, 'McDavid: Hat Trick', 'Scored three goals.', '2026-02-18')"
    )
    conn.execute(
        "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
        "VALUES ('news002', 8471675, 'Crosby: Injured', 'Upper body injury.', '2026-02-17')"
    )

    # --- Team games (upcoming, computed relative to today) ---
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    day_after = (date.today() + timedelta(days=2)).isoformat()
    next_week = (date.today() + timedelta(days=7)).isoformat()
    conn.execute(
        "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
        "VALUES ('EDM', '20252026', ?, 'CGY', 'home')", (tomorrow,)
    )
    conn.execute(
        "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
        "VALUES ('EDM', '20252026', ?, 'VAN', 'away')", (day_after,)
    )
    conn.execute(
        "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
        "VALUES ('EDM', '20252026', ?, 'SEA', 'home')", (next_week,)
    )

    conn.commit()
    return conn


# ---- get_my_roster ----


class TestGetMyRoster:
    """Tests for get_my_roster function."""

    def test_returns_all_roster_players(self, db: sqlite3.Connection) -> None:
        """Roster returns all 4 players on team1."""
        roster = get_my_roster(db, "team1", "20252026")
        assert len(roster) == 4

    def test_skater_has_correct_stats(self, db: sqlite3.Connection) -> None:
        """McDavid's stats use season totals for G/A but per-game sums for hits/blocks."""
        roster = get_my_roster(db, "team1", "20252026")
        mcdavid = next(p for p in roster if p["player_name"] == "Connor McDavid")
        assert mcdavid["goals"] == 30  # from season total
        assert mcdavid["assists"] == 40
        # hits = 10 * 15 games = 150, blocks = 5 * 15 = 75
        assert mcdavid["hits"] == 150
        assert mcdavid["blocks"] == 75
        assert mcdavid["games_played"] == 15

    def test_hits_blocks_from_season_total(self, db: sqlite3.Connection) -> None:
        """Hits and blocks come from the season totals row (is_season_total=1)."""
        roster = get_my_roster(db, "team1", "20252026")
        mcdavid = next(p for p in roster if p["player_name"] == "Connor McDavid")
        # Season total has hits=150, blocks=75
        assert mcdavid["hits"] == 150
        assert mcdavid["blocks"] == 75

    def test_fantasy_points_calculated(self, db: sqlite3.Connection) -> None:
        """Fantasy points are correctly calculated from stats."""
        roster = get_my_roster(db, "team1", "20252026")
        mcdavid = next(p for p in roster if p["player_name"] == "Connor McDavid")
        # 30G + 40A + 150*0.1 + 75*0.1 = 30 + 40 + 15 + 7.5 = 92.5
        assert mcdavid["fantasy_points"] == 92.5

    def test_goalie_on_roster(self, db: sqlite3.Connection) -> None:
        """Goalie appears with goalie-specific stats."""
        roster = get_my_roster(db, "team1", "20252026")
        saros = next(p for p in roster if p["player_name"] == "Juuse Saros")
        assert saros["position"] == "G"
        assert "wins" in saros
        assert "shutouts" in saros
        assert "goals" not in saros  # goalies don't have skater stats

    def test_injured_player_has_injury(self, db: sqlite3.Connection) -> None:
        """Crosby shows injury info."""
        roster = get_my_roster(db, "team1", "20252026")
        crosby = next(p for p in roster if p["player_name"] == "Sidney Crosby")
        assert crosby["injury"] is not None
        assert crosby["injury"]["injury_type"] == "Upper Body"
        assert crosby["injury"]["status"] == "Day-to-Day"

    def test_healthy_player_no_injury(self, db: sqlite3.Connection) -> None:
        """McDavid has no injury entry."""
        roster = get_my_roster(db, "team1", "20252026")
        mcdavid = next(p for p in roster if p["player_name"] == "Connor McDavid")
        assert mcdavid["injury"] is None

    def test_empty_team_returns_empty(self, db: sqlite3.Connection) -> None:
        """Non-existent team returns empty roster."""
        roster = get_my_roster(db, "nonexistent", "20252026")
        assert roster == []


# ---- get_roster_analysis ----


class TestGetRosterAnalysis:
    """Tests for get_roster_analysis function."""

    def test_position_counts(self, db: sqlite3.Connection) -> None:
        """Position counts are correct (2F, 1D, 1G)."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        assert analysis["position_counts"]["F"] == 2
        assert analysis["position_counts"]["D"] == 1
        assert analysis["position_counts"]["G"] == 1

    def test_bottom_performers(self, db: sqlite3.Connection) -> None:
        """Bottom 3 are sorted by FP/G ascending."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        bottom = analysis["bottom_performers"]
        assert len(bottom) <= 3
        fpgs = [p["fpts_per_game"] for p in bottom]
        assert fpgs == sorted(fpgs)

    def test_injured_players_listed(self, db: sqlite3.Connection) -> None:
        """Crosby appears in injured list."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        injured_names = [p["player_name"] for p in analysis["injured_players"]]
        assert "Sidney Crosby" in injured_names


# ---- search_free_agents ----


class TestSearchFreeAgents:
    """Tests for search_free_agents function."""

    def test_excludes_rostered_players(self, db: sqlite3.Connection) -> None:
        """Rostered players do not appear in free agent results."""
        fa = search_free_agents(db, "20252026", min_games=1)
        names = [p["player_name"] for p in fa]
        assert "Connor McDavid" not in names
        assert "Sidney Crosby" not in names
        assert "Leon Draisaitl" in names

    def test_min_games_filter(self, db: sqlite3.Connection) -> None:
        """min_games filter excludes players with fewer games."""
        fa = search_free_agents(db, "20252026", min_games=20)
        assert len(fa) == 0

    def test_position_filter(self, db: sqlite3.Connection) -> None:
        """Position filter works."""
        fa = search_free_agents(db, "20252026", position="D", min_games=1)
        assert len(fa) == 0  # Makar is rostered, no other D

    def test_sorted_by_fpts_per_game(self, db: sqlite3.Connection) -> None:
        """Results sorted by FP/G descending."""
        fa = search_free_agents(db, "20252026", min_games=1)
        fpgs = [p["fpts_per_game"] for p in fa]
        assert fpgs == sorted(fpgs, reverse=True)

    def test_hits_blocks_correct_for_free_agents(self, db: sqlite3.Connection) -> None:
        """Free agent hits/blocks come from per-game rows, not season totals."""
        fa = search_free_agents(db, "20252026", min_games=1)
        draisaitl = next(p for p in fa if p["player_name"] == "Leon Draisaitl")
        # 7 hits * 15 games = 105, 4 blocks * 15 = 60
        assert draisaitl["hits"] == 105
        assert draisaitl["blocks"] == 60


# ---- get_player_stats ----


class TestGetPlayerStats:
    """Tests for get_player_stats function."""

    def test_returns_player_info(self, db: sqlite3.Connection) -> None:
        """Player info block has correct fields."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert result is not None
        assert result["player"]["id"] == 8478402
        assert result["is_goalie"] is False

    def test_season_stats_present(self, db: sqlite3.Connection) -> None:
        """Season stats block has expected keys."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        stats = result["season_stats"]
        assert "goals" in stats
        assert "assists" in stats
        assert "hits" in stats
        assert "blocks" in stats
        assert "fantasy_points" in stats

    def test_game_log_present(self, db: sqlite3.Connection) -> None:
        """Game log has up to recent_games entries."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert len(result["game_log"]) == 5
        # Ordered by date DESC
        dates = [g["game_date"] for g in result["game_log"]]
        assert dates == sorted(dates, reverse=True)

    def test_game_log_has_fantasy_points(self, db: sqlite3.Connection) -> None:
        """Each game log entry has calculated fantasy points."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        for game in result["game_log"]:
            assert "fantasy_points" in game
            assert game["fantasy_points"] > 0

    def test_goalie_stats(self, db: sqlite3.Connection) -> None:
        """Goalie player returns goalie-specific stats."""
        result = get_player_stats(db, "Juuse Saros", "20252026")
        assert result["is_goalie"] is True
        assert "wins" in result["season_stats"]
        assert "shutouts" in result["season_stats"]

    def test_injury_included(self, db: sqlite3.Connection) -> None:
        """Injured player has injury info."""
        result = get_player_stats(db, "Sidney Crosby", "20252026")
        assert result["injury"] is not None

    def test_news_included(self, db: sqlite3.Connection) -> None:
        """Player news is included."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert len(result["news"]) >= 1
        assert result["news"][0]["headline"] == "McDavid: Hat Trick"

    def test_unknown_player_returns_none(self, db: sqlite3.Connection) -> None:
        """Unknown player name returns None."""
        result = get_player_stats(db, "Wayne Gretzky", "20252026")
        assert result is None


# ---- compare_players ----


class TestComparePlayers:
    """Tests for compare_players function."""

    def test_compare_two_skaters(self, db: sqlite3.Connection) -> None:
        """Comparing two skaters returns both."""
        result = compare_players(db, ["Connor McDavid", "Sidney Crosby"], "20252026")
        assert len(result) == 2
        names = {r["player"]["full_name"] for r in result}
        assert names == {"Connor McDavid", "Sidney Crosby"}

    def test_unknown_player_skipped(self, db: sqlite3.Connection) -> None:
        """Unknown player is silently skipped."""
        result = compare_players(
            db, ["Connor McDavid", "Wayne Gretzky"], "20252026"
        )
        assert len(result) == 1

    def test_compare_has_stats(self, db: sqlite3.Connection) -> None:
        """Each comparison entry has season stats merged in."""
        result = compare_players(db, ["Connor McDavid"], "20252026")
        assert result[0]["goals"] == 30
        assert result[0]["fantasy_points"] == 92.5


# ---- get_player_trends ----


class TestGetPlayerTrends:
    """Tests for get_player_trends function."""

    def test_returns_windows(self, db: sqlite3.Connection) -> None:
        """Trend data has last_7, last_14, and season windows."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        assert result is not None
        assert "last_7" in result["windows"]
        assert "last_14" in result["windows"]
        assert "season" in result["windows"]

    def test_season_games_count(self, db: sqlite3.Connection) -> None:
        """Season window has correct total game count."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        assert result["windows"]["season"]["games"] == 15

    def test_trend_flag_exists(self, db: sqlite3.Connection) -> None:
        """Trend flag is one of hot/cold/neutral."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        assert result["trend"] in ("hot", "cold", "neutral")

    def test_unknown_player_returns_none(self, db: sqlite3.Connection) -> None:
        """Unknown player returns None."""
        result = get_player_trends(db, "Wayne Gretzky", "20252026")
        assert result is None

    def test_consistent_performance_is_neutral(self, db: sqlite3.Connection) -> None:
        """Player with same stats every game should be neutral."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        # All games have the same stats, so all windows should match
        assert result["trend"] == "neutral"


# ---- get_recent_news ----


class TestGetRecentNews:
    """Tests for get_recent_news function."""

    def test_all_news(self, db: sqlite3.Connection) -> None:
        """No filter returns all news items."""
        news = get_recent_news(db)
        assert len(news) >= 2

    def test_player_filter(self, db: sqlite3.Connection) -> None:
        """Player name filter returns only that player's news."""
        news = get_recent_news(db, player_name="Connor McDavid")
        assert len(news) == 1
        assert news[0]["player_name"] == "Connor McDavid"

    def test_team_filter(self, db: sqlite3.Connection) -> None:
        """Team filter returns news for all roster players."""
        news = get_recent_news(db, team_id="team1")
        player_names = {n["player_name"] for n in news}
        assert "Connor McDavid" in player_names
        assert "Sidney Crosby" in player_names

    def test_unknown_player_returns_empty(self, db: sqlite3.Connection) -> None:
        """Unknown player name returns empty list."""
        news = get_recent_news(db, player_name="Wayne Gretzky")
        assert news == []

    def test_limit_respected(self, db: sqlite3.Connection) -> None:
        """Limit parameter caps results."""
        news = get_recent_news(db, limit=1)
        assert len(news) == 1


# ---- get_schedule_analysis ----


class TestGetScheduleAnalysis:
    """Tests for get_schedule_analysis function."""

    def test_team_abbrev_lookup(self, db: sqlite3.Connection) -> None:
        """3-letter team abbreviation works directly."""
        result = get_schedule_analysis(db, "EDM", "20252026", days_ahead=30)
        assert result is not None
        assert result["team"] == "EDM"

    def test_player_name_resolves_team(self, db: sqlite3.Connection) -> None:
        """Player name resolves to their team's schedule."""
        result = get_schedule_analysis(db, "Connor McDavid", "20252026", days_ahead=30)
        assert result is not None
        assert result["team"] == "EDM"

    def test_back_to_back_detected(self, db: sqlite3.Connection) -> None:
        """Consecutive game dates are flagged as back-to-backs."""
        result = get_schedule_analysis(db, "EDM", "20252026", days_ahead=30)
        # Tomorrow and day-after-tomorrow are consecutive
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        day_after = (date.today() + timedelta(days=2)).isoformat()
        assert len(result["back_to_backs"]) >= 1
        assert (tomorrow, day_after) in result["back_to_backs"]

    def test_unknown_player_returns_none(self, db: sqlite3.Connection) -> None:
        """Unknown player returns None."""
        result = get_schedule_analysis(db, "Wayne Gretzky", "20252026")
        assert result is None

    def test_game_count(self, db: sqlite3.Connection) -> None:
        """Game count matches number of games in window."""
        result = get_schedule_analysis(db, "EDM", "20252026", days_ahead=30)
        assert result["game_count"] == len(result["games"])


# ---- get_league_standings ----


class TestGetLeagueStandings:
    """Tests for get_league_standings function."""

    def test_returns_standings(self, db: sqlite3.Connection) -> None:
        """Standings returned with team names."""
        standings = get_league_standings(db)
        assert len(standings) == 2

    def test_ordered_by_rank(self, db: sqlite3.Connection) -> None:
        """Standings are ordered by rank ascending."""
        standings = get_league_standings(db)
        ranks = [s["rank"] for s in standings]
        assert ranks == sorted(ranks)

    def test_team_name_included(self, db: sqlite3.Connection) -> None:
        """Team name from fantasy_teams is joined in."""
        standings = get_league_standings(db)
        assert standings[0]["team_name"] == "My Team"
        assert standings[1]["team_name"] == "Other Team"

    def test_stats_present(self, db: sqlite3.Connection) -> None:
        """Key stats are present in standing entries."""
        standings = get_league_standings(db)
        s = standings[0]
        assert "wins" in s
        assert "losses" in s
        assert "points" in s
        assert "points_for" in s
        assert "streak" in s


# ---- get_injuries ----


class TestGetInjuries:
    """Tests for get_injuries function."""

    def test_all_scope(self, db: sqlite3.Connection) -> None:
        """All scope returns all injuries."""
        injuries = get_injuries(db, scope="all")
        assert len(injuries) >= 1

    def test_my_roster_scope(self, db: sqlite3.Connection) -> None:
        """my_roster scope returns only rostered injured players."""
        injuries = get_injuries(db, scope="my_roster", team_id="team1")
        names = [i["full_name"] for i in injuries]
        assert "Sidney Crosby" in names

    def test_team_scope(self, db: sqlite3.Connection) -> None:
        """team scope filters by NHL team abbreviation."""
        injuries = get_injuries(db, scope="team", team_id="PIT")
        assert len(injuries) >= 1
        assert all(i["team_abbrev"] == "PIT" for i in injuries)

    def test_no_injuries_returns_empty(self, db: sqlite3.Connection) -> None:
        """Team with no injuries returns empty list."""
        injuries = get_injuries(db, scope="team", team_id="COL")
        assert injuries == []

    def test_my_roster_no_team_id_returns_empty(self, db: sqlite3.Connection) -> None:
        """my_roster scope with no team_id returns empty."""
        injuries = get_injuries(db, scope="my_roster")
        assert injuries == []


# ---- toi_per_game ----


class TestToiPerGame:
    """Tests for toi_per_game calculation in _get_skater_season_stats."""

    def test_normal_gp(self, db: sqlite3.Connection) -> None:
        """toi_per_game = round(toi / gp) for normal GP."""
        # McDavid: toi=72000, 15 games => 72000/15 = 4800
        stats = _get_skater_season_stats(db, 8478402, "20252026")
        assert stats["toi_per_game"] == round(72000 / 15)

    def test_zero_gp(self, db: sqlite3.Connection) -> None:
        """toi_per_game = 0 when GP is 0."""
        # Insert a player with season totals but no per-game rows
        upsert_player(db, {
            "id": 9999999, "full_name": "Ghost Player",
            "first_name": "Ghost", "last_name": "Player",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (9999999, NULL, '20252026', 1, 0, 0, 0, 0, 0, 0, 0, 0, 5000)"
        )
        db.commit()
        stats = _get_skater_season_stats(db, 9999999, "20252026")
        assert stats["toi_per_game"] == 0

    def test_toi_per_game_in_roster(self, db: sqlite3.Connection) -> None:
        """toi_per_game flows through to get_my_roster."""
        roster = get_my_roster(db, "team1", "20252026")
        skaters = [p for p in roster if p["position"] != "G"]
        for s in skaters:
            assert "toi_per_game" in s
            assert s["toi_per_game"] > 0

    def test_toi_per_game_in_free_agents(self, db: sqlite3.Connection) -> None:
        """toi_per_game flows through to search_free_agents."""
        fas = search_free_agents(db, "20252026", position="any", min_games=1)
        skaters = [p for p in fas if p["position"] != "G"]
        for s in skaters:
            assert "toi_per_game" in s


# ---- 30-day trend window ----


class TestTrendWindow30:
    """Tests for 30-day trend window in get_player_trends."""

    def test_last_30_window_exists(self, db: sqlite3.Connection) -> None:
        """Trend data includes last_30 window."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        assert result is not None
        assert "last_30" in result["windows"]

    def test_last_30_games_count(self, db: sqlite3.Connection) -> None:
        """last_30 window capped at available games (15)."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        # Only 15 games available, so games = 15 (min(30, 15))
        assert result["windows"]["last_30"]["games"] == 15

    def test_last_30_fpg_calculated(self, db: sqlite3.Connection) -> None:
        """last_30 FP/G is calculated correctly."""
        result = get_player_trends(db, "Connor McDavid", "20252026")
        assert result["windows"]["last_30"]["fpts_per_game"] > 0


# ---- game log TOI ----


class TestGameLogToi:
    """Tests for TOI field in game log."""

    def test_toi_in_skater_game_log(self, db: sqlite3.Connection) -> None:
        """Skater game log entries include toi field."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert result is not None
        for g in result["game_log"]:
            assert "toi" in g
            assert g["toi"] == 1200  # All McDavid per-game rows have toi=1200


# ---- high-TOI underperformer ----


class TestHighToiUnderperformer:
    """Tests for high-TOI underperformer detection in get_trade_candidates."""

    def test_high_toi_forward_detected(self, db: sqlite3.Connection) -> None:
        """Forward with high TOI and below-median FP/G gets detected."""
        # Add a high-FP/G player on team2 to establish a meaningful median
        upsert_player(db, {
            "id": 8888887, "full_name": "Good Forward",
            "first_name": "Good", "last_name": "Forward",
            "team_abbrev": "TST", "position": "R",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8888887, NULL, '20252026', 1, 20, 20, 40, 30, 15, 100, 5, 0, 12000)"
        )
        for d in range(10, 20):
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8888887, '2025-10-{d:02d}', '20252026', 0, 2, 2, 4, 3, 1, 10, 0, 0, 1200)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Good Forward', 'R', 'active', 5000000)"
        )

        # Add a high-TOI forward on team2 with low FP/G
        upsert_player(db, {
            "id": 8888888, "full_name": "Slow Forward",
            "first_name": "Slow", "last_name": "Forward",
            "team_abbrev": "TST", "position": "L",
        })
        # Season totals: very few goals/assists but high TOI
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8888888, NULL, '20252026', 1, 2, 3, 5, 10, 5, 30, 0, 0, 15000)"
        )
        # 10 per-game rows with toi=1500 (25 min/game >> 960 threshold)
        for d in range(10, 20):
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8888888, '2025-10-{d:02d}', '20252026', 0, 0, 0, 0, 1, 0, 3, 0, 0, 1500)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Slow Forward', 'L', 'active', 2000000)"
        )
        # Line deployment: L2/PP1 so the player passes the line filter
        db.execute(
            "INSERT INTO line_combinations "
            "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
            "VALUES (8888888, 'TST', 'Slow Forward', 'L', 2, 1, datetime('now'))"
        )
        db.commit()

        candidates = get_trade_candidates(db, "team1", "20252026")
        high_toi = [c for c in candidates if c.get("signal") == "high_toi_underperformer"]
        assert len(high_toi) >= 1
        assert high_toi[0]["player_name"] == "Slow Forward"
        assert high_toi[0]["toi_per_game"] > FORWARD_PLAYABLE_TOI_PER_GAME

    def test_trending_up_signal(self, db: sqlite3.Connection) -> None:
        """Existing trending-up candidates have signal = 'trending_up'."""
        # Add a player with sharply improving recent stats on team2
        upsert_player(db, {
            "id": 7777777, "full_name": "Hot Streak",
            "first_name": "Hot", "last_name": "Streak",
            "team_abbrev": "TST", "position": "C",
        })
        # Low season totals
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (7777777, NULL, '20252026', 1, 5, 5, 10, 20, 10, 40, 0, 0, 14400)"
        )
        # 15 per-game rows: first 8 are cold, last 7 are very hot
        for d in range(10, 18):
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (7777777, '2025-10-{d:02d}', '20252026', 0, 0, 0, 0, 1, 1, 3, 0, 0, 960)"
            )
        for d in range(18, 25):
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (7777777, '2025-10-{d:02d}', '20252026', 0, 3, 3, 6, 3, 1, 5, 0, 0, 960)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Hot Streak', 'C', 'active', 3000000)"
        )
        db.commit()

        candidates = get_trade_candidates(db, "team1", "20252026")
        trending = [c for c in candidates if c.get("signal") == "trending_up"]
        assert len(trending) >= 1


# ---- news in drop/pickup candidates ----


class TestNewsIntegration:
    """Tests for news integration in drop and pickup recommendations."""

    def test_drop_candidate_recent_news(self, db: sqlite3.Connection) -> None:
        """Drop candidates include recent_news list when news exists within 42 days."""
        drops = get_drop_candidates(db, "team1", "20252026")
        mcdavid = next((d for d in drops if d["player_name"] == "Connor McDavid"), None)
        crosby = next((d for d in drops if d["player_name"] == "Sidney Crosby"), None)
        if mcdavid:
            assert isinstance(mcdavid["recent_news"], list)
            assert len(mcdavid["recent_news"]) >= 1
            assert mcdavid["recent_news"][0]["headline"] == "McDavid: Hat Trick"
        if crosby:
            assert isinstance(crosby["recent_news"], list)
            assert crosby["recent_news"][0]["headline"] == "Crosby: Injured"

    def test_drop_candidate_no_news(self, db: sqlite3.Connection) -> None:
        """Drop candidates have empty recent_news list when no news exists."""
        drops = get_drop_candidates(db, "team1", "20252026")
        makar = next((d for d in drops if d["player_name"] == "Cale Makar"), None)
        if makar:
            assert makar["recent_news"] == []

    def test_pickup_news_in_output(self, db: sqlite3.Connection) -> None:
        """Pickup recommendations include news list for pickups with news."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert isinstance(r.get("pickup_recent_news", []), list)


# ---- free agent enrichment (trend, news) ----


class TestFreeAgentEnrichment:
    """Tests for trend and news fields on search_free_agents results."""

    def test_free_agent_has_trend_field(self, db: sqlite3.Connection) -> None:
        """Free agents include a trend field (hot/cold/neutral)."""
        results = search_free_agents(db, "20252026", min_games=1)
        for fa in results:
            assert "trend" in fa
            assert fa["trend"] in ("hot", "cold", "neutral")

    def test_free_agent_has_recent_14_fpg(self, db: sqlite3.Connection) -> None:
        """Free agents include recent_14_fpg."""
        results = search_free_agents(db, "20252026", min_games=1)
        for fa in results:
            assert "recent_14_fpg" in fa
            assert isinstance(fa["recent_14_fpg"], float)

    def test_free_agent_has_recent_news(self, db: sqlite3.Connection) -> None:
        """Free agents include recent_news list."""
        results = search_free_agents(db, "20252026", min_games=1)
        for fa in results:
            assert "recent_news" in fa
            assert isinstance(fa["recent_news"], list)
            # Draisaitl has no news in the fixture
            if fa["player_name"] == "Leon Draisaitl":
                assert fa["recent_news"] == []

    def test_free_agent_has_line_context(self, db: sqlite3.Connection) -> None:
        """Free agents include ev_line and pp_unit."""
        results = search_free_agents(db, "20252026", min_games=1)
        for fa in results:
            assert "ev_line" in fa
            assert "pp_unit" in fa

    def test_free_agent_news_within_42_days(self, db: sqlite3.Connection) -> None:
        """News older than 42 days is not included."""
        # Insert old news for Draisaitl
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('old_news', 8477934, 'Old headline', 'Old content.', '2025-12-01')"
        )
        db.commit()
        results = search_free_agents(db, "20252026", min_games=1)
        drai = next((fa for fa in results if fa["player_name"] == "Leon Draisaitl"), None)
        assert drai is not None
        assert drai["recent_news"] == []

    def test_free_agent_news_within_window(self, db: sqlite3.Connection) -> None:
        """News within 42 days is included."""
        recent_date = (date.today() - timedelta(days=10)).isoformat()
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            f"VALUES ('recent_news', 8477934, 'Draisaitl: On fire', 'Hot streak.', '{recent_date}')"
        )
        db.commit()
        results = search_free_agents(db, "20252026", min_games=1)
        drai = next((fa for fa in results if fa["player_name"] == "Leon Draisaitl"), None)
        assert drai is not None
        assert len(drai["recent_news"]) >= 1
        assert drai["recent_news"][0]["headline"] == "Draisaitl: On fire"


# ---- trade candidates enrichment (injury, news, trend, line) ----


class TestTradeCandidateEnrichment:
    """Tests for enriched fields on get_trade_candidates results."""

    @pytest.fixture(autouse=True)
    def _setup_opponent_roster(self, db: sqlite3.Connection) -> None:
        """Add a trending-up player on team2 for trade candidate detection."""
        upsert_player(db, {
            "id": 7777777, "full_name": "Hot Streak",
            "first_name": "Hot", "last_name": "Streak",
            "team_abbrev": "TOR", "position": "C",
        })
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Hot Streak', 'C', 'active', 3000000)"
        )
        # Season totals: low FP/G
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (7777777, NULL, '20252026', 1, 10, 15, 25, 50, 30, 100, 5, 4, 36000)"
        )
        # Recent games: much higher production (trending up)
        for d in range(1, 21):
            gd = (date.today() - timedelta(days=d)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (7777777, '{gd}', '20252026', 0, 3, 3, 6, 3, 1, 5, 0, 0, 960)"
            )
        # Add news for Hot Streak
        recent = (date.today() - timedelta(days=3)).isoformat()
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            f"VALUES ('news_hot', 7777777, 'Hot Streak: Promoted to 1st line', 'New role.', '{recent}')"
        )
        # Add injury for Hot Streak
        db.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (7777777, 'rotowire', 'Lower Body', 'Day-to-Day', '2026-02-20')"
        )
        db.commit()

    def test_trade_candidate_has_injury(self, db: sqlite3.Connection) -> None:
        """Trade candidates include injury status."""
        candidates = get_trade_candidates(db, "team1", "20252026")
        for c in candidates:
            assert "injury" in c

    def test_trade_candidate_has_news(self, db: sqlite3.Connection) -> None:
        """Trade candidates include recent_news."""
        candidates = get_trade_candidates(db, "team1", "20252026")
        for c in candidates:
            assert "recent_news" in c

    def test_trade_candidate_has_trend(self, db: sqlite3.Connection) -> None:
        """Trade candidates include trend and recent_14_fpg."""
        candidates = get_trade_candidates(db, "team1", "20252026")
        for c in candidates:
            assert "trend" in c
            assert c["trend"] in ("hot", "cold", "neutral")
            assert "recent_14_fpg" in c

    def test_trade_candidate_news_content(self, db: sqlite3.Connection) -> None:
        """Trade candidate for Hot Streak has correct news headline."""
        candidates = get_trade_candidates(db, "team1", "20252026")
        hot = next((c for c in candidates if c["player_name"] == "Hot Streak"), None)
        if hot:
            assert isinstance(hot["recent_news"], list)
            assert len(hot["recent_news"]) >= 1
            assert hot["recent_news"][0]["headline"] == "Hot Streak: Promoted to 1st line"
            assert hot["injury"] is not None

    def test_trade_candidate_line_info(self, db: sqlite3.Connection) -> None:
        """Trade candidates include line_info with ev_line and pp_unit."""
        candidates = get_trade_candidates(db, "team1", "20252026")
        for c in candidates:
            assert "line_info" in c
            assert "ev_line" in c["line_info"]
            assert "pp_unit" in c["line_info"]


# ---- suggest_trades enrichment ----


class TestSuggestTradesEnrichment:
    """Tests for enriched fields on suggest_trades results."""

    @pytest.fixture(autouse=True)
    def _setup_opponent_roster(self, db: sqlite3.Connection) -> None:
        """Add tradeable players on team2."""
        upsert_player(db, {
            "id": 6666666, "full_name": "Trade Target",
            "first_name": "Trade", "last_name": "Target",
            "team_abbrev": "VAN", "position": "C",
        })
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Trade Target', 'C', 'active', 5000000)"
        )
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (6666666, NULL, '20252026', 1, 15, 25, 40, 60, 35, 120, 8, 6, 48000)"
        )
        for d in range(1, 21):
            gd = (date.today() - timedelta(days=d)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (6666666, '{gd}', '20252026', 0, 1, 2, 3, 4, 2, 8, 1, 0, 1100)"
            )
        recent = (date.today() - timedelta(days=5)).isoformat()
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            f"VALUES ('news_tt', 6666666, 'Trade Target: PP1 deployment', 'New PP role.', '{recent}')"
        )
        db.commit()

    def test_suggest_trades_returns_enriched_fields(self, db: sqlite3.Connection) -> None:
        """Trade suggestions include trend, injury, news for both sides."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        if result is None:
            pytest.skip("No trade result returned")
        for s in result["suggestions"]:
            # Send side
            assert "send_trend" in s
            assert s["send_trend"] in ("hot", "cold", "neutral")
            assert "send_recent_14_fpg" in s
            assert "send_injury" in s
            assert "send_news" in s
            # Receive side
            assert "receive_trend" in s
            assert s["receive_trend"] in ("hot", "cold", "neutral")
            assert "receive_recent_14_fpg" in s
            assert "receive_injury" in s
            assert "receive_news" in s
            assert "receive_ev_line" in s
            assert "receive_pp_unit" in s

    def test_suggest_trades_news_content(self, db: sqlite3.Connection) -> None:
        """Trade suggestion for Trade Target includes news headline."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        if result is None:
            pytest.skip("No trade result returned")
        for s in result["suggestions"]:
            if s["receive_player"] == "Trade Target":
                assert s["receive_news"] == "Trade Target: PP1 deployment"

    def test_suggest_trades_opponent_not_found(self, db: sqlite3.Connection) -> None:
        """suggest_trades returns None for unknown opponent."""
        result = suggest_trades(db, "team1", "Nonexistent Team", "20252026")
        assert result is None


# ---- pickup recommendations use recent FP/G ----


class TestPickupRecentFPG:
    """Tests for pickup recommendations using recent 14-game FP/G."""

    def test_pickup_has_recent_fpg_fields(self, db: sqlite3.Connection) -> None:
        """Pickup recs include season and recent FP/G for both sides."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_season_fpg" in r
            assert "pickup_recent_fpg" in r
            assert "drop_season_fpg" in r
            assert "drop_recent_fpg" in r

    def test_pickup_has_trend(self, db: sqlite3.Connection) -> None:
        """Pickup recs include pickup_trend."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_trend" in r
            assert r["pickup_trend"] in ("hot", "cold", "neutral")

    def test_pickup_upgrade_based_on_regressed(self, db: sqlite3.Connection) -> None:
        """fpg_upgrade is calculated from regressed FP/G, not raw recent."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            expected = round(r["pickup_regressed_fpg"] - r["drop_recent_fpg"], 2)
            assert r["fpg_upgrade"] == expected

    def test_pickup_reason_mentions_recent(self, db: sqlite3.Connection) -> None:
        """Pickup reasons reference 'recent FP/G' not just 'FP/G'."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            reason = r["reason"]
            if "IR stash" not in reason:
                assert "recent FP/G" in reason


# ---- roster analysis GP limits ----


class TestRosterAnalysisGPLimits:
    """Tests for GP limits in get_roster_analysis."""

    def test_gp_limits_present(self, db: sqlite3.Connection) -> None:
        """Roster analysis includes gp_limits key."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        assert "gp_limits" in analysis
        for g in ("F", "D", "G"):
            assert g in analysis["gp_limits"]
            gl = analysis["gp_limits"][g]
            assert "used" in gl
            assert "limit" in gl
            assert "remaining" in gl
            assert "pct" in gl

    def test_gp_limits_values(self, db: sqlite3.Connection) -> None:
        """GP limits have correct max values."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        assert analysis["gp_limits"]["F"]["limit"] == 984
        assert analysis["gp_limits"]["D"]["limit"] == 492
        assert analysis["gp_limits"]["G"]["limit"] == 82

    def test_gp_remaining_equals_limit_minus_used(self, db: sqlite3.Connection) -> None:
        """remaining = limit - used."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        for g in ("F", "D", "G"):
            gl = analysis["gp_limits"][g]
            assert gl["remaining"] == gl["limit"] - gl["used"]

    def test_gp_pct_calculation(self, db: sqlite3.Connection) -> None:
        """pct = used / limit * 100."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        for g in ("F", "D", "G"):
            gl = analysis["gp_limits"][g]
            expected_pct = round(gl["used"] / gl["limit"] * 100, 1)
            assert gl["pct"] == expected_pct


# ---- free agent peripheral_fpg ----


class TestFreeAgentPeripheralFPG:
    """Tests for peripheral_fpg in search_free_agents."""

    def test_skater_has_peripheral_fpg(self, db: sqlite3.Connection) -> None:
        """Free agent skaters include peripheral_fpg field."""
        fas = search_free_agents(db, "20252026", min_games=1)
        skaters = [f for f in fas if f["position"] != "G"]
        for f in skaters:
            assert "peripheral_fpg" in f
            assert f["peripheral_fpg"] >= 0.0

    def test_peripheral_fpg_calculation(self, db: sqlite3.Connection) -> None:
        """peripheral_fpg = (hits + blocks) * 0.1 / GP."""
        fas = search_free_agents(db, "20252026", min_games=1)
        skaters = [f for f in fas if f["position"] != "G"]
        for f in skaters:
            gp = f["games_played"]
            if gp > 0:
                expected = round((f["hits"] + f["blocks"]) * 0.1 / gp, 2)
                assert f["peripheral_fpg"] == expected


# ---- pickup recommendations GP-aware total value ----


class TestPickupTotalValue:
    """Tests for GP-aware total value in pickup recommendations."""

    def test_recommendations_have_total_value(self, db: sqlite3.Connection) -> None:
        """Each recommendation includes est_games and total_value."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        for r in data["recommendations"]:
            assert "est_games" in r
            assert "total_value" in r
            assert r["est_games"] >= 0
            assert r["total_value"] == round(r["fpg_upgrade"] * r["est_games"], 1)

    def test_gp_remaining_in_result(self, db: sqlite3.Connection) -> None:
        """Result dict includes gp_remaining."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        gp = data.get("gp_remaining")
        # May be None for very sparse test DBs, or a dict
        if gp is not None:
            for g in ("F", "D", "G"):
                assert g in gp

    def test_sorted_by_total_value(self, db: sqlite3.Connection) -> None:
        """Recommendations are sorted by total_value descending."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        if len(recs) >= 2:
            vals = [r["total_value"] for r in recs]
            assert vals == sorted(vals, reverse=True)


# ---- roster analysis salary ----


class TestRosterAnalysisSalary:
    """Tests for salary data in get_roster_analysis."""

    def test_salary_dict_present(self, db: sqlite3.Connection) -> None:
        """Roster analysis includes salary key with total/cap/space."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        assert "salary" in analysis
        sal = analysis["salary"]
        assert "total" in sal
        assert "cap" in sal
        assert "space" in sal

    def test_salary_total_correct(self, db: sqlite3.Connection) -> None:
        """Salary total matches sum of roster salaries."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        # McDavid 12.5M + Crosby 8.7M + Makar 9M + Saros 5M = 35.2M
        expected = 12_500_000 + 8_700_000 + 9_000_000 + 5_000_000
        assert analysis["salary"]["total"] == expected

    def test_salary_cap_constant(self, db: sqlite3.Connection) -> None:
        """Cap uses the SALARY_CAP constant."""
        from assistant.queries import SALARY_CAP
        analysis = get_roster_analysis(db, "team1", "20252026")
        assert analysis["salary"]["cap"] == SALARY_CAP

    def test_salary_space_equals_cap_minus_total(self, db: sqlite3.Connection) -> None:
        """space = cap - total."""
        analysis = get_roster_analysis(db, "team1", "20252026")
        sal = analysis["salary"]
        assert sal["space"] == sal["cap"] - sal["total"]


# ---- pickup recommendations injury-aware est_games ----


class TestPickupInjuryAdjustment:
    """Tests for injury-adjusted est_games in pickup recommendations."""

    def test_long_term_injury_zero_est_games(self, db: sqlite3.Connection) -> None:
        """FA with days_out > 60 and injury gets est_games=0."""
        # Add a free agent with an injury and very old last game
        upsert_player(db, {
            "id": 9999901, "full_name": "Injured LongTerm",
            "first_name": "Injured", "last_name": "LongTerm",
            "team_abbrev": "TOR", "position": "C",
        })
        # Season totals — high FP/G so he'd rank well if not injured
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (9999901, NULL, '20252026', 1, 30, 30, 60, 50, 20, 150, 10, 4, 60000)"
        )
        # Per-game rows 90 days ago (so days_out > 60)
        old_date = (date.today() - timedelta(days=90)).isoformat()
        for i in range(15):
            gd = (date.today() - timedelta(days=90 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (9999901, '{gd}', '20252026', 0, 2, 2, 4, 3, 1, 10, 1, 0, 1200)"
            )
        # Add injury
        db.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (9999901, 'rotowire', 'ACL', 'IR-LT', '2026-01-01')"
        )
        db.commit()

        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        # Find our injured player if present
        long_term = [r for r in recs if r["pickup_name"] == "Injured LongTerm"]
        if long_term:
            assert long_term[0]["est_games"] == 0
            assert long_term[0]["total_value"] == 0.0

    def test_recently_active_ir_normal_est_games(self, db: sqlite3.Connection) -> None:
        """FA with injury but played recently keeps normal est_games."""
        upsert_player(db, {
            "id": 9999902, "full_name": "Active IRPlayer",
            "first_name": "Active", "last_name": "IRPlayer",
            "team_abbrev": "NYR", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (9999902, NULL, '20252026', 1, 25, 35, 60, 80, 30, 160, 8, 6, 58000)"
        )
        # Per-game rows very recent (days_out < 5)
        for i in range(15):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (9999902, '{gd}', '20252026', 0, 2, 2, 4, 5, 2, 11, 1, 0, 1200)"
            )
        # IR but recently active (cap management)
        db.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (9999902, 'rotowire', 'Lower Body', 'IR-LT', '2026-02-20')"
        )
        db.commit()

        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        active_ir = [r for r in recs if r["pickup_name"] == "Active IRPlayer"]
        if active_ir:
            # Should have normal est_games (> 0) since he played yesterday
            assert active_ir[0]["est_games"] > 0

    def test_pickup_injury_fields_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation includes pickup_injury and pickup_days_out."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_injury" in r
            assert "pickup_days_out" in r


# ---- pickup recommendations Bayesian regression ----


class TestPickupBayesianRegression:
    """Tests for Bayesian regression to mean in pickup recommendations."""

    def test_regressed_fpg_field_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation includes pickup_regressed_fpg."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_regressed_fpg" in r
            assert isinstance(r["pickup_regressed_fpg"], float)

    def test_regressed_fpg_less_than_or_equal_raw(self, db: sqlite3.Connection) -> None:
        """Regressed FP/G <= raw for above-median players (all pickups are above median)."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert r["pickup_regressed_fpg"] <= r["pickup_recent_fpg"] + 0.01

    def test_deployment_fields_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation includes GP, TOI/G, PP TOI, ev_line, pp_unit."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_games_played" in r
            assert "pickup_toi_per_game" in r
            assert "pickup_pp_toi" in r
            assert "pickup_ev_line" in r
            assert "pickup_pp_unit" in r

    def test_small_sample_warning_in_reason(self, db: sqlite3.Connection) -> None:
        """Players with GP < REGRESSION_K get small sample warning."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            gp = r.get("pickup_games_played", 0)
            if gp < 25:
                assert "small sample" in r["reason"]
                assert f"{gp} GP" in r["reason"]

    def test_upgrade_uses_regressed_fpg(self, db: sqlite3.Connection) -> None:
        """fpg_upgrade matches regressed_fpg - drop_recent_fpg."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            expected = round(r["pickup_regressed_fpg"] - r["drop_recent_fpg"], 2)
            assert r["fpg_upgrade"] == expected


# ---- IR stash recommendations ----


class TestPickupIRStash:
    """Tests for IR stash candidates in pickup recommendations."""

    def test_ir_stash_present_when_slot_open(self, db: sqlite3.Connection) -> None:
        """With no status_id='3' rows, ir_slot_open=True and ir_stash exists."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        assert "ir_slot_open" in data
        assert "ir_stash" in data
        assert data["ir_slot_open"] is True
        assert isinstance(data["ir_stash"], list)

    def test_ir_stash_closed_when_slot_occupied(self, db: sqlite3.Connection) -> None:
        """With a status_id='3' row, ir_slot_open=False."""
        # Occupy the IR slot
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'IR Player', 'C', '3', 0)"
        )
        db.commit()
        data = get_pickup_recommendations(db, "team1", "20252026")
        assert data["ir_slot_open"] is False
        assert data["ir_stash"] == []

    def test_ir_stash_only_ir_eligible(self, db: sqlite3.Connection) -> None:
        """All IR stash candidates have injury status containing 'IR'."""
        # Add an IR-eligible FA
        upsert_player(db, {
            "id": 9999910, "full_name": "IR Stash Guy",
            "first_name": "IR", "last_name": "Stash Guy",
            "team_abbrev": "MTL", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (9999910, NULL, '20252026', 1, 20, 20, 40, 50, 20, 100, 5, 4, 40000)"
        )
        for i in range(15):
            gd = (date.today() - timedelta(days=15 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (9999910, '{gd}', '20252026', 0, 1, 1, 2, 3, 1, 7, 0, 0, 1100)"
            )
        db.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (9999910, 'rotowire', 'Knee', 'IR', '2026-02-15')"
        )
        db.commit()

        data = get_pickup_recommendations(db, "team1", "20252026")
        for candidate in data["ir_stash"]:
            inj = candidate.get("pickup_injury", {})
            assert "IR" in inj.get("status", "").upper()

    def test_ir_stash_excludes_season_ending(self, db: sqlite3.Connection) -> None:
        """Players with days_out > 60 are excluded from IR stash."""
        upsert_player(db, {
            "id": 9999911, "full_name": "Season Ender",
            "first_name": "Season", "last_name": "Ender",
            "team_abbrev": "BOS", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (9999911, NULL, '20252026', 1, 25, 25, 50, 40, 15, 120, 8, 2, 50000)"
        )
        # Last game 90 days ago
        for i in range(15):
            gd = (date.today() - timedelta(days=90 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (9999911, '{gd}', '20252026', 0, 2, 2, 4, 3, 1, 8, 1, 0, 1100)"
            )
        db.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (9999911, 'rotowire', 'ACL', 'IR-LT', '2025-12-01')"
        )
        db.commit()

        data = get_pickup_recommendations(db, "team1", "20252026")
        names = [c["pickup_name"] for c in data["ir_stash"]]
        assert "Season Ender" not in names

    def test_ir_stash_includes_recent_news(self, db: sqlite3.Connection) -> None:
        """IR stash candidates include pickup_recent_news list."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        for candidate in data["ir_stash"]:
            assert "pickup_recent_news" in candidate
            assert isinstance(candidate["pickup_recent_news"], list)


# ---- Per-game PP TOI ----


class TestRecentPpToi:
    """Tests for _get_recent_pp_toi helper."""

    def test_returns_per_game_pp_toi(self, db: sqlite3.Connection) -> None:
        """Returns a list of per-game PP TOI values, newest first."""
        # Use an existing FA player (id=9999901 from fixture has per-game rows)
        result = _get_recent_pp_toi(db, 9999901, "20252026")
        assert isinstance(result, list)
        for val in result:
            assert isinstance(val, int)

    def test_empty_for_nonexistent_player(self, db: sqlite3.Connection) -> None:
        """Returns empty list for player with no stats."""
        result = _get_recent_pp_toi(db, 99999999, "20252026")
        assert result == []

    def test_limit_param(self, db: sqlite3.Connection) -> None:
        """Respects the n limit parameter."""
        result = _get_recent_pp_toi(db, 9999901, "20252026", n=3)
        assert len(result) <= 3


class TestPickupPpToiInOutput:
    """Pickup recommendations include per-game PP TOI data."""

    def test_pickup_team_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation has pickup_team field."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_team" in r
            assert isinstance(r["pickup_team"], str)
            assert len(r["pickup_team"]) > 0

    def test_pp_toi_per_game_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation has pickup_pp_toi_per_game field."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_pp_toi_per_game" in r
            assert isinstance(r["pickup_pp_toi_per_game"], (int, float))

    def test_pp_toi_recent_present(self, db: sqlite3.Connection) -> None:
        """Each recommendation has pickup_pp_toi_recent list."""
        recs = get_pickup_recommendations(db, "team1", "20252026")["recommendations"]
        for r in recs:
            assert "pickup_pp_toi_recent" in r
            assert isinstance(r["pickup_pp_toi_recent"], list)


# ---------------------------------------------------------------------------
# Coverage: _is_goalie, _position_group, _get_fantasy_gp
# ---------------------------------------------------------------------------


class TestIsGoalie:
    """Tests for _is_goalie helper."""

    def test_goalie_returns_true(self, db: sqlite3.Connection) -> None:
        assert _is_goalie(db, 8477424) is True  # Saros is G

    def test_skater_returns_false(self, db: sqlite3.Connection) -> None:
        assert _is_goalie(db, 8478402) is False  # McDavid is C

    def test_missing_player_returns_false(self, db: sqlite3.Connection) -> None:
        assert _is_goalie(db, 9999999) is False


class TestPositionGroup:
    """Tests for _position_group helper."""

    def test_none_defaults_to_f(self) -> None:
        assert _position_group(None) == "F"

    def test_empty_string_defaults_to_f(self) -> None:
        assert _position_group("") == "F"

    def test_goalie(self) -> None:
        assert _position_group("G") == "G"

    def test_defense(self) -> None:
        assert _position_group("D") == "D"

    def test_center(self) -> None:
        assert _position_group("C") == "F"

    def test_left_wing(self) -> None:
        assert _position_group("L") == "F"

    def test_right_wing(self) -> None:
        assert _position_group("R") == "F"


class TestGetFantasyGp:
    """Tests for _get_fantasy_gp helper."""

    def test_with_real_fantrax_data(self, db: sqlite3.Connection) -> None:
        """When fantasy_gp_per_position has data, uses it."""
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'F', 714, 984, 270)"
        )
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'D', 369, 492, 123)"
        )
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'G', 65, 82, 17)"
        )
        db.commit()

        result = _get_fantasy_gp(db, "team1")
        assert result["F"]["used"] == 714
        assert result["F"]["remaining"] == 270
        assert result["D"]["remaining"] == 123
        assert result["G"]["remaining"] == 17
        assert result["F"]["pct"] == round(714 / 984 * 100, 1)

    def test_fallback_sums_nhl_gp(self, db: sqlite3.Connection) -> None:
        """Without fantasy_gp_per_position data, falls back to summing NHL GP."""
        roster = [
            {"position": "C", "games_played": 50},
            {"position": "C", "games_played": 40},
            {"position": "D", "games_played": 45},
            {"position": "G", "games_played": 30},
        ]
        result = _get_fantasy_gp(db, "team_no_data", roster)
        assert result["F"]["used"] == 90
        assert result["D"]["used"] == 45
        assert result["G"]["used"] == 30


# ---------------------------------------------------------------------------
# Coverage: _get_goalie_season_stats empty return
# ---------------------------------------------------------------------------


class TestGetGoalieSeasonStatsEmpty:
    """Test _get_goalie_season_stats returns {} for player with no data."""

    def test_no_goalie_data_returns_empty(self, db: sqlite3.Connection) -> None:
        """Player with no goalie stats returns empty dict."""
        # McDavid has no goalie stats
        result = _get_goalie_season_stats(db, 8478402, "20252026")
        assert result == {}


# ---------------------------------------------------------------------------
# Coverage: search_free_agents goalie path + salary lookup
# ---------------------------------------------------------------------------


class TestSearchFreeAgentsGoalie:
    """Test goalie free agent path in search_free_agents."""

    def test_goalie_free_agent(self, db: sqlite3.Connection) -> None:
        """Free agent goalie appears with goalie-specific stats."""
        # Add a free agent goalie
        upsert_player(db, {
            "id": 8470000, "full_name": "Free Goalie",
            "first_name": "Free", "last_name": "Goalie",
            "team_abbrev": "NYR", "position": "G",
        })
        db.execute(
            "INSERT INTO goalie_stats "
            "(player_id, game_date, season, is_season_total, "
            "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            "VALUES (8470000, NULL, '20252026', 1, 15, 8, 3, 2, 1000, 50, 1050, 72000)"
        )
        for i in range(10):
            gd = f"2025-10-{10 + i:02d}"
            db.execute(
                "INSERT INTO goalie_stats "
                "(player_id, game_date, season, is_season_total, "
                "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
                f"VALUES (8470000, '{gd}', '20252026', 0, 1, 0, 0, 0, 30, 2, 32, 3600)"
            )
        db.commit()

        fa = search_free_agents(db, "20252026", position="G", min_games=5)
        assert len(fa) >= 1
        goalie = next(p for p in fa if p["player_name"] == "Free Goalie")
        assert "wins" in goalie
        assert "shutouts" in goalie
        assert "gaa" in goalie
        assert goalie["peripheral_fpg"] == 0.0

    def test_salary_lookup_present(self, db: sqlite3.Connection) -> None:
        """Free agents include salary from fantrax_players table."""
        db.execute(
            "INSERT INTO fantrax_players "
            "(fantrax_id, player_name, team_abbrev, position, salary) "
            "VALUES ('fa001', 'Leon Draisaitl', 'EDM', 'C', 14000000)"
        )
        db.commit()

        fa = search_free_agents(db, "20252026", min_games=1)
        drai = next(p for p in fa if p["player_name"] == "Leon Draisaitl")
        assert drai["salary"] == 14000000


# ---------------------------------------------------------------------------
# Coverage: get_player_stats salary and line context
# ---------------------------------------------------------------------------


class TestGetPlayerStatsSalary:
    """Test salary lookup in get_player_stats."""

    def test_salary_from_fantrax(self, db: sqlite3.Connection) -> None:
        """get_player_stats includes salary from fantrax_players."""
        db.execute(
            "INSERT INTO fantrax_players "
            "(fantrax_id, player_name, team_abbrev, position, salary) "
            "VALUES ('fa002', 'Connor McDavid', 'EDM', 'C', 12500000)"
        )
        db.commit()

        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert result["salary"] == 12500000

    def test_salary_zero_when_not_found(self, db: sqlite3.Connection) -> None:
        """Salary is 0 when player not in fantrax_players."""
        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert result["salary"] == 0

    def test_line_context_included(self, db: sqlite3.Connection) -> None:
        """get_player_stats includes line_context when present."""
        db.execute(
            "INSERT INTO line_combinations "
            "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, "
            "ev_linemates, pp_linemates, rating, updated_at) "
            "VALUES (8478402, 'EDM', 'Connor McDavid', 'C', 1, 1, "
            "'[\"Draisaitl\", \"Hyman\"]', '[\"Draisaitl\", \"Nugent-Hopkins\"]', "
            "9.5, '2026-02-18')"
        )
        db.commit()

        result = get_player_stats(db, "Connor McDavid", "20252026")
        assert result["line_context"] is not None
        assert result["line_context"]["ev_line"] == 1
        assert result["line_context"]["pp_unit"] == 1


# ---------------------------------------------------------------------------
# Coverage: get_league_standings GP data
# ---------------------------------------------------------------------------


class TestGetLeagueStandingsGP:
    """Test GP remaining data in get_league_standings."""

    def test_gp_remaining_included(self, db: sqlite3.Connection) -> None:
        """Standings include gp_remaining when fantasy_gp_per_position has data."""
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'F', 700, 984, 284)"
        )
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'D', 369, 492, 123)"
        )
        db.execute(
            "INSERT INTO fantasy_gp_per_position "
            "(team_id, position, gp_used, gp_limit, gp_remaining) "
            "VALUES ('team1', 'G', 65, 82, 17)"
        )
        db.commit()

        standings = get_league_standings(db)
        team1 = next(s for s in standings if s["team_name"] == "My Team")
        assert "gp_remaining" in team1
        assert team1["gp_remaining"]["F"]["remaining"] == 284


# ---------------------------------------------------------------------------
# Coverage: get_injuries scope="my_roster"
# ---------------------------------------------------------------------------


class TestGetInjuriesMyRoster:
    """Test get_injuries with scope='my_roster'."""

    def test_my_roster_scope(self, db: sqlite3.Connection) -> None:
        """Scope 'my_roster' returns only injured players from team1."""
        result = get_injuries(db, scope="my_roster", team_id="team1")
        assert len(result) >= 1
        names = [r["full_name"] for r in result]
        assert "Sidney Crosby" in names

    def test_my_roster_no_injuries(self, db: sqlite3.Connection) -> None:
        """Team with no injured players returns empty list."""
        result = get_injuries(db, scope="my_roster", team_id="team2")
        assert result == []


# ---------------------------------------------------------------------------
# Coverage: suggest_trades
# ---------------------------------------------------------------------------


class TestSuggestTrades:
    """Tests for suggest_trades function."""

    def test_suggest_trades_returns_dict(self, db: sqlite3.Connection) -> None:
        """suggest_trades returns a dict with expected keys."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        assert isinstance(result, dict)
        assert "my_team" in result
        assert "opponent" in result
        assert "suggestions" in result

    def test_suggest_trades_has_avg_fpg(self, db: sqlite3.Connection) -> None:
        """Both teams have avg_fpg for F/D/G."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        for key in ("F", "D", "G"):
            assert key in result["my_team"]["avg_fpg"]
            assert key in result["opponent"]["avg_fpg"]

    def test_suggest_trades_nonexistent_opponent(self, db: sqlite3.Connection) -> None:
        """Nonexistent opponent returns None."""
        result = suggest_trades(db, "team1", "team_fake", "20252026")
        assert result is None


# ---------------------------------------------------------------------------
# Coverage: get_player_trends hot/cold
# ---------------------------------------------------------------------------


class TestGetPlayerTrendsHotCold:
    """Test hot/cold trend detection in get_player_trends."""

    def test_hot_trend_detected(self, db: sqlite3.Connection) -> None:
        """Player with much higher last 7 vs season is flagged hot."""
        # Add a player with hot streak
        upsert_player(db, {
            "id": 8480001, "full_name": "Hot Streak",
            "first_name": "Hot", "last_name": "Streak",
            "team_abbrev": "TST", "position": "C",
        })
        # Season total with low avg
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8480001, NULL, '20252026', 1, 10, 10, 20, 50, 25, 100, 0, 0, 30000)"
        )
        # 20 per-game rows: first 13 low, last 7 high
        game_dates = [f"2025-10-{d:02d}" for d in range(5, 25)]
        for gd in game_dates[:13]:
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8480001, '{gd}', '20252026', 0, 0, 0, 0, 2, 1, 3, 0, 0, 900)"
            )
        for gd in game_dates[13:]:
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8480001, '{gd}', '20252026', 0, 3, 3, 6, 10, 5, 15, 2, 0, 1200)"
            )
        db.commit()

        result = get_player_trends(db, "Hot Streak", "20252026")
        assert result["trend"] == "hot"

    def test_cold_trend_detected(self, db: sqlite3.Connection) -> None:
        """Player with much lower last 7 vs season is flagged cold."""
        upsert_player(db, {
            "id": 8480002, "full_name": "Cold Streak",
            "first_name": "Cold", "last_name": "Streak",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8480002, NULL, '20252026', 1, 20, 20, 40, 100, 50, 200, 0, 0, 30000)"
        )
        game_dates = [f"2025-10-{d:02d}" for d in range(5, 25)]
        for gd in game_dates[:13]:
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8480002, '{gd}', '20252026', 0, 2, 2, 4, 8, 4, 12, 1, 0, 1200)"
            )
        for gd in game_dates[13:]:
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8480002, '{gd}', '20252026', 0, 0, 0, 0, 1, 0, 1, -1, 0, 600)"
            )
        db.commit()

        result = get_player_trends(db, "Cold Streak", "20252026")
        assert result["trend"] == "cold"

    def test_no_games_returns_neutral(self, db: sqlite3.Connection) -> None:
        """Player with no game logs returns neutral trend."""
        upsert_player(db, {
            "id": 8480003, "full_name": "No Games",
            "first_name": "No", "last_name": "Games",
            "team_abbrev": "TST", "position": "C",
        })
        db.commit()

        result = get_player_trends(db, "No Games", "20252026")
        assert result["trend"] == "neutral"
        assert result["windows"] == {}


# ---------------------------------------------------------------------------
# Coverage: search_free_agents position="F"
# ---------------------------------------------------------------------------


class TestSearchFreeAgentsPositionF:
    """Test search_free_agents with position='F'."""

    def test_forward_filter(self, db: sqlite3.Connection) -> None:
        """Position 'F' returns all forward positions (C, L, R)."""
        fa = search_free_agents(db, "20252026", position="F", min_games=1)
        for p in fa:
            assert p["position"] in ("C", "L", "R")


# ---------------------------------------------------------------------------
# Coverage: get_recent_news scope="my_roster"
# ---------------------------------------------------------------------------


class TestGetRecentNewsMyRoster:
    """Test get_recent_news with team_id filter."""

    def test_my_roster_news(self, db: sqlite3.Connection) -> None:
        """team_id filter returns only my team's player news."""
        news = get_recent_news(db, team_id="team1")
        names = [n["player_name"] for n in news]
        assert any("McDavid" in n for n in names)

    def test_empty_team_no_news(self, db: sqlite3.Connection) -> None:
        """Non-existent team returns empty news."""
        news = get_recent_news(db, team_id="nonexistent")
        assert news == []


# ---------------------------------------------------------------------------
# Coverage: _get_skater_season_stats returns {} (line 200)
# ---------------------------------------------------------------------------


class TestSkaterSeasonStatsEmpty:
    """_get_skater_season_stats returns {} when player has no data at all."""

    def test_no_data_returns_empty(self, db: sqlite3.Connection) -> None:
        upsert_player(db, {
            "id": 1111111, "full_name": "Empty Stats",
            "first_name": "Empty", "last_name": "Stats",
            "team_abbrev": "TST", "position": "C",
        })
        db.commit()
        assert _get_skater_season_stats(db, 1111111, "20252026") == {}


# ---------------------------------------------------------------------------
# Coverage: get_my_roster skips empty player_name (line 362)
# ---------------------------------------------------------------------------


class TestRosterSkipsEmptyName:
    """get_my_roster skips roster slots with blank player_name."""

    def test_empty_name_slot_skipped(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', '', 'C', 'active', 0)"
        )
        db.commit()
        roster = get_my_roster(db, "team1", "20252026")
        assert len(roster) == 4  # empty slot excluded


# ---------------------------------------------------------------------------
# Coverage: search_free_agents edge cases (lines 565, 612)
# ---------------------------------------------------------------------------


class TestSearchFreeAgentsEdgeCases:
    """Edge cases: no stats excluded, hot trend detected."""

    def test_no_stats_excluded(self, db: sqlite3.Connection) -> None:
        """Player with no season stats is excluded (line 565)."""
        upsert_player(db, {
            "id": 1111112, "full_name": "Statless FA",
            "first_name": "Statless", "last_name": "FA",
            "team_abbrev": "TST", "position": "C",
        })
        db.commit()
        fa = search_free_agents(db, "20252026", min_games=0)
        assert "Statless FA" not in [p["player_name"] for p in fa]

    def test_hot_trend_free_agent(self, db: sqlite3.Connection) -> None:
        """FA with high recent 14 FPG vs low season FPG is 'hot' (line 612)."""
        upsert_player(db, {
            "id": 1111113, "full_name": "Hot FA",
            "first_name": "Hot", "last_name": "FA",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (1111113, NULL, '20252026', 1, 10, 10, 20, 60, 30, 100, 0, 0, 40000)"
        )
        for d in range(1, 21):
            gd = (date.today() - timedelta(days=d)).isoformat()
            if d <= 14:
                g, a, h, b = 3, 3, 10, 5
            else:
                g, a, h, b = 0, 0, 1, 0
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (1111113, '{gd}', '20252026', 0, {g}, {a}, {g + a}, {h}, {b}, 5, 0, 0, 1000)"
            )
        db.commit()
        fa = search_free_agents(db, "20252026", min_games=1)
        hot = next(p for p in fa if p["player_name"] == "Hot FA")
        assert hot["trend"] == "hot"


# ---------------------------------------------------------------------------
# Coverage: get_injuries blank/unresolvable player (lines 1096, 1099)
# ---------------------------------------------------------------------------


class TestGetInjuriesSlotEdgeCases:
    """get_injuries scope='my_roster' skips blank and unresolvable names."""

    def test_empty_name_skipped(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', '', 'C', 'active', 0)"
        )
        db.commit()
        result = get_injuries(db, scope="my_roster", team_id="team1")
        assert any(r["full_name"] == "Sidney Crosby" for r in result)

    def test_unresolvable_name_skipped(self, db: sqlite3.Connection) -> None:
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Unknown XYZABC', 'C', 'active', 0)"
        )
        db.commit()
        result = get_injuries(db, scope="my_roster", team_id="team1")
        assert any(r["full_name"] == "Sidney Crosby" for r in result)


# ---------------------------------------------------------------------------
# Coverage: suggest_trades cross-position + dedup
# (lines 1515, 1643-1657, 1685-1693)
# ---------------------------------------------------------------------------


class TestSuggestTradesCrossPosition:
    """Cross-position swaps and dedup in suggest_trades."""

    @pytest.fixture(autouse=True)
    def _setup(self, db: sqlite3.Connection) -> None:
        # Weak D on team1 to lower my D avg
        upsert_player(db, {
            "id": 2222222, "full_name": "Weak Dman",
            "first_name": "Weak", "last_name": "Dman",
            "team_abbrev": "TST", "position": "D",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (2222222, NULL, '20252026', 1, 0, 0, 0, 21, 7, 30, 0, 0, 9800)"
        )
        for i in range(7):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (2222222, '{gd}', '20252026', 0, 0, 0, 0, 3, 1, 4, 0, 0, 1400)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Weak Dman', 'D', 'active', 1000000)"
        )

        # Opp team: 1 weak F, 3 strong D (within 0.3 of McDavid 6.5),
        # 1 weak D, 1 low-GP player
        upsert_player(db, {
            "id": 3333333, "full_name": "Opp Forward",
            "first_name": "Opp", "last_name": "Forward",
            "team_abbrev": "OPP", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (3333333, NULL, '20252026', 1, 3, 5, 8, 30, 15, 40, 0, 0, 30000)"
        )
        for i in range(10):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (3333333, '{gd}', '20252026', 0, 0, 1, 1, 3, 1, 4, 0, 0, 1000)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Opp Forward', 'C', 'active', 2000000)"
        )

        # 3 strong D on opp: FPG ~6.5, 6.4, 6.3 (within 0.3 of McDavid)
        for pid, name, h_val in [
            (4444441, "Strong Dman1", 10),
            (4444442, "Strong Dman2", 9),
            (4444443, "Strong Dman3", 8),
        ]:
            upsert_player(db, {
                "id": pid, "full_name": name,
                "first_name": name, "last_name": "",
                "team_abbrev": "OPP", "position": "D",
            })
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES ({pid}, NULL, '20252026', 1, 20, 30, 50, {h_val * 10}, 50, 100, 0, 0, 50000)"
            )
            for i in range(10):
                gd = (date.today() - timedelta(days=1 + i)).isoformat()
                db.execute(
                    "INSERT INTO skater_stats "
                    "(player_id, game_date, season, is_season_total, goals, assists, points, "
                    "hits, blocks, shots, plus_minus, pim, toi) "
                    f"VALUES ({pid}, '{gd}', '20252026', 0, 2, 3, 5, {h_val}, 5, 10, 0, 0, 1300)"
                )
            db.execute(
                "INSERT INTO fantasy_roster_slots "
                "(team_id, player_name, position_short, status_id, salary) "
                f"VALUES ('team2', '{name}', 'D', 'active', 3000000)"
            )

        # Weak D on opp (lowers opp D avg)
        upsert_player(db, {
            "id": 4444444, "full_name": "Weak OppD",
            "first_name": "Weak", "last_name": "OppD",
            "team_abbrev": "OPP", "position": "D",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (4444444, NULL, '20252026', 1, 0, 0, 0, 10, 0, 10, 0, 0, 20000)"
        )
        for i in range(10):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (4444444, '{gd}', '20252026', 0, 0, 0, 0, 1, 0, 1, 0, 0, 1000)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Weak OppD', 'D', 'active', 1000000)"
        )

        # Low-GP player on team2 (triggers line 1515 GP<5 skip)
        upsert_player(db, {
            "id": 5555555, "full_name": "Few Games",
            "first_name": "Few", "last_name": "Games",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (5555555, NULL, '20252026', 1, 1, 1, 2, 5, 2, 10, 0, 0, 5000)"
        )
        for i in range(3):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (5555555, '{gd}', '20252026', 0, 0, 0, 0, 2, 1, 3, 0, 0, 900)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team2', 'Few Games', 'C', 'active', 1000000)"
        )
        db.commit()

    def test_cross_position_swaps(self, db: sqlite3.Connection) -> None:
        """Cross-position swaps produce F->D suggestions."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        assert result is not None
        cross = [s for s in result["suggestions"]
                 if "->" in s.get("position_group", "")]
        assert len(cross) >= 1

    def test_send_player_capped_at_2(self, db: sqlite3.Connection) -> None:
        """Same sender appears at most 2 times (dedup)."""
        result = suggest_trades(db, "team1", "Other Team", "20252026")
        assert result is not None
        from collections import Counter
        counts = Counter(s["send_player"] for s in result["suggestions"])
        for c in counts.values():
            assert c <= 2


# ---------------------------------------------------------------------------
# Coverage: get_pickup_recommendations cross-pos, reasons, news, injury
# (lines 1794, 1838, 1844, 1853, 1857, 1874, 1934-1951, 1961, 1977)
# ---------------------------------------------------------------------------


class TestPickupCrossPositionAndReasons:
    """Cross-pos phase 2 + _build_reason branches + FA news + injury adj."""

    @pytest.fixture(autouse=True)
    def _setup(self, db: sqlite3.Connection) -> None:
        # Weak D on team1 (drop candidate with very low FPG)
        upsert_player(db, {
            "id": 2222222, "full_name": "Weak Dman",
            "first_name": "Weak", "last_name": "Dman",
            "team_abbrev": "TST", "position": "D",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (2222222, NULL, '20252026', 1, 0, 0, 0, 21, 7, 30, 0, 0, 9800)"
        )
        for i in range(7):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (2222222, '{gd}', '20252026', 0, 0, 0, 0, 3, 1, 4, 0, 0, 1400)"
            )
        db.execute(
            "INSERT INTO fantasy_roster_slots "
            "(team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Weak Dman', 'D', 'active', 1000000)"
        )

        # GP data: D at 81%, F at 51%, G at 85%
        for pos, used, limit, rem in [
            ("D", 400, 492, 92), ("F", 500, 984, 484), ("G", 70, 82, 12),
        ]:
            db.execute(
                "INSERT INTO fantasy_gp_per_position "
                "(team_id, position, gp_used, gp_limit, gp_remaining) "
                f"VALUES ('team1', '{pos}', {used}, {limit}, {rem})"
            )

        # Hot FA with PP1 + Line 1 + recent news
        upsert_player(db, {
            "id": 1111113, "full_name": "Hot PP1 FA",
            "first_name": "Hot", "last_name": "PP1 FA",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (1111113, NULL, '20252026', 1, 10, 10, 20, 60, 30, 100, 0, 0, 40000)"
        )
        for d in range(1, 21):
            gd = (date.today() - timedelta(days=d)).isoformat()
            if d <= 14:
                g, a, h, b = 3, 3, 10, 5
            else:
                g, a, h, b = 0, 0, 1, 0
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (1111113, '{gd}', '20252026', 0, {g}, {a}, {g + a}, {h}, {b}, 5, 0, 0, 1000)"
            )
        db.execute(
            "INSERT INTO line_combinations "
            "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, "
            "ev_linemates, pp_linemates, rating, updated_at) "
            "VALUES (1111113, 'TST', 'Hot PP1 FA', 'C', 1, 1, '[]', '[]', 9.0, '2026-02-18')"
        )
        recent = (date.today() - timedelta(days=3)).isoformat()
        db.execute(
            "INSERT INTO player_news "
            "(rotowire_news_id, player_id, headline, content, published_at) "
            f"VALUES ('news_hotfa', 1111113, 'Hot PP1 FA: Top line', 'Big role.', '{recent}')"
        )

        # Mid-term injured FA (days_out ~40, Day-to-Day → filters in IR stash)
        upsert_player(db, {
            "id": 1111114, "full_name": "MidTerm Injured",
            "first_name": "MidTerm", "last_name": "Injured",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (1111114, NULL, '20252026', 1, 20, 20, 40, 60, 30, 120, 5, 4, 48000)"
        )
        for i in range(15):
            gd = (date.today() - timedelta(days=40 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (1111114, '{gd}', '20252026', 0, 1, 1, 2, 4, 2, 8, 0, 0, 1100)"
            )
        db.execute(
            "INSERT INTO player_injuries "
            "(player_id, source, injury_type, status, updated_at) "
            "VALUES (1111114, 'rotowire', 'Knee', 'Day-to-Day', '2026-02-01')"
        )

        # Strong IR FA (picked as regular recommendation → skipped in IR stash)
        upsert_player(db, {
            "id": 1111116, "full_name": "Strong IR FA",
            "first_name": "Strong", "last_name": "IR FA",
            "team_abbrev": "TST", "position": "C",
        })
        db.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (1111116, NULL, '20252026', 1, 30, 30, 60, 100, 50, 200, 10, 4, 50000)"
        )
        for i in range(15):
            gd = (date.today() - timedelta(days=1 + i)).isoformat()
            db.execute(
                "INSERT INTO skater_stats "
                "(player_id, game_date, season, is_season_total, goals, assists, points, "
                "hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (1111116, '{gd}', '20252026', 0, 2, 2, 4, 7, 3, 13, 1, 0, 1100)"
            )
        db.execute(
            "INSERT INTO player_injuries "
            "(player_id, source, injury_type, status, updated_at) "
            "VALUES (1111116, 'rotowire', 'Shoulder', 'IR', '2026-02-20')"
        )

        db.commit()

    def test_recommendations_produced(self, db: sqlite3.Connection) -> None:
        """Pickup recommendations are produced with enriched data."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        assert len(data["recommendations"]) >= 1

    def test_gp_remaining_from_db(self, db: sqlite3.Connection) -> None:
        """GP remaining reflects fantasy_gp_per_position data."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        gp = data["gp_remaining"]
        assert gp is not None
        assert gp["D"] == 92
        assert gp["F"] == 484

    def test_fa_news_collected(self, db: sqlite3.Connection) -> None:
        """Hot PP1 FA's news appears in recommendations."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        for r in data["recommendations"]:
            if r["pickup_name"] == "Hot PP1 FA":
                assert len(r["pickup_recent_news"]) >= 1
                break

    def test_pp1_and_line_in_reason(self, db: sqlite3.Connection) -> None:
        """PP1 and Line 1 deployment appear in pickup reason."""
        data = get_pickup_recommendations(db, "team1", "20252026")
        for r in data["recommendations"]:
            if r["pickup_name"] == "Hot PP1 FA":
                assert "PP1" in r["reason"]
                assert "Line 1" in r["reason"]
                break


# ---------------------------------------------------------------------------
# NHL team standings
# ---------------------------------------------------------------------------


def _insert_team_stats(db, team, season="20252026", **kwargs):
    """Insert a row into nhl_team_stats for testing."""
    defaults = {
        "games_played": 60, "wins": 30, "losses": 20, "ot_losses": 10,
        "points": 70, "goals_for": 180, "goals_against": 190,
        "goals_for_per_game": 3.0, "goals_against_per_game": 3.17,
        "l10_record": "5-3-2", "streak": "L6", "division": "Atlantic",
    }
    defaults.update(kwargs)
    db.execute(
        "INSERT OR REPLACE INTO nhl_team_stats "
        "(team, season, games_played, wins, losses, ot_losses, points, "
        "goals_for, goals_against, goals_for_per_game, goals_against_per_game, "
        "l10_record, streak, division) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (team, season, defaults["games_played"], defaults["wins"],
         defaults["losses"], defaults["ot_losses"], defaults["points"],
         defaults["goals_for"], defaults["goals_against"],
         defaults["goals_for_per_game"], defaults["goals_against_per_game"],
         defaults["l10_record"], defaults["streak"], defaults["division"]),
    )
    db.commit()


class TestNhlStandings:
    """Tests for get_nhl_standings query."""

    @pytest.fixture
    def db(self, tmp_path: Path) -> sqlite3.Connection:
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_db(db_path)
        yield conn
        conn.close()

    def test_returns_all_teams(self, db: sqlite3.Connection) -> None:
        _insert_team_stats(db, "TOR", points=70)
        _insert_team_stats(db, "MTL", points=50)
        result = get_nhl_standings(db, "20252026")
        assert len(result) == 2
        assert result[0]["team"] == "TOR"  # sorted by points DESC
        assert result[1]["team"] == "MTL"

    def test_filter_by_team(self, db: sqlite3.Connection) -> None:
        _insert_team_stats(db, "TOR")
        _insert_team_stats(db, "MTL")
        result = get_nhl_standings(db, "20252026", team="MTL")
        assert len(result) == 1
        assert result[0]["team"] == "MTL"

    def test_empty_table(self, db: sqlite3.Connection) -> None:
        result = get_nhl_standings(db, "20252026")
        assert result == []


class TestScheduleEnrichment:
    """Tests that get_schedule_analysis includes opponent stats."""

    @pytest.fixture
    def db(self, tmp_path: Path) -> sqlite3.Connection:
        db_path = tmp_path / "test.db"
        init_db(db_path)
        conn = get_db(db_path)

        # Insert a future game
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        conn.execute(
            "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
            "VALUES (?, ?, ?, ?, ?)",
            ("TOR", "20252026", tomorrow, "MTL", "home"),
        )
        conn.commit()
        yield conn
        conn.close()

    def test_opp_stats_included(self, db: sqlite3.Connection) -> None:
        """Opponent stats appear in game entries when nhl_team_stats has data."""
        _insert_team_stats(db, "MTL", wins=25, losses=30, ot_losses=5, points=55,
                           goals_for_per_game=2.8, goals_against_per_game=3.5,
                           l10_record="3-6-1", streak="L3")

        result = get_schedule_analysis(db, "TOR", "20252026", days_ahead=7)
        assert result is not None
        assert len(result["games"]) == 1
        game = result["games"][0]
        assert "opp" in game
        assert game["opp"]["rec"] == "25-30-5"
        assert game["opp"]["gf_g"] == 2.8
        assert game["opp"]["streak"] == "L3"
        assert "l14" in game["opp"]

    def test_no_opp_stats_graceful(self, db: sqlite3.Connection) -> None:
        """Schedule works normally when nhl_team_stats is empty."""
        result = get_schedule_analysis(db, "TOR", "20252026", days_ahead=7)
        assert result is not None
        assert len(result["games"]) == 1
        assert "opp" not in result["games"][0]
