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
    get_injuries,
    get_trade_candidates,
    get_drop_candidates,
    get_pickup_recommendations,
    suggest_trades,
    _get_skater_season_stats,
)


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
        db.commit()

        candidates = get_trade_candidates(db, "team1", "20252026")
        high_toi = [c for c in candidates if c.get("signal") == "high_toi_underperformer"]
        assert len(high_toi) >= 1
        assert high_toi[0]["player_name"] == "Slow Forward"
        assert high_toi[0]["toi_per_game"] > 960

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
        """Drop candidates include recent_news when news exists within 42 days."""
        drops = get_drop_candidates(db, "team1", "20252026")
        # McDavid and Crosby both have recent news
        mcdavid = next((d for d in drops if d["player_name"] == "Connor McDavid"), None)
        crosby = next((d for d in drops if d["player_name"] == "Sidney Crosby"), None)
        if mcdavid:
            assert mcdavid["recent_news"] == "McDavid: Hat Trick"
        if crosby:
            assert crosby["recent_news"] == "Crosby: Injured"

    def test_drop_candidate_no_news(self, db: sqlite3.Connection) -> None:
        """Drop candidates have recent_news=None when no news exists."""
        drops = get_drop_candidates(db, "team1", "20252026")
        makar = next((d for d in drops if d["player_name"] == "Cale Makar"), None)
        if makar:
            assert makar["recent_news"] is None

    def test_pickup_reason_includes_news(self, db: sqlite3.Connection) -> None:
        """Pickup recommendations append news to reason when available."""
        recs = get_pickup_recommendations(db, "team1", "20252026")
        # Draisaitl is a free agent (C) — no news for him, so reason won't have News:
        for r in recs:
            if "News:" in r["reason"]:
                # Verify the format
                assert " | News: " in r["reason"]


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
        """Free agents include recent_news (None if no news)."""
        results = search_free_agents(db, "20252026", min_games=1)
        for fa in results:
            assert "recent_news" in fa
            # Draisaitl has no news in the fixture
            if fa["player_name"] == "Leon Draisaitl":
                assert fa["recent_news"] is None

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
        assert drai["recent_news"] is None

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
        assert drai["recent_news"] == "Draisaitl: On fire"


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
            assert hot["recent_news"] == "Hot Streak: Promoted to 1st line"
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
        recs = get_pickup_recommendations(db, "team1", "20252026")
        for r in recs:
            assert "pickup_season_fpg" in r
            assert "pickup_recent_fpg" in r
            assert "drop_season_fpg" in r
            assert "drop_recent_fpg" in r

    def test_pickup_has_trend(self, db: sqlite3.Connection) -> None:
        """Pickup recs include pickup_trend."""
        recs = get_pickup_recommendations(db, "team1", "20252026")
        for r in recs:
            assert "pickup_trend" in r
            assert r["pickup_trend"] in ("hot", "cold", "neutral")

    def test_pickup_upgrade_based_on_recent(self, db: sqlite3.Connection) -> None:
        """fpg_upgrade is calculated from recent 14-game FP/G, not season."""
        recs = get_pickup_recommendations(db, "team1", "20252026")
        for r in recs:
            expected = round(r["pickup_recent_fpg"] - r["drop_recent_fpg"], 2)
            assert r["fpg_upgrade"] == expected

    def test_pickup_reason_mentions_recent(self, db: sqlite3.Connection) -> None:
        """Pickup reasons reference 'recent FP/G' not just 'FP/G'."""
        recs = get_pickup_recommendations(db, "team1", "20252026")
        for r in recs:
            reason = r["reason"]
            if "IR stash" not in reason:
                assert "recent FP/G" in reason
