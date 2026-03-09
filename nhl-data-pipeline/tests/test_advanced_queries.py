"""Tests for advanced analysis functions in assistant/queries.py."""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player
from assistant.queries import (
    get_trade_candidates,
    get_drop_candidates,
    get_pickup_recommendations,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database with test data for advanced analysis.

    team1 (user): McDavid (C), Crosby (C, injured), Makar (D), Saros (G)
    team2 (other): Draisaitl (C, hot-trending)
    Free agent: Kucherov (RW)
    """
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
    upsert_player(conn, {
        "id": 8477934, "full_name": "Leon Draisaitl",
        "first_name": "Leon", "last_name": "Draisaitl",
        "team_abbrev": "EDM", "position": "C",
    })
    # Free agent (not on any roster)
    upsert_player(conn, {
        "id": 8479636, "full_name": "Nikita Kucherov",
        "first_name": "Nikita", "last_name": "Kucherov",
        "team_abbrev": "TBL", "position": "RW",
    })

    # --- Season totals (must match sum of per-game rows below) ---
    # McDavid: 20 games * (2G, 3A, 10H, 5B) = 40G, 60A, 200H, 100B
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8478402, NULL, '20252026', 1, 40, 60, 100, 200, 100, 280, 20, 0, 24000)"
    )
    # Crosby: 13*(1G,2A,5H,3B) + 7*(0G,0A,2H,1B) = 13G, 26A, 79H, 46B
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8471675, NULL, '20252026', 1, 13, 26, 39, 79, 46, 151, 6, 0, 20600)"
    )
    # Makar: 20 games * (1G, 3A, 3H, 8B) = 20G, 60A, 60H, 160B
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8480069, NULL, '20252026', 1, 20, 60, 80, 60, 160, 240, 40, 0, 26000)"
    )
    # Draisaitl: 13*(1G,1A,5H,3B) + 7*(3G,3A,10H,5B) = 34G, 34A, 135H, 74B
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8477934, NULL, '20252026', 1, 34, 34, 68, 135, 74, 209, 14, 0, 22600)"
    )
    # Kucherov: 20 games * (1G, 2A, 1H, 1B) = 20G, 40A, 20H, 20B
    conn.execute(
        "INSERT INTO skater_stats "
        "(player_id, game_date, season, is_season_total, goals, assists, points, "
        "hits, blocks, shots, plus_minus, pim, toi) "
        "VALUES (8479636, NULL, '20252026', 1, 20, 40, 60, 20, 20, 200, 20, 0, 22000)"
    )

    # --- Per-game rows: 20 games for all skaters ---
    # Use varying dates: Oct 5 to Oct 24
    game_dates = [f"2025-10-{d:02d}" for d in range(5, 25)]

    for gd in game_dates:
        # McDavid: consistently good (2G, 3A, 10H, 5B per game = 6.5 FP/G)
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8478402, '{gd}', '20252026', 0, 2, 3, 5, 10, 5, 14, 1, 0, 1200)"
        )
        # Makar: consistent (1G, 3A, 3H, 8B per game = 5.1 FP/G)
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8480069, '{gd}', '20252026', 0, 1, 3, 4, 3, 8, 12, 2, 0, 1300)"
        )

    # Crosby: low recent production (cold streak)
    # First 13 games: decent (1G, 2A, 5H, 3B = 3.8 FP/G)
    for gd in game_dates[:13]:
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8471675, '{gd}', '20252026', 0, 1, 2, 3, 5, 3, 10, 1, 0, 1100)"
        )
    # Last 7 games: cold (0G, 0A, 2H, 1B = 0.3 FP/G)
    for gd in game_dates[13:]:
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8471675, '{gd}', '20252026', 0, 0, 0, 0, 2, 1, 3, -1, 0, 900)"
        )

    # Draisaitl (on team2): hot streak — season avg ~4.1 FP/G, last 7 = ~7 FP/G
    # First 13 games: low (1G, 1A, 5H, 3B = 2.8 FP/G)
    for gd in game_dates[:13]:
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8477934, '{gd}', '20252026', 0, 1, 1, 2, 5, 3, 8, 0, 0, 1000)"
        )
    # Last 7 games: hot (3G, 3A, 10H, 5B = 7.5 FP/G)
    for gd in game_dates[13:]:
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8477934, '{gd}', '20252026', 0, 3, 3, 6, 10, 5, 15, 2, 0, 1200)"
        )

    # Kucherov (free agent): consistent 3.5 FP/G (high FP/G free agent)
    for gd in game_dates:
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            f"VALUES (8479636, '{gd}', '20252026', 0, 1, 2, 3, 1, 1, 10, 1, 0, 1100)"
        )

    # --- Goalie stats ---
    # Saros: 15 games, 10W, 5L, 0OTL, 1SO (matches per-game rows below)
    conn.execute(
        "INSERT INTO goalie_stats "
        "(player_id, game_date, season, is_season_total, "
        "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
        "VALUES (8477424, NULL, '20252026', 1, 10, 5, 0, 1, 450, 30, 480, 54000)"
    )
    for i, gd in enumerate(game_dates[:15]):
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

    # --- Fantasy teams ---
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team1', 'lg1', 'My Team', 'MT')"
    )
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team2', 'lg1', 'Other Team', 'OT')"
    )

    # --- Rosters ---
    # team1: McDavid, Crosby, Makar, Saros
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
    # team2: Draisaitl
    conn.execute(
        "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
        "VALUES ('team2', 'Leon Draisaitl', 'C', 'active', 8500000)"
    )

    # --- Fantasy standings ---
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team1', 1, 0, 0, 100, 5000.5, 4200.0, 'W3', 70, 71.4)"
    )
    conn.execute(
        "INSERT INTO fantasy_standings "
        "(league_id, team_id, rank, wins, losses, points, "
        "points_for, points_against, streak, games_played, fantasy_points_per_game) "
        "VALUES ('lg1', 'team2', 15, 0, 0, 40, 3200.0, 4300.0, 'L5', 70, 45.7)"
    )

    # --- Injuries ---
    conn.execute(
        "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
        "VALUES (8471675, 'rotowire', 'Upper Body', 'Day-to-Day', '2026-02-18')"
    )

    # --- Line deployments (needed for trade/pickup line filters) ---
    conn.execute(
        "INSERT INTO line_combinations "
        "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
        "VALUES (8479636, 'TBL', 'Nikita Kucherov', 'RW', 1, 1, datetime('now'))"
    )
    conn.execute(
        "INSERT INTO line_combinations "
        "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
        "VALUES (8478402, 'COL', 'Connor McDavid', 'C', 1, 1, datetime('now'))"
    )
    conn.execute(
        "INSERT INTO line_combinations "
        "(player_id, team_abbrev, player_name, position, ev_line, pp_unit, updated_at) "
        "VALUES (8471677, 'EDM', 'Leon Draisaitl', 'C', 1, 1, datetime('now'))"
    )

    conn.commit()
    return conn


SEASON = "20252026"


# ---- get_trade_candidates ----


class TestGetTradeCandidates:
    """Tests for get_trade_candidates function."""

    def test_returns_trending_up_players(self, db: sqlite3.Connection) -> None:
        """Draisaitl (hot streak on team2) should appear as a trade target."""
        results = get_trade_candidates(db, "team1", SEASON)
        names = [r["player_name"] for r in results]
        assert "Leon Draisaitl" in names

    def test_excludes_own_team(self, db: sqlite3.Connection) -> None:
        """Players on team1 should not appear in results."""
        results = get_trade_candidates(db, "team1", SEASON)
        names = [r["player_name"] for r in results]
        assert "Connor McDavid" not in names
        assert "Sidney Crosby" not in names
        assert "Cale Makar" not in names

    def test_includes_owner_info(self, db: sqlite3.Connection) -> None:
        """Results should include owner team name and rank."""
        results = get_trade_candidates(db, "team1", SEASON)
        drai = next(r for r in results if r["player_name"] == "Leon Draisaitl")
        assert drai["owner_team_name"] == "Other Team"
        assert drai["owner_rank"] == 15

    def test_trend_pct_is_positive(self, db: sqlite3.Connection) -> None:
        """Trend percentage should be > 20% for included players."""
        results = get_trade_candidates(db, "team1", SEASON)
        for r in results:
            assert r["trend_pct"] > 20.0

    def test_respects_limit(self, db: sqlite3.Connection) -> None:
        """Limit parameter caps results."""
        results = get_trade_candidates(db, "team1", SEASON, limit=1)
        assert len(results) <= 1

    def test_no_candidates_when_all_neutral(self, db: sqlite3.Connection) -> None:
        """Consistent players (McDavid on team2 scenario) wouldn't trend up >20%."""
        # From team2's perspective, looking at team1 players:
        # McDavid is consistent (2G, 3A every game) so no >20% trend
        results = get_trade_candidates(db, "team2", SEASON)
        names = [r["player_name"] for r in results]
        assert "Connor McDavid" not in names


# ---- get_drop_candidates ----


class TestGetDropCandidates:
    """Tests for get_drop_candidates function."""

    def test_returns_up_to_five(self, db: sqlite3.Connection) -> None:
        """Returns at most 5 players."""
        results = get_drop_candidates(db, "team1", SEASON)
        assert len(results) <= 5

    def test_returns_all_roster_players(self, db: sqlite3.Connection) -> None:
        """With 4 players on team1, returns all 4 (< 5 cap)."""
        results = get_drop_candidates(db, "team1", SEASON)
        assert len(results) == 4

    def test_sorted_by_recent_fpg_ascending(self, db: sqlite3.Connection) -> None:
        """Results should be sorted worst-first by last-14-game FP/G."""
        results = get_drop_candidates(db, "team1", SEASON)
        fpgs = [r["recent_14_fpg"] for r in results]
        assert fpgs == sorted(fpgs)

    def test_worst_performers_first(self, db: sqlite3.Connection) -> None:
        """Lowest recent FP/G players appear first (Saros ~1.4 FP/G, Crosby ~2.05)."""
        results = get_drop_candidates(db, "team1", SEASON)
        # Saros has lowest season FP/G (~1.4), Crosby next (~2.05 recent)
        assert results[0]["player_name"] == "Juuse Saros"
        assert results[1]["player_name"] == "Sidney Crosby"

    def test_cold_trend_flagged(self, db: sqlite3.Connection) -> None:
        """Crosby with cold last 7 games (dragging down L14) should be flagged cold."""
        results = get_drop_candidates(db, "team1", SEASON)
        crosby = next(r for r in results if r["player_name"] == "Sidney Crosby")
        # Crosby season_fpg ≈ 2.575, recent_14_fpg ≈ 2.05
        # 2.05 < 2.575 * 0.85 = 2.189 → cold
        assert crosby["trend"] == "cold"

    def test_includes_injury_info(self, db: sqlite3.Connection) -> None:
        """Crosby's injury should be included."""
        results = get_drop_candidates(db, "team1", SEASON)
        crosby = next(r for r in results if r["player_name"] == "Sidney Crosby")
        assert crosby["injury"] is not None
        assert crosby["injury"]["injury_type"] == "Upper Body"

    def test_includes_salary(self, db: sqlite3.Connection) -> None:
        """Results include salary information."""
        results = get_drop_candidates(db, "team1", SEASON)
        for r in results:
            assert "salary" in r


# ---- get_pickup_recommendations ----


class TestGetPickupRecommendations:
    """Tests for get_pickup_recommendations function."""

    def test_returns_recommendations(self, db: sqlite3.Connection) -> None:
        """Should return at least one recommendation (Kucherov is a free agent F)."""
        data = get_pickup_recommendations(db, "team1", SEASON)
        assert isinstance(data, dict)
        assert "recommendations" in data
        assert "claims_remaining" in data
        assert len(data["recommendations"]) > 0

    def test_only_upgrades(self, db: sqlite3.Connection) -> None:
        """All recommendations should have positive FP/G upgrade."""
        results = get_pickup_recommendations(db, "team1", SEASON)["recommendations"]
        for r in results:
            assert r["fpg_upgrade"] > 0

    def test_no_duplicate_pickups(self, db: sqlite3.Connection) -> None:
        """Same free agent should not be recommended twice."""
        results = get_pickup_recommendations(db, "team1", SEASON)["recommendations"]
        pickup_names = [r["pickup_name"] for r in results]
        assert len(pickup_names) == len(set(pickup_names))

    def test_no_duplicate_drops(self, db: sqlite3.Connection) -> None:
        """Same drop candidate should not appear twice."""
        results = get_pickup_recommendations(db, "team1", SEASON)["recommendations"]
        drop_names = [r["drop_name"] for r in results]
        assert len(drop_names) == len(set(drop_names))

    def test_matches_position_group(self, db: sqlite3.Connection) -> None:
        """Pickup and drop should share position group (F/D/G)."""
        results = get_pickup_recommendations(db, "team1", SEASON)["recommendations"]
        pos_groups = {"C": "F", "LW": "F", "RW": "F", "F": "F", "D": "D", "G": "G"}
        for r in results:
            pickup_pg = pos_groups.get(r["pickup_position"], "F")
            drop_pg = pos_groups.get(r["drop_position"], "F")
            assert pickup_pg == drop_pg

    def test_includes_reason(self, db: sqlite3.Connection) -> None:
        """Each recommendation should have a reason string."""
        results = get_pickup_recommendations(db, "team1", SEASON)["recommendations"]
        for r in results:
            assert r["reason"]

    def test_empty_when_no_drops(self, db: sqlite3.Connection) -> None:
        """Returns empty recommendations for a team with no roster."""
        data = get_pickup_recommendations(db, "team_nonexistent", SEASON)
        assert isinstance(data, dict)
        assert data["recommendations"] == []
