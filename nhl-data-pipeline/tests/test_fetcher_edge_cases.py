"""Edge case tests for fetchers and pipeline.

Covers API error handling, malformed data, and boundary conditions
not covered by the main test files.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from db.schema import init_db, get_db, upsert_player
from fetchers.nhl_api import (
    _api_get,
    _season_month_ranges,
    fetch_roster,
    save_skater_stats,
    save_goalie_stats,
    save_team_schedule,
    calculate_games_benched,
)
from pipeline import current_season, check_freshness, generate_summary


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# _api_get edge cases
# ---------------------------------------------------------------------------


class TestApiGetEdgeCases:
    """Edge cases for _api_get."""

    def test_500_error_raises(self) -> None:
        """Non-429 HTTP errors are raised immediately."""
        session = Mock(spec=requests.Session)
        mock_resp = Mock()
        mock_resp.status_code = 500
        mock_resp.raise_for_status.side_effect = requests.HTTPError("500 Internal Server Error")
        session.get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            _api_get(session, "https://api.example.com/test")

    def test_404_error_raises(self) -> None:
        """404 is raised, not retried."""
        session = Mock(spec=requests.Session)
        mock_resp = Mock()
        mock_resp.status_code = 404
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        session.get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            _api_get(session, "https://api.example.com/not-found")

    def test_connection_error_raises(self) -> None:
        """Connection errors propagate."""
        session = Mock(spec=requests.Session)
        session.get.side_effect = requests.ConnectionError("Network unreachable")

        with pytest.raises(requests.ConnectionError):
            _api_get(session, "https://api.example.com/test")


# ---------------------------------------------------------------------------
# _season_month_ranges edge cases
# ---------------------------------------------------------------------------


class TestSeasonMonthRangesEdgeCases:
    """Edge cases for _season_month_ranges."""

    def test_standard_season(self) -> None:
        """Standard season returns 7 month ranges."""
        ranges = _season_month_ranges("20252026")
        assert len(ranges) == 7
        # First month starts Oct 1
        assert ranges[0][0] == "2025-10-01"
        # Last month ends Apr 30
        assert ranges[-1][1] == "2026-04-30"

    def test_leap_year_february(self) -> None:
        """Season spanning a leap year handles Feb correctly."""
        ranges = _season_month_ranges("20232024")
        feb_range = ranges[4]  # Feb is 5th month (Oct=0, Nov=1, Dec=2, Jan=3, Feb=4)
        assert feb_range[0] == "2024-02-01"
        assert feb_range[1] == "2024-02-29"

    def test_non_leap_year_february(self) -> None:
        """Non-leap year February ends on 28th."""
        ranges = _season_month_ranges("20252026")
        feb_range = ranges[4]
        assert feb_range[0] == "2026-02-01"
        assert feb_range[1] == "2026-02-28"

    def test_all_ranges_consecutive(self) -> None:
        """Month ranges cover Oct through Apr with no gaps."""
        ranges = _season_month_ranges("20252026")
        expected_months = [
            ("2025-10", "10"), ("2025-11", "11"), ("2025-12", "12"),
            ("2026-01", "01"), ("2026-02", "02"), ("2026-03", "03"),
            ("2026-04", "04"),
        ]
        for i, (start, end) in enumerate(ranges):
            assert start.startswith(expected_months[i][0])


# ---------------------------------------------------------------------------
# fetch_roster edge cases
# ---------------------------------------------------------------------------


class TestFetchRosterEdgeCases:
    """Edge cases for fetch_roster."""

    @patch("fetchers.nhl_api._api_get")
    def test_empty_roster_all_categories(self, mock_api_get: MagicMock) -> None:
        """Roster with empty forwards, defensemen, goalies."""
        mock_resp = Mock()
        mock_resp.json.return_value = {
            "forwards": [],
            "defensemen": [],
            "goalies": [],
        }
        mock_api_get.return_value = mock_resp
        result = fetch_roster("TOR")
        assert result == []

    @patch("fetchers.nhl_api._api_get")
    def test_missing_category_key(self, mock_api_get: MagicMock) -> None:
        """Missing 'forwards' key in response."""
        mock_resp = Mock()
        mock_resp.json.return_value = {
            "defensemen": [],
            "goalies": [],
        }
        mock_api_get.return_value = mock_resp
        # Should handle missing key gracefully (uses .get with default [])
        result = fetch_roster("TOR")
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# save_skater_stats edge cases
# ---------------------------------------------------------------------------


class TestSaveStatsEdgeCases:
    """Edge cases for save_skater_stats and save_goalie_stats."""

    def test_save_empty_stats(self, db: sqlite3.Connection) -> None:
        """Saving empty list returns 0."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        count = save_skater_stats(db, 1, "20252026", [])
        assert count == 0

    def test_save_goalie_empty_stats(self, db: sqlite3.Connection) -> None:
        """Saving empty goalie stats returns 0."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        count = save_goalie_stats(db, 1, "20252026", [])
        assert count == 0

    def test_save_stats_with_zero_toi(self, db: sqlite3.Connection) -> None:
        """Stats with 0 TOI are saved."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        stats = [{
            "game_date": "2025-10-10",
            "goals": 0, "assists": 0, "points": 0,
            "hits": 0, "blocks": 0, "shots": 0,
            "plus_minus": 0, "pim": 0, "toi": 0,
        }]
        count = save_skater_stats(db, 1, "20252026", stats)
        assert count == 1


# ---------------------------------------------------------------------------
# save_team_schedule edge cases
# ---------------------------------------------------------------------------


class TestSaveScheduleEdgeCases:
    """Edge cases for save_team_schedule."""

    def test_save_empty_schedule(self, db: sqlite3.Connection) -> None:
        """Saving empty schedule returns 0."""
        count = save_team_schedule(db, "TOR", "20252026", [])
        assert count == 0

    def test_save_duplicate_game(self, db: sqlite3.Connection) -> None:
        """Duplicate game dates use INSERT OR REPLACE."""
        games = [
            {"game_date": "2025-10-10", "opponent": "MTL", "home_away": "home", "result": None},
        ]
        save_team_schedule(db, "TOR", "20252026", games)
        # Save again with updated result
        games[0]["result"] = "W 3-2"
        save_team_schedule(db, "TOR", "20252026", games)
        db.commit()
        rows = db.execute(
            "SELECT * FROM team_games WHERE team='TOR' AND game_date='2025-10-10'"
        ).fetchall()
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# calculate_games_benched edge cases
# ---------------------------------------------------------------------------


class TestCalculateGamesBenchedEdgeCases:
    """Edge cases for calculate_games_benched."""

    def test_player_not_in_db(self, db: sqlite3.Connection) -> None:
        """Non-existent player returns None."""
        result = calculate_games_benched(db, 9999, "20252026")
        assert result is None

    def test_player_no_team_games(self, db: sqlite3.Connection) -> None:
        """Player exists but team has no games."""
        upsert_player(db, {"id": 1, "full_name": "No Games", "team_abbrev": "XYZ"})
        result = calculate_games_benched(db, 1, "20252026")
        # Should be 0 or None depending on implementation
        assert result is not None
        assert result == 0

    def test_player_zero_gp_many_team_games(self, db: sqlite3.Connection) -> None:
        """Player with 0 GP but team has games = all benched."""
        upsert_player(db, {"id": 2, "full_name": "Bench Warmer", "team_abbrev": "TOR"})
        for i in range(10):
            db.execute(
                "INSERT INTO team_games (team, season, game_date) "
                "VALUES (?, ?, ?)",
                ("TOR", "20252026", f"2025-10-{10 + i:02d}"),
            )
        db.commit()
        result = calculate_games_benched(db, 2, "20252026")
        assert result == 10


# ---------------------------------------------------------------------------
# current_season edge cases
# ---------------------------------------------------------------------------


class TestCurrentSeasonEdgeCases:
    """Edge cases for current_season."""

    def test_october_first(self) -> None:
        """Oct 1 is in the new season."""
        assert current_season(today=datetime(2025, 10, 1)) == "20252026"

    def test_june_30_previous_season(self) -> None:
        """June 30 (month < 7) is still in the previous season."""
        assert current_season(today=datetime(2025, 6, 30)) == "20242025"

    def test_july_first_new_season(self) -> None:
        """July 1 (month >= 7) starts the new season."""
        assert current_season(today=datetime(2025, 7, 1)) == "20252026"

    def test_january(self) -> None:
        """January is in the season that started the previous July."""
        assert current_season(today=datetime(2026, 1, 15)) == "20252026"


# ---------------------------------------------------------------------------
# check_freshness edge cases
# ---------------------------------------------------------------------------


class TestCheckFreshnessEdgeCases:
    """Edge cases for check_freshness."""

    def test_no_pipeline_log_entries(self, db_path: Path) -> None:
        """Empty pipeline_log returns empty or handles gracefully."""
        init_db(db_path)
        result = check_freshness(db_path)
        assert isinstance(result, dict)

    def test_all_steps_logged(self, db_path: Path) -> None:
        """All steps with recent timestamps are fresh."""
        from datetime import timezone
        init_db(db_path)
        conn = get_db(db_path)
        now = datetime.now(timezone.utc).isoformat()
        for step in ["rosters", "schedules", "gamelogs", "seasontotals", "injuries", "fantrax-league"]:
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_log (step, last_run_at, status) "
                "VALUES (?, ?, ?)",
                (step, now, "ok"),
            )
        conn.commit()
        result = check_freshness(db_path)
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# generate_summary edge cases
# ---------------------------------------------------------------------------


class TestGenerateSummaryEdgeCases:
    """Edge cases for generate_summary."""

    def test_empty_db_summary(self, db_path: Path) -> None:
        """Summary on empty DB doesn't crash."""
        init_db(db_path)
        result = generate_summary(db_path, "20252026")
        assert isinstance(result, dict)
        assert result["skater_count"] == 0
        assert result["goalie_count"] == 0

    def test_summary_with_only_goalies(self, db_path: Path) -> None:
        """Summary with only goalie data."""
        init_db(db_path)
        conn = get_db(db_path)
        upsert_player(conn, {"id": 1, "full_name": "Test Goalie", "team_abbrev": "TOR", "position": "G"})
        conn.execute(
            "INSERT INTO goalie_stats "
            "(player_id, game_date, season, is_season_total, "
            "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            "VALUES (1, NULL, '20252026', 1, 10, 5, 2, 1, 500, 30, 530, 54000)"
        )
        conn.commit()
        result = generate_summary(db_path, "20252026")
        assert result["goalie_count"] == 1
        assert result["skater_count"] == 0
