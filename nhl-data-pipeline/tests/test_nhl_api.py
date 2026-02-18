"""Tests for fetchers/nhl_api.py — NHL Web API stats fetcher."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from db.schema import PlayerDict, get_db, init_db, upsert_player
from fetchers.nhl_api import (
    ALL_TEAMS,
    calculate_games_benched,
    fetch_all_rosters,
    fetch_goalie_game_log,
    fetch_roster,
    fetch_skater_game_log,
    fetch_team_schedule,
    save_goalie_stats,
    save_skater_stats,
    save_team_schedule,
    sync_all,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database connection."""
    init_db(db_path)
    return get_db(db_path)


def make_mock_response(
    json_data: Any = None, status_code: int = 200, raise_for_status: bool = False
) -> Mock:
    """Create a mock requests response."""
    response = Mock()
    response.status_code = status_code
    response.json.return_value = json_data
    if raise_for_status:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Server Error"
        )
    else:
        response.raise_for_status.return_value = None
    return response


# =============================================================================
# Roster Fetching Tests (8 tests)
# =============================================================================


class TestFetchRoster:
    """Tests for fetch_roster function."""

    def test_fetch_roster_parses_players(self) -> None:
        """Parses player id, name, team, position from mock roster JSON."""
        mock_json = {
            "forwards": [
                {
                    "id": 8478402,
                    "firstName": {"default": "Connor"},
                    "lastName": {"default": "McDavid"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [
                {
                    "id": 8480803,
                    "firstName": {"default": "Evan"},
                    "lastName": {"default": "Bouchard"},
                    "positionCode": "D",
                }
            ],
            "goalies": [],
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            players = fetch_roster("EDM")

            assert len(players) == 2
            assert players[0]["id"] == 8478402
            assert players[0]["full_name"] == "Connor McDavid"
            assert players[0]["first_name"] == "Connor"
            assert players[0]["last_name"] == "McDavid"
            assert players[0]["team_abbrev"] == "EDM"
            assert players[0]["position"] == "C"

    def test_fetch_roster_inserts_to_db(self, db: sqlite3.Connection) -> None:
        """Fetched players are upserted into players table correctly."""
        mock_json = {
            "forwards": [
                {
                    "id": 8478402,
                    "firstName": {"default": "Connor"},
                    "lastName": {"default": "McDavid"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [],
            "goalies": [],
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            players = fetch_roster("EDM")
            for player in players:
                upsert_player(db, player)

            cursor = db.execute("SELECT * FROM players WHERE id = 8478402")
            row = cursor.fetchone()
            assert row is not None
            assert row["full_name"] == "Connor McDavid"
            assert row["team_abbrev"] == "EDM"

    def test_fetch_all_rosters_loops_32_teams(self, db: sqlite3.Connection) -> None:
        """fetch_all_rosters makes 32 API calls (one per team)."""
        mock_json = {"forwards": [], "defensemen": [], "goalies": []}
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                fetch_all_rosters(db, rate_limit=0)

            assert mock_session.get.call_count == 32

    def test_fetch_roster_handles_500_error(self) -> None:
        """Raises HTTPError when API returns 500 status."""
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(
                status_code=500, raise_for_status=True
            )
            mock_session_cls.return_value = mock_session

            with pytest.raises(requests.HTTPError):
                fetch_roster("EDM")

    def test_fetch_roster_handles_timeout(self) -> None:
        """Raises Timeout exception when request times out."""
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = requests.Timeout("Connection timed out")
            mock_session_cls.return_value = mock_session

            with pytest.raises(requests.Timeout):
                fetch_roster("EDM")

    def test_fetch_all_rosters_continues_on_team_failure(
        self, db: sqlite3.Connection
    ) -> None:
        """One team returns 500, remaining teams still fetched and upserted."""
        success_json = {
            "forwards": [
                {
                    "id": 8478402,
                    "firstName": {"default": "Connor"},
                    "lastName": {"default": "McDavid"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [],
            "goalies": [],
        }

        call_count = 0

        def mock_get(url: str, **kwargs: Any) -> Mock:
            nonlocal call_count
            call_count += 1
            # Fail on the 3rd team (CBJ), succeed on others
            if "CBJ" in url:
                return make_mock_response(status_code=500, raise_for_status=True)
            return make_mock_response(success_json)

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                count, failed = fetch_all_rosters(db, rate_limit=0)

            # Should have 31 players (one per successful team)
            assert count == 31
            assert failed == ["CBJ"]

    def test_fetch_roster_empty_roster(self) -> None:
        """Team returns 0 players — returns empty list, no error."""
        mock_json = {"forwards": [], "defensemen": [], "goalies": []}
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            players = fetch_roster("EDM")
            assert players == []

    def test_fetch_roster_special_characters_in_names(self) -> None:
        """Handles accents/hyphens (e.g., Pierre-Luc Dubois, Patrice Bergeron)."""
        mock_json = {
            "forwards": [
                {
                    "id": 8479400,
                    "firstName": {"default": "Pierre-Luc"},
                    "lastName": {"default": "Dubois"},
                    "positionCode": "C",
                },
                {
                    "id": 8470638,
                    "firstName": {"default": "Patrice"},
                    "lastName": {"default": "Bergeron"},
                    "positionCode": "C",
                },
            ],
            "defensemen": [],
            "goalies": [],
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            players = fetch_roster("BOS")
            assert len(players) == 2
            assert players[0]["full_name"] == "Pierre-Luc Dubois"
            assert players[0]["first_name"] == "Pierre-Luc"


# =============================================================================
# Skater Game Log Fetching Tests (7 tests)
# =============================================================================


class TestFetchSkaterGameLog:
    """Tests for skater game log fetching."""

    def test_fetch_skater_game_log_parses_stats(self) -> None:
        """Parses goals, assists, hits, blocks, toi, pp_toi from mock response."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": 2,
                    "assists": 1,
                    "points": 3,
                    "plusMinus": 2,
                    "pim": 0,
                    "shots": 5,
                    "hits": 1,
                    "blockedShots": 0,
                    "toi": "22:30",
                    "powerPlayToi": "4:15",
                    "powerPlayGoals": 1,
                    "powerPlayPoints": 2,
                    "shorthandedGoals": 0,
                    "shorthandedPoints": 0,
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")

            assert len(stats) == 1
            assert stats[0]["game_date"] == "2024-01-15"
            assert stats[0]["goals"] == 2
            assert stats[0]["assists"] == 1
            assert stats[0]["hits"] == 1
            assert stats[0]["blocks"] == 0

    def test_skater_toi_converted_to_seconds(self) -> None:
        """TOI string '18:30' is converted to 1110 seconds before returning."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": 0,
                    "assists": 0,
                    "toi": "18:30",
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")
            assert stats[0]["toi"] == 1110  # 18*60 + 30

    def test_save_skater_stats_inserts_rows(self, db: sqlite3.Connection) -> None:
        """Game log stats are inserted into skater_stats table with correct values."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats = [
            {
                "game_date": "2024-01-15",
                "toi": 1350,  # 22:30 in seconds
                "goals": 2,
                "assists": 1,
                "points": 3,
                "plus_minus": 2,
                "pim": 0,
                "shots": 5,
                "hits": 1,
                "blocks": 0,
                "powerplay_goals": 1,
                "powerplay_points": 2,
                "shorthanded_goals": 0,
                "shorthanded_points": 0,
            }
        ]

        count = save_skater_stats(db, 8478402, "20232024", stats)

        assert count == 1
        cursor = db.execute(
            "SELECT * FROM skater_stats WHERE player_id = 8478402 AND game_date = '2024-01-15'"
        )
        row = cursor.fetchone()
        assert row["goals"] == 2
        assert row["assists"] == 1
        assert row["toi"] == 1350
        assert row["is_season_total"] == 0

    def test_skater_missing_hits_blocks_default_zero(self) -> None:
        """Missing hits and blocks fields default to 0."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": 1,
                    "assists": 0,
                    "toi": "15:00",
                    # hits and blockedShots missing
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")
            assert stats[0]["hits"] == 0
            assert stats[0]["blocks"] == 0

    def test_skater_empty_game_log_returns_empty(self) -> None:
        """Player with 0 games returns empty list, no DB inserts."""
        mock_json = {"gameLog": []}
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")
            assert stats == []

    def test_skater_zero_stats_game(self, db: sqlite3.Connection) -> None:
        """Player dressed but got 0 everything — row inserted with all zeros."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats = [
            {
                "game_date": "2024-01-15",
                "toi": 300,  # 5:00 - minimal ice time
                "goals": 0,
                "assists": 0,
                "points": 0,
                "plus_minus": 0,
                "pim": 0,
                "shots": 0,
                "hits": 0,
                "blocks": 0,
                "powerplay_goals": 0,
                "powerplay_points": 0,
                "shorthanded_goals": 0,
                "shorthanded_points": 0,
            }
        ]

        count = save_skater_stats(db, 8478402, "20232024", stats)
        assert count == 1

        cursor = db.execute(
            "SELECT * FROM skater_stats WHERE player_id = 8478402 AND game_date = '2024-01-15'"
        )
        row = cursor.fetchone()
        assert row["goals"] == 0
        assert row["assists"] == 0
        assert row["toi"] == 300

    def test_skater_high_toi_overtime(self) -> None:
        """Unusually high TOI values (e.g., '35:00') handled correctly."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": 1,
                    "assists": 2,
                    "toi": "35:00",  # OT game
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")
            assert stats[0]["toi"] == 2100  # 35*60


# =============================================================================
# Goalie Game Log Fetching Tests (3 tests)
# =============================================================================


class TestFetchGoalieGameLog:
    """Tests for goalie game log fetching."""

    def test_fetch_goalie_game_log_parses_stats(self) -> None:
        """Parses wins, losses, shutouts, toi from mock response."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "W",
                    "shotsAgainst": 30,
                    "goalsAgainst": 2,
                    "savePctg": 0.933,
                    "shutouts": 0,
                    "toi": "60:00",
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_goalie_game_log(8477424, "20232024")

            assert len(stats) == 1
            assert stats[0]["game_date"] == "2024-01-15"
            assert stats[0]["wins"] == 1
            assert stats[0]["losses"] == 0
            assert stats[0]["shutouts"] == 0
            assert stats[0]["shots_against"] == 30
            assert stats[0]["goals_against"] == 2

    def test_save_goalie_stats_inserts_rows(self, db: sqlite3.Connection) -> None:
        """Game log stats are inserted into goalie_stats table correctly."""
        upsert_player(db, {"id": 8477424, "full_name": "Juuse Saros"})
        stats = [
            {
                "game_date": "2024-01-15",
                "toi": 3600,  # 60:00
                "saves": 28,
                "goals_against": 2,
                "shots_against": 30,
                "wins": 1,
                "losses": 0,
                "ot_losses": 0,
                "shutouts": 0,
            }
        ]

        count = save_goalie_stats(db, 8477424, "20232024", stats)

        assert count == 1
        cursor = db.execute(
            "SELECT * FROM goalie_stats WHERE player_id = 8477424 AND game_date = '2024-01-15'"
        )
        row = cursor.fetchone()
        assert row["saves"] == 28
        assert row["wins"] == 1
        assert row["toi"] == 3600

    def test_goalie_toi_converted_to_seconds(self) -> None:
        """TOI string converted to seconds for goalies too."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "W",
                    "toi": "58:30",
                    "shotsAgainst": 25,
                    "goalsAgainst": 1,
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_goalie_game_log(8477424, "20232024")
            assert stats[0]["toi"] == 3510  # 58*60 + 30


# =============================================================================
# Season Totals Tests (2 tests)
# =============================================================================


class TestSeasonTotals:
    """Tests for season total rows."""

    def test_season_total_creates_summary_row(self, db: sqlite3.Connection) -> None:
        """is_season_total=1 row created with NULL game_date."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats = [
            {
                "game_date": None,
                "toi": 72000,  # Season total TOI
                "goals": 50,
                "assists": 80,
                "points": 130,
                "plus_minus": 30,
                "pim": 18,
                "shots": 300,
                "hits": 20,
                "blocks": 15,
                "powerplay_goals": 15,
                "powerplay_points": 40,
                "shorthanded_goals": 2,
                "shorthanded_points": 3,
            }
        ]

        count = save_skater_stats(db, 8478402, "20232024", stats, is_season_total=True)

        assert count == 1
        cursor = db.execute(
            "SELECT * FROM skater_stats WHERE player_id = 8478402 AND is_season_total = 1"
        )
        row = cursor.fetchone()
        assert row["game_date"] is None
        assert row["goals"] == 50
        assert row["is_season_total"] == 1

    def test_season_total_replaces_on_rerun(self, db: sqlite3.Connection) -> None:
        """Running twice replaces the row, doesn't create duplicates."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats = [
            {
                "game_date": None,
                "toi": 72000,
                "goals": 50,
                "assists": 80,
                "points": 130,
                "plus_minus": 30,
                "pim": 18,
                "shots": 300,
                "hits": 20,
                "blocks": 15,
                "powerplay_goals": 15,
                "powerplay_points": 40,
                "shorthanded_goals": 2,
                "shorthanded_points": 3,
            }
        ]

        save_skater_stats(db, 8478402, "20232024", stats, is_season_total=True)
        # Update goals to 55
        stats[0]["goals"] = 55
        save_skater_stats(db, 8478402, "20232024", stats, is_season_total=True)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8478402 AND is_season_total = 1"
        )
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT goals FROM skater_stats WHERE player_id = 8478402 AND is_season_total = 1"
        )
        assert cursor.fetchone()["goals"] == 55


# =============================================================================
# Team Schedule Tests (2 tests)
# =============================================================================


class TestTeamSchedule:
    """Tests for team schedule fetching."""

    def test_fetch_team_schedule_parses_games(self) -> None:
        """Parses game_date, opponent, home_away from mock response."""
        mock_json = {
            "games": [
                {
                    "gameDate": "2024-01-15",
                    "awayTeam": {"abbrev": "CGY"},
                    "homeTeam": {"abbrev": "EDM"},
                    "gameOutcome": {"lastPeriodType": "REG"},
                },
                {
                    "gameDate": "2024-01-17",
                    "awayTeam": {"abbrev": "EDM"},
                    "homeTeam": {"abbrev": "VAN"},
                    "gameOutcome": {"lastPeriodType": "OT"},
                },
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            games = fetch_team_schedule("EDM", "20232024")

            assert len(games) == 2
            assert games[0]["game_date"] == "2024-01-15"
            assert games[0]["opponent"] == "CGY"
            assert games[0]["home_away"] == "home"
            assert games[1]["opponent"] == "VAN"
            assert games[1]["home_away"] == "away"

    def test_save_team_schedule_no_duplicates(self, db: sqlite3.Connection) -> None:
        """Re-running inserts same games doesn't create duplicates (INSERT OR REPLACE)."""
        games = [
            {"game_date": "2024-01-15", "opponent": "CGY", "home_away": "home", "result": "W"},
            {"game_date": "2024-01-17", "opponent": "VAN", "home_away": "away", "result": "L"},
        ]

        save_team_schedule(db, "EDM", "20232024", games)
        # Modify result and re-run
        games[0]["result"] = "L"
        save_team_schedule(db, "EDM", "20232024", games)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM team_games WHERE team = 'EDM' AND season = '20232024'"
        )
        assert cursor.fetchone()["cnt"] == 2

        # Check result was updated
        cursor = db.execute(
            "SELECT result FROM team_games WHERE team = 'EDM' AND game_date = '2024-01-15'"
        )
        assert cursor.fetchone()["result"] == "L"


# =============================================================================
# Rate Limiting Tests (1 test)
# =============================================================================


class TestRateLimiting:
    """Tests for rate limiting."""

    def test_sync_all_sleeps_between_requests(self, db: sqlite3.Connection) -> None:
        """Verifies time.sleep() called between API requests."""
        mock_roster_json = {"forwards": [], "defensemen": [], "goalies": []}
        mock_schedule_json = {"games": []}

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_roster_json)
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep") as mock_sleep:
                sync_all(db, "20232024", rate_limit=0.5)

                # Should sleep between requests
                assert mock_sleep.call_count > 0


# =============================================================================
# API Response Edge Cases Tests (4 tests)
# =============================================================================


class TestApiEdgeCases:
    """Tests for API response edge cases."""

    def test_malformed_json_response(self) -> None:
        """API returns invalid JSON — raises appropriate error."""
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.side_effect = json.JSONDecodeError("Error", "", 0)
            mock_session.get.return_value = mock_response
            mock_session_cls.return_value = mock_session

            with pytest.raises(json.JSONDecodeError):
                fetch_roster("EDM")

    def test_player_null_name_fields(self) -> None:
        """Player with null/missing name fields — uses empty string or skips gracefully."""
        mock_json = {
            "forwards": [
                {
                    "id": 8478402,
                    "firstName": {"default": None},
                    "lastName": {"default": "McDavid"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [],
            "goalies": [],
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            players = fetch_roster("EDM")
            assert len(players) == 1
            # Should handle None gracefully
            assert players[0]["first_name"] == "" or players[0]["first_name"] is None

    def test_unexpected_data_types(self) -> None:
        """String where int expected (e.g., goals='5') — handles type coercion or errors clearly."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": "5",  # String instead of int
                    "assists": 3,
                    "toi": "20:00",
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_skater_game_log(8478402, "20232024")
            # Should coerce to int
            assert stats[0]["goals"] == 5

    def test_player_changed_teams_mid_season(self, db: sqlite3.Connection) -> None:
        """Player appears on two rosters — second upsert updates team_abbrev."""
        # First team
        upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "EDM",
                "position": "C",
            },
        )

        cursor = db.execute("SELECT team_abbrev FROM players WHERE id = 8478402")
        assert cursor.fetchone()["team_abbrev"] == "EDM"

        # Trade to TOR
        upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "TOR",
                "position": "C",
            },
        )

        cursor = db.execute("SELECT team_abbrev FROM players WHERE id = 8478402")
        assert cursor.fetchone()["team_abbrev"] == "TOR"


# =============================================================================
# Games Benched Edge Cases Tests (3 tests)
# =============================================================================


class TestGamesBenchedEdgeCases:
    """Tests for games benched calculation edge cases."""

    def test_games_benched_player_not_found(self, db: sqlite3.Connection) -> None:
        """Player not in DB → returns None."""
        result = calculate_games_benched(db, 9999999, "20232024")
        assert result is None

    def test_games_benched_no_team_games(self, db: sqlite3.Connection) -> None:
        """Team has no entries in team_games → returns 0."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM"},
        )
        # No team games inserted

        result = calculate_games_benched(db, 8478402, "20232024")
        assert result == 0

    def test_games_benched_player_gp_exceeds_team(self, db: sqlite3.Connection) -> None:
        """Player GP > team games (data inconsistency) → returns 0 (floor)."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM"},
        )

        # Insert 5 team games
        for i in range(1, 6):
            db.execute(
                """
                INSERT INTO team_games (team, season, game_date)
                VALUES ('EDM', '20232024', ?)
                """,
                (f"2024-01-{i:02d}",),
            )

        # Insert 7 player games (more than team games - data inconsistency)
        for i in range(1, 8):
            db.execute(
                """
                INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
                VALUES (8478402, ?, '20232024', 0, 1200)
                """,
                (f"2024-01-{i:02d}",),
            )
        db.commit()

        result = calculate_games_benched(db, 8478402, "20232024")
        assert result == 0  # Floor at 0, don't return negative


# =============================================================================
# Save/DB Edge Cases Tests (2 tests)
# =============================================================================


class TestSaveDbEdgeCases:
    """Tests for save/DB edge cases."""

    def test_save_stats_fk_constraint_with_enforcement(
        self, db: sqlite3.Connection
    ) -> None:
        """With FK enforcement enabled, saving stats for missing player raises IntegrityError."""
        # Enable foreign key enforcement
        db.execute("PRAGMA foreign_keys = ON")

        stats = [
            {
                "game_date": "2024-01-15",
                "toi": 1200,
                "goals": 1,
                "assists": 0,
                "points": 1,
                "plus_minus": 0,
                "pim": 0,
                "shots": 3,
                "hits": 0,
                "blocks": 0,
                "powerplay_goals": 0,
                "powerplay_points": 0,
                "shorthanded_goals": 0,
                "shorthanded_points": 0,
            }
        ]

        with pytest.raises(sqlite3.IntegrityError):
            save_skater_stats(db, 9999999, "20232024", stats)

    def test_save_large_game_log(self, db: sqlite3.Connection) -> None:
        """82+ game entries in a season — all rows inserted correctly."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})

        # Create 85 games (more than regular season)
        stats = []
        for i in range(85):
            month = (i // 28) + 10  # Start in October
            day = (i % 28) + 1
            if month > 12:
                month = month - 12
            stats.append(
                {
                    "game_date": f"2024-{month:02d}-{day:02d}",
                    "toi": 1200 + i,
                    "goals": i % 3,
                    "assists": i % 2,
                    "points": (i % 3) + (i % 2),
                    "plus_minus": 0,
                    "pim": 0,
                    "shots": 3,
                    "hits": 1,
                    "blocks": 0,
                    "powerplay_goals": 0,
                    "powerplay_points": 0,
                    "shorthanded_goals": 0,
                    "shorthanded_points": 0,
                }
            )

        count = save_skater_stats(db, 8478402, "20232024", stats)
        assert count == 85

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8478402 AND is_season_total = 0"
        )
        assert cursor.fetchone()["cnt"] == 85


# =============================================================================
# Integration Tests (2 tests)
# =============================================================================


class TestIntegration:
    """Integration tests for full pipeline."""

    def test_full_sync_flow(self, db: sqlite3.Connection) -> None:
        """Full pipeline: roster fetch → game log fetch → verify both tables populated."""
        mock_roster_json = {
            "forwards": [
                {
                    "id": 8478402,
                    "firstName": {"default": "Connor"},
                    "lastName": {"default": "McDavid"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [],
            "goalies": [
                {
                    "id": 8477424,
                    "firstName": {"default": "Stuart"},
                    "lastName": {"default": "Skinner"},
                    "positionCode": "G",
                }
            ],
        }
        mock_skater_log_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "goals": 2,
                    "assists": 1,
                    "toi": "22:00",
                }
            ]
        }
        mock_goalie_log_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "W",
                    "toi": "60:00",
                    "shotsAgainst": 30,
                    "goalsAgainst": 2,
                }
            ]
        }
        mock_schedule_json = {
            "games": [
                {
                    "gameDate": "2024-01-15",
                    "awayTeam": {"abbrev": "CGY"},
                    "homeTeam": {"abbrev": "EDM"},
                }
            ]
        }

        call_index = [0]

        def mock_get(url: str, **kwargs: Any) -> Mock:
            call_index[0] += 1
            if "roster" in url:
                return make_mock_response(mock_roster_json)
            elif "game-log" in url:
                if "8478402" in url:
                    return make_mock_response(mock_skater_log_json)
                elif "8477424" in url:
                    return make_mock_response(mock_goalie_log_json)
            elif "schedule" in url:
                return make_mock_response(mock_schedule_json)
            return make_mock_response({})

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                with patch("fetchers.nhl_api.ALL_TEAMS", ["EDM"]):
                    result = sync_all(db, "20232024", rate_limit=0)

            # Verify players
            cursor = db.execute("SELECT COUNT(*) as cnt FROM players")
            assert cursor.fetchone()["cnt"] == 2

            # Verify skater stats
            cursor = db.execute("SELECT COUNT(*) as cnt FROM skater_stats")
            assert cursor.fetchone()["cnt"] >= 1

            # Verify goalie stats
            cursor = db.execute("SELECT COUNT(*) as cnt FROM goalie_stats")
            assert cursor.fetchone()["cnt"] >= 1

    def test_games_benched_calculation(self, db: sqlite3.Connection) -> None:
        """Team has 10 games, player has 7 GP → calculate_games_benched returns 3."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM"},
        )

        # Insert 10 team games for EDM
        for i in range(1, 11):
            db.execute(
                """
                INSERT INTO team_games (team, season, game_date)
                VALUES ('EDM', '20232024', ?)
                """,
                (f"2024-01-{i:02d}",),
            )

        # Insert 7 game logs for player (GP = 7)
        for i in range(1, 8):
            db.execute(
                """
                INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
                VALUES (8478402, ?, '20232024', 0, 1200)
                """,
                (f"2024-01-{i:02d}",),
            )
        db.commit()

        result = calculate_games_benched(db, 8478402, "20232024")
        assert result == 3
