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
    _api_get,
    calculate_games_benched,
    fetch_all_rosters,
    fetch_goalie_game_log,
    fetch_player_landing,
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
# Skater Game Log Fetching Tests (10 tests)
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
            assert stats[0]["pp_toi"] == 255  # 4:15 = 255 seconds

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

    def test_is_season_total_sets_game_date_null(self, db: sqlite3.Connection) -> None:
        """is_season_total=True stores game_date as NULL and is_season_total=1."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats = [
            {
                "game_date": None,
                "toi": 72000,
                "goals": 40,
                "assists": 60,
                "points": 100,
                "plus_minus": 20,
                "pim": 10,
                "shots": 250,
                "hits": 15,
                "blocks": 10,
                "powerplay_goals": 10,
                "powerplay_points": 30,
                "shorthanded_goals": 1,
                "shorthanded_points": 2,
            }
        ]

        count = save_skater_stats(db, 8478402, "20232024", stats, is_season_total=True)
        assert count == 1

        cursor = db.execute(
            "SELECT game_date, is_season_total FROM skater_stats WHERE player_id = 8478402"
        )
        row = cursor.fetchone()
        assert row["game_date"] is None
        assert row["is_season_total"] == 1

    def test_same_game_date_replaces_via_save(self, db: sqlite3.Connection) -> None:
        """INSERT OR REPLACE overwrites on duplicate (player_id, game_date)."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        stats_v1 = [
            {
                "game_date": "2024-01-15",
                "toi": 1200,
                "goals": 2,
                "assists": 1,
                "points": 3,
                "plus_minus": 1,
                "pim": 0,
                "shots": 5,
                "hits": 1,
                "blocks": 0,
                "powerplay_goals": 0,
                "powerplay_points": 0,
                "shorthanded_goals": 0,
                "shorthanded_points": 0,
            }
        ]
        save_skater_stats(db, 8478402, "20232024", stats_v1)

        stats_v2 = [
            {
                "game_date": "2024-01-15",
                "toi": 1200,
                "goals": 3,
                "assists": 1,
                "points": 4,
                "plus_minus": 2,
                "pim": 0,
                "shots": 6,
                "hits": 1,
                "blocks": 0,
                "powerplay_goals": 0,
                "powerplay_points": 0,
                "shorthanded_goals": 0,
                "shorthanded_points": 0,
            }
        ]
        save_skater_stats(db, 8478402, "20232024", stats_v2)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8478402 AND game_date = '2024-01-15'"
        )
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT goals FROM skater_stats WHERE player_id = 8478402 AND game_date = '2024-01-15'"
        )
        assert cursor.fetchone()["goals"] == 3

    def test_stats_with_max_int_values(self, db: sqlite3.Connection) -> None:
        """Very large int values (2^31-1) do not cause overflow in SQLite."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        max_val = 2**31 - 1
        stats = [
            {
                "game_date": "2024-01-15",
                "toi": max_val,
                "goals": max_val,
                "assists": max_val,
                "points": max_val,
                "plus_minus": max_val,
                "pim": max_val,
                "shots": max_val,
                "hits": max_val,
                "blocks": max_val,
                "powerplay_goals": max_val,
                "powerplay_points": max_val,
                "shorthanded_goals": max_val,
                "shorthanded_points": max_val,
            }
        ]

        count = save_skater_stats(db, 8478402, "20232024", stats)
        assert count == 1

        cursor = db.execute(
            "SELECT goals, toi FROM skater_stats WHERE player_id = 8478402 AND game_date = '2024-01-15'"
        )
        row = cursor.fetchone()
        assert row["goals"] == max_val
        assert row["toi"] == max_val


# =============================================================================
# Player Landing Page Tests (5 tests)
# =============================================================================


class TestFetchPlayerLanding:
    """Tests for fetch_player_landing function."""

    def test_normal_response_returns_raw_json(self) -> None:
        """Returns the raw JSON dict from the API as-is."""
        mock_json = {
            "firstName": {"default": "Connor"},
            "lastName": {"default": "McDavid"},
            "position": "C",
            "featuredStats": {
                "season": 20232024,
                "regularSeason": {
                    "subSeason": {
                        "gamesPlayed": 76,
                        "goals": 50,
                        "assists": 80,
                        "points": 130,
                    }
                },
            },
            "seasonTotals": [
                {"season": 20232024, "leagueAbbrev": "NHL", "goals": 50}
            ],
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            result = fetch_player_landing(8478402)

            assert result == mock_json
            assert result["featuredStats"]["regularSeason"]["subSeason"]["goals"] == 50
            assert result["seasonTotals"][0]["season"] == 20232024

    def test_404_raises_http_error(self) -> None:
        """404 response raises HTTPError (player not found)."""
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(
                status_code=404, raise_for_status=True
            )
            mock_session_cls.return_value = mock_session

            with pytest.raises(requests.HTTPError):
                fetch_player_landing(9999999)

    def test_malformed_missing_nested_keys(self) -> None:
        """Response missing featuredStats/seasonTotals keys — returns dict as-is."""
        mock_json = {
            "firstName": {"default": "Connor"},
            "lastName": {"default": "McDavid"},
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            result = fetch_player_landing(8478402)

            assert result == mock_json
            assert "featuredStats" not in result

    def test_empty_featured_stats_section(self) -> None:
        """Empty featuredStats dict returned without error."""
        mock_json = {
            "firstName": {"default": "Connor"},
            "lastName": {"default": "McDavid"},
            "featuredStats": {},
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            result = fetch_player_landing(8478402)

            assert result["featuredStats"] == {}

    def test_network_timeout(self) -> None:
        """Network timeout raises requests.Timeout."""
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = requests.Timeout("Connection timed out")
            mock_session_cls.return_value = mock_session

            with pytest.raises(requests.Timeout):
                fetch_player_landing(8478402)


# =============================================================================
# Goalie Game Log Fetching Tests (9 tests)
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

    def test_empty_game_log_returns_empty_list(self) -> None:
        """Empty gameLog array returns empty list."""
        mock_json = {"gameLog": []}
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_goalie_game_log(8477424, "20232024")
            assert stats == []

    def test_missing_save_pctg_and_gaa(self) -> None:
        """Response without savePctg/goalsAgainstAvg keys does not crash."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "W",
                    "shotsAgainst": 30,
                    "goalsAgainst": 2,
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
            assert stats[0]["saves"] == 28  # 30 - 2
            assert stats[0]["wins"] == 1
            assert "savePctg" not in stats[0]
            assert "goalsAgainstAvg" not in stats[0]

    def test_zero_saves_zero_shots_game(self) -> None:
        """Zero shots against produces saves=0."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "L",
                    "shotsAgainst": 0,
                    "goalsAgainst": 0,
                    "shutouts": 0,
                    "toi": "5:00",
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_goalie_game_log(8477424, "20232024")
            assert stats[0]["saves"] == 0
            assert stats[0]["shots_against"] == 0
            assert stats[0]["goals_against"] == 0

    def test_goalie_toi_zero_did_not_play(self) -> None:
        """toi='0:00' converts to toi=0 (goalie did not play)."""
        mock_json = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "",
                    "shotsAgainst": 0,
                    "goalsAgainst": 0,
                    "shutouts": 0,
                    "toi": "0:00",
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            stats = fetch_goalie_game_log(8477424, "20232024")
            assert stats[0]["toi"] == 0

    def test_same_game_date_replaces_goalie(self, db: sqlite3.Connection) -> None:
        """Same (player_id, game_date) replaces via INSERT OR REPLACE."""
        upsert_player(db, {"id": 8477424, "full_name": "Juuse Saros"})
        base_stat = {
            "game_date": "2024-01-15",
            "toi": 3600,
            "saves": 28,
            "goals_against": 2,
            "shots_against": 30,
            "wins": 1,
            "losses": 0,
            "ot_losses": 0,
            "shutouts": 0,
        }
        save_goalie_stats(db, 8477424, "20232024", [base_stat])

        updated_stat = {
            "game_date": "2024-01-15",
            "toi": 3600,
            "saves": 35,
            "goals_against": 2,
            "shots_against": 37,
            "wins": 1,
            "losses": 0,
            "ot_losses": 0,
            "shutouts": 0,
        }
        save_goalie_stats(db, 8477424, "20232024", [updated_stat])

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM goalie_stats WHERE player_id = 8477424 AND game_date = '2024-01-15'"
        )
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT saves FROM goalie_stats WHERE player_id = 8477424 AND game_date = '2024-01-15'"
        )
        assert cursor.fetchone()["saves"] == 35

    def test_goalie_season_total_path(self, db: sqlite3.Connection) -> None:
        """is_season_total=True uses DELETE+INSERT path, game_date is NULL."""
        upsert_player(db, {"id": 8477424, "full_name": "Juuse Saros"})
        stats = [
            {
                "game_date": None,
                "toi": 180000,
                "saves": 1500,
                "goals_against": 120,
                "shots_against": 1620,
                "wins": 30,
                "losses": 15,
                "ot_losses": 5,
                "shutouts": 3,
            }
        ]

        save_goalie_stats(db, 8477424, "20232024", stats, is_season_total=True)
        stats[0]["wins"] = 32
        save_goalie_stats(db, 8477424, "20232024", stats, is_season_total=True)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM goalie_stats WHERE player_id = 8477424 AND is_season_total = 1"
        )
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT game_date, wins FROM goalie_stats WHERE player_id = 8477424 AND is_season_total = 1"
        )
        row = cursor.fetchone()
        assert row["game_date"] is None
        assert row["wins"] == 32


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
# Team Schedule Tests (5 tests)
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

    def test_empty_schedule(self) -> None:
        """Empty games array returns empty list."""
        mock_json = {"games": []}
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            games = fetch_team_schedule("EDM", "20232024")
            assert games == []

    def test_schedule_with_future_dates(self) -> None:
        """Future dates handled normally, result is None."""
        mock_json = {
            "games": [
                {
                    "gameDate": "2025-04-15",
                    "awayTeam": {"abbrev": "CGY"},
                    "homeTeam": {"abbrev": "EDM"},
                }
            ]
        }
        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.return_value = make_mock_response(mock_json)
            mock_session_cls.return_value = mock_session

            games = fetch_team_schedule("EDM", "20242025")
            assert len(games) == 1
            assert games[0]["game_date"] == "2025-04-15"
            assert games[0]["result"] is None

    def test_doubleheader_same_date_save(self, db: sqlite3.Connection) -> None:
        """Two games on same date — UNIQUE constraint means second overwrites first."""
        games = [
            {"game_date": "2024-01-15", "opponent": "CGY", "home_away": "home", "result": None},
            {"game_date": "2024-01-15", "opponent": "VAN", "home_away": "away", "result": None},
        ]

        save_team_schedule(db, "EDM", "20232024", games)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM team_games WHERE team = 'EDM' AND game_date = '2024-01-15'"
        )
        # UNIQUE(team, season, game_date) means second overwrites first
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT opponent, home_away FROM team_games WHERE team = 'EDM' AND game_date = '2024-01-15'"
        )
        row = cursor.fetchone()
        assert row["opponent"] == "VAN"
        assert row["home_away"] == "away"


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
                sync_all(db, "20232024", rate_limit=0.1)

                # Should sleep between requests
                assert mock_sleep.call_count > 0

    def test_api_get_retries_on_429(self) -> None:
        """_api_get retries with backoff on 429, then succeeds."""
        session = MagicMock()
        resp_429 = make_mock_response(status_code=429, raise_for_status=True)
        resp_429.headers = {}
        resp_200 = make_mock_response(json_data={"ok": True})
        resp_200.headers = {}
        session.get.side_effect = [resp_429, resp_200]

        with patch("fetchers.nhl_api.time.sleep") as mock_sleep:
            result = _api_get(session, "https://example.com/test")

        assert result.json() == {"ok": True}
        assert session.get.call_count == 2
        mock_sleep.assert_called_once_with(1)  # first backoff = 1s

    def test_api_get_respects_retry_after_header(self) -> None:
        """_api_get uses Retry-After header value when present."""
        session = MagicMock()
        resp_429 = make_mock_response(status_code=429, raise_for_status=True)
        resp_429.headers = {"Retry-After": "3"}
        resp_200 = make_mock_response(json_data={"ok": True})
        resp_200.headers = {}
        session.get.side_effect = [resp_429, resp_200]

        with patch("fetchers.nhl_api.time.sleep") as mock_sleep:
            result = _api_get(session, "https://example.com/test")

        assert result.json() == {"ok": True}
        mock_sleep.assert_called_once_with(3.0)

    def test_api_get_raises_after_max_retries(self) -> None:
        """_api_get raises HTTPError after exhausting retries."""
        session = MagicMock()
        resp_429 = make_mock_response(status_code=429, raise_for_status=True)
        resp_429.headers = {}
        session.get.return_value = resp_429

        with patch("fetchers.nhl_api.time.sleep"):
            with pytest.raises(requests.HTTPError):
                _api_get(session, "https://example.com/test")

        # 1 initial + 4 retries = 5 total
        assert session.get.call_count == 5

    def test_api_get_raises_immediately_on_non_429(self) -> None:
        """_api_get does not retry on non-429 errors."""
        session = MagicMock()
        resp_500 = make_mock_response(status_code=500, raise_for_status=True)
        resp_500.headers = {}
        session.get.return_value = resp_500

        with pytest.raises(requests.HTTPError):
            _api_get(session, "https://example.com/test")

        assert session.get.call_count == 1


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
            mock_response.status_code = 200
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
# Sync All Tests (3 tests)
# =============================================================================


class TestSyncAll:
    """Tests for sync_all orchestration."""

    def test_one_player_fetch_fails_others_continue(self, db: sqlite3.Connection) -> None:
        """One player game-log fetch fails, other player still gets stats."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM", "position": "C"},
        )
        upsert_player(
            db,
            {"id": 8479318, "full_name": "Leon Draisaitl", "team_abbrev": "EDM", "position": "C"},
        )

        mock_roster_json = {"forwards": [], "defensemen": [], "goalies": []}
        mock_schedule_json = {"games": []}
        mock_skater_log_json = {
            "gameLog": [
                {"gameDate": "2024-01-15", "goals": 1, "assists": 2, "toi": "20:00"}
            ]
        }

        def mock_get(url: str, **kwargs: Any) -> Mock:
            if "roster" in url:
                return make_mock_response(mock_roster_json)
            elif "schedule" in url:
                return make_mock_response(mock_schedule_json)
            elif "game-log" in url:
                if "8478402" in url:
                    return make_mock_response(status_code=500, raise_for_status=True)
                else:
                    return make_mock_response(mock_skater_log_json)
            return make_mock_response({})

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                with patch("fetchers.nhl_api.ALL_TEAMS", ["EDM"]):
                    result = sync_all(db, "20232024", rate_limit=0)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8478402"
        )
        assert cursor.fetchone()["cnt"] == 0

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8479318"
        )
        assert cursor.fetchone()["cnt"] == 1

    def test_mix_skaters_and_goalies_on_same_team(self, db: sqlite3.Connection) -> None:
        """Skaters go to skater path, goalies go to goalie path."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM", "position": "C"},
        )
        upsert_player(
            db,
            {"id": 8477424, "full_name": "Stuart Skinner", "team_abbrev": "EDM", "position": "G"},
        )

        mock_roster_json = {"forwards": [], "defensemen": [], "goalies": []}
        mock_schedule_json = {"games": []}
        mock_skater_log = {
            "gameLog": [
                {"gameDate": "2024-01-15", "goals": 2, "assists": 1, "toi": "22:00"}
            ]
        }
        mock_goalie_log = {
            "gameLog": [
                {
                    "gameDate": "2024-01-15",
                    "decision": "W",
                    "shotsAgainst": 30,
                    "goalsAgainst": 2,
                    "toi": "60:00",
                }
            ]
        }

        def mock_get(url: str, **kwargs: Any) -> Mock:
            if "roster" in url:
                return make_mock_response(mock_roster_json)
            elif "schedule" in url:
                return make_mock_response(mock_schedule_json)
            elif "game-log" in url:
                if "8478402" in url:
                    return make_mock_response(mock_skater_log)
                elif "8477424" in url:
                    return make_mock_response(mock_goalie_log)
            return make_mock_response({})

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                with patch("fetchers.nhl_api.ALL_TEAMS", ["EDM"]):
                    result = sync_all(db, "20232024", rate_limit=0)

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8478402"
        )
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM goalie_stats WHERE player_id = 8477424"
        )
        assert cursor.fetchone()["cnt"] == 1

        # No cross-contamination
        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM skater_stats WHERE player_id = 8477424"
        )
        assert cursor.fetchone()["cnt"] == 0
        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM goalie_stats WHERE player_id = 8478402"
        )
        assert cursor.fetchone()["cnt"] == 0

    def test_empty_roster_team_doesnt_break_loop(self, db: sqlite3.Connection) -> None:
        """Team with empty roster doesn't break the sync loop."""
        mock_edm_roster = {"forwards": [], "defensemen": [], "goalies": []}
        mock_tor_roster = {
            "forwards": [
                {
                    "id": 8479318,
                    "firstName": {"default": "Auston"},
                    "lastName": {"default": "Matthews"},
                    "positionCode": "C",
                }
            ],
            "defensemen": [],
            "goalies": [],
        }
        mock_schedule_json = {"games": []}
        mock_skater_log = {
            "gameLog": [
                {"gameDate": "2024-01-15", "goals": 1, "assists": 0, "toi": "19:00"}
            ]
        }

        def mock_get(url: str, **kwargs: Any) -> Mock:
            if "roster" in url:
                if "EDM" in url:
                    return make_mock_response(mock_edm_roster)
                elif "TOR" in url:
                    return make_mock_response(mock_tor_roster)
            elif "schedule" in url:
                return make_mock_response(mock_schedule_json)
            elif "game-log" in url:
                return make_mock_response(mock_skater_log)
            return make_mock_response({})

        with patch("fetchers.nhl_api.requests.Session") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.get.side_effect = mock_get
            mock_session_cls.return_value = mock_session

            with patch("fetchers.nhl_api.time.sleep"):
                with patch("fetchers.nhl_api.ALL_TEAMS", ["EDM", "TOR"]):
                    result = sync_all(db, "20232024", rate_limit=0)

        cursor = db.execute("SELECT COUNT(*) as cnt FROM players")
        assert cursor.fetchone()["cnt"] == 1

        cursor = db.execute("SELECT full_name FROM players WHERE id = 8479318")
        assert cursor.fetchone()["full_name"] == "Auston Matthews"

        assert result["skater_games"] >= 1


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
