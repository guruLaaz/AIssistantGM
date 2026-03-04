"""Tests for fetchers/fantrax_league.py — Fantrax league data fetcher."""

from __future__ import annotations

import pickle
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
import requests

from db.schema import get_db, init_db
from fetchers.fantrax_league import (
    _extract_teams_from_roster_data,
    _fantrax_api_call,
    _get_authenticated_session,
    _parse_roster_slots,
    fetch_gp_per_position,
    fetch_player_salaries,
    fetch_roster,
    fetch_standings,
    fetch_teams,
    save_gp_per_position,
    save_player_salaries,
    save_roster,
    save_standings,
    save_teams,
    sync_fantrax_league,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database."""
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


@pytest.fixture
def fantrax_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a Fantrax config dict with valid cookie file."""
    cookie_path = tmp_path / "test_cookies.pkl"
    with open(cookie_path, "wb") as f:
        pickle.dump([{"name": "sid", "value": "xyz"}], f)
    return {
        "username": "test@example.com",
        "password": "testpass",
        "league_id": "abc123",
        "cookie_file": cookie_path,
    }


# =============================================================================
# _fantrax_api_call Tests
# =============================================================================


class TestFantraxApiCall:
    """Tests for _fantrax_api_call helper."""

    def test_sends_correct_payload(self) -> None:
        """POST body has correct method and leagueId."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "responses": [{"data": {"key": "val"}}],
        }
        session.post.return_value = mock_resp

        _fantrax_api_call(session, "lg123", "getStandings")

        session.post.assert_called_once()
        kwargs = session.post.call_args
        json_body = kwargs.kwargs["json"] if "json" in kwargs.kwargs else kwargs[1].get("json")
        # Handle both positional and keyword args
        if json_body is None:
            json_body = kwargs[1]["json"]

        assert json_body["msgs"][0]["method"] == "getStandings"
        assert json_body["msgs"][0]["data"]["leagueId"] == "lg123"

    def test_returns_response_data(self) -> None:
        """Returns the data dict from the first response."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "responses": [{"data": {"key": "val"}}],
        }
        session.post.return_value = mock_resp

        result = _fantrax_api_call(session, "lg123", "getStandings")

        assert result == {"key": "val"}

    def test_raises_on_page_error(self) -> None:
        """RuntimeError raised when response has pageError."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "pageError": {"code": "WARNING_NOT_LOGGED_IN"},
        }
        session.post.return_value = mock_resp

        with pytest.raises(RuntimeError, match="WARNING_NOT_LOGGED_IN"):
            _fantrax_api_call(session, "lg123", "getStandings")

    def test_merges_extra_data(self) -> None:
        """extra_data params appear in the request payload."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "responses": [{"data": {}}],
        }
        session.post.return_value = mock_resp

        _fantrax_api_call(
            session, "lg123", "getTeamRosterInfo",
            extra_data={"teamId": "t1", "view": "STATS"},
        )

        kwargs = session.post.call_args
        json_body = kwargs.kwargs.get("json") or kwargs[1].get("json")
        data = json_body["msgs"][0]["data"]
        assert data["teamId"] == "t1"
        assert data["view"] == "STATS"
        assert data["leagueId"] == "lg123"


# =============================================================================
# _get_authenticated_session Tests
# =============================================================================


class TestGetAuthenticatedSession:
    """Tests for _get_authenticated_session helper."""

    @patch("fetchers.fantrax_league._load_cookies_for_session", return_value=True)
    def test_loads_cookies_from_file(
        self,
        mock_load: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """Returns session and league_id when cookies load successfully."""
        session, league_id = _get_authenticated_session(fantrax_config)

        assert league_id == "abc123"
        assert session is not None
        mock_load.assert_called_once()

    @patch("fetchers.fantrax_league._load_cookies_for_session", return_value=False)
    def test_raises_without_cookies(
        self,
        mock_load: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """RuntimeError raised when no cookies are available."""
        with pytest.raises(RuntimeError, match="No Fantrax cookies"):
            _get_authenticated_session(fantrax_config)


# =============================================================================
# fetch_teams Tests
# =============================================================================


class TestFetchTeams:
    """Tests for fetch_teams function."""

    def test_parses_team_info_from_roster_data(self) -> None:
        """Team info list is parsed into list of team dicts via _roster_data."""
        roster_data = {
            "fantasyTeams": [
                {
                    "id": "team1",
                    "name": "Team One",
                    "shortName": "T1",
                    "logoUrl256": "http://example.com/t1.png",
                },
                {
                    "id": "team2",
                    "name": "Team Two",
                    "shortName": "T2",
                    "logoUrl128": "http://example.com/t2_128.png",
                },
            ],
            "tables": [],
        }

        teams = fetch_teams(MagicMock(), "lg123", _roster_data=roster_data)

        assert len(teams) == 2
        names = {t["name"] for t in teams}
        assert "Team One" in names
        assert "Team Two" in names
        t1 = next(t for t in teams if t["id"] == "team1")
        assert t1["short_name"] == "T1"
        assert t1["logo_url"] == "http://example.com/t1.png"

    def test_empty_response(self) -> None:
        """Empty fantasyTeams list returns empty list."""
        roster_data = {"fantasyTeams": [], "tables": []}

        teams = fetch_teams(MagicMock(), "lg123", _roster_data=roster_data)

        assert teams == []

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_standalone_discovers_team_from_standings(
        self, mock_api: MagicMock,
    ) -> None:
        """Without _roster_data, fetches standings then roster to find teams."""
        # First call: getStandings to discover a team_id
        standings_response = {
            "tableList": [{
                "header": {"cells": []},
                "rows": [{
                    "fixedCells": [{"content": "1"}, {"teamId": "t1"}],
                    "cells": [],
                }],
            }],
        }
        # Second call: getTeamRosterInfo to get fantasyTeams
        roster_response = {
            "fantasyTeams": [
                {"id": "t1", "name": "Team One", "shortName": "T1"},
            ],
            "tables": [],
        }
        mock_api.side_effect = [standings_response, roster_response]

        teams = fetch_teams(MagicMock(), "lg123")

        assert len(teams) == 1
        assert teams[0]["name"] == "Team One"
        assert mock_api.call_count == 2


# =============================================================================
# fetch_standings Tests
# =============================================================================


class TestFetchStandings:
    """Tests for fetch_standings function."""

    def _make_standings_response(
        self,
        field_keys: dict[str, str] | None = None,
    ) -> dict:
        """Build a realistic standings API response.

        Args:
            field_keys: Override field key names. Defaults use standard keys.
        """
        keys = {
            "win": "win",
            "loss": "loss",
            "tie": "tie",
            "points": "points",
            "winpc": "winpc",
            "gamesback": "pointsBehindLeader",
            "wwOrder": "wwOrder",
            "cr": "maxClaimsSeason",
            "fantasyPoints": "fantasyPoints",
            "pointsAgainst": "pointsAgainst",
            "streak": "streak",
            "sc": "sc",
            "FPtsPerGame": "FPtsPerGame",
        }
        if field_keys:
            keys.update(field_keys)

        header_cells = [{"key": v} for v in keys.values()]
        field_list = list(keys.values())

        def make_cell(value: Any) -> dict:
            return {"content": str(value)}

        cells = []
        for key in field_list:
            value_map = {
                "win": "10",
                "loss": "5",
                "tie": "2",
                "points": "22",
                "winpc": "0.588",
                "gamesback": "3.5",
                "pointsBehindLeader": "3.5",
                "wwOrder": "2",
                "cr": "15",
                "maxClaimsSeason": "15",
                "claimsRemaining": "15",
                "fantasyPoints": "1234.5",
                "fPts": "1234.5",
                "pointsFor": "1234.5",
                "pointsAgainst": "1100.0",
                "streak": "W3",
                "sc": "17",
                "gp": "17",
                "FPtsPerGame": "72.6",
                "fpGp": "72.6",
                "fPtsPerGp": "72.6",
            }
            cells.append(make_cell(value_map.get(key, "0")))

        return {
            "tableList": [{
                "header": {"cells": header_cells},
                "rows": [{
                    "fixedCells": [
                        {"content": "1"},
                        {"teamId": "team_abc"},
                    ],
                    "cells": cells,
                }],
            }],
        }

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_parses_standings_table(self, mock_api: MagicMock) -> None:
        """Standings table is parsed with all expected fields."""
        mock_api.return_value = self._make_standings_response()

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 1
        s = standings[0]
        assert s["team_id"] == "team_abc"
        assert s["rank"] == 1
        assert s["wins"] == 10
        assert s["losses"] == 5
        assert s["ties"] == 2
        assert s["points"] == 22
        assert s["claims_remaining"] == 15
        assert s["waiver_order"] == 2
        assert s["points_for"] == 1234.5
        assert s["games_played"] == 17
        assert s["fantasy_points_per_game"] == 72.6

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_empty_table_list(self, mock_api: MagicMock) -> None:
        """Empty tableList returns empty list."""
        mock_api.return_value = {"tableList": []}

        standings = fetch_standings(MagicMock(), "lg123")

        assert standings == []

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_alternative_field_names(self, mock_api: MagicMock) -> None:
        """Alternative field names (fPts, gp, fpGp) are parsed correctly."""
        mock_api.return_value = self._make_standings_response(
            field_keys={
                "fantasyPoints": "fPts",
                "sc": "gp",
                "FPtsPerGame": "fpGp",
            },
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 1
        s = standings[0]
        assert s["points_for"] == 1234.5
        assert s["games_played"] == 17
        assert s["fantasy_points_per_game"] == 72.6


# =============================================================================
# fetch_roster Tests
# =============================================================================


class TestFetchRoster:
    """Tests for fetch_roster function."""

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_parses_roster_slots(self, mock_api: MagicMock) -> None:
        """Roster response is parsed into list of slot dicts with header-mapped cells."""
        mock_api.return_value = {
            "tables": [{
                "header": {"cells": [
                    {"key": "age"},
                    {"key": "opponent"},
                    {"key": "salary"},
                    {"key": "fpts"},
                    {"key": "fptsPerGame"},
                ]},
                "rows": [
                    {
                        "scorer": {
                            "scorerId": "p001",
                            "name": "Connor McDavid",
                            "posShortNames": "C",
                        },
                        "statusId": "1",
                        "posId": 204,
                        "cells": [
                            {"content": "28"},
                            {"content": ""},
                            {"content": "12,500,000"},
                            {"content": "120.5"},
                            {"content": "7.1"},
                        ],
                    },
                ],
            }],
        }

        slots = fetch_roster(MagicMock(), "lg123", "team1")

        assert len(slots) == 1
        slot = slots[0]
        assert slot["player_id"] == "p001"
        assert slot["player_name"] == "Connor McDavid"
        assert slot["position_short"] == "C"
        assert slot["status_id"] == "1"
        assert slot["position_id"] == "204"
        assert slot["salary"] == 12500000.0
        assert slot["total_fantasy_points"] == 120.5
        assert slot["fantasy_points_per_game"] == 7.1

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_empty_roster(self, mock_api: MagicMock) -> None:
        """Empty roster response returns empty list."""
        mock_api.return_value = {"tables": []}

        slots = fetch_roster(MagicMock(), "lg123", "team1")

        assert slots == []

    def test_parses_from_roster_data_param(self) -> None:
        """fetch_roster with _roster_data skips API call."""
        roster_data = {
            "tables": [{
                "header": {"cells": [{"key": "salary"}, {"key": "fpts"}]},
                "rows": [{
                    "scorer": {"scorerId": "p1", "name": "Test"},
                    "posId": 207,
                    "cells": [{"content": "1,000,000"}, {"content": "50.0"}],
                }],
            }],
        }

        slots = fetch_roster(MagicMock(), "lg123", "team1", _roster_data=roster_data)

        assert len(slots) == 1
        assert slots[0]["salary"] == 1000000.0
        assert slots[0]["total_fantasy_points"] == 50.0


# =============================================================================
# save_teams Tests
# =============================================================================


class TestSaveTeams:
    """Tests for save_teams function."""

    def test_inserts_rows(self, db: sqlite3.Connection) -> None:
        """Saves teams and they appear in fantasy_teams table."""
        teams = [
            {"id": "t1", "name": "Team One", "short_name": "T1", "logo_url": None},
            {"id": "t2", "name": "Team Two", "short_name": "T2", "logo_url": None},
        ]
        count = save_teams(db, "lg123", teams)

        assert count == 2
        rows = db.execute("SELECT * FROM fantasy_teams").fetchall()
        assert len(rows) == 2

    def test_upsert_updates_existing(self, db: sqlite3.Connection) -> None:
        """Saving team twice with different name updates the row."""
        teams_v1 = [{"id": "t1", "name": "Old Name", "short_name": "ON", "logo_url": None}]
        teams_v2 = [{"id": "t1", "name": "New Name", "short_name": "NN", "logo_url": None}]

        save_teams(db, "lg123", teams_v1)
        save_teams(db, "lg123", teams_v2)

        rows = db.execute("SELECT * FROM fantasy_teams").fetchall()
        assert len(rows) == 1
        assert rows[0]["name"] == "New Name"

    def test_empty_list_returns_zero(self, db: sqlite3.Connection) -> None:
        """Empty team list returns 0."""
        count = save_teams(db, "lg123", [])

        assert count == 0


# =============================================================================
# save_standings Tests
# =============================================================================


class TestSaveStandings:
    """Tests for save_standings function."""

    def _make_standing(self, **overrides: Any) -> dict[str, Any]:
        """Create a standing dict with sensible defaults."""
        base: dict[str, Any] = {
            "team_id": "t1",
            "rank": 1,
            "wins": 10,
            "losses": 5,
            "ties": 2,
            "points": 22,
            "win_percentage": 0.588,
            "games_back": 0,
            "waiver_order": 1,
            "claims_remaining": 15,
            "points_for": 1234.5,
            "points_against": 1100.0,
            "streak": "W3",
            "games_played": 17,
            "fantasy_points_per_game": 72.6,
        }
        base.update(overrides)
        return base

    def test_inserts_rows(self, db: sqlite3.Connection) -> None:
        """Saves standings with correct rank and points_for."""
        standings = [
            self._make_standing(team_id="t1", rank=1, points_for=1500.0),
            self._make_standing(team_id="t2", rank=2, points_for=1200.0),
        ]
        count = save_standings(db, "lg123", standings)

        assert count == 2
        rows = db.execute(
            "SELECT * FROM fantasy_standings ORDER BY rank"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["rank"] == 1
        assert rows[0]["points_for"] == 1500.0
        assert rows[1]["rank"] == 2

    def test_replaces_on_resync(self, db: sqlite3.Connection) -> None:
        """Re-syncing replaces old standings for the same league."""
        standings_v1 = [
            self._make_standing(team_id="t1"),
            self._make_standing(team_id="t2"),
        ]
        standings_v2 = [
            self._make_standing(team_id="t1"),
            self._make_standing(team_id="t2"),
            self._make_standing(team_id="t3"),
        ]

        save_standings(db, "lg123", standings_v1)
        save_standings(db, "lg123", standings_v2)

        rows = db.execute("SELECT * FROM fantasy_standings").fetchall()
        assert len(rows) == 3

    def test_claims_remaining_stored(self, db: sqlite3.Connection) -> None:
        """claims_remaining value is persisted in the database."""
        standings = [self._make_standing(claims_remaining=15)]
        save_standings(db, "lg123", standings)

        row = db.execute("SELECT claims_remaining FROM fantasy_standings").fetchone()
        assert row["claims_remaining"] == 15


# =============================================================================
# save_roster Tests
# =============================================================================


class TestSaveRoster:
    """Tests for save_roster function."""

    def _make_slot(self, **overrides: Any) -> dict[str, Any]:
        """Create a roster slot dict with sensible defaults."""
        base: dict[str, Any] = {
            "player_id": "p001",
            "player_name": "Connor McDavid",
            "position_id": "C",
            "position_short": "C",
            "status_id": "ACTIVE",
            "salary": 5.5,
            "total_fantasy_points": 120.5,
            "fantasy_points_per_game": 7.1,
        }
        base.update(overrides)
        return base

    def test_inserts_slots(self, db: sqlite3.Connection) -> None:
        """Saves roster slots to fantasy_roster_slots table."""
        slots = [
            self._make_slot(player_id="p001"),
            self._make_slot(player_id="p002", player_name="Leon Draisaitl"),
        ]
        count = save_roster(db, "team1", slots)

        assert count == 2
        rows = db.execute("SELECT * FROM fantasy_roster_slots").fetchall()
        assert len(rows) == 2

    def test_replaces_on_resync(self, db: sqlite3.Connection) -> None:
        """Re-syncing replaces roster slots for the same team."""
        slots_v1 = [
            self._make_slot(player_id="p001"),
            self._make_slot(player_id="p002"),
            self._make_slot(player_id="p003"),
        ]
        slots_v2 = [
            self._make_slot(player_id="p001"),
            self._make_slot(player_id="p004"),
        ]

        save_roster(db, "team1", slots_v1)
        save_roster(db, "team1", slots_v2)

        rows = db.execute("SELECT * FROM fantasy_roster_slots").fetchall()
        assert len(rows) == 2

    def test_empty_returns_zero(self, db: sqlite3.Connection) -> None:
        """Empty roster list returns 0."""
        count = save_roster(db, "team1", [])

        assert count == 0


# =============================================================================
# sync_fantrax_league Tests
# =============================================================================


class TestSyncFantraxLeague:
    """Tests for sync_fantrax_league orchestrator."""

    @patch("fetchers.fantrax_league.save_roster")
    @patch("fetchers.fantrax_league.fetch_roster")
    @patch("fetchers.fantrax_league.save_teams")
    @patch("fetchers.fantrax_league.fetch_teams")
    @patch("fetchers.fantrax_league._fantrax_api_call")
    @patch("fetchers.fantrax_league.save_standings")
    @patch("fetchers.fantrax_league.fetch_standings")
    @patch("fetchers.fantrax_league._get_authenticated_session")
    def test_returns_summary_dict(
        self,
        mock_auth: MagicMock,
        mock_fetch_standings: MagicMock,
        mock_save_standings: MagicMock,
        mock_api_call: MagicMock,
        mock_fetch_teams: MagicMock,
        mock_save_teams: MagicMock,
        mock_fetch_roster: MagicMock,
        mock_save_roster: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Returns dict with correct keys and counts."""
        mock_auth.return_value = (MagicMock(), "lg123")
        mock_fetch_standings.return_value = [{"team_id": "t1"}]
        mock_save_standings.return_value = 1
        mock_api_call.return_value = {}  # roster data passed through
        mock_fetch_teams.return_value = [
            {"id": "t1", "name": "Team One", "short_name": "T1", "logo_url": None},
        ]
        mock_save_teams.return_value = 1
        mock_fetch_roster.return_value = [{"player_id": "p1"}, {"player_id": "p2"}]
        mock_save_roster.return_value = 2

        result = sync_fantrax_league(db)

        assert result["teams_synced"] == 1
        assert result["standings_synced"] == 1
        assert result["roster_slots_synced"] == 2

    @patch("fetchers.fantrax_league.save_roster")
    @patch("fetchers.fantrax_league.fetch_roster")
    @patch("fetchers.fantrax_league.save_teams")
    @patch("fetchers.fantrax_league.fetch_teams")
    @patch("fetchers.fantrax_league._fantrax_api_call")
    @patch("fetchers.fantrax_league.save_standings")
    @patch("fetchers.fantrax_league.fetch_standings")
    @patch("fetchers.fantrax_league._get_authenticated_session")
    def test_calls_fetch_roster_per_team(
        self,
        mock_auth: MagicMock,
        mock_fetch_standings: MagicMock,
        mock_save_standings: MagicMock,
        mock_api_call: MagicMock,
        mock_fetch_teams: MagicMock,
        mock_save_teams: MagicMock,
        mock_fetch_roster: MagicMock,
        mock_save_roster: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """fetch_roster is called once per team from standings."""
        mock_auth.return_value = (MagicMock(), "lg123")
        # Standings drive the loop — 3 teams
        mock_fetch_standings.return_value = [
            {"team_id": "t1"},
            {"team_id": "t2"},
            {"team_id": "t3"},
        ]
        mock_save_standings.return_value = 3
        mock_api_call.return_value = {}
        mock_fetch_teams.return_value = [
            {"id": "t1"}, {"id": "t2"}, {"id": "t3"},
        ]
        mock_save_teams.return_value = 3
        mock_fetch_roster.return_value = []
        mock_save_roster.return_value = 0

        sync_fantrax_league(db)

        assert mock_fetch_roster.call_count == 3

    @patch("fetchers.fantrax_league._get_authenticated_session")
    def test_no_cookies_raises(
        self,
        mock_auth: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """RuntimeError from _get_authenticated_session propagates."""
        mock_auth.side_effect = RuntimeError("No Fantrax cookies")

        with pytest.raises(RuntimeError, match="No Fantrax cookies"):
            sync_fantrax_league(db)


# =============================================================================
# Part A — Edge-case unit tests
# =============================================================================


class TestApiCallEdgeCases:
    """Edge-case tests for _fantrax_api_call."""

    def test_http_error_propagates(self) -> None:
        """HTTPError from raise_for_status propagates uncaught."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        session.post.return_value = mock_resp

        with pytest.raises(requests.HTTPError, match="403 Forbidden"):
            _fantrax_api_call(session, "lg123", "getStandings")

    def test_page_error_missing_code_uses_unknown(self) -> None:
        """pageError with no 'code' key produces 'unknown' in the message."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"pageError": {}}
        session.post.return_value = mock_resp

        with pytest.raises(RuntimeError, match="unknown"):
            _fantrax_api_call(session, "lg123", "getStandings")

    def test_json_decode_error_propagates(self) -> None:
        """ValueError from resp.json() propagates uncaught."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.side_effect = ValueError("Expecting value")
        session.post.return_value = mock_resp

        with pytest.raises(ValueError, match="Expecting value"):
            _fantrax_api_call(session, "lg123", "getStandings")

    def test_extra_data_none_not_merged(self) -> None:
        """extra_data=None results in payload data containing only leagueId."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"responses": [{"data": {}}]}
        session.post.return_value = mock_resp

        _fantrax_api_call(session, "lg123", "someMethod", extra_data=None)

        kwargs = session.post.call_args
        json_body = kwargs.kwargs.get("json") or kwargs[1].get("json")
        data = json_body["msgs"][0]["data"]
        assert data == {"leagueId": "lg123"}


# ---------------------------------------------------------------------------
# fetch_standings edge cases
# ---------------------------------------------------------------------------


def _make_standings_row(
    team_id: str = "t1",
    rank: int = 1,
    cells: list[dict[str, Any]] | None = None,
    fixed_cells: list[dict] | None = None,
) -> dict:
    """Build a single standings row for test responses."""
    if fixed_cells is None:
        fixed_cells = [{"content": str(rank)}, {"teamId": team_id}]
    if cells is None:
        cells = []
    return {"fixedCells": fixed_cells, "cells": cells}


def _make_standings_data(
    header_keys: list[str],
    rows: list[dict],
) -> dict:
    """Build a complete standings API response dict."""
    return {
        "tableList": [{
            "header": {"cells": [{"key": k} for k in header_keys]},
            "rows": rows,
        }],
    }


class TestFetchStandingsEdgeCases:
    """Edge-case tests for fetch_standings parsing logic."""

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_dash_cell_returns_default(self, mock_api: MagicMock) -> None:
        """A cell with content '-' is treated as the default (0)."""
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints"],
            rows=[_make_standings_row(cells=[{"content": "-"}])],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 1
        assert standings[0]["points_for"] == 0

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_comma_formatted_numbers(self, mock_api: MagicMock) -> None:
        """Comma-formatted number '1,234.5' is parsed as 1234.5."""
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints"],
            rows=[_make_standings_row(cells=[{"content": "1,234.5"}])],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert standings[0]["points_for"] == 1234.5

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_cell_index_out_of_bounds(self, mock_api: MagicMock) -> None:
        """Header with more fields than row cells returns defaults for missing."""
        # Header declares 3 fields but row only has 1 cell
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints", "win", "loss"],
            rows=[_make_standings_row(cells=[{"content": "500.0"}])],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 1
        assert standings[0]["points_for"] == 500.0
        assert standings[0]["wins"] == 0  # out of bounds → default
        assert standings[0]["losses"] == 0

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_empty_fixed_cells(self, mock_api: MagicMock) -> None:
        """Empty fixedCells list → team_id='' and rank=0, no crash."""
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints"],
            rows=[_make_standings_row(
                fixed_cells=[],
                cells=[{"content": "100"}],
            )],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 1
        assert standings[0]["team_id"] == ""
        assert standings[0]["rank"] == 0

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_single_fixed_cell(self, mock_api: MagicMock) -> None:
        """Only 1 fixedCell (rank only) → team_id='' and rank is parsed."""
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints"],
            rows=[_make_standings_row(
                fixed_cells=[{"content": "3"}],
                cells=[{"content": "100"}],
            )],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert standings[0]["team_id"] == ""
        assert standings[0]["rank"] == 3

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_claims_remaining_from_maxClaimsSeason(
        self, mock_api: MagicMock,
    ) -> None:
        """maxClaimsSeason field maps to claims_remaining."""
        mock_api.return_value = _make_standings_data(
            header_keys=["maxClaimsSeason"],
            rows=[_make_standings_row(cells=[{"content": "42"}])],
        )

        standings = fetch_standings(MagicMock(), "lg123")
        assert standings[0]["claims_remaining"] == 42

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_claims_remaining_none_when_field_absent(
        self, mock_api: MagicMock,
    ) -> None:
        """No maxClaimsSeason/cr/claimsRemaining in headers → claims_remaining is None."""
        mock_api.return_value = _make_standings_data(
            header_keys=["win", "loss"],
            rows=[_make_standings_row(cells=[
                {"content": "10"},
                {"content": "5"},
            ])],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert standings[0]["claims_remaining"] is None

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_points_behind_leader_maps_to_games_back(
        self, mock_api: MagicMock,
    ) -> None:
        """pointsBehindLeader field maps to games_back."""
        mock_api.return_value = _make_standings_data(
            header_keys=["pointsBehindLeader"],
            rows=[_make_standings_row(cells=[{"content": "25.3"}])],
        )

        standings = fetch_standings(MagicMock(), "lg123")
        assert standings[0]["games_back"] == 25.3

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_waiver_order_none_when_absent(self, mock_api: MagicMock) -> None:
        """No wwOrder in headers → waiver_order is None."""
        mock_api.return_value = _make_standings_data(
            header_keys=["win", "loss"],
            rows=[_make_standings_row(cells=[
                {"content": "10"},
                {"content": "5"},
            ])],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert standings[0]["waiver_order"] is None

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_multiple_rows_parsed(self, mock_api: MagicMock) -> None:
        """Three rows produce three standings with correct team_ids and ranks."""
        mock_api.return_value = _make_standings_data(
            header_keys=["fantasyPoints"],
            rows=[
                _make_standings_row(team_id="t1", rank=1, cells=[{"content": "300"}]),
                _make_standings_row(team_id="t2", rank=2, cells=[{"content": "250"}]),
                _make_standings_row(team_id="t3", rank=3, cells=[{"content": "200"}]),
            ],
        )

        standings = fetch_standings(MagicMock(), "lg123")

        assert len(standings) == 3
        assert [s["team_id"] for s in standings] == ["t1", "t2", "t3"]
        assert [s["rank"] for s in standings] == [1, 2, 3]


# ---------------------------------------------------------------------------
# fetch_roster edge cases
# ---------------------------------------------------------------------------


class TestFetchRosterEdgeCases:
    """Edge-case tests for fetch_roster / _parse_roster_slots parsing logic."""

    def test_empty_scorer_skipped(self) -> None:
        """Row with 'scorer': {} is skipped (empty dict is falsy)."""
        roster_data = {
            "tables": [{
                "header": {"cells": []},
                "rows": [{"scorer": {}, "cells": []}],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert len(slots) == 0

    def test_row_without_scorer_key_skipped(self) -> None:
        """Row with no 'scorer' key at all is skipped."""
        roster_data = {
            "tables": [{
                "header": {"cells": []},
                "rows": [{"cells": []}],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert len(slots) == 0

    def test_salary_with_dollar_sign(self) -> None:
        """Salary '$5,500' in cells is parsed to 5500.0."""
        roster_data = {
            "tables": [{
                "header": {"cells": [{"key": "salary"}]},
                "rows": [{
                    "scorer": {"scorerId": "p1", "name": "Test"},
                    "posId": 204,
                    "cells": [{"content": "$5,500"}],
                }],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert len(slots) == 1
        assert slots[0]["salary"] == 5500.0

    def test_salary_dash_returns_none(self) -> None:
        """Salary '-' in cells returns None."""
        roster_data = {
            "tables": [{
                "header": {"cells": [{"key": "salary"}]},
                "rows": [{
                    "scorer": {"scorerId": "p1", "name": "Test"},
                    "posId": 204,
                    "cells": [{"content": "-"}],
                }],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert slots[0]["salary"] is None

    def test_fpts_from_header_mapped_cells(self) -> None:
        """Fantasy points and FP/G are extracted from cells via header mapping."""
        roster_data = {
            "tables": [{
                "header": {"cells": [
                    {"key": "salary"},
                    {"key": "fpts"},
                    {"key": "fptsPerGame"},
                ]},
                "rows": [{
                    "scorer": {"scorerId": "p1", "name": "Test"},
                    "posId": 207,
                    "cells": [
                        {"content": "1,500,000"},
                        {"content": "100.5"},
                        {"content": "5.5"},
                    ],
                }],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert slots[0]["total_fantasy_points"] == 100.5
        assert slots[0]["fantasy_points_per_game"] == 5.5

    def test_position_id_from_posId(self) -> None:
        """position_id comes from row's posId field."""
        roster_data = {
            "tables": [{
                "header": {"cells": []},
                "rows": [{
                    "scorer": {"scorerId": "p1", "name": "Test"},
                    "posId": 230,
                    "cells": [],
                }],
            }],
        }
        slots = _parse_roster_slots(roster_data)
        assert slots[0]["position_id"] == "230"


# ---------------------------------------------------------------------------
# fetch_teams edge cases
# ---------------------------------------------------------------------------


class TestFetchTeamsEdgeCases:
    """Edge-case tests for fetch_teams / _extract_teams_from_roster_data."""

    def test_missing_fantasyTeams_key(self) -> None:
        """Roster data with no fantasyTeams key → empty list."""
        teams = _extract_teams_from_roster_data({})
        assert teams == []

    def test_team_with_missing_fields(self) -> None:
        """Team with only 'id' — name/short/logo use defaults."""
        roster_data = {
            "fantasyTeams": [{"id": "t1"}],
        }
        teams = _extract_teams_from_roster_data(roster_data)

        assert len(teams) == 1
        assert teams[0]["id"] == "t1"
        assert teams[0]["name"] == ""
        assert teams[0]["short_name"] == ""
        assert teams[0]["logo_url"] is None

    def test_logo_falls_back_to_128(self) -> None:
        """Logo URL falls back to logoUrl128 when logoUrl256 is missing."""
        roster_data = {
            "fantasyTeams": [
                {"id": "t1", "name": "Test", "logoUrl128": "http://img/128.png"},
            ],
        }
        teams = _extract_teams_from_roster_data(roster_data)
        assert teams[0]["logo_url"] == "http://img/128.png"

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_standalone_no_standings_rows(self, mock_api: MagicMock) -> None:
        """Standalone fetch_teams with empty standings → empty list."""
        mock_api.return_value = {"tableList": [{"header": {"cells": []}, "rows": []}]}

        teams = fetch_teams(MagicMock(), "lg123")
        assert teams == []


# ---------------------------------------------------------------------------
# save_standings edge cases
# ---------------------------------------------------------------------------


class TestSaveStandingsEdgeCases:
    """Edge-case tests for save_standings DB storage."""

    def test_null_optional_fields_stored(self, db: sqlite3.Connection) -> None:
        """waiver_order=None and claims_remaining=None are stored as NULL."""
        standings = [{
            "team_id": "t1",
            "rank": 1,
            "wins": 0,
            "losses": 0,
            "ties": 0,
            "points": 0,
            "win_percentage": 0,
            "games_back": 0,
            "waiver_order": None,
            "claims_remaining": None,
            "points_for": 0,
            "points_against": 0,
            "streak": "",
            "games_played": 0,
            "fantasy_points_per_game": 0,
        }]
        save_standings(db, "lg1", standings)

        row = db.execute("SELECT * FROM fantasy_standings WHERE team_id='t1'").fetchone()
        assert row["waiver_order"] is None
        assert row["claims_remaining"] is None
        assert row["streak"] == ""

    def test_league_isolation(self, db: sqlite3.Connection) -> None:
        """Re-saving league lg1 does not affect league lg2 data."""
        base = {
            "team_id": "t1", "rank": 1, "wins": 5, "losses": 3,
            "ties": 0, "points": 10, "win_percentage": 0.625,
            "games_back": 0, "waiver_order": 1, "claims_remaining": 10,
            "points_for": 500, "points_against": 400, "streak": "W2",
            "games_played": 8, "fantasy_points_per_game": 62.5,
        }
        save_standings(db, "lg1", [base])
        save_standings(db, "lg2", [{**base, "team_id": "t2", "points_for": 999}])

        # Re-save lg1 with different data
        save_standings(db, "lg1", [{**base, "points_for": 100}])

        lg2_row = db.execute(
            "SELECT * FROM fantasy_standings WHERE league_id='lg2'"
        ).fetchone()
        assert lg2_row is not None
        assert lg2_row["points_for"] == 999


# ---------------------------------------------------------------------------
# save_roster edge cases
# ---------------------------------------------------------------------------


class TestSaveRosterEdgeCases:
    """Edge-case tests for save_roster DB storage."""

    def test_all_none_optional_fields(self, db: sqlite3.Connection) -> None:
        """Insert with all optional fields as None succeeds."""
        slots = [{
            "player_id": None,
            "player_name": None,
            "position_id": None,
            "position_short": None,
            "status_id": None,
            "salary": None,
            "total_fantasy_points": None,
            "fantasy_points_per_game": None,
        }]
        count = save_roster(db, "t1", slots)

        assert count == 1
        row = db.execute("SELECT * FROM fantasy_roster_slots").fetchone()
        assert row["player_id"] is None
        assert row["salary"] is None

    def test_team_isolation(self, db: sqlite3.Connection) -> None:
        """Re-saving team t1 does not affect team t2 roster."""
        slot_base = {
            "player_id": "p1", "player_name": "Player One",
            "position_id": "C", "position_short": "C",
            "status_id": "ACTIVE", "salary": 5.0,
            "total_fantasy_points": 100, "fantasy_points_per_game": 5.0,
        }
        save_roster(db, "t1", [slot_base])
        save_roster(db, "t2", [{**slot_base, "player_id": "p2", "player_name": "Player Two"}])

        # Re-save t1 with different roster
        save_roster(db, "t1", [{**slot_base, "player_id": "p3", "player_name": "Player Three"}])

        t2_rows = db.execute(
            "SELECT * FROM fantasy_roster_slots WHERE team_id='t2'"
        ).fetchall()
        assert len(t2_rows) == 1
        assert t2_rows[0]["player_name"] == "Player Two"


# ---------------------------------------------------------------------------
# sync edge cases
# ---------------------------------------------------------------------------


class TestSyncEdgeCases:
    """Edge-case tests for sync_fantrax_league orchestrator."""

    @patch("fetchers.fantrax_league.save_player_salaries", return_value=0)
    @patch("fetchers.fantrax_league.fetch_player_salaries", return_value=[])
    @patch("fetchers.fantrax_league.save_roster")
    @patch("fetchers.fantrax_league.fetch_roster")
    @patch("fetchers.fantrax_league.save_teams")
    @patch("fetchers.fantrax_league.fetch_teams")
    @patch("fetchers.fantrax_league._fantrax_api_call")
    @patch("fetchers.fantrax_league.save_standings")
    @patch("fetchers.fantrax_league.fetch_standings")
    @patch("fetchers.fantrax_league._get_authenticated_session")
    def test_zero_teams_returns_zero_counts(
        self,
        mock_auth: MagicMock,
        mock_fetch_standings: MagicMock,
        mock_save_standings: MagicMock,
        mock_api_call: MagicMock,
        mock_fetch_teams: MagicMock,
        mock_save_teams: MagicMock,
        mock_fetch_roster: MagicMock,
        mock_save_roster: MagicMock,
        mock_fetch_salaries: MagicMock,
        mock_save_salaries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Empty standings → no teams, no rosters fetched."""
        mock_auth.return_value = (MagicMock(), "lg123")
        mock_fetch_standings.return_value = []  # no team_ids
        mock_save_standings.return_value = 0

        result = sync_fantrax_league(db)

        assert result["roster_slots_synced"] == 0
        assert result["teams_synced"] == 0
        mock_fetch_roster.assert_not_called()
        mock_fetch_teams.assert_not_called()

    @patch("fetchers.fantrax_league.save_player_salaries", return_value=0)
    @patch("fetchers.fantrax_league.fetch_player_salaries", return_value=[])
    @patch("fetchers.fantrax_league.save_standings")
    @patch("fetchers.fantrax_league.fetch_standings")
    @patch("fetchers.fantrax_league._get_authenticated_session")
    def test_config_passed_to_auth(
        self,
        mock_auth: MagicMock,
        mock_fetch_standings: MagicMock,
        mock_save_standings: MagicMock,
        mock_fetch_salaries: MagicMock,
        mock_save_salaries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Custom config dict is forwarded to _get_authenticated_session."""
        mock_auth.return_value = (MagicMock(), "lg123")
        mock_fetch_standings.return_value = []
        mock_save_standings.return_value = 0

        custom_config = {"league_id": "custom", "cookie_file": "/tmp/c.pkl"}
        sync_fantrax_league(db, config=custom_config)

        mock_auth.assert_called_once_with(custom_config)


# =============================================================================
# Part B — Integration tests (real API)
# =============================================================================

@pytest.fixture(scope="class")
def fantrax_session():
    """Get real authenticated Fantrax session. Requires valid cookies."""
    try:
        session, league_id = _get_authenticated_session()
        return session, league_id
    except RuntimeError:
        pytest.skip("No Fantrax cookies available")


@pytest.mark.integration
class TestFantraxLeagueIntegration:
    """Integration tests that hit the real Fantrax API.

    Run with:  pytest --integration -s
    """

    def test_fetch_teams_returns_teams(
        self, fantrax_session: tuple,
    ) -> None:
        """fetch_teams returns a non-empty list of teams with id and name."""
        session, league_id = fantrax_session

        teams = fetch_teams(session, league_id)

        assert len(teams) > 0, "Expected at least one team"
        for team in teams:
            assert "id" in team
            assert "name" in team
            assert isinstance(team["id"], str) and team["id"] != ""
        print(f"\nTeams ({len(teams)}):")
        for t in teams:
            print(f"  {t['id']}: {t['name']}")

    def test_fetch_standings_returns_standings(
        self, fantrax_session: tuple,
    ) -> None:
        """fetch_standings returns standings with expected keys and sequential ranks."""
        session, league_id = fantrax_session

        # Also grab the raw response for header discovery
        raw = _fantrax_api_call(session, league_id, "getStandings")
        header_cells = raw.get("tableList", [{}])[0].get("header", {}).get("cells", [])
        header_keys = [c.get("key", "???") for c in header_cells]
        print(f"\n*** Standings header field keys: {header_keys}")

        standings = fetch_standings(session, league_id)

        assert len(standings) > 0, "Expected at least one standings entry"
        for s in standings:
            assert "team_id" in s
            assert "rank" in s
            assert "points_for" in s

        ranks = [s["rank"] for s in standings]
        assert ranks == list(range(1, len(ranks) + 1)), "Ranks should be sequential from 1"

        has_points = any(s["points_for"] > 0 for s in standings)
        assert has_points, "At least one team should have points_for > 0"

    def test_fetch_standings_claims_remaining(
        self, fantrax_session: tuple,
    ) -> None:
        """Check if claims_remaining is populated in standings."""
        session, league_id = fantrax_session

        standings = fetch_standings(session, league_id)
        cr_values = [s["claims_remaining"] for s in standings]

        if all(v is None for v in cr_values):
            print("\nWARNING: CR field not found — check header keys log above")
        else:
            found = [v for v in cr_values if v is not None]
            print(f"\nclaims_remaining values: {found}")
            for v in found:
                assert isinstance(v, int) and v >= 0

    def test_fetch_roster_returns_slots(
        self, fantrax_session: tuple,
    ) -> None:
        """fetch_roster returns a non-empty list of slots with player data."""
        session, league_id = fantrax_session

        teams = fetch_teams(session, league_id)
        assert len(teams) > 0, "Need at least one team for roster test"
        team_id = teams[0]["id"]

        slots = fetch_roster(session, league_id, team_id)

        assert len(slots) > 0, f"Expected roster slots for team {team_id}"
        for slot in slots:
            assert "player_id" in slot
            assert "player_name" in slot
        has_name = any(slot["player_name"] for slot in slots)
        assert has_name, "At least one slot should have a player_name"

        print(f"\nFirst 3 roster slots for {team_id}:")
        for s in slots[:3]:
            print(f"  {s['player_id']}: {s['player_name']} ({s.get('position_id', '?')})")

    def test_full_sync_to_temp_db(
        self, fantrax_session: tuple, tmp_path: Path,
    ) -> None:
        """Full sync writes data to a temp DB."""
        db_path = tmp_path / "integration.db"
        init_db(db_path)
        conn = get_db(db_path)

        try:
            result = sync_fantrax_league(conn)

            assert result["teams_synced"] > 0
            assert result["standings_synced"] > 0
            assert result["roster_slots_synced"] > 0

            teams_count = conn.execute("SELECT COUNT(*) FROM fantasy_teams").fetchone()[0]
            standings_count = conn.execute("SELECT COUNT(*) FROM fantasy_standings").fetchone()[0]
            roster_count = conn.execute("SELECT COUNT(*) FROM fantasy_roster_slots").fetchone()[0]

            assert teams_count > 0
            assert standings_count > 0
            assert roster_count > 0

            print(f"\nSync summary: {result}")
            print(f"DB counts: teams={teams_count}, standings={standings_count}, roster={roster_count}")
        finally:
            conn.close()

    def test_standings_header_field_discovery(
        self, fantrax_session: tuple,
    ) -> None:
        """Diagnostic: log all header field keys from raw getStandings response."""
        session, league_id = fantrax_session

        data = _fantrax_api_call(session, league_id, "getStandings")

        table_list = data.get("tableList", [])
        assert len(table_list) > 0, "Expected at least one standings table"

        header_cells = table_list[0].get("header", {}).get("cells", [])
        field_keys = [c.get("key", "???") for c in header_cells]

        print(f"\n*** ALL standings header field keys ({len(field_keys)}):")
        for i, key in enumerate(field_keys):
            print(f"  [{i}] {key}")

        assert len(field_keys) > 0, "Expected at least one header field"


# =============================================================================
# fetch_gp_per_position Tests
# =============================================================================


class TestFetchGpPerPosition:
    """Tests for fetch_gp_per_position."""

    def test_parses_gp_data(self) -> None:
        """Extracts position, gp_used, gp_limit, gp_remaining, pace."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "responses": [{
                "data": {
                    "gamePlayedPerPosData": {
                        "tableData": [
                            {"posShort": "F", "gp": 714, "max": 984,
                             "remaining": "270", "pace": "978(-6)"},
                            {"posShort": "D", "gp": 369, "max": 492,
                             "remaining": "123", "pace": "505(+13)"},
                            {"posShort": "G", "gp": 65, "max": 82,
                             "remaining": "17", "pace": "89(+7)"},
                        ],
                    },
                },
            }],
        }
        session.post.return_value = mock_resp

        result = fetch_gp_per_position(session, "lg123", "team1")

        assert len(result) == 3
        f_row = next(r for r in result if r["position"] == "F")
        assert f_row["gp_used"] == 714
        assert f_row["gp_limit"] == 984
        assert f_row["gp_remaining"] == 270
        assert f_row["pace"] == "978(-6)"

        d_row = next(r for r in result if r["position"] == "D")
        assert d_row["gp_used"] == 369
        assert d_row["gp_remaining"] == 123

    def test_empty_table_data(self) -> None:
        """Returns empty list when tableData is missing."""
        session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "responses": [{"data": {"gamePlayedPerPosData": {}}}],
        }
        session.post.return_value = mock_resp

        result = fetch_gp_per_position(session, "lg123", "team1")
        assert result == []


# =============================================================================
# save_gp_per_position Tests
# =============================================================================


class TestSaveGpPerPosition:
    """Tests for save_gp_per_position."""

    def test_saves_and_retrieves(self, db: sqlite3.Connection) -> None:
        """GP data is saved and can be queried back."""
        rows = [
            {"position": "F", "gp_used": 714, "gp_limit": 984,
             "gp_remaining": 270, "pace": "978(-6)"},
            {"position": "D", "gp_used": 369, "gp_limit": 492,
             "gp_remaining": 123, "pace": "505(+13)"},
            {"position": "G", "gp_used": 65, "gp_limit": 82,
             "gp_remaining": 17, "pace": "89(+7)"},
        ]
        count = save_gp_per_position(db, "team1", rows)
        assert count == 3

        saved = db.execute(
            "SELECT * FROM fantasy_gp_per_position WHERE team_id = ?",
            ("team1",),
        ).fetchall()
        assert len(saved) == 3

        d_row = next(r for r in saved if r["position"] == "D")
        assert d_row["gp_used"] == 369
        assert d_row["gp_remaining"] == 123

    def test_upsert_updates_existing(self, db: sqlite3.Connection) -> None:
        """Saving again for the same team updates existing rows."""
        rows_v1 = [
            {"position": "D", "gp_used": 369, "gp_limit": 492,
             "gp_remaining": 123, "pace": "505(+13)"},
        ]
        save_gp_per_position(db, "team1", rows_v1)

        rows_v2 = [
            {"position": "D", "gp_used": 375, "gp_limit": 492,
             "gp_remaining": 117, "pace": "510(+18)"},
        ]
        save_gp_per_position(db, "team1", rows_v2)

        saved = db.execute(
            "SELECT gp_used, gp_remaining FROM fantasy_gp_per_position "
            "WHERE team_id = ? AND position = ?",
            ("team1", "D"),
        ).fetchone()
        assert saved["gp_used"] == 375
        assert saved["gp_remaining"] == 117


class TestFetchPlayerSalaries:
    """Tests for fetch_player_salaries."""

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_parses_single_page(self, mock_api: MagicMock) -> None:
        """Parses player salary data from a single-page response."""
        mock_api.return_value = {
            "paginatedResultSet": {
                "totalNumPages": 1,
                "pageNumber": 1,
                "maxResultsPerPage": 500,
            },
            "statsTable": [
                {
                    "scorer": {
                        "scorerId": "abc1",
                        "name": "Connor McDavid",
                        "teamShortName": "EDM",
                        "posShortNames": "F",
                    },
                    "cells": [
                        {"content": "1"},
                        {"content": "FA"},
                        {"content": "30"},
                        {"content": ""},
                        {"content": "12,500,000"},
                        {"content": "110"},
                        {"content": "1.78"},
                        {"content": "100%"},
                        {"content": "0%"},
                    ],
                },
            ],
        }
        session = MagicMock()
        result = fetch_player_salaries(session, "lg123")
        assert len(result) == 1
        assert result[0]["fantrax_id"] == "abc1"
        assert result[0]["player_name"] == "Connor McDavid"
        assert result[0]["salary"] == 12_500_000
        assert result[0]["position"] == "F"

    @patch("fetchers.fantrax_league._fantrax_api_call")
    def test_paginates_multiple_pages(self, mock_api: MagicMock) -> None:
        """Fetches multiple pages when totalNumPages > 1."""
        def api_side_effect(_session, _league, _method, extra_data=None):
            page = extra_data.get("pageNumber", 1) if extra_data else 1
            return {
                "paginatedResultSet": {
                    "totalNumPages": 2,
                    "pageNumber": page,
                    "maxResultsPerPage": 500,
                },
                "statsTable": [
                    {
                        "scorer": {
                            "scorerId": f"p{page}",
                            "name": f"Player {page}",
                            "teamShortName": "TST",
                            "posShortNames": "F",
                        },
                        "cells": [
                            {"content": "1"},
                            {"content": "FA"},
                            {"content": "10"},
                            {"content": ""},
                            {"content": "1,000,000"},
                            {"content": "10"},
                            {"content": "1.0"},
                            {"content": "50%"},
                            {"content": "0%"},
                        ],
                    },
                ],
            }

        mock_api.side_effect = api_side_effect
        session = MagicMock()
        result = fetch_player_salaries(session, "lg123")
        assert len(result) == 2
        assert mock_api.call_count == 2


class TestSavePlayerSalaries:
    """Tests for save_player_salaries."""

    def test_saves_and_retrieves(self, db: sqlite3.Connection) -> None:
        """Player salary data is saved and can be queried back."""
        players = [
            {"fantrax_id": "abc1", "player_name": "Connor McDavid",
             "team_abbrev": "EDM", "position": "F", "salary": 12_500_000},
            {"fantrax_id": "abc2", "player_name": "Ilya Sorokin",
             "team_abbrev": "NYI", "position": "G", "salary": 8_250_000},
        ]
        count = save_player_salaries(db, players)
        assert count == 2

        saved = db.execute(
            "SELECT * FROM fantrax_players ORDER BY player_name"
        ).fetchall()
        assert len(saved) == 2
        assert saved[0]["player_name"] == "Connor McDavid"
        assert saved[0]["salary"] == 12_500_000

    def test_full_replace_deletes_old_data(self, db: sqlite3.Connection) -> None:
        """Saving again replaces all previous data."""
        players_v1 = [
            {"fantrax_id": "abc1", "player_name": "Old Player",
             "team_abbrev": "TST", "position": "F", "salary": 1_000_000},
        ]
        save_player_salaries(db, players_v1)

        players_v2 = [
            {"fantrax_id": "abc2", "player_name": "New Player",
             "team_abbrev": "TST", "position": "D", "salary": 2_000_000},
        ]
        save_player_salaries(db, players_v2)

        saved = db.execute("SELECT * FROM fantrax_players").fetchall()
        assert len(saved) == 1
        assert saved[0]["player_name"] == "New Player"
