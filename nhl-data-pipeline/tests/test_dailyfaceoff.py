"""Tests for fetchers/dailyfaceoff.py — DailyFaceoff line combinations fetcher."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from db.schema import get_db, init_db, upsert_player
from fetchers.dailyfaceoff import (
    TEAM_SLUGS,
    fetch_all_lines,
    fetch_team_lines,
    parse_team_lines,
    save_team_lines,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    init_db(db_path)
    conn = get_db(db_path)
    upsert_player(conn, {
        "id": 8478402,
        "full_name": "Connor McDavid",
        "first_name": "Connor",
        "last_name": "McDavid",
        "team_abbrev": "EDM",
        "position": "C",
    })
    upsert_player(conn, {
        "id": 8477934,
        "full_name": "Leon Draisaitl",
        "first_name": "Leon",
        "last_name": "Draisaitl",
        "team_abbrev": "EDM",
        "position": "C",
    })
    upsert_player(conn, {
        "id": 8479339,
        "full_name": "Zach Hyman",
        "first_name": "Zach",
        "last_name": "Hyman",
        "team_abbrev": "EDM",
        "position": "L",
    })
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Mock helpers — matches current DailyFaceoff __NEXT_DATA__ structure
# ---------------------------------------------------------------------------

def _make_player(
    name: str,
    position: str = "c",
    rating: float | None = 8.5,
    category: str = "ev",
    group_id: str = "f1",
) -> dict:
    """Build a player entry matching the current DailyFaceoff flat structure."""
    return {
        "name": name,
        "positionIdentifier": position,
        "rating": rating,
        "categoryIdentifier": category,
        "groupIdentifier": group_id,
    }


# The current DailyFaceoff structure: combinations is a dict with a flat
# "players" list.  Each player carries its own category/group identifiers.
# The same player appears once per category (EV, PP, PK).
MOCK_COMBINATIONS: dict[str, Any] = {
    "teamId": 1,
    "teamAbbreviation": "EDM",
    "teamName": "Edmonton Oilers",
    "teamSlug": "edmonton-oilers",
    "players": [
        # EV forward line 1
        _make_player("Zach Hyman", "lw", 8.0, "ev", "f1"),
        _make_player("Connor McDavid", "c", 8.5, "ev", "f1"),
        _make_player("Someone Else", "rw", 7.0, "ev", "f1"),
        # EV forward line 2
        _make_player("Leon Draisaitl", "c", 8.0, "ev", "f2"),
        _make_player("Another Guy", "lw", 6.5, "ev", "f2"),
        _make_player("Third Guy", "rw", 6.0, "ev", "f2"),
        # PP unit 1
        _make_player("Connor McDavid", "c", 8.5, "pp", "pp1"),
        _make_player("Leon Draisaitl", "c", 8.0, "pp", "pp1"),
        _make_player("Zach Hyman", "lw", 8.0, "pp", "pp1"),
        _make_player("PP Dman", "d", 7.0, "pp", "pp1"),
        _make_player("PP Dman2", "d", 6.5, "pp", "pp1"),
        # PP unit 2
        _make_player("Another Guy", "lw", 6.5, "pp", "pp2"),
        _make_player("Third Guy", "rw", 6.0, "pp", "pp2"),
        # PK unit 1
        _make_player("Connor McDavid", "c", 8.5, "pk", "pk1"),
        _make_player("Zach Hyman", "lw", 8.0, "pk", "pk1"),
        # EV defense pair 1
        _make_player("Top D1", "d", 7.5, "ev", "d1"),
        _make_player("Top D2", "d", 7.0, "ev", "d1"),
        # Off-ice player — should be skipped
        _make_player("Benched Guy", "c", 5.0, "oi", "oi"),
        # Goalie — should be skipped
        _make_player("Some Goalie", "g", 8.0, "ev", "g"),
    ],
    "lines": [],
}


def _build_html(combinations: dict) -> str:
    """Build a minimal HTML page with __NEXT_DATA__ containing combinations."""
    next_data = {
        "props": {
            "pageProps": {
                "combinations": combinations,
            }
        }
    }
    return (
        '<html><head></head><body>'
        f'<script id="__NEXT_DATA__" type="application/json">'
        f'{json.dumps(next_data)}'
        '</script></body></html>'
    )


def _make_response(html: str, status_code: int = 200) -> Mock:
    resp = Mock()
    resp.status_code = status_code
    resp.text = html
    resp.raise_for_status.return_value = None
    return resp


# =============================================================================
# TEAM_SLUGS Tests
# =============================================================================


class TestTeamSlugs:
    def test_all_32_teams_present(self) -> None:
        assert len(TEAM_SLUGS) == 32

    def test_known_teams(self) -> None:
        assert TEAM_SLUGS["TOR"] == "toronto-maple-leafs"
        assert TEAM_SLUGS["EDM"] == "edmonton-oilers"
        assert TEAM_SLUGS["MTL"] == "montreal-canadiens"


# =============================================================================
# fetch_team_lines Tests
# =============================================================================


class TestFetchTeamLines:
    def test_extracts_combinations(self) -> None:
        html = _build_html(MOCK_COMBINATIONS)
        session = MagicMock()
        session.get.return_value = _make_response(html)

        combos = fetch_team_lines("edmonton-oilers", session=session)

        assert isinstance(combos, dict)
        assert "players" in combos
        assert len(combos["players"]) == len(MOCK_COMBINATIONS["players"])

    def test_raises_on_missing_next_data(self) -> None:
        session = MagicMock()
        session.get.return_value = _make_response("<html><body>No data</body></html>")

        with pytest.raises(ValueError, match="Could not find __NEXT_DATA__"):
            fetch_team_lines("edmonton-oilers", session=session)

    def test_raises_on_http_error(self) -> None:
        session = MagicMock()
        resp = Mock()
        resp.raise_for_status.side_effect = requests.HTTPError("404")
        session.get.return_value = resp

        with pytest.raises(requests.HTTPError):
            fetch_team_lines("fake-team", session=session)


# =============================================================================
# parse_team_lines Tests
# =============================================================================


class TestParseTeamLines:
    def test_groups_by_player(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        names = {p["player_name"] for p in players}
        assert "Connor McDavid" in names
        assert "Leon Draisaitl" in names
        assert "Zach Hyman" in names

    def test_skips_off_ice_and_goalies(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        names = {p["player_name"] for p in players}
        assert "Benched Guy" not in names
        assert "Some Goalie" not in names

    def test_ev_line_assignment(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["ev_line"] == 1
        assert mcdavid["ev_group"] == "f1"

        drai = next(p for p in players if p["player_name"] == "Leon Draisaitl")
        assert drai["ev_line"] == 2
        assert drai["ev_group"] == "f2"

    def test_pp_unit_assignment(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["pp_unit"] == 1
        assert mcdavid["pp_group"] == "pp1"

        another = next(p for p in players if p["player_name"] == "Another Guy")
        assert another["pp_unit"] == 2

    def test_pk_unit_assignment(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["pk_unit"] == 1

    def test_ev_linemates(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert "Zach Hyman" in mcdavid["ev_linemates"]
        assert "Someone Else" in mcdavid["ev_linemates"]
        assert "Connor McDavid" not in mcdavid["ev_linemates"]

    def test_pp_linemates(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert "Leon Draisaitl" in mcdavid["pp_linemates"]
        assert len(mcdavid["pp_linemates"]) == 4

    def test_defense_line_number(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        d1 = next(p for p in players if p["player_name"] == "Top D1")
        assert d1["ev_line"] == 1
        assert d1["ev_group"] == "d1"

    def test_rating_extracted(self) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["rating"] == 8.5

    def test_empty_combinations(self) -> None:
        players = parse_team_lines({"players": []}, "EDM")
        assert players == []

    def test_player_with_no_name_skipped(self) -> None:
        combos = {
            "players": [
                {"name": "", "positionIdentifier": "c", "rating": 5,
                 "categoryIdentifier": "ev", "groupIdentifier": "f1"},
            ],
        }
        players = parse_team_lines(combos, "EDM")
        assert len(players) == 0


# =============================================================================
# save_team_lines Tests
# =============================================================================


class TestSaveTeamLines:
    def test_saves_players_to_db(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        saved, unmatched = save_team_lines(db, "EDM", players)

        assert saved > 0
        rows = db.execute("SELECT * FROM line_combinations WHERE team_abbrev = 'EDM'").fetchall()
        assert len(rows) == saved

    def test_resolves_player_ids(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        save_team_lines(db, "EDM", players)

        row = db.execute(
            "SELECT player_id FROM line_combinations WHERE player_name = 'Connor McDavid'"
        ).fetchone()
        assert row["player_id"] == 8478402

    def test_unmatched_get_null_id(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        saved, unmatched = save_team_lines(db, "EDM", players)

        assert unmatched > 0  # "Someone Else", "Another Guy", etc.
        null_rows = db.execute(
            "SELECT COUNT(*) AS cnt FROM line_combinations "
            "WHERE team_abbrev = 'EDM' AND player_id IS NULL"
        ).fetchone()
        assert null_rows["cnt"] == unmatched

    def test_clears_old_data_on_resave(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        save_team_lines(db, "EDM", players)

        count_before = db.execute(
            "SELECT COUNT(*) AS cnt FROM line_combinations WHERE team_abbrev = 'EDM'"
        ).fetchone()["cnt"]

        # Save again (simulating re-fetch)
        save_team_lines(db, "EDM", players)

        count_after = db.execute(
            "SELECT COUNT(*) AS cnt FROM line_combinations WHERE team_abbrev = 'EDM'"
        ).fetchone()["cnt"]

        assert count_after == count_before  # no duplicates

    def test_linemates_stored_as_json(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        save_team_lines(db, "EDM", players)

        row = db.execute(
            "SELECT ev_linemates, pp_linemates FROM line_combinations "
            "WHERE player_name = 'Connor McDavid'"
        ).fetchone()
        ev_mates = json.loads(row["ev_linemates"])
        pp_mates = json.loads(row["pp_linemates"])
        assert isinstance(ev_mates, list)
        assert isinstance(pp_mates, list)
        assert "Zach Hyman" in ev_mates

    def test_updated_at_set(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        save_team_lines(db, "EDM", players)

        row = db.execute(
            "SELECT updated_at FROM line_combinations LIMIT 1"
        ).fetchone()
        assert row["updated_at"] is not None
        assert "T" in row["updated_at"]  # ISO format


# =============================================================================
# fetch_all_lines Tests
# =============================================================================


class TestFetchAllLines:
    @patch("fetchers.dailyfaceoff.time.sleep")
    @patch("fetchers.dailyfaceoff.fetch_team_lines")
    @patch("fetchers.dailyfaceoff.parse_team_lines")
    @patch("fetchers.dailyfaceoff.save_team_lines")
    def test_iterates_all_teams(
        self,
        mock_save: MagicMock,
        mock_parse: MagicMock,
        mock_fetch: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_fetch.return_value = MOCK_COMBINATIONS
        mock_parse.return_value = [{"player_name": "Test", "position": "C",
                                     "ev_line": 1, "pp_unit": None, "pk_unit": None,
                                     "ev_group": "f1", "pp_group": None, "pk_group": None,
                                     "ev_linemates": [], "pp_linemates": [],
                                     "rating": 8.0}]
        mock_save.return_value = (1, 0)

        result = fetch_all_lines(db)

        assert mock_fetch.call_count == 32
        assert result["players_saved"] == 32
        assert result["teams_failed"] == 0
        assert mock_sleep.call_count == 31  # sleep between teams

    @patch("fetchers.dailyfaceoff.time.sleep")
    @patch("fetchers.dailyfaceoff.fetch_team_lines")
    def test_continues_on_team_failure(
        self,
        mock_fetch: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_fetch.side_effect = requests.ConnectionError("Network error")

        result = fetch_all_lines(db)

        assert result["teams_failed"] == 32
        assert result["players_saved"] == 0

    @patch("fetchers.dailyfaceoff.time.sleep")
    @patch("fetchers.dailyfaceoff.fetch_team_lines")
    @patch("fetchers.dailyfaceoff.parse_team_lines")
    @patch("fetchers.dailyfaceoff.save_team_lines")
    def test_accumulates_unmatched(
        self,
        mock_save: MagicMock,
        mock_parse: MagicMock,
        mock_fetch: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_fetch.return_value = {"players": []}
        mock_parse.return_value = []
        mock_save.return_value = (5, 3)

        result = fetch_all_lines(db)

        assert result["players_saved"] == 5 * 32
        assert result["unmatched"] == 3 * 32


# =============================================================================
# Formatter helper _line_tag Tests
# =============================================================================


class TestLineTag:
    def test_line_tag_both(self) -> None:
        from assistant.formatters import _line_tag
        assert _line_tag({"ev_line": 1, "pp_unit": 1}) == "L1/PP1"

    def test_line_tag_ev_only(self) -> None:
        from assistant.formatters import _line_tag
        assert _line_tag({"ev_line": 2, "pp_unit": None}) == "L2"

    def test_line_tag_pp_only(self) -> None:
        from assistant.formatters import _line_tag
        assert _line_tag({"ev_line": None, "pp_unit": 2}) == "PP2"

    def test_line_tag_none(self) -> None:
        from assistant.formatters import _line_tag
        assert _line_tag(None) == ""

    def test_line_tag_empty(self) -> None:
        from assistant.formatters import _line_tag
        assert _line_tag({"ev_line": None, "pp_unit": None}) == ""


# =============================================================================
# Query helper _get_line_context Tests
# =============================================================================


class TestGetLineContext:
    def test_returns_none_when_no_data(self, db: sqlite3.Connection) -> None:
        from assistant.queries import _get_line_context
        result = _get_line_context(db, 8478402)
        assert result is None

    def test_returns_line_data(self, db: sqlite3.Connection) -> None:
        from assistant.queries import _get_line_context
        players = parse_team_lines(MOCK_COMBINATIONS, "EDM")
        save_team_lines(db, "EDM", players)

        result = _get_line_context(db, 8478402)
        assert result is not None
        assert result["ev_line"] == 1
        assert result["pp_unit"] == 1
        assert result["pk_unit"] == 1
        assert "Zach Hyman" in result["ev_linemates"]
        assert "Leon Draisaitl" in result["pp_linemates"]


# =============================================================================
# Live integration test — catches API structure changes
# =============================================================================


class TestLiveDailyFaceoff:
    """Fetch a real DailyFaceoff page and parse it.

    Catches JSON structure changes that unit tests with mock data cannot.
    Run with: pytest --integration tests/test_dailyfaceoff.py::TestLiveDailyFaceoff -v
    """

    @pytest.mark.integration
    def test_fetch_and_parse_one_team(self) -> None:
        """Fetch EDM lines from the live site and verify parsing works."""
        combos = fetch_team_lines("edmonton-oilers")

        assert isinstance(combos, dict)
        assert "players" in combos
        assert len(combos["players"]) > 0

        players = parse_team_lines(combos, "EDM")

        assert len(players) > 10  # a real team has 20+ players
        names = {p["player_name"] for p in players}
        assert len(names) > 10

        # At least some players should have EV line assignments
        ev_assigned = [p for p in players if p["ev_line"] is not None]
        assert len(ev_assigned) > 5

        # At least some should have PP assignments
        pp_assigned = [p for p in players if p["pp_unit"] is not None]
        assert len(pp_assigned) > 0

        # Linemates should be populated
        first_liner = next(p for p in players if p["ev_line"] == 1 and p["ev_group"] == "f1")
        assert len(first_liner["ev_linemates"]) >= 1
