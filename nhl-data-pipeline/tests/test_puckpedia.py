"""Tests for fetchers/puckpedia.py — PuckPedia line combinations fetcher."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from db.schema import get_db, init_db, upsert_player
from fetchers.puckpedia import (
    TEAM_ABBREVS,
    fetch_all_lines,
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
# Mock data — matches the dict returned by fetch_team_lines (Playwright)
# ---------------------------------------------------------------------------

MOCK_RAW: dict[str, Any] = {
    "lw": [
        {"id": "1", "name": "Zach Hyman"},
        {"id": "2", "name": "Another Guy"},
    ],
    "centers": [
        {"id": "3", "name": "Connor McDavid"},
        {"id": "4", "name": "Leon Draisaitl"},
    ],
    "rw": [
        {"id": "5", "name": "Someone Else"},
        {"id": "6", "name": "Third Guy"},
    ],
    "ld": [
        {"id": "7", "name": "Top D1"},
    ],
    "rd": [
        {"id": "8", "name": "Top D2"},
    ],
    "goalies": [
        {"id": "9", "name": "Some Goalie"},
    ],
    "pp1": [
        {"id": "3", "name": "Connor McDavid"},
        {"id": "4", "name": "Leon Draisaitl"},
        {"id": "1", "name": "Zach Hyman"},
        {"id": "10", "name": "PP Dman"},
        {"id": "11", "name": "PP Dman2"},
    ],
    "pp2": [
        {"id": "2", "name": "Another Guy"},
        {"id": "6", "name": "Third Guy"},
    ],
    "pk1": [
        {"id": "3", "name": "Connor McDavid"},
        {"id": "1", "name": "Zach Hyman"},
    ],
    "pk2": [
        {"id": "4", "name": "Leon Draisaitl"},
        {"id": "2", "name": "Another Guy"},
    ],
}


# =============================================================================
# TEAM_ABBREVS Tests
# =============================================================================


class TestTeamAbbrevs:
    def test_all_32_teams_present(self) -> None:
        assert len(TEAM_ABBREVS) == 32

    def test_known_teams(self) -> None:
        assert "TOR" in TEAM_ABBREVS
        assert "EDM" in TEAM_ABBREVS
        assert "MTL" in TEAM_ABBREVS


# =============================================================================
# parse_team_lines Tests
# =============================================================================


class TestParseTeamLines:
    def test_groups_by_player(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        names = {p["player_name"] for p in players}
        assert "Connor McDavid" in names
        assert "Leon Draisaitl" in names
        assert "Zach Hyman" in names

    def test_goalies_excluded(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        names = {p["player_name"] for p in players}
        assert "Some Goalie" not in names

    def test_ev_line_assignment(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["ev_line"] == 1
        assert mcdavid["ev_group"] == "f1"

        drai = next(p for p in players if p["player_name"] == "Leon Draisaitl")
        assert drai["ev_line"] == 2
        assert drai["ev_group"] == "f2"

    def test_forward_position_assignment(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        hyman = next(p for p in players if p["player_name"] == "Zach Hyman")
        assert hyman["position"] == "lw"

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["position"] == "c"

        other = next(p for p in players if p["player_name"] == "Someone Else")
        assert other["position"] == "rw"

    def test_pp_unit_assignment(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["pp_unit"] == 1
        assert mcdavid["pp_group"] == "pp1"

        another = next(p for p in players if p["player_name"] == "Another Guy")
        assert another["pp_unit"] == 2

    def test_pk_unit_assignment(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["pk_unit"] == 1
        assert mcdavid["pk_group"] == "pk1"

        drai = next(p for p in players if p["player_name"] == "Leon Draisaitl")
        assert drai["pk_unit"] == 2
        assert drai["pk_group"] == "pk2"

    def test_ev_linemates(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert "Zach Hyman" in mcdavid["ev_linemates"]
        assert "Someone Else" in mcdavid["ev_linemates"]
        assert "Connor McDavid" not in mcdavid["ev_linemates"]

    def test_pp_linemates(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert "Leon Draisaitl" in mcdavid["pp_linemates"]
        assert len(mcdavid["pp_linemates"]) == 4

    def test_defense_line_number(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        d1 = next(p for p in players if p["player_name"] == "Top D1")
        assert d1["ev_line"] == 1
        assert d1["ev_group"] == "d1"
        assert d1["position"] == "d"

    def test_defense_linemates(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        d1 = next(p for p in players if p["player_name"] == "Top D1")
        assert d1["ev_linemates"] == ["Top D2"]

    def test_rating_always_none(self) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["rating"] is None

    def test_empty_data(self) -> None:
        empty_raw = {
            "lw": [], "centers": [], "rw": [],
            "ld": [], "rd": [], "goalies": [],
            "pp1": [], "pp2": [], "pk1": [], "pk2": [],
        }
        players = parse_team_lines(empty_raw, "EDM")
        assert players == []

    def test_pp_only_player_gets_record(self) -> None:
        """A player appearing only in PP (not EV) should still get a record."""
        players = parse_team_lines(MOCK_RAW, "EDM")

        pp_dman = next(p for p in players if p["player_name"] == "PP Dman")
        assert pp_dman["pp_unit"] == 1
        assert pp_dman["ev_line"] is None

    def test_merge_ev_pp_pk(self) -> None:
        """Player appearing in EV + PP + PK should have all fields merged."""
        players = parse_team_lines(MOCK_RAW, "EDM")

        mcdavid = next(p for p in players if p["player_name"] == "Connor McDavid")
        assert mcdavid["ev_line"] == 1
        assert mcdavid["pp_unit"] == 1
        assert mcdavid["pk_unit"] == 1


# =============================================================================
# save_team_lines Tests
# =============================================================================


class TestSaveTeamLines:
    def test_saves_players_to_db(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")
        saved, unmatched = save_team_lines(db, "EDM", players)

        assert saved > 0
        rows = db.execute("SELECT * FROM line_combinations WHERE team_abbrev = 'EDM'").fetchall()
        assert len(rows) == saved

    def test_resolves_player_ids(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")
        save_team_lines(db, "EDM", players)

        row = db.execute(
            "SELECT player_id FROM line_combinations WHERE player_name = 'Connor McDavid'"
        ).fetchone()
        assert row["player_id"] == 8478402

    def test_unmatched_get_null_id(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")
        saved, unmatched = save_team_lines(db, "EDM", players)

        assert unmatched > 0  # "Someone Else", "Another Guy", etc.
        null_rows = db.execute(
            "SELECT COUNT(*) AS cnt FROM line_combinations "
            "WHERE team_abbrev = 'EDM' AND player_id IS NULL"
        ).fetchone()
        assert null_rows["cnt"] == unmatched

    def test_clears_old_data_on_resave(self, db: sqlite3.Connection) -> None:
        players = parse_team_lines(MOCK_RAW, "EDM")
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
        players = parse_team_lines(MOCK_RAW, "EDM")
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
        players = parse_team_lines(MOCK_RAW, "EDM")
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
    @patch("fetchers.puckpedia.time.sleep")
    @patch("fetchers.puckpedia._close_browser")
    @patch("fetchers.puckpedia._launch_browser")
    @patch("fetchers.puckpedia.fetch_team_lines")
    @patch("fetchers.puckpedia.parse_team_lines")
    @patch("fetchers.puckpedia.save_team_lines")
    def test_iterates_all_teams(
        self,
        mock_save: MagicMock,
        mock_parse: MagicMock,
        mock_fetch: MagicMock,
        mock_launch: MagicMock,
        mock_close: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_launch.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_fetch.return_value = MOCK_RAW
        mock_parse.return_value = [{"player_name": "Test", "position": "c",
                                     "ev_line": 1, "pp_unit": None, "pk_unit": None,
                                     "ev_group": "f1", "pp_group": None, "pk_group": None,
                                     "ev_linemates": [], "pp_linemates": [],
                                     "rating": None}]
        mock_save.return_value = (1, 0)

        result = fetch_all_lines(db)

        assert mock_fetch.call_count == 32
        assert result["players_saved"] == 32
        assert result["teams_failed"] == 0
        assert mock_sleep.call_count == 31  # sleep between teams
        mock_close.assert_called_once()

    @patch("fetchers.puckpedia.time.sleep")
    @patch("fetchers.puckpedia._close_browser")
    @patch("fetchers.puckpedia._launch_browser")
    @patch("fetchers.puckpedia.fetch_team_lines")
    def test_continues_on_team_failure(
        self,
        mock_fetch: MagicMock,
        mock_launch: MagicMock,
        mock_close: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_launch.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_fetch.side_effect = Exception("Page load failed")

        result = fetch_all_lines(db)

        assert result["teams_failed"] == 32
        assert result["players_saved"] == 0
        mock_close.assert_called_once()

    @patch("fetchers.puckpedia.time.sleep")
    @patch("fetchers.puckpedia._close_browser")
    @patch("fetchers.puckpedia._launch_browser")
    @patch("fetchers.puckpedia.fetch_team_lines")
    @patch("fetchers.puckpedia.parse_team_lines")
    @patch("fetchers.puckpedia.save_team_lines")
    def test_accumulates_unmatched(
        self,
        mock_save: MagicMock,
        mock_parse: MagicMock,
        mock_fetch: MagicMock,
        mock_launch: MagicMock,
        mock_close: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        mock_launch.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_fetch.return_value = {"lw": [], "centers": [], "rw": [],
                                   "ld": [], "rd": [], "goalies": [],
                                   "pp1": [], "pp2": [], "pk1": [], "pk2": []}
        mock_parse.return_value = []
        mock_save.return_value = (5, 3)

        result = fetch_all_lines(db)

        assert result["players_saved"] == 5 * 32
        assert result["unmatched"] == 3 * 32

    @patch("fetchers.puckpedia.time.sleep")
    @patch("fetchers.puckpedia._close_browser")
    @patch("fetchers.puckpedia._launch_browser")
    @patch("fetchers.puckpedia.fetch_team_lines")
    def test_browser_closed_on_error(
        self,
        mock_fetch: MagicMock,
        mock_launch: MagicMock,
        mock_close: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Browser is always closed even if an unexpected error occurs."""
        mock_launch.return_value = (MagicMock(), MagicMock(), MagicMock())
        mock_fetch.side_effect = KeyboardInterrupt()

        with pytest.raises(KeyboardInterrupt):
            fetch_all_lines(db)

        mock_close.assert_called_once()


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
        players = parse_team_lines(MOCK_RAW, "EDM")
        save_team_lines(db, "EDM", players)

        result = _get_line_context(db, 8478402)
        assert result is not None
        assert result["ev_line"] == 1
        assert result["pp_unit"] == 1
        assert result["pk_unit"] == 1
        assert "Zach Hyman" in result["ev_linemates"]
        assert "Leon Draisaitl" in result["pp_linemates"]


# =============================================================================
# Live integration test — catches page structure changes
# =============================================================================


class TestLivePuckPedia:
    """Fetch a real PuckPedia depth chart and parse it.

    Catches DOM structure changes that unit tests with mock data cannot.
    Run with: pytest --integration tests/test_puckpedia.py::TestLivePuckPedia -v
    """

    @pytest.mark.integration
    def test_fetch_and_parse_one_team(self) -> None:
        """Fetch EDM lines from the live site and verify parsing works."""
        from fetchers.puckpedia import _launch_browser, _close_browser, fetch_team_lines

        pw, browser, page = _launch_browser()
        try:
            raw = fetch_team_lines(page, "EDM")

            assert isinstance(raw, dict)
            assert len(raw["lw"]) > 0
            assert len(raw["centers"]) > 0

            players = parse_team_lines(raw, "EDM")

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
        finally:
            _close_browser(pw, browser)
