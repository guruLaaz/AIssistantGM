"""Tests for fetchers/rotowire.py — Rotowire news and injuries fetcher."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

from db.schema import get_db, init_db, upsert_player
from fetchers.rotowire import (
    backfill_news_player_ids,
    discover_rotowire_ids,
    fetch_injuries,
    match_player_name,
    save_injuries,
    save_news,
    search_rotowire_player,
    sync_rotowire,
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
    """Provide an initialized database with known players."""
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
        "id": 8471679,
        "full_name": "Carey Price",
        "first_name": "Carey",
        "last_name": "Price",
        "team_abbrev": "MTL",
        "position": "G",
    })
    upsert_player(conn, {
        "id": 8480018,
        "full_name": "Nick Suzuki",
        "first_name": "Nick",
        "last_name": "Suzuki",
        "team_abbrev": "MTL",
        "position": "C",
    })
    return conn


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------

MOCK_INJURY_JSON: list[dict[str, Any]] = [
    {
        "ID": "4712",
        "URL": "/hockey/player/connor-mcdavid-4712",
        "firstname": "Connor",
        "lastname": "McDavid",
        "player": "Connor McDavid",
        "team": "EDM",
        "position": "C",
        "injury": "Upper Body",
        "status": "Day-To-Day",
        "rDate": "<i>Subscribers Only</i>",
        "date": "Feb 17 10:00 PM",
    },
    {
        "ID": "5419",
        "URL": "/hockey/player/nick-suzuki-5419",
        "firstname": "Nick",
        "lastname": "Suzuki",
        "player": "Nick Suzuki",
        "team": "MTL",
        "position": "C",
        "injury": "Lower Body",
        "status": "IR",
        "rDate": "<i>Subscribers Only</i>",
        "date": "Feb 16 7:00 PM",
    },
]


def make_mock_response(
    text: str = "",
    json_data: Any = None,
    status_code: int = 200,
    raise_for_status: bool = False,
) -> Mock:
    """Create a mock requests response."""
    response = Mock()
    response.status_code = status_code
    response.text = text
    response.content = text.encode("utf-8") if isinstance(text, str) else text
    response.json.return_value = json_data
    if raise_for_status:
        response.raise_for_status.side_effect = requests.HTTPError(
            f"{status_code} Server Error"
        )
    else:
        response.raise_for_status.return_value = None
    return response


# =============================================================================
# News Storage Tests (4 tests)
# =============================================================================


class TestSaveNews:
    """Tests for save_news storage utility."""

    def test_save_news_inserts_rows(self, db: sqlite3.Connection) -> None:
        """save_news creates rows in player_news with correct values."""
        items = [
            {
                "rotowire_news_id": "nhl582917",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: Scores hat trick",
                "content": "McDavid scored three goals.",
                "published_at": "Tue, 17 Feb 2026 2:06:00 PM PST",
            },
        ]
        count = save_news(db, items)

        assert count == 1
        cursor = db.execute("SELECT * FROM player_news")
        rows = cursor.fetchall()
        assert len(rows) == 1
        assert rows[0]["headline"] == "Connor McDavid: Scores hat trick"
        assert rows[0]["content"] == "McDavid scored three goals."
        assert rows[0]["published_at"] == "Tue, 17 Feb 2026 2:06:00 PM PST"

    def test_news_deduplication(self, db: sqlite3.Connection) -> None:
        """Saving the same items twice inserts only once."""
        items = [
            {
                "rotowire_news_id": "nhl582917",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: Scores hat trick",
                "content": "McDavid scored three goals.",
                "published_at": "Tue, 17 Feb 2026 2:06:00 PM PST",
            },
        ]
        first_count = save_news(db, items)
        second_count = save_news(db, items)

        assert first_count == 1
        assert second_count == 0
        cursor = db.execute("SELECT COUNT(*) as cnt FROM player_news")
        assert cursor.fetchone()["cnt"] == 1

    def test_news_matches_player_id(self, db: sqlite3.Connection) -> None:
        """News for Connor McDavid gets player_id=8478402."""
        items = [
            {
                "rotowire_news_id": "nhl582917",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: Scores hat trick",
                "content": "McDavid scored three goals.",
                "published_at": "Tue, 17 Feb 2026 2:06:00 PM PST",
            },
        ]
        save_news(db, items)

        cursor = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl582917'"
        )
        assert cursor.fetchone()["player_id"] == 8478402

    def test_unmatched_news_null_player_id(self, db: sqlite3.Connection) -> None:
        """News for unknown player stored with player_id=NULL."""
        items = [
            {
                "rotowire_news_id": "nhl999999",
                "player_name": "Unknown Player",
                "headline": "Unknown Player: Some headline",
                "content": "Some content.",
                "published_at": "Tue, 17 Feb 2026 1:00:00 PM PST",
            },
        ]
        save_news(db, items)

        cursor = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl999999'"
        )
        assert cursor.fetchone()["player_id"] is None

    def test_save_news_empty_list(self, db: sqlite3.Connection) -> None:
        """Empty list returns 0 and creates no rows."""
        count = save_news(db, [])

        assert count == 0
        cursor = db.execute("SELECT COUNT(*) as cnt FROM player_news")
        assert cursor.fetchone()["cnt"] == 0

    def test_save_news_long_content(self, db: sqlite3.Connection) -> None:
        """Very long headline and content are stored correctly."""
        long_headline = "Connor McDavid: " + "A" * 1000
        long_content = "B" * 2000
        items = [
            {
                "rotowire_news_id": "nhl_long_001",
                "player_name": "Connor McDavid",
                "headline": long_headline,
                "content": long_content,
                "published_at": "Tue, 17 Feb 2026 2:06:00 PM PST",
            },
        ]
        count = save_news(db, items)

        assert count == 1
        row = db.execute(
            "SELECT headline, content FROM player_news "
            "WHERE rotowire_news_id = 'nhl_long_001'"
        ).fetchone()
        assert row["headline"] == long_headline
        assert row["content"] == long_content
        assert len(row["content"]) == 2000

    def test_save_news_special_characters(self, db: sqlite3.Connection) -> None:
        """Quotes, backslashes, and unicode are stored faithfully."""
        special_content = (
            "He said \"wow!\" and noted it's a backslash: \\ "
            "plus unicode: \u00e9\u00f1\u00fc and snowman: \u2603"
        )
        items = [
            {
                "rotowire_news_id": "nhl_special_001",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: O'Brien's \"big\" play",
                "content": special_content,
                "published_at": "Tue, 17 Feb 2026 2:06:00 PM PST",
            },
        ]
        count = save_news(db, items)

        assert count == 1
        row = db.execute(
            "SELECT headline, content FROM player_news "
            "WHERE rotowire_news_id = 'nhl_special_001'"
        ).fetchone()
        assert row["content"] == special_content
        assert "O'Brien" in row["headline"]

    def test_save_news_null_optional_fields(self, db: sqlite3.Connection) -> None:
        """News with missing headline/content/published_at stores NULLs."""
        items = [
            {
                "rotowire_news_id": "nhl_null_001",
                "player_name": "Unknown Player",
            },
        ]
        count = save_news(db, items)

        assert count == 1
        row = db.execute(
            "SELECT * FROM player_news WHERE rotowire_news_id = 'nhl_null_001'"
        ).fetchone()
        assert row["headline"] is None
        assert row["content"] is None
        assert row["published_at"] is None
        assert row["player_id"] is None

# =============================================================================
# Injury Report JSON Tests (8 tests)
# =============================================================================


class TestFetchInjuries:
    """Tests for injury report fetching."""

    def test_parse_injury_json(self) -> None:
        """fetch_injuries returns the full list of injury dicts."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(
            json_data=MOCK_INJURY_JSON
        )

        injuries = fetch_injuries(session=mock_session)

        assert len(injuries) == 2
        assert injuries[0]["player"] == "Connor McDavid"
        assert injuries[1]["player"] == "Nick Suzuki"

    def test_save_injuries_maps_fields(self, db: sqlite3.Connection) -> None:
        """DB row has correct field mapping from injury dict."""
        upserted, unmatched = save_injuries(db, MOCK_INJURY_JSON)

        cursor = db.execute(
            "SELECT * FROM player_injuries WHERE player_id = 8478402"
        )
        row = cursor.fetchone()
        assert row["injury_type"] == "Upper Body"
        assert row["status"] == "Day-To-Day"
        assert row["updated_at"] == "Feb 17 10:00 PM"
        assert row["source"] == "rotowire"

    def test_injury_stores_rotowire_id(self, db: sqlite3.Connection) -> None:
        """Matched player's rotowire_id updated in players table."""
        save_injuries(db, MOCK_INJURY_JSON)

        cursor = db.execute(
            "SELECT rotowire_id FROM players WHERE id = 8478402"
        )
        assert cursor.fetchone()["rotowire_id"] == 4712

    def test_injury_matches_player_id(self, db: sqlite3.Connection) -> None:
        """Injury for Connor McDavid saved with player_id=8478402."""
        save_injuries(db, MOCK_INJURY_JSON)

        cursor = db.execute(
            "SELECT player_id FROM player_injuries WHERE injury_type = 'Upper Body'"
        )
        assert cursor.fetchone()["player_id"] == 8478402

    def test_injury_upsert_no_duplicates(self, db: sqlite3.Connection) -> None:
        """Running save_injuries twice updates existing row, no duplicates."""
        save_injuries(db, MOCK_INJURY_JSON)

        # Second run with updated status
        updated = [
            {**MOCK_INJURY_JSON[0], "status": "Out", "injury": "Knee"},
            MOCK_INJURY_JSON[1],
        ]
        save_injuries(db, updated)

        # Still only 2 rows
        cursor = db.execute("SELECT COUNT(*) as cnt FROM player_injuries")
        assert cursor.fetchone()["cnt"] == 2

        # Status was updated
        cursor = db.execute(
            "SELECT status, injury_type FROM player_injuries WHERE player_id = 8478402"
        )
        row = cursor.fetchone()
        assert row["status"] == "Out"
        assert row["injury_type"] == "Knee"

    def test_injury_subscribers_rdate_handled(
        self, db: sqlite3.Connection
    ) -> None:
        """Record with subscribers-only rDate HTML processes without error."""
        injuries = [
            {
                "ID": "4712",
                "URL": "/hockey/player/connor-mcdavid-4712",
                "firstname": "Connor",
                "lastname": "McDavid",
                "player": "Connor McDavid",
                "team": "EDM",
                "position": "C",
                "injury": "Upper Body",
                "status": "Day-To-Day",
                "rDate": "<i>Subscribers Only</i>",
                "date": "Feb 17 10:00 PM",
            }
        ]
        upserted, unmatched = save_injuries(db, injuries)
        assert upserted == 1

    def test_injury_network_error(self) -> None:
        """Non-200 raises HTTPError; ConnectionError propagates."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(
            status_code=500, raise_for_status=True
        )
        with pytest.raises(requests.HTTPError):
            fetch_injuries(session=mock_session)

        mock_session.get.side_effect = requests.ConnectionError("Network down")
        with pytest.raises(requests.ConnectionError):
            fetch_injuries(session=mock_session)

    def test_unmatched_injury_stores_null(self, db: sqlite3.Connection) -> None:
        """Injury for unknown player stored with player_id=NULL."""
        injuries = [
            {
                "ID": "9999",
                "URL": "/hockey/player/unknown-guy-9999",
                "firstname": "Unknown",
                "lastname": "Guy",
                "player": "Unknown Guy",
                "team": "TOR",
                "position": "C",
                "injury": "Knee",
                "status": "IR",
                "rDate": "<i>Subscribers Only</i>",
                "date": "Feb 17 5:00 PM",
            }
        ]
        upserted, unmatched = save_injuries(db, injuries)

        assert upserted == 1
        assert unmatched == 1
        cursor = db.execute(
            "SELECT * FROM player_injuries WHERE source = 'rotowire' AND player_id IS NULL"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row["injury_type"] == "Knee"
        assert row["status"] == "IR"

    def test_injury_save_uses_team_hint(
        self, db: sqlite3.Connection
    ) -> None:
        """save_injuries disambiguates duplicate names via team field."""
        # Two players with the same name on different teams
        upsert_player(db, {
            "id": 100, "full_name": "Sebastian Aho",
            "first_name": "Sebastian", "last_name": "Aho",
            "team_abbrev": "CAR", "position": "C",
        })
        upsert_player(db, {
            "id": 200, "full_name": "Sebastian Aho",
            "first_name": "Sebastian", "last_name": "Aho",
            "team_abbrev": "NYI", "position": "D",
        })

        injuries = [
            {
                "ID": "9001",
                "player": "Sebastian Aho",
                "team": "CAR",
                "injury": "Upper Body",
                "status": "Day-To-Day",
                "date": "Feb 18 10:00 PM",
                "rDate": "",
            },
            {
                "ID": "9002",
                "player": "Sebastian Aho",
                "team": "NYI",
                "injury": "Lower Body",
                "status": "IR",
                "date": "Feb 18 11:00 PM",
                "rDate": "",
            },
        ]
        upserted, unmatched = save_injuries(db, injuries)
        assert upserted == 2
        assert unmatched == 0

        row_car = db.execute(
            "SELECT player_id FROM player_injuries WHERE player_id = 100"
        ).fetchone()
        row_nyi = db.execute(
            "SELECT player_id FROM player_injuries WHERE player_id = 200"
        ).fetchone()
        assert row_car is not None
        assert row_nyi is not None

    def test_save_injuries_full_refresh_clears_recovered(
        self, db: sqlite3.Connection
    ) -> None:
        """Recovered players are removed on second save_injuries call.

        If a player was on the injury list in run 1 but not in run 2,
        their injury record should be deleted (full refresh behavior).
        """
        # Run 1: Both McDavid and Suzuki injured
        save_injuries(db, MOCK_INJURY_JSON)
        cnt = db.execute(
            "SELECT COUNT(*) as cnt FROM player_injuries WHERE source = 'rotowire'"
        ).fetchone()["cnt"]
        assert cnt == 2

        # Run 2: Only Suzuki still injured (McDavid recovered)
        second_run = [MOCK_INJURY_JSON[1]]  # just Suzuki
        save_injuries(db, second_run)

        # McDavid's injury should be gone
        mcd = db.execute(
            "SELECT * FROM player_injuries WHERE player_id = 8478402 AND source = 'rotowire'"
        ).fetchone()
        assert mcd is None

        # Suzuki's injury should still be there
        suz = db.execute(
            "SELECT * FROM player_injuries WHERE player_id = 8480018 AND source = 'rotowire'"
        ).fetchone()
        assert suz is not None

        # Total should be 1
        cnt2 = db.execute(
            "SELECT COUNT(*) as cnt FROM player_injuries WHERE source = 'rotowire'"
        ).fetchone()["cnt"]
        assert cnt2 == 1


# =============================================================================
# Player Name Matching Tests (6 tests)
# =============================================================================


class TestMatchPlayerName:
    """Tests for match_player_name function."""

    def test_match_exact(self, db: sqlite3.Connection) -> None:
        """Exact full_name match returns player_id."""
        assert match_player_name(db, "Connor McDavid") == 8478402

    def test_match_case_insensitive(self, db: sqlite3.Connection) -> None:
        """Case-insensitive match returns player_id."""
        assert match_player_name(db, "connor mcdavid") == 8478402

    def test_match_accent_normalized(self, db: sqlite3.Connection) -> None:
        """Accent-normalized match (e->e) returns player_id."""
        # Add a player with accented name
        upsert_player(db, {
            "id": 9999999,
            "full_name": "Jos\u00e9 Test",
            "first_name": "Jos\u00e9",
            "last_name": "Test",
            "team_abbrev": "MTL",
            "position": "C",
        })
        assert match_player_name(db, "Jose Test") == 9999999

    def test_match_first_initial_last_name(
        self, db: sqlite3.Connection
    ) -> None:
        """First initial + last name pattern matches."""
        assert match_player_name(db, "C. McDavid") == 8478402

    def test_match_no_match_returns_none(self, db: sqlite3.Connection) -> None:
        """Unrecognized name returns None."""
        assert match_player_name(db, "Unknown Player") is None

    def test_match_none_empty_returns_none(
        self, db: sqlite3.Connection
    ) -> None:
        """None and empty string both return None."""
        assert match_player_name(db, None) is None
        assert match_player_name(db, "") is None

    # -- team_abbrev disambiguation tests --

    def _insert_two_ahos(self, db: sqlite3.Connection) -> None:
        """Helper: insert two players named Sebastian Aho on different teams."""
        upsert_player(db, {
            "id": 100, "full_name": "Sebastian Aho",
            "first_name": "Sebastian", "last_name": "Aho",
            "team_abbrev": "CAR", "position": "C",
        })
        upsert_player(db, {
            "id": 200, "full_name": "Sebastian Aho",
            "first_name": "Sebastian", "last_name": "Aho",
            "team_abbrev": "NYI", "position": "D",
        })

    def test_match_disambiguates_by_team(
        self, db: sqlite3.Connection
    ) -> None:
        """team_abbrev picks the correct player when names collide."""
        self._insert_two_ahos(db)
        assert match_player_name(db, "Sebastian Aho", team_abbrev="CAR") == 100
        assert match_player_name(db, "Sebastian Aho", team_abbrev="NYI") == 200

    def test_match_duplicate_name_no_team_hint(
        self, db: sqlite3.Connection
    ) -> None:
        """Duplicate name without team_abbrev still returns a result."""
        self._insert_two_ahos(db)
        result = match_player_name(db, "Sebastian Aho")
        assert result in (100, 200)

    def test_match_team_hint_wrong_team(
        self, db: sqlite3.Connection
    ) -> None:
        """Name matches but team doesn't — falls back to name-only match."""
        assert match_player_name(db, "Connor McDavid", team_abbrev="MTL") == 8478402

    def test_match_drops_middle_name(self, db: sqlite3.Connection) -> None:
        """'Elias Nils Pettersson' matches DB's 'Elias Pettersson' by dropping middle name."""
        upsert_player(db, {
            "id": 8480012,
            "full_name": "Elias Pettersson",
            "first_name": "Elias",
            "last_name": "Pettersson",
            "team_abbrev": "VAN",
            "position": "C",
        })
        assert match_player_name(db, "Elias Nils Pettersson", team_abbrev="VAN") == 8480012

    def test_match_nickname_substitution(self, db: sqlite3.Connection) -> None:
        """'Gabriel Perreault' matches DB's 'Gabe Perreault' via nickname map."""
        upsert_player(db, {
            "id": 8484210,
            "full_name": "Gabe Perreault",
            "first_name": "Gabe",
            "last_name": "Perreault",
            "team_abbrev": "NYR",
            "position": "RW",
        })
        assert match_player_name(db, "Gabriel Perreault", team_abbrev="NYR") == 8484210

    def test_match_nickname_reverse(self, db: sqlite3.Connection) -> None:
        """'Jake Middleton' matches DB's 'Jacob Middleton' (reverse direction)."""
        upsert_player(db, {
            "id": 8478136,
            "full_name": "Jacob Middleton",
            "first_name": "Jacob",
            "last_name": "Middleton",
            "team_abbrev": "MIN",
            "position": "D",
        })
        assert match_player_name(db, "Jake Middleton", team_abbrev="MIN") == 8478136

    def test_match_nickname_zachary_zack(self, db: sqlite3.Connection) -> None:
        """'Zack Bolduc' matches DB's 'Zachary Bolduc' via nickname map."""
        upsert_player(db, {
            "id": 8482737,
            "full_name": "Zachary Bolduc",
            "first_name": "Zachary",
            "last_name": "Bolduc",
            "team_abbrev": "MTL",
            "position": "C",
        })
        assert match_player_name(db, "Zack Bolduc", team_abbrev="MTL") == 8482737

    def test_match_nickname_maxwell_max(self, db: sqlite3.Connection) -> None:
        """'Maxwell Crozier' matches 'Max Crozier' via nickname map."""
        upsert_player(db, {
            "id": 8481719,
            "full_name": "Max Crozier",
            "first_name": "Max",
            "last_name": "Crozier",
            "team_abbrev": "TBL",
            "position": "D",
        })
        assert match_player_name(db, "Maxwell Crozier", "TBL") == 8481719

    def test_match_nickname_no_false_positive(self, db: sqlite3.Connection) -> None:
        """Nickname substitution doesn't match unrelated players."""
        assert match_player_name(db, "Gabriel Smith") is None

    def test_match_hyphenated_name(self, db: sqlite3.Connection) -> None:
        """'Oscar Fisker-Molgaard' matches 'Oscar Fisker Molgaard' via hyphen normalization."""
        upsert_player(db, {
            "id": 8484168,
            "full_name": "Oscar Fisker Molgaard",
            "first_name": "Oscar",
            "last_name": "Fisker Molgaard",
            "team_abbrev": "SEA",
            "position": "C",
        })
        assert match_player_name(db, "Oscar Fisker-Molgaard", "SEA") == 8484168

    def test_match_combining_diacritics(
        self, db: sqlite3.Connection
    ) -> None:
        """Precomposed and decomposed unicode both normalize to same ASCII."""
        upsert_player(db, {
            "id": 7777777,
            "full_name": "Ren\u00e9 Bor\u00e9",
            "first_name": "Ren\u00e9",
            "last_name": "Bor\u00e9",
            "team_abbrev": "MTL",
            "position": "C",
        })
        # Decomposed form: "e" + combining acute accent U+0301
        decomposed_name = "Rene\u0301 Bore\u0301"
        assert match_player_name(db, decomposed_name) == 7777777


# =============================================================================
# Rotowire Player Search Tests (5 tests)
# =============================================================================


class TestSearchRotowirePlayer:
    """Tests for search_rotowire_player function."""

    def test_search_returns_hockey_players(self) -> None:
        """Normal response returns list of dicts with correct keys."""
        json_data = {
            "players": [
                {
                    "rotoPlayerID": "4712",
                    "name": "Connor McDavid",
                    "text": "EDM",
                    "span": "C",
                    "link": "/hockey/player/connor-mcdavid-4712",
                },
            ]
        }
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(json_data=json_data)

        results = search_rotowire_player("Connor McDavid", session=mock_session)

        assert len(results) == 1
        assert results[0]["rotowire_id"] == 4712
        assert results[0]["name"] == "Connor McDavid"
        assert results[0]["team"] == "EDM"
        assert results[0]["position"] == "C"
        assert results[0]["link"] == "/hockey/player/connor-mcdavid-4712"
        assert mock_session.get.call_args.kwargs["params"] == {
            "searchTerm": "Connor McDavid"
        }

    def test_search_player_not_found(self) -> None:
        """Empty players list returns empty result."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(
            json_data={"players": []}
        )

        results = search_rotowire_player("Nonexistent Player", session=mock_session)

        assert results == []

    def test_search_network_error(self) -> None:
        """ConnectionError and HTTPError propagate."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.ConnectionError("Network down")
        with pytest.raises(requests.ConnectionError):
            search_rotowire_player("Test", session=mock_session)

        mock_session.get.side_effect = None
        mock_session.get.return_value = make_mock_response(
            status_code=500, raise_for_status=True
        )
        with pytest.raises(requests.HTTPError):
            search_rotowire_player("Test", session=mock_session)

    def test_search_malformed_json(self) -> None:
        """Response missing 'players' key returns empty list."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(
            json_data={"error": "bad request"}
        )

        results = search_rotowire_player("Test", session=mock_session)

        assert results == []

    def test_search_filters_non_hockey(self) -> None:
        """Only players with /hockey/ links are returned."""
        json_data = {
            "players": [
                {
                    "rotoPlayerID": "4712",
                    "name": "Connor McDavid",
                    "text": "EDM",
                    "span": "C",
                    "link": "/hockey/player/connor-mcdavid-4712",
                },
                {
                    "rotoPlayerID": "9999",
                    "name": "Some Baseball Player",
                    "text": "NYY",
                    "span": "OF",
                    "link": "/baseball/player/some-player-9999",
                },
            ]
        }
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(json_data=json_data)

        results = search_rotowire_player("McDavid", session=mock_session)

        assert len(results) == 1
        assert results[0]["name"] == "Connor McDavid"


# =============================================================================
# Discover Rotowire IDs Tests (5 tests)
# =============================================================================


class TestDiscoverRotowireIds:
    """Tests for discover_rotowire_ids function."""

    @patch("fetchers.rotowire.time.sleep")
    @patch("fetchers.rotowire.search_rotowire_player")
    def test_discover_finds_missing_ids(
        self,
        mock_search: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Players with NULL rotowire_id get discovered and updated."""
        mock_search.return_value = [
            {"rotowire_id": 4712, "name": "Any", "team": "EDM",
             "position": "C", "link": "/hockey/player/any-4712"},
        ]

        count = discover_rotowire_ids(db)

        assert count == 3
        assert mock_search.call_count == 3
        assert mock_sleep.call_count == 2
        row = db.execute(
            "SELECT rotowire_id FROM players WHERE id = 8478402"
        ).fetchone()
        assert row["rotowire_id"] == 4712

    @patch("fetchers.rotowire.time.sleep")
    @patch("fetchers.rotowire.search_rotowire_player")
    def test_discover_skips_existing_ids(
        self,
        mock_search: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Players that already have rotowire_id are not searched."""
        db.execute("UPDATE players SET rotowire_id = 2000 WHERE id = 8471679")
        db.execute("UPDATE players SET rotowire_id = 3000 WHERE id = 8480018")
        db.commit()

        mock_search.return_value = [
            {"rotowire_id": 4712, "name": "Connor McDavid", "team": "EDM",
             "position": "C", "link": "/hockey/player/connor-mcdavid-4712"},
        ]

        count = discover_rotowire_ids(db)

        assert count == 1
        assert mock_search.call_count == 1

    @patch("fetchers.rotowire.time.sleep")
    @patch("fetchers.rotowire.search_rotowire_player")
    def test_discover_no_players_missing(
        self,
        mock_search: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """All players have rotowire_id — returns 0, no API calls."""
        db.execute("UPDATE players SET rotowire_id = 1000 WHERE id = 8478402")
        db.execute("UPDATE players SET rotowire_id = 2000 WHERE id = 8471679")
        db.execute("UPDATE players SET rotowire_id = 3000 WHERE id = 8480018")
        db.commit()

        count = discover_rotowire_ids(db)

        assert count == 0
        mock_search.assert_not_called()

    @patch("fetchers.rotowire.time.sleep")
    @patch("fetchers.rotowire.search_rotowire_player")
    def test_discover_search_returns_empty(
        self,
        mock_search: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Search returns no results — players stay without rotowire_id."""
        mock_search.return_value = []

        count = discover_rotowire_ids(db)

        assert count == 0
        assert mock_search.call_count == 3
        row = db.execute(
            "SELECT rotowire_id FROM players WHERE id = 8478402"
        ).fetchone()
        assert row["rotowire_id"] is None

    @patch("fetchers.rotowire.time.sleep")
    @patch("fetchers.rotowire.search_rotowire_player")
    def test_discover_network_error_continues(
        self,
        mock_search: MagicMock,
        mock_sleep: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """ConnectionError on one player doesn't stop others."""
        mock_search.side_effect = [
            requests.ConnectionError("Timeout"),
            [{"rotowire_id": 5000, "name": "Carey Price", "team": "MTL",
              "position": "G", "link": "/hockey/player/carey-price-5000"}],
            [{"rotowire_id": 6000, "name": "Nick Suzuki", "team": "MTL",
              "position": "C", "link": "/hockey/player/nick-suzuki-6000"}],
        ]

        count = discover_rotowire_ids(db)

        assert count == 2
        assert mock_search.call_count == 3


# =============================================================================
# Orchestrator Tests (2 tests)
# =============================================================================


# =============================================================================
# Backfill News Player IDs Tests (4 tests)
# =============================================================================


class TestBackfillNewsPlayerIds:
    """Tests for backfill_news_player_ids function."""

    def test_backfill_matches_after_player_added(
        self, db: sqlite3.Connection
    ) -> None:
        """News inserted with NULL player_id gets matched after backfill."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_1', NULL, 'Connor McDavid: Scores goal', 'He scored.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 1
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_1'"
        ).fetchone()
        assert row["player_id"] == 8478402

    def test_backfill_leaves_matched_news(
        self, db: sqlite3.Connection
    ) -> None:
        """News already linked to a player is not touched by backfill."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_2', 8478402, 'Connor McDavid: Hat trick', 'Three goals.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_2'"
        ).fetchone()
        assert row["player_id"] == 8478402

    def test_backfill_skips_no_colon(self, db: sqlite3.Connection) -> None:
        """Headlines without a colon are skipped (no player name to extract)."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_3', NULL, 'No colon here', 'Some content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_3'"
        ).fetchone()
        assert row["player_id"] is None

    def test_backfill_idempotent(self, db: sqlite3.Connection) -> None:
        """Running backfill twice yields 0 on the second run."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_4', NULL, 'Nick Suzuki: Assists twice', 'Two helpers.', '2026-02-17')"
        )
        db.commit()

        first = backfill_news_player_ids(db)
        second = backfill_news_player_ids(db)

        assert first == 1
        assert second == 0

    def test_backfill_null_headline(self, db: sqlite3.Connection) -> None:
        """NULL headline is treated as empty string, skipped gracefully."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_null', NULL, NULL, 'Content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_null'"
        ).fetchone()
        assert row["player_id"] is None

    def test_backfill_empty_headline(self, db: sqlite3.Connection) -> None:
        """Empty string headline is skipped (no colon)."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_empty', NULL, '', 'Content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0

    def test_backfill_colon_only_headline(self, db: sqlite3.Connection) -> None:
        """Headline that is just a colon extracts empty player name, no match."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_colon', NULL, ': Just a colon', 'Content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0

    def test_backfill_unrecognized_player(self, db: sqlite3.Connection) -> None:
        """News with valid headline format but unknown player stays unlinked."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_unk', NULL, 'Fake Player: Does things', 'Content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 0
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_unk'"
        ).fetchone()
        assert row["player_id"] is None

    def test_backfill_empty_db_no_news(self, db: sqlite3.Connection) -> None:
        """Empty player_news table returns 0 with no errors."""
        matched = backfill_news_player_ids(db)

        assert matched == 0

    def test_backfill_multiple_unmatched(self, db: sqlite3.Connection) -> None:
        """Backfill handles a batch of mixed matchable and unmatchable news."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_m1', NULL, 'Connor McDavid: Goal', 'Scored.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_m2', NULL, 'Carey Price: Save', 'Saved.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_m3', NULL, 'Unknown Guy: Nothing', 'Nothing.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_m4', NULL, 'No colon headline', 'Skipped.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 2
        # McDavid matched
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_m1'"
        ).fetchone()
        assert row["player_id"] == 8478402
        # Price matched
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_m2'"
        ).fetchone()
        assert row["player_id"] == 8471679
        # Unknown stays NULL
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_m3'"
        ).fetchone()
        assert row["player_id"] is None
        # No-colon stays NULL
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_m4'"
        ).fetchone()
        assert row["player_id"] is None

    def test_backfill_accent_name_match(self, db: sqlite3.Connection) -> None:
        """Backfill matches accented player names via accent normalization."""
        upsert_player(db, {
            "id": 9999999,
            "full_name": "Jos\u00e9 Test\u00e9",
            "first_name": "Jos\u00e9",
            "last_name": "Test\u00e9",
            "team_abbrev": "MTL",
            "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_accent', NULL, 'Jose Teste: Scored', 'Goal.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 1
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_accent'"
        ).fetchone()
        assert row["player_id"] == 9999999

    def test_backfill_multiple_colons_in_headline(
        self, db: sqlite3.Connection
    ) -> None:
        """Only text before the first colon is used as the player name."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_bf_multicol', NULL, 'Connor McDavid: Goal: Hat trick details', 'Content.', '2026-02-17')"
        )
        db.commit()

        matched = backfill_news_player_ids(db)

        assert matched == 1
        row = db.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_bf_multicol'"
        ).fetchone()
        assert row["player_id"] == 8478402


# =============================================================================
# Orchestrator Tests (2 tests)
# =============================================================================


class TestSyncRotowire:
    """Tests for sync_rotowire orchestrator."""

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    def test_sync_calls_injuries(
        self,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """sync_rotowire calls injury fetch/save."""
        mock_fetch_injuries.return_value = [{"mock": "injury"}]
        mock_save_injuries.return_value = (5, 2)

        result = sync_rotowire(db)

        mock_fetch_injuries.assert_called_once()
        mock_save_injuries.assert_called_once()

        assert result["injuries_upserted"] == 5
        assert result["injuries_unmatched"] == 2

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    def test_sync_continues_on_failure(
        self,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """If injury fetch fails, sync returns zeros."""
        mock_fetch_injuries.side_effect = requests.ConnectionError("Injuries down")

        result = sync_rotowire(db)

        assert result["injuries_upserted"] == 0
        assert result["injuries_unmatched"] == 0

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    def test_sync_returns_correct_dict_shape(
        self,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Return dict has exactly the expected keys, both int."""
        mock_fetch_injuries.return_value = []
        mock_save_injuries.return_value = (0, 0)

        result = sync_rotowire(db)

        assert set(result.keys()) == {"injuries_upserted", "injuries_unmatched"}
        assert isinstance(result["injuries_upserted"], int)
        assert isinstance(result["injuries_unmatched"], int)

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    def test_sync_passes_session(
        self,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Session argument is forwarded to fetch_injuries."""
        mock_fetch_injuries.return_value = []
        mock_save_injuries.return_value = (0, 0)
        mock_session = MagicMock()

        sync_rotowire(db, session=mock_session)

        mock_fetch_injuries.assert_called_once_with(mock_session)

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    def test_sync_handles_save_injuries_exception(
        self,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """If save_injuries raises, sync catches it and returns zeros."""
        mock_fetch_injuries.return_value = [{"mock": "injury"}]
        mock_save_injuries.side_effect = sqlite3.OperationalError("DB locked")

        result = sync_rotowire(db)

        assert result["injuries_upserted"] == 0
        assert result["injuries_unmatched"] == 0
