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
    fetch_injuries,
    fetch_news,
    match_player_name,
    save_injuries,
    save_news,
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

MOCK_RSS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>RotoWire.com Latest NHL News</title>
  <item>
    <guid>nhl582917</guid>
    <title>Connor McDavid: Scores hat trick</title>
    <link>https://www.rotowire.com//hockey/player/connor-mcdavid-4712</link>
    <description>McDavid scored three goals in Tuesday's 5-2 win.</description>
    <pubDate>Tue, 17 Feb 2026 2:06:00 PM PST</pubDate>
  </item>
  <item>
    <guid>nhl582911</guid>
    <title>Nick Suzuki: One of each against Denmark</title>
    <link>https://www.rotowire.com//hockey/player/nick-suzuki-5419</link>
    <description>Suzuki tallied a goal and an assist Tuesday.</description>
    <pubDate>Tue, 17 Feb 2026 1:32:00 PM PST</pubDate>
  </item>
  <item>
    <guid>nhl582908</guid>
    <title>Unknown Player: Redeems struggles with win</title>
    <link>https://www.rotowire.com//hockey/player/unknown-player-9999</link>
    <description>Unknown turned aside 22 of 24 shots.</description>
    <pubDate>Tue, 17 Feb 2026 1:07:00 PM PST</pubDate>
  </item>
</channel>
</rss>
"""

MOCK_EMPTY_RSS_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
<channel>
  <title>RotoWire.com Latest NHL News</title>
</channel>
</rss>
"""

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
# RSS News Fetching Tests (9 tests)
# =============================================================================


class TestFetchNews:
    """Tests for RSS news fetching."""

    def test_parse_rss_feed(self) -> None:
        """fetch_news parses 3 items with correct fields from mock XML."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(text=MOCK_RSS_XML)

        items = fetch_news(session=mock_session)

        assert len(items) == 3
        assert items[0]["rotowire_news_id"] == "nhl582917"
        assert items[0]["player_name"] == "Connor McDavid"
        assert items[0]["headline"] == "Connor McDavid: Scores hat trick"
        assert "three goals" in items[0]["content"]
        assert items[0]["published_at"] == "Tue, 17 Feb 2026 2:06:00 PM PST"

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

    def test_extract_player_name_from_title(self) -> None:
        """Player name extracted correctly from title before the colon."""
        mock_session = MagicMock()
        rss = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
  <item>
    <guid>nhl111111</guid>
    <title>Kris Letang: Practices Tuesday</title>
    <link>https://www.rotowire.com//hockey/player/kris-letang-2682</link>
    <description>Letang practiced.</description>
    <pubDate>Tue, 17 Feb 2026 2:06:00 PM PST</pubDate>
  </item>
</channel></rss>
"""
        mock_session.get.return_value = make_mock_response(text=rss)

        items = fetch_news(session=mock_session)

        assert len(items) == 1
        assert items[0]["player_name"] == "Kris Letang"

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

    def test_empty_rss_feed(self) -> None:
        """Empty feed returns empty list."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(text=MOCK_EMPTY_RSS_XML)

        items = fetch_news(session=mock_session)

        assert items == []

    def test_malformed_xml(self) -> None:
        """Malformed XML returns empty list without raising."""
        mock_session = MagicMock()
        mock_session.get.return_value = make_mock_response(
            text="<rss><not-valid-xml"
        )

        items = fetch_news(session=mock_session)

        assert items == []

    def test_news_network_error(self) -> None:
        """ConnectionError propagates from fetch_news."""
        mock_session = MagicMock()
        mock_session.get.side_effect = requests.ConnectionError("Network down")

        with pytest.raises(requests.ConnectionError):
            fetch_news(session=mock_session)


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


# =============================================================================
# Orchestrator Tests (2 tests)
# =============================================================================


class TestSyncRotowire:
    """Tests for sync_rotowire orchestrator."""

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    @patch("fetchers.rotowire.fetch_news")
    @patch("fetchers.rotowire.save_news")
    def test_sync_calls_all(
        self,
        mock_save_news: MagicMock,
        mock_fetch_news: MagicMock,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """sync_rotowire calls both news and injury fetch/save."""
        mock_fetch_news.return_value = [{"mock": "news"}]
        mock_save_news.return_value = 3
        mock_fetch_injuries.return_value = [{"mock": "injury"}]
        mock_save_injuries.return_value = (5, 2)

        result = sync_rotowire(db)

        mock_fetch_news.assert_called_once()
        mock_save_news.assert_called_once()
        mock_fetch_injuries.assert_called_once()
        mock_save_injuries.assert_called_once()

        assert result["news_added"] == 3
        assert result["injuries_upserted"] == 5
        assert result["injuries_unmatched"] == 2

    @patch("fetchers.rotowire.fetch_injuries")
    @patch("fetchers.rotowire.save_injuries")
    @patch("fetchers.rotowire.fetch_news")
    @patch("fetchers.rotowire.save_news")
    def test_sync_continues_on_failure(
        self,
        mock_save_news: MagicMock,
        mock_fetch_news: MagicMock,
        mock_save_injuries: MagicMock,
        mock_fetch_injuries: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """If news fetch fails, injuries still run."""
        mock_fetch_news.side_effect = requests.ConnectionError("News down")
        mock_fetch_injuries.return_value = [{"mock": "injury"}]
        mock_save_injuries.return_value = (5, 0)

        result = sync_rotowire(db)

        # News failed but injuries ran
        mock_fetch_injuries.assert_called_once()
        mock_save_injuries.assert_called_once()

        assert result["news_added"] == 0
        assert result["injuries_upserted"] == 5
        assert result["injuries_unmatched"] == 0
