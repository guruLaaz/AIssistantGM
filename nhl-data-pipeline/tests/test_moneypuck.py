"""Tests for the MoneyPuck injury data fetcher."""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from fetchers.moneypuck import fetch_injuries, save_injuries, _STATUS_MAP

SAMPLE_CSV = (
    "playerId,playerName,teamCode,position,dateOfReturn,daysUntilReturn,"
    "gamesStillToMiss,gamesMissedSoFar,lastGameDate,yahooInjuryDescription,"
    "playerInjuryStatus\n"
    "8475151,Kyle Palmieri,NYI,RW,2026-09-15,190,19,101,2025-11-28,Knee,IR\n"
    "8482159,Tyson Foerster,PHI,D,2099-12-31,-999,-999,37,2025-12-01,Arm,IR\n"
    "8478873,Troy Terry,ANA,RW,2026-03-15,6,3,6,2026-02-25,Upper Body,O\n"
    "8482896,Tyson Kozak,BUF,C,2026-03-10,1,0,3,2026-03-03,Undisclosed,DTD\n"
    "8479982,Conor Timmins,BUF,D,2099-12-31,-999,-999,31,2025-12-18,Leg,IR-NR\n"
    "8483468,Jiri Kulich,BUF,C,2026-09-15,190,18,52,2025-11-01,Ear,IR-LT\n"
)


class TestFetchInjuries:
    """Tests for fetch_injuries (HTTP + CSV parsing)."""

    @patch("fetchers.moneypuck.requests.Session")
    def test_parses_csv(self, mock_session_cls: MagicMock) -> None:
        """Fetched CSV is parsed into list of dicts with correct keys."""
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp

        rows = fetch_injuries(session=mock_session)
        assert len(rows) == 6
        assert rows[0]["playerName"] == "Kyle Palmieri"
        assert rows[0]["dateOfReturn"] == "2026-09-15"
        assert rows[0]["playerInjuryStatus"] == "IR"

    @patch("fetchers.moneypuck.requests.Session")
    def test_all_expected_keys(self, mock_session_cls: MagicMock) -> None:
        """Each row dict has all expected CSV columns."""
        mock_resp = MagicMock()
        mock_resp.text = SAMPLE_CSV
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp

        rows = fetch_injuries(session=mock_session)
        expected_keys = {
            "playerId", "playerName", "teamCode", "position",
            "dateOfReturn", "daysUntilReturn", "gamesStillToMiss",
            "gamesMissedSoFar", "lastGameDate", "yahooInjuryDescription",
            "playerInjuryStatus",
        }
        for row in rows:
            assert set(row.keys()) == expected_keys


class TestSaveInjuries:
    """Tests for save_injuries (DB upsert)."""

    @pytest.fixture
    def db(self) -> sqlite3.Connection:
        """In-memory DB with minimal schema for injury tests."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE players (
                id INTEGER PRIMARY KEY,
                full_name TEXT,
                first_name TEXT,
                last_name TEXT,
                team_abbrev TEXT,
                position TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE player_injuries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER,
                source TEXT NOT NULL,
                injury_type TEXT,
                status TEXT,
                updated_at TEXT,
                expected_return TEXT,
                UNIQUE (player_id, source)
            )
        """)
        # Insert test players
        conn.execute(
            "INSERT INTO players VALUES (8475151, 'Kyle Palmieri', 'Kyle', 'Palmieri', 'NYI', 'RW')"
        )
        conn.execute(
            "INSERT INTO players VALUES (8482159, 'Tyson Foerster', 'Tyson', 'Foerster', 'PHI', 'D')"
        )
        conn.execute(
            "INSERT INTO players VALUES (8478873, 'Troy Terry', 'Troy', 'Terry', 'ANA', 'RW')"
        )
        conn.commit()
        return conn

    def test_upserts_matched_players(self, db: sqlite3.Connection) -> None:
        """Matched players are inserted with correct fields."""
        injuries = [
            {
                "playerName": "Kyle Palmieri", "teamCode": "NYI",
                "dateOfReturn": "2026-09-15", "yahooInjuryDescription": "Knee",
                "playerInjuryStatus": "IR",
            },
        ]
        upserted, unmatched = save_injuries(db, injuries)
        assert upserted == 1
        assert unmatched == 0

        row = db.execute(
            "SELECT * FROM player_injuries WHERE player_id = 8475151"
        ).fetchone()
        assert row["source"] == "moneypuck"
        assert row["status"] == "IR"
        assert row["expected_return"] == "2026-09-15"
        assert row["injury_type"] == "Knee"

    def test_normalizes_status(self, db: sqlite3.Connection) -> None:
        """Status codes IR-NR and IR-LT normalize to IR, DTD to Day-To-Day."""
        injuries = [
            {"playerName": "Kyle Palmieri", "teamCode": "NYI",
             "dateOfReturn": "2026-09-15", "yahooInjuryDescription": "Knee",
             "playerInjuryStatus": "IR-NR"},
            {"playerName": "Troy Terry", "teamCode": "ANA",
             "dateOfReturn": "2026-03-15", "yahooInjuryDescription": "Upper Body",
             "playerInjuryStatus": "DTD"},
        ]
        save_injuries(db, injuries)

        palmieri = db.execute(
            "SELECT status FROM player_injuries WHERE player_id = 8475151"
        ).fetchone()
        assert palmieri["status"] == "IR"

        terry = db.execute(
            "SELECT status FROM player_injuries WHERE player_id = 8478873"
        ).fetchone()
        assert terry["status"] == "Day-To-Day"

    def test_indefinite_date_stored(self, db: sqlite3.Connection) -> None:
        """2099-12-31 is stored as-is so the season-ending filter catches it."""
        injuries = [
            {"playerName": "Tyson Foerster", "teamCode": "PHI",
             "dateOfReturn": "2099-12-31", "yahooInjuryDescription": "Arm",
             "playerInjuryStatus": "IR"},
        ]
        save_injuries(db, injuries)
        row = db.execute(
            "SELECT expected_return FROM player_injuries WHERE player_id = 8482159"
        ).fetchone()
        assert row["expected_return"] == "2099-12-31"

    def test_unmatched_players_counted(self, db: sqlite3.Connection) -> None:
        """Unmatched players are counted and inserted with player_id=NULL."""
        injuries = [
            {"playerName": "Unknown Player", "teamCode": "TST",
             "dateOfReturn": "2026-05-01", "yahooInjuryDescription": "Leg",
             "playerInjuryStatus": "IR"},
        ]
        upserted, unmatched = save_injuries(db, injuries)
        assert upserted == 1
        assert unmatched == 1

        row = db.execute(
            "SELECT * FROM player_injuries WHERE source = 'moneypuck' AND player_id IS NULL"
        ).fetchone()
        assert row is not None

    def test_full_refresh_clears_old(self, db: sqlite3.Connection) -> None:
        """Old moneypuck rows are deleted before re-inserting."""
        # Insert initial data
        db.execute(
            "INSERT INTO player_injuries (player_id, source, status, expected_return) "
            "VALUES (8475151, 'moneypuck', 'IR', '2026-09-15')"
        )
        db.commit()

        # Save with empty list
        save_injuries(db, [])
        rows = db.execute(
            "SELECT * FROM player_injuries WHERE source = 'moneypuck'"
        ).fetchall()
        assert len(rows) == 0

    def test_does_not_touch_rotowire(self, db: sqlite3.Connection) -> None:
        """Full refresh only clears moneypuck rows, not rotowire."""
        db.execute(
            "INSERT INTO player_injuries (player_id, source, status) "
            "VALUES (8475151, 'rotowire', 'IR')"
        )
        db.commit()

        save_injuries(db, [])
        rows = db.execute(
            "SELECT * FROM player_injuries WHERE source = 'rotowire'"
        ).fetchall()
        assert len(rows) == 1
