"""Edge case tests for db/schema.py.

Covers boundary values for upsert_player, init_db edge cases,
and table constraint behaviors.
"""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player, get_player_with_news


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
# init_db edge cases
# ---------------------------------------------------------------------------


class TestInitDbEdgeCases:
    """Edge cases for init_db."""

    def test_init_db_three_times(self, db_path: Path) -> None:
        """Three consecutive init_db calls don't break anything."""
        init_db(db_path)
        init_db(db_path)
        init_db(db_path)
        conn = get_db(db_path)
        tables = conn.execute(
            "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table'"
        ).fetchone()
        assert tables["cnt"] >= 10

    def test_init_db_then_insert_data(self, db: sqlite3.Connection) -> None:
        """Data inserted after init survives another init."""
        upsert_player(db, {"id": 1, "full_name": "Survivor"})
        db.commit()
        # Re-init same db
        init_db(Path(db.execute("PRAGMA database_list").fetchone()["file"]))
        row = db.execute("SELECT full_name FROM players WHERE id = 1").fetchone()
        assert row["full_name"] == "Survivor"


# ---------------------------------------------------------------------------
# get_db edge cases
# ---------------------------------------------------------------------------


class TestGetDbEdgeCases:
    """Edge cases for get_db."""

    def test_in_memory_db(self) -> None:
        """get_db works with :memory: path."""
        init_db(":memory:")
        conn = get_db(":memory:")
        assert conn is not None

    def test_row_factory_access(self, db: sqlite3.Connection) -> None:
        """Row factory allows both index and key access."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        row = db.execute("SELECT id, full_name FROM players WHERE id = 1").fetchone()
        assert row["id"] == 1
        assert row[0] == 1


# ---------------------------------------------------------------------------
# upsert_player edge cases
# ---------------------------------------------------------------------------


class TestUpsertPlayerEdgeCases:
    """Edge cases for upsert_player."""

    def test_player_id_zero(self, db: sqlite3.Connection) -> None:
        """Player ID 0 is valid."""
        pid = upsert_player(db, {"id": 0, "full_name": "Zero ID"})
        assert pid == 0
        row = db.execute("SELECT full_name FROM players WHERE id = 0").fetchone()
        assert row["full_name"] == "Zero ID"

    def test_player_id_large(self, db: sqlite3.Connection) -> None:
        """Very large player ID works."""
        large_id = 2**31 - 1
        pid = upsert_player(db, {"id": large_id, "full_name": "Big ID"})
        assert pid == large_id

    def test_special_characters_in_name(self, db: sqlite3.Connection) -> None:
        """Names with quotes, accents, hyphens stored correctly."""
        special_name = "André-Pierre O'Malley-Côté"
        upsert_player(db, {"id": 999, "full_name": special_name})
        row = db.execute("SELECT full_name FROM players WHERE id = 999").fetchone()
        assert row["full_name"] == special_name

    def test_unicode_name(self, db: sqlite3.Connection) -> None:
        """Unicode characters in names stored correctly."""
        unicode_name = "Ивáн Проворóв"
        upsert_player(db, {"id": 998, "full_name": unicode_name})
        row = db.execute("SELECT full_name FROM players WHERE id = 998").fetchone()
        assert row["full_name"] == unicode_name

    def test_upsert_preserves_rotowire_id_when_none(self, db: sqlite3.Connection) -> None:
        """Existing rotowire_id is preserved when new upsert has None."""
        upsert_player(db, {"id": 100, "full_name": "Test", "rotowire_id": 55555})
        db.commit()
        # Upsert without rotowire_id
        upsert_player(db, {"id": 100, "full_name": "Test Updated"})
        db.commit()
        row = db.execute("SELECT rotowire_id, full_name FROM players WHERE id = 100").fetchone()
        assert row["rotowire_id"] == 55555
        assert row["full_name"] == "Test Updated"

    def test_upsert_all_fields_none(self, db: sqlite3.Connection) -> None:
        """Upsert with only id, all other fields None/missing."""
        pid = upsert_player(db, {"id": 101})
        assert pid == 101
        row = db.execute("SELECT * FROM players WHERE id = 101").fetchone()
        assert row["full_name"] is None
        assert row["position"] is None

    def test_multiple_rapid_upserts(self, db: sqlite3.Connection) -> None:
        """Rapid sequential upserts all succeed."""
        for i in range(50):
            upsert_player(db, {"id": 200, "full_name": f"Name_{i}", "team_abbrev": "TOR"})
        db.commit()
        row = db.execute("SELECT full_name FROM players WHERE id = 200").fetchone()
        assert row["full_name"] == "Name_49"
        count = db.execute("SELECT COUNT(*) as cnt FROM players WHERE id = 200").fetchone()
        assert count["cnt"] == 1


# ---------------------------------------------------------------------------
# Constraint edge cases
# ---------------------------------------------------------------------------


class TestConstraintEdgeCases:
    """Tests for schema constraint behaviors."""

    def test_skater_stats_insert_or_replace(self, db: sqlite3.Connection) -> None:
        """INSERT OR REPLACE updates existing row."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        db.execute(
            "INSERT INTO skater_stats (player_id, game_date, season, is_season_total, goals, toi) "
            "VALUES (1, '2025-10-10', '20252026', 0, 2, 1200)"
        )
        db.commit()
        db.execute(
            "INSERT OR REPLACE INTO skater_stats (player_id, game_date, season, is_season_total, goals, toi) "
            "VALUES (1, '2025-10-10', '20252026', 0, 5, 1200)"
        )
        db.commit()
        row = db.execute(
            "SELECT goals FROM skater_stats WHERE player_id = 1 AND game_date = '2025-10-10'"
        ).fetchone()
        assert row["goals"] == 5

    def test_fantasy_roster_slots_no_unique_constraint(self, db: sqlite3.Connection) -> None:
        """fantasy_roster_slots allows same player on same team (no UNIQUE)."""
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('t1', 'Test Player', 'C', 'active', 0)"
        )
        db.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('t1', 'Test Player', 'C', 'active', 0)"
        )
        db.commit()
        count = db.execute(
            "SELECT COUNT(*) as cnt FROM fantasy_roster_slots WHERE team_id='t1'"
        ).fetchone()
        assert count["cnt"] == 2

    def test_pipeline_log_upsert(self, db: sqlite3.Connection) -> None:
        """pipeline_log allows REPLACE on (step) primary key."""
        db.execute(
            "INSERT OR REPLACE INTO pipeline_log (step, last_run_at, status) "
            "VALUES ('rosters', '2026-02-20T10:00:00', 'ok')"
        )
        db.execute(
            "INSERT OR REPLACE INTO pipeline_log (step, last_run_at, status) "
            "VALUES ('rosters', '2026-02-20T12:00:00', 'ok')"
        )
        db.commit()
        rows = db.execute(
            "SELECT * FROM pipeline_log WHERE step='rosters'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["last_run_at"] == "2026-02-20T12:00:00"

    def test_season_not_null_constraint(self, db: sqlite3.Connection) -> None:
        """Season column has NOT NULL constraint in skater_stats."""
        upsert_player(db, {"id": 1, "full_name": "Test"})
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi) "
                "VALUES (1, '2025-10-10', NULL, 0, 1000)"
            )


# ---------------------------------------------------------------------------
# get_player_with_news edge cases
# ---------------------------------------------------------------------------


class TestGetPlayerWithNewsEdgeCases:
    """Additional edge cases for get_player_with_news."""

    def test_player_with_many_news(self, db: sqlite3.Connection) -> None:
        """Player with many news items returns all of them."""
        upsert_player(db, {"id": 300, "full_name": "Prolific"})
        for i in range(25):
            db.execute(
                "INSERT INTO player_news (rotowire_news_id, player_id, headline, published_at) "
                f"VALUES ('news_{i}', 300, 'Headline {i}', '2026-02-{(i % 28 + 1):02d}')"
            )
        db.commit()
        result = get_player_with_news(db, 300)
        assert len(result["news"]) == 25

    def test_player_news_with_empty_strings(self, db: sqlite3.Connection) -> None:
        """News with empty string fields are returned correctly."""
        upsert_player(db, {"id": 301, "full_name": "Empty News"})
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('empty_news', 301, '', '', '')"
        )
        db.commit()
        result = get_player_with_news(db, 301)
        assert len(result["news"]) == 1
        assert result["news"][0]["headline"] == ""
        assert result["news"][0]["content"] == ""
