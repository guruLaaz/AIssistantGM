"""Tests for db/schema.py — database initialization and player upsert."""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player, get_player_with_news, get_unlinked_news


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database connection."""
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


class TestInitDb:
    """Tests for init_db function."""

    def test_init_db_creates_all_tables(self, db: sqlite3.Connection) -> None:
        """All 9 tables exist after init_db."""
        cursor = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row["name"] for row in cursor.fetchall()}
        expected = {
            "players",
            "skater_stats",
            "goalie_stats",
            "team_games",
            "player_news",
            "player_injuries",
            "pipeline_log",
            "fantasy_teams",
            "fantasy_standings",
            "fantasy_roster_slots",
        }
        assert expected <= tables

    def test_init_db_is_idempotent(self, db_path: Path) -> None:
        """Calling init_db twice doesn't error or duplicate tables."""
        init_db(db_path)
        init_db(db_path)  # Should not raise
        conn = get_db(db_path)
        cursor = conn.execute(
            "SELECT COUNT(*) as cnt FROM sqlite_master WHERE type='table' AND name='players'"
        )
        assert cursor.fetchone()["cnt"] == 1


class TestTableColumns:
    """Tests for table column structure."""

    def test_players_table_columns(self, db: sqlite3.Connection) -> None:
        """Players table has expected columns."""
        cursor = db.execute("PRAGMA table_info(players)")
        columns = {row["name"] for row in cursor.fetchall()}
        expected = {
            "id",
            "full_name",
            "first_name",
            "last_name",
            "team_abbrev",
            "team_id",
            "position",
            "rotowire_id",
        }
        assert expected <= columns

    def test_skater_stats_table_columns(self, db: sqlite3.Connection) -> None:
        """Skater stats table has TOI as int and is_season_total flag."""
        cursor = db.execute("PRAGMA table_info(skater_stats)")
        columns = {row["name"]: row["type"] for row in cursor.fetchall()}
        assert "toi" in columns
        assert columns["toi"].upper() == "INTEGER"
        assert "pp_toi" in columns
        assert columns["pp_toi"].upper() == "INTEGER"
        assert "is_season_total" in columns
        assert "game_date" in columns

    def test_goalie_stats_table_columns(self, db: sqlite3.Connection) -> None:
        """Goalie stats table has goalie-specific columns and integer TOI."""
        cursor = db.execute("PRAGMA table_info(goalie_stats)")
        columns = {row["name"]: row["type"] for row in cursor.fetchall()}
        assert "toi" in columns
        assert columns["toi"].upper() == "INTEGER"
        assert "saves" in columns
        assert "goals_against" in columns

    def test_team_games_table_structure(self, db: sqlite3.Connection) -> None:
        """Team games table has team, season, and game_date."""
        cursor = db.execute("PRAGMA table_info(team_games)")
        columns = {row["name"] for row in cursor.fetchall()}
        assert {"team", "season", "game_date"} <= columns


class TestUpsertPlayer:
    """Tests for upsert_player function."""

    def test_upsert_player_inserts_new(self, db: sqlite3.Connection) -> None:
        """Upserting a new player creates row and returns player_id."""
        player_id = upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "first_name": "Connor",
                "last_name": "McDavid",
                "team_abbrev": "EDM",
                "team_id": 22,
                "position": "C",
            },
        )
        assert player_id == 8478402
        cursor = db.execute("SELECT * FROM players WHERE id = ?", (8478402,))
        row = cursor.fetchone()
        assert row["full_name"] == "Connor McDavid"

    def test_upsert_player_updates_existing(self, db: sqlite3.Connection) -> None:
        """Upserting existing player updates fields, returns same id."""
        upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "EDM",
                "position": "C",
            },
        )
        # Trade him to another team
        player_id = upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "TOR",
                "position": "C",
            },
        )
        assert player_id == 8478402
        cursor = db.execute("SELECT team_abbrev FROM players WHERE id = ?", (8478402,))
        assert cursor.fetchone()["team_abbrev"] == "TOR"

    def test_upsert_player_preserves_rotowire_id(
        self, db: sqlite3.Connection
    ) -> None:
        """Existing rotowire_id is preserved if not provided in upsert."""
        upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "EDM",
                "position": "C",
                "rotowire_id": 12345,
            },
        )
        # Update without rotowire_id
        upsert_player(
            db,
            {
                "id": 8478402,
                "full_name": "Connor McDavid",
                "team_abbrev": "EDM",
                "position": "C",
            },
        )
        cursor = db.execute("SELECT rotowire_id FROM players WHERE id = ?", (8478402,))
        assert cursor.fetchone()["rotowire_id"] == 12345

    def test_upsert_player_with_empty_string_names(
        self, db: sqlite3.Connection
    ) -> None:
        """Empty string names are stored as-is, not converted to NULL."""
        player_id = upsert_player(
            db,
            {"id": 1000001, "full_name": "", "first_name": "", "last_name": ""},
        )
        assert player_id == 1000001
        cursor = db.execute("SELECT * FROM players WHERE id = ?", (1000001,))
        row = cursor.fetchone()
        assert row["full_name"] == ""
        assert row["first_name"] == ""
        assert row["last_name"] == ""

    def test_upsert_player_with_very_long_name(
        self, db: sqlite3.Connection
    ) -> None:
        """Names exceeding 200 chars are stored without truncation."""
        long_name = "A" * 250
        player_id = upsert_player(
            db,
            {"id": 1000002, "full_name": long_name, "first_name": long_name, "last_name": long_name},
        )
        assert player_id == 1000002
        cursor = db.execute("SELECT full_name FROM players WHERE id = ?", (1000002,))
        assert cursor.fetchone()["full_name"] == long_name

    def test_upsert_player_with_only_id(self, db: sqlite3.Connection) -> None:
        """Upsert with only 'id' stores row with all optional fields NULL."""
        player_id = upsert_player(db, {"id": 1000003})
        assert player_id == 1000003
        cursor = db.execute("SELECT * FROM players WHERE id = ?", (1000003,))
        row = cursor.fetchone()
        assert row is not None
        assert row["full_name"] is None
        assert row["team_abbrev"] is None
        assert row["position"] is None
        assert row["rotowire_id"] is None


class TestUniqueConstraints:
    """Tests for unique constraints on stats tables."""

    def test_skater_stats_unique_constraint(self, db: sqlite3.Connection) -> None:
        """Duplicate (player_id, game_date) raises IntegrityError."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
            VALUES (8478402, '2024-01-15', '20232024', 0, 1200)
            """
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
                VALUES (8478402, '2024-01-15', '20232024', 0, 1100)
                """
            )

    def test_goalie_stats_unique_constraint(self, db: sqlite3.Connection) -> None:
        """Duplicate (player_id, game_date) raises IntegrityError."""
        upsert_player(db, {"id": 8477424, "full_name": "Juuse Saros"})
        db.execute(
            """
            INSERT INTO goalie_stats (player_id, game_date, season, is_season_total, toi, saves, goals_against)
            VALUES (8477424, '2024-01-15', '20232024', 0, 3600, 30, 2)
            """
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO goalie_stats (player_id, game_date, season, is_season_total, toi, saves, goals_against)
                VALUES (8477424, '2024-01-15', '20232024', 0, 3600, 28, 3)
                """
            )

    def test_team_games_unique_constraint(self, db: sqlite3.Connection) -> None:
        """Duplicate (team, season, game_date) raises IntegrityError."""
        db.execute(
            """
            INSERT INTO team_games (team, season, game_date)
            VALUES ('EDM', '20232024', '2024-01-15')
            """
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO team_games (team, season, game_date)
                VALUES ('EDM', '20232024', '2024-01-15')
                """
            )


class TestForeignKeys:
    """Tests for foreign key behavior with and without PRAGMA enforcement."""

    def test_skater_stats_fk_not_enforced_by_default(
        self, db: sqlite3.Connection
    ) -> None:
        """Stats for non-existent player succeed — FK enforcement off by default."""
        db.execute(
            """
            INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
            VALUES (9999999, '2024-03-01', '20232024', 0, 1000)
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT player_id FROM skater_stats WHERE player_id = 9999999"
        )
        assert cursor.fetchone() is not None

    def test_skater_stats_fk_enforced_when_pragma_enabled(
        self, db_path: Path
    ) -> None:
        """FK enforcement rejects stats for non-existent player when enabled."""
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
                VALUES (9999999, '2024-03-01', '20232024', 0, 1000)
                """
            )
        conn.close()

    def test_news_fk_allows_valid_player_id(self, db_path: Path) -> None:
        """News insert with valid FK succeeds even with enforcement ON."""
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            "INSERT INTO players (id, full_name) VALUES (8478402, 'Connor McDavid')"
        )
        conn.execute(
            """
            INSERT INTO player_news (rotowire_news_id, player_id, headline)
            VALUES ('nhl_fk_test', 8478402, 'Valid FK headline')
            """
        )
        conn.commit()
        cursor = conn.execute(
            "SELECT player_id FROM player_news WHERE rotowire_news_id = 'nhl_fk_test'"
        )
        assert cursor.fetchone()["player_id"] == 8478402
        conn.close()

    def test_injury_allows_null_player_id_with_fk_enabled(
        self, db_path: Path
    ) -> None:
        """Nullable FK allows NULL player_id even with enforcement ON."""
        init_db(db_path)
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (NULL, 'rotowire', 'Upper Body', 'Day-to-Day')
            """
        )
        conn.commit()
        cursor = conn.execute(
            "SELECT player_id FROM player_injuries WHERE source = 'rotowire'"
        )
        assert cursor.fetchone()["player_id"] is None
        conn.close()


class TestSeasonTotals:
    """Tests for season total rows."""

    def test_season_total_row_allows_null_game_date(
        self, db: sqlite3.Connection
    ) -> None:
        """Season total row with is_season_total=1 and NULL game_date is valid."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO skater_stats (player_id, game_date, season, is_season_total, toi)
            VALUES (8478402, NULL, '20232024', 1, 72000)
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT * FROM skater_stats WHERE player_id = 8478402 AND is_season_total = 1"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row["game_date"] is None
        assert row["toi"] == 72000


class TestPlayerNews:
    """Tests for player_news table."""

    def test_player_news_deduplication(self, db: sqlite3.Connection) -> None:
        """Duplicate rotowire_news_id raises IntegrityError."""
        db.execute(
            """
            INSERT INTO player_news (rotowire_news_id, player_id, headline)
            VALUES ('nhl99999', NULL, 'First headline')
            """
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO player_news (rotowire_news_id, player_id, headline)
                VALUES ('nhl99999', NULL, 'Duplicate headline')
                """
            )

    def test_player_news_allows_null_player_id(self, db: sqlite3.Connection) -> None:
        """News with unmatched player (NULL player_id) stores OK."""
        db.execute(
            """
            INSERT INTO player_news (rotowire_news_id, player_id, headline)
            VALUES ('nhl88888', NULL, 'News about unknown player')
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT * FROM player_news WHERE rotowire_news_id = 'nhl88888'"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row["player_id"] is None

    def test_player_news_very_long_rotowire_news_id(
        self, db: sqlite3.Connection
    ) -> None:
        """Very long rotowire_news_id is stored and uniqueness enforced."""
        long_id = "nhl_" + "x" * 500
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline) "
            "VALUES (?, NULL, 'Long ID headline')",
            (long_id,),
        )
        db.commit()
        cursor = db.execute(
            "SELECT rotowire_news_id FROM player_news WHERE rotowire_news_id = ?",
            (long_id,),
        )
        assert cursor.fetchone()["rotowire_news_id"] == long_id
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                "INSERT INTO player_news (rotowire_news_id, player_id, headline) "
                "VALUES (?, NULL, 'Dup')",
                (long_id,),
            )

    def test_player_news_empty_vs_null_headline(
        self, db: sqlite3.Connection
    ) -> None:
        """Empty string headline is distinct from NULL headline."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline) "
            "VALUES ('nhl_empty_hl', NULL, '')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline) "
            "VALUES ('nhl_null_hl', NULL, NULL)"
        )
        db.commit()
        cursor = db.execute(
            "SELECT headline FROM player_news WHERE rotowire_news_id = 'nhl_empty_hl'"
        )
        assert cursor.fetchone()["headline"] == ""
        cursor = db.execute(
            "SELECT headline FROM player_news WHERE rotowire_news_id = 'nhl_null_hl'"
        )
        assert cursor.fetchone()["headline"] is None

    def test_player_news_various_published_at_formats(
        self, db: sqlite3.Connection
    ) -> None:
        """published_at accepts any text: ISO datetime, date-only, epoch string."""
        formats = [
            ("nhl_iso", "2026-02-17T14:30:00Z"),
            ("nhl_date", "2026-02-17"),
            ("nhl_epoch", "1739800200"),
        ]
        for news_id, published_at in formats:
            db.execute(
                "INSERT INTO player_news (rotowire_news_id, player_id, headline, published_at) "
                "VALUES (?, NULL, 'Headline', ?)",
                (news_id, published_at),
            )
        db.commit()
        for news_id, expected in formats:
            cursor = db.execute(
                "SELECT published_at FROM player_news WHERE rotowire_news_id = ?",
                (news_id,),
            )
            assert cursor.fetchone()["published_at"] == expected


class TestPlayerInjuries:
    """Tests for player_injuries table."""

    def test_player_injuries_upsert_per_source(self, db: sqlite3.Connection) -> None:
        """Only one record per (player_id, source) — duplicate raises IntegrityError."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (8478402, 'rotowire', 'Upper Body', 'Day-to-Day')
            """
        )
        db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            db.execute(
                """
                INSERT INTO player_injuries (player_id, source, injury_type, status)
                VALUES (8478402, 'rotowire', 'Lower Body', 'Out')
                """
            )

    def test_same_player_different_sources_both_stored(
        self, db: sqlite3.Connection
    ) -> None:
        """Same player with different sources stores both rows."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (8478402, 'rotowire', 'Upper Body', 'Day-to-Day')
            """
        )
        db.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (8478402, 'nhl_api', 'Upper Body', 'IR')
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT source FROM player_injuries WHERE player_id = 8478402 ORDER BY source"
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert rows[0]["source"] == "nhl_api"
        assert rows[1]["source"] == "rotowire"

    def test_injury_upsert_via_replace_updates_fields(
        self, db: sqlite3.Connection
    ) -> None:
        """INSERT OR REPLACE on (player_id, source) updates the existing row."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (8478402, 'rotowire', 'Upper Body', 'Day-to-Day')
            """
        )
        db.commit()
        db.execute(
            """
            INSERT OR REPLACE INTO player_injuries (player_id, source, injury_type, status, updated_at)
            VALUES (8478402, 'rotowire', 'Lower Body', 'Out', '2026-02-19')
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT * FROM player_injuries WHERE player_id = 8478402 AND source = 'rotowire'"
        )
        row = cursor.fetchone()
        assert row["injury_type"] == "Lower Body"
        assert row["status"] == "Out"
        assert row["updated_at"] == "2026-02-19"
        cursor = db.execute(
            "SELECT COUNT(*) as cnt FROM player_injuries WHERE player_id = 8478402 AND source = 'rotowire'"
        )
        assert cursor.fetchone()["cnt"] == 1

    def test_injury_with_null_status(self, db: sqlite3.Connection) -> None:
        """Injury with NULL status stores successfully."""
        upsert_player(db, {"id": 8478402, "full_name": "Connor McDavid"})
        db.execute(
            """
            INSERT INTO player_injuries (player_id, source, injury_type, status)
            VALUES (8478402, 'rotowire', 'Undisclosed', NULL)
            """
        )
        db.commit()
        cursor = db.execute(
            "SELECT status FROM player_injuries WHERE player_id = 8478402"
        )
        assert cursor.fetchone()["status"] is None


class TestGetDb:
    """Tests for get_db function."""

    def test_get_db_returns_row_factory(self, db: sqlite3.Connection) -> None:
        """Rows from get_db connection are accessible by column name."""
        upsert_player(
            db,
            {"id": 8478402, "full_name": "Connor McDavid", "team_abbrev": "EDM"},
        )
        cursor = db.execute("SELECT * FROM players WHERE id = 8478402")
        row = cursor.fetchone()
        # Access by column name should work
        assert row["full_name"] == "Connor McDavid"
        assert row["team_abbrev"] == "EDM"


class TestGamesBenchedQuery:
    """Tests for games benched calculation."""

    def test_games_benched_query(self, db: sqlite3.Connection) -> None:
        """Team has 10 games, player has 7 GP, query returns 3 benched."""
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

        # Query games benched
        cursor = db.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM team_games WHERE team = 'EDM' AND season = '20232024')
                - (SELECT COUNT(*) FROM skater_stats WHERE player_id = 8478402 AND season = '20232024' AND is_season_total = 0)
            AS games_benched
            """
        )
        row = cursor.fetchone()
        assert row["games_benched"] == 3


class TestGetPlayerWithNews:
    """Tests for get_player_with_news function."""

    def test_returns_player_with_empty_news(self, db: sqlite3.Connection) -> None:
        """Player with no news returns player fields and empty news list."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })

        result = get_player_with_news(db, 8478402)

        assert result is not None
        assert result["full_name"] == "Connor McDavid"
        assert result["news"] == []

    def test_returns_player_with_news(self, db: sqlite3.Connection) -> None:
        """Player with linked news returns correct news list."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl001', 8478402, 'McDavid: Hat trick', 'Three goals.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl002', 8478402, 'McDavid: Injured', 'Upper body.', '2026-02-16')"
        )
        db.commit()

        result = get_player_with_news(db, 8478402)

        assert result is not None
        assert len(result["news"]) == 2
        assert result["news"][0]["headline"] == "McDavid: Hat trick"
        assert result["news"][1]["headline"] == "McDavid: Injured"

    def test_returns_none_for_missing_player(self, db: sqlite3.Connection) -> None:
        """Nonexistent player_id returns None."""
        result = get_player_with_news(db, 9999999)

        assert result is None

    def test_news_ordered_desc(self, db: sqlite3.Connection) -> None:
        """News items ordered by published_at descending."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_old', 8478402, 'Old news', 'Old.', '2026-01-01')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_new', 8478402, 'New news', 'New.', '2026-02-18')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_mid', 8478402, 'Mid news', 'Mid.', '2026-02-01')"
        )
        db.commit()

        result = get_player_with_news(db, 8478402)

        dates = [n["published_at"] for n in result["news"]]
        assert dates == ["2026-02-18", "2026-02-01", "2026-01-01"]

    def test_news_with_null_fields(self, db: sqlite3.Connection) -> None:
        """News items with NULL content and published_at are returned correctly."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_nulls', 8478402, 'Headline only', NULL, NULL)"
        )
        db.commit()

        result = get_player_with_news(db, 8478402)

        assert len(result["news"]) == 1
        assert result["news"][0]["headline"] == "Headline only"
        assert result["news"][0]["content"] is None
        assert result["news"][0]["published_at"] is None

    def test_does_not_return_other_players_news(
        self, db: sqlite3.Connection
    ) -> None:
        """Only news linked to the requested player is returned."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        upsert_player(db, {
            "id": 8471679, "full_name": "Carey Price",
            "team_abbrev": "MTL", "position": "G",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_mc', 8478402, 'McDavid news', 'Content.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_cp', 8471679, 'Price news', 'Content.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_unk', NULL, 'Unlinked news', 'Content.', '2026-02-17')"
        )
        db.commit()

        result = get_player_with_news(db, 8478402)

        assert len(result["news"]) == 1
        assert result["news"][0]["rotowire_news_id"] == "nhl_mc"

    def test_player_id_zero(self, db: sqlite3.Connection) -> None:
        """player_id=0 is a valid ID and returns None if not in DB."""
        result = get_player_with_news(db, 0)

        assert result is None

    def test_player_id_negative(self, db: sqlite3.Connection) -> None:
        """Negative player_id returns None (no such player)."""
        result = get_player_with_news(db, -1)

        assert result is None

    def test_news_fields_are_correct_keys(self, db: sqlite3.Connection) -> None:
        """Each news dict contains exactly the expected keys."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_keys', 8478402, 'Headline', 'Content.', '2026-02-17')"
        )
        db.commit()

        result = get_player_with_news(db, 8478402)
        news_item = result["news"][0]

        expected_keys = {"id", "rotowire_news_id", "headline", "content", "published_at"}
        assert set(news_item.keys()) == expected_keys


class TestGetUnlinkedNews:
    """Tests for get_unlinked_news function."""

    def test_returns_only_unlinked(self, db: sqlite3.Connection) -> None:
        """Only news with player_id=NULL is returned."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_linked', 8478402, 'Linked news', 'Content.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_unlinked', NULL, 'Unlinked news', 'Content.', '2026-02-16')"
        )
        db.commit()

        result = get_unlinked_news(db)

        assert len(result) == 1
        assert result[0]["rotowire_news_id"] == "nhl_unlinked"

    def test_returns_empty_when_all_linked(self, db: sqlite3.Connection) -> None:
        """No unlinked news returns empty list."""
        upsert_player(db, {
            "id": 8478402, "full_name": "Connor McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_linked', 8478402, 'Linked news', 'Content.', '2026-02-17')"
        )
        db.commit()

        result = get_unlinked_news(db)

        assert result == []

    def test_empty_db_returns_empty(self, db: sqlite3.Connection) -> None:
        """No news at all returns empty list without error."""
        result = get_unlinked_news(db)

        assert result == []

    def test_all_news_unlinked(self, db: sqlite3.Connection) -> None:
        """When all news has NULL player_id, all are returned."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u1', NULL, 'News 1', 'Content.', '2026-02-17')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u2', NULL, 'News 2', 'Content.', '2026-02-16')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u3', NULL, 'News 3', 'Content.', '2026-02-15')"
        )
        db.commit()

        result = get_unlinked_news(db)

        assert len(result) == 3

    def test_unlinked_news_ordered_desc(self, db: sqlite3.Connection) -> None:
        """Unlinked news is ordered by published_at descending."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u_old', NULL, 'Old', 'Old.', '2026-01-01')"
        )
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u_new', NULL, 'New', 'New.', '2026-02-18')"
        )
        db.commit()

        result = get_unlinked_news(db)

        assert result[0]["published_at"] == "2026-02-18"
        assert result[1]["published_at"] == "2026-01-01"

    def test_unlinked_news_fields_are_correct_keys(
        self, db: sqlite3.Connection
    ) -> None:
        """Each unlinked news dict contains exactly the expected keys."""
        db.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('nhl_u_keys', NULL, 'Headline', 'Content.', '2026-02-17')"
        )
        db.commit()

        result = get_unlinked_news(db)

        expected_keys = {"id", "rotowire_news_id", "headline", "content", "published_at"}
        assert set(result[0].keys()) == expected_keys


class TestFantasySchemaIntegration:
    """Tests for the new fantasy league tables."""

    def test_fantasy_teams_table_exists(self, db):
        """Verify fantasy_teams table was created."""
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fantasy_teams'"
        ).fetchone()
        assert row is not None

    def test_fantasy_standings_table_exists(self, db):
        """Verify fantasy_standings table was created."""
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fantasy_standings'"
        ).fetchone()
        assert row is not None

    def test_fantasy_roster_slots_table_exists(self, db):
        """Verify fantasy_roster_slots table was created."""
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='fantasy_roster_slots'"
        ).fetchone()
        assert row is not None

    def test_fantasy_standings_unique_constraint(self, db):
        """Verify UNIQUE(league_id, team_id) on fantasy_standings."""
        db.execute(
            "INSERT INTO fantasy_standings (league_id, team_id, rank) VALUES ('lg1', 't1', 1)"
        )
        db.execute(
            "INSERT OR REPLACE INTO fantasy_standings (league_id, team_id, rank) VALUES ('lg1', 't1', 2)"
        )
        db.commit()
        rows = db.execute(
            "SELECT * FROM fantasy_standings WHERE league_id='lg1' AND team_id='t1'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["rank"] == 2

    def test_fantasy_standings_claims_remaining_column(self, db):
        """Verify claims_remaining column exists and stores integers."""
        db.execute(
            "INSERT INTO fantasy_standings (league_id, team_id, rank, claims_remaining) "
            "VALUES ('lg1', 't1', 1, 15)"
        )
        db.commit()
        row = db.execute(
            "SELECT claims_remaining FROM fantasy_standings WHERE team_id='t1'"
        ).fetchone()
        assert row["claims_remaining"] == 15
