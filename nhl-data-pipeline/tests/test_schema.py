"""Tests for db/schema.py — database initialization and player upsert."""

import sqlite3
from pathlib import Path

import pytest
from db.schema import init_db, get_db, upsert_player


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database connection."""
    init_db(db_path)
    return get_db(db_path)


class TestInitDb:
    """Tests for init_db function."""

    def test_init_db_creates_all_tables(self, db: sqlite3.Connection) -> None:
        """All 6 tables exist after init_db."""
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
