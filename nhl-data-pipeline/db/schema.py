"""Database schema initialization and player upsert."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TypedDict


class PlayerDict(TypedDict, total=False):
    """Player data for upsert operations."""

    id: int  # Required - NHL API player ID (upsert key)
    full_name: str
    first_name: str
    last_name: str
    team_abbrev: str
    team_id: int
    position: str
    rotowire_id: int | None


def init_db(db_path: Path) -> None:
    """Create all 6 tables if they don't exist.

    Idempotent - safe to call multiple times.

    Args:
        db_path: Path to SQLite database file.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY,
            full_name TEXT,
            first_name TEXT,
            last_name TEXT,
            team_abbrev TEXT,
            team_id INTEGER,
            position TEXT,
            rotowire_id INTEGER
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS skater_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_date TEXT,
            season TEXT NOT NULL,
            is_season_total INTEGER NOT NULL DEFAULT 0,
            toi INTEGER NOT NULL DEFAULT 0,
            goals INTEGER DEFAULT 0,
            assists INTEGER DEFAULT 0,
            points INTEGER DEFAULT 0,
            plus_minus INTEGER DEFAULT 0,
            pim INTEGER DEFAULT 0,
            shots INTEGER DEFAULT 0,
            hits INTEGER DEFAULT 0,
            blocks INTEGER DEFAULT 0,
            powerplay_goals INTEGER DEFAULT 0,
            powerplay_points INTEGER DEFAULT 0,
            shorthanded_goals INTEGER DEFAULT 0,
            shorthanded_points INTEGER DEFAULT 0,
            FOREIGN KEY (player_id) REFERENCES players(id),
            UNIQUE (player_id, game_date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS goalie_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            game_date TEXT,
            season TEXT NOT NULL,
            is_season_total INTEGER NOT NULL DEFAULT 0,
            toi INTEGER NOT NULL DEFAULT 0,
            saves INTEGER DEFAULT 0,
            goals_against INTEGER DEFAULT 0,
            shots_against INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            ot_losses INTEGER DEFAULT 0,
            shutouts INTEGER DEFAULT 0,
            FOREIGN KEY (player_id) REFERENCES players(id),
            UNIQUE (player_id, game_date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS team_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT NOT NULL,
            season TEXT NOT NULL,
            game_date TEXT NOT NULL,
            opponent TEXT,
            home_away TEXT,
            result TEXT,
            UNIQUE (team, season, game_date)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS player_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rotowire_news_id TEXT UNIQUE NOT NULL,
            player_id INTEGER,
            headline TEXT,
            content TEXT,
            published_at TEXT,
            FOREIGN KEY (player_id) REFERENCES players(id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS player_injuries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER,
            source TEXT NOT NULL,
            injury_type TEXT,
            status TEXT,
            updated_at TEXT,
            FOREIGN KEY (player_id) REFERENCES players(id),
            UNIQUE (player_id, source)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_log (
            step TEXT PRIMARY KEY,
            last_run_at TEXT NOT NULL,
            status TEXT NOT NULL
        )
        """
    )

    conn.commit()
    conn.close()


def get_db(db_path: Path) -> sqlite3.Connection:
    """Get a database connection with row_factory set.

    Args:
        db_path: Path to SQLite database file.

    Returns:
        Connection with row_factory=sqlite3.Row for column-name access.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def upsert_player(conn: sqlite3.Connection, player: PlayerDict) -> int:
    """Insert or update a player row.

    If player exists (by id), updates fields. Preserves existing rotowire_id
    if not provided in the update.

    Args:
        conn: Database connection.
        player: Player data dict with 'id' required.

    Returns:
        The player's id (primary key).
    """
    player_id = player["id"]

    # Check if player exists and get current rotowire_id
    cursor = conn.execute("SELECT rotowire_id FROM players WHERE id = ?", (player_id,))
    existing = cursor.fetchone()

    if existing:
        # Update - preserve rotowire_id if not provided
        rotowire_id = player.get("rotowire_id")
        if rotowire_id is None and existing["rotowire_id"] is not None:
            rotowire_id = existing["rotowire_id"]

        conn.execute(
            """
            UPDATE players SET
                full_name = COALESCE(?, full_name),
                first_name = COALESCE(?, first_name),
                last_name = COALESCE(?, last_name),
                team_abbrev = COALESCE(?, team_abbrev),
                team_id = COALESCE(?, team_id),
                position = COALESCE(?, position),
                rotowire_id = COALESCE(?, rotowire_id)
            WHERE id = ?
            """,
            (
                player.get("full_name"),
                player.get("first_name"),
                player.get("last_name"),
                player.get("team_abbrev"),
                player.get("team_id"),
                player.get("position"),
                rotowire_id,
                player_id,
            ),
        )
    else:
        # Insert new player
        conn.execute(
            """
            INSERT INTO players (id, full_name, first_name, last_name, team_abbrev, team_id, position, rotowire_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                player.get("full_name"),
                player.get("first_name"),
                player.get("last_name"),
                player.get("team_abbrev"),
                player.get("team_id"),
                player.get("position"),
                player.get("rotowire_id"),
            ),
        )

    conn.commit()
    return player_id
