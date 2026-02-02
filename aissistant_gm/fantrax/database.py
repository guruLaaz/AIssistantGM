"""SQLite database manager for caching Fantrax data."""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Generator
import json


class DatabaseManager:
    """Manages SQLite database connection and operations for Fantrax cache."""

    SCHEMA_VERSION = 4  # Added transactions, scoring_periods, matchups tables

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize database manager.

        Args:
            db_path: Path to SQLite database file. If None, uses default location.
        """
        self.db_path = db_path or self._get_default_path()
        self._ensure_directory()
        self._initialize_schema()

    def _get_default_path(self) -> Path:
        """Get default database path using platformdirs."""
        try:
            import platformdirs
            data_dir = Path(platformdirs.user_data_dir("fantrax-cli"))
        except ImportError:
            # Fallback if platformdirs not installed
            data_dir = Path.home() / ".fantrax"
        return data_dir / "fantrax_cache.db"

    def _ensure_directory(self) -> None:
        """Ensure the database directory exists."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Get a database connection as a context manager.

        Yields:
            sqlite3.Connection with row_factory set to sqlite3.Row
        """
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _initialize_schema(self) -> None:
        """Create database tables if they don't exist."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Schema version tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                )
            """)

            # Check current schema version
            cursor.execute("SELECT version FROM schema_version LIMIT 1")
            row = cursor.fetchone()
            current_version = row[0] if row else 0

            # If schema version changed, drop all tables and recreate
            if current_version != 0 and current_version < self.SCHEMA_VERSION:
                # Drop all tables except schema_version
                tables_to_drop = [
                    'sync_log', 'free_agents', 'player_trends', 'daily_scores',
                    'roster_slots', 'player_news', 'players', 'standings', 'teams', 'league_metadata',
                    'transactions', 'transaction_players', 'scoring_periods', 'matchups'
                ]
                for table in tables_to_drop:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()

            # League metadata
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS league_metadata (
                    league_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    year TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    last_sync_at TEXT NOT NULL
                )
            """)

            # Teams
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS teams (
                    id TEXT PRIMARY KEY,
                    league_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    short_name TEXT NOT NULL,
                    logo_url TEXT,
                    last_sync_at TEXT NOT NULL,
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id)
                )
            """)

            # Players
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS players (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    short_name TEXT,
                    team_name TEXT,
                    team_short_name TEXT,
                    position_short_names TEXT,
                    day_to_day INTEGER DEFAULT 0,
                    out INTEGER DEFAULT 0,
                    injured_reserve INTEGER DEFAULT 0,
                    suspended INTEGER DEFAULT 0,
                    last_sync_at TEXT NOT NULL
                )
            """)

            # Player news
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS player_news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id TEXT NOT NULL,
                    news_date TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    analysis TEXT,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(player_id, news_date, headline),
                    FOREIGN KEY (player_id) REFERENCES players(id)
                )
            """)

            # Roster slots
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS roster_slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    team_id TEXT NOT NULL,
                    player_id TEXT,
                    position_id TEXT NOT NULL,
                    position_short TEXT NOT NULL,
                    status_id TEXT,
                    salary REAL,
                    total_fantasy_points REAL,
                    fantasy_points_per_game REAL,
                    last_sync_at TEXT NOT NULL,
                    FOREIGN KEY (team_id) REFERENCES teams(id),
                    FOREIGN KEY (player_id) REFERENCES players(id)
                )
            """)

            # Daily scores
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id TEXT NOT NULL,
                    team_id TEXT NOT NULL,
                    scoring_date TEXT NOT NULL,
                    fantasy_points REAL NOT NULL DEFAULT 0,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(player_id, scoring_date),
                    FOREIGN KEY (player_id) REFERENCES players(id),
                    FOREIGN KEY (team_id) REFERENCES teams(id)
                )
            """)

            # Player trends
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS player_trends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id TEXT NOT NULL,
                    period_type TEXT NOT NULL,
                    period_start TEXT,
                    period_end TEXT,
                    total_points REAL NOT NULL DEFAULT 0,
                    games_played INTEGER NOT NULL DEFAULT 0,
                    fpg REAL NOT NULL DEFAULT 0,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(player_id, period_type),
                    FOREIGN KEY (player_id) REFERENCES players(id)
                )
            """)

            # Free agents
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS free_agents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    player_id TEXT NOT NULL,
                    rank INTEGER,
                    salary TEXT,
                    total_fpts REAL,
                    fpg REAL,
                    age TEXT,
                    sort_key TEXT NOT NULL,
                    position_filter TEXT,
                    last_sync_at TEXT NOT NULL,
                    FOREIGN KEY (player_id) REFERENCES players(id)
                )
            """)

            # Standings
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS standings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id TEXT NOT NULL,
                    team_id TEXT NOT NULL,
                    rank INTEGER NOT NULL,
                    wins INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    ties INTEGER DEFAULT 0,
                    points INTEGER DEFAULT 0,
                    win_percentage REAL DEFAULT 0,
                    games_back REAL DEFAULT 0,
                    waiver_order INTEGER,
                    points_for REAL DEFAULT 0,
                    points_against REAL DEFAULT 0,
                    streak TEXT,
                    games_played INTEGER DEFAULT 0,
                    fpg REAL DEFAULT 0,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(league_id, team_id),
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id),
                    FOREIGN KEY (team_id) REFERENCES teams(id)
                )
            """)

            # Sync log
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_type TEXT NOT NULL,
                    league_id TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    api_calls_made INTEGER DEFAULT 0,
                    error_message TEXT,
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id)
                )
            """)

            # Transactions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id TEXT PRIMARY KEY,
                    league_id TEXT NOT NULL,
                    team_id TEXT NOT NULL,
                    transaction_date TEXT NOT NULL,
                    last_sync_at TEXT NOT NULL,
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id),
                    FOREIGN KEY (team_id) REFERENCES teams(id)
                )
            """)

            # Transaction players (one transaction can have multiple players)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_players (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,
                    last_sync_at TEXT NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
                    FOREIGN KEY (player_id) REFERENCES players(id)
                )
            """)

            # Scoring periods
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scoring_periods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id TEXT NOT NULL,
                    period_number INTEGER NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    is_playoffs INTEGER DEFAULT 0,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(league_id, period_number),
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id)
                )
            """)

            # Matchups
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS matchups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_id TEXT NOT NULL,
                    period_number INTEGER NOT NULL,
                    matchup_key INTEGER NOT NULL,
                    away_team_id TEXT,
                    away_team_name TEXT,
                    away_score REAL DEFAULT 0,
                    home_team_id TEXT,
                    home_team_name TEXT,
                    home_score REAL DEFAULT 0,
                    last_sync_at TEXT NOT NULL,
                    UNIQUE(league_id, period_number, matchup_key),
                    FOREIGN KEY (league_id) REFERENCES league_metadata(league_id)
                )
            """)

            # Create indexes for common queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_roster_slots_team_id
                ON roster_slots(team_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_scores_player_date
                ON daily_scores(player_id, scoring_date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_scores_date
                ON daily_scores(scoring_date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_scores_team_date
                ON daily_scores(team_id, scoring_date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_player_trends_player
                ON player_trends(player_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_free_agents_sort
                ON free_agents(sort_key, position_filter)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_standings_league_rank
                ON standings(league_id, rank)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_player_news_player_date
                ON player_news(player_id, news_date DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_transactions_league_date
                ON transactions(league_id, transaction_date DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_transaction_players_tx
                ON transaction_players(transaction_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_matchups_league_period
                ON matchups(league_id, period_number)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_scoring_periods_league
                ON scoring_periods(league_id, period_number)
            """)

            # Set schema version (delete first to avoid multiple rows)
            cursor.execute("DELETE FROM schema_version")
            cursor.execute("""
                INSERT INTO schema_version (version) VALUES (?)
            """, (self.SCHEMA_VERSION,))

    def clear_all(self) -> None:
        """Clear all data from the database (keeps schema)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            tables = [
                'sync_log', 'free_agents', 'player_trends', 'daily_scores',
                'roster_slots', 'player_news', 'players', 'standings', 'teams', 'league_metadata',
                'transaction_players', 'transactions', 'matchups', 'scoring_periods'
            ]
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")

    # ==================== League Metadata ====================

    def save_league_metadata(
        self,
        league_id: str,
        name: str,
        year: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> None:
        """Save or update league metadata."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO league_metadata
                (league_id, name, year, start_date, end_date, last_sync_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                league_id,
                name,
                year,
                start_date.isoformat() if start_date else None,
                end_date.isoformat() if end_date else None,
                datetime.now().isoformat()
            ))

    def get_league_metadata(self, league_id: str) -> Optional[dict]:
        """Get league metadata by ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM league_metadata WHERE league_id = ?
            """, (league_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    # ==================== Teams ====================

    def save_teams(self, league_id: str, teams: list[dict]) -> None:
        """Save or update multiple teams."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            for team in teams:
                cursor.execute("""
                    INSERT OR REPLACE INTO teams
                    (id, league_id, name, short_name, logo_url, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    team['id'],
                    league_id,
                    team['name'],
                    team.get('short_name', team.get('short', '')),
                    team.get('logo_url'),
                    now
                ))

    def get_teams(self, league_id: str) -> list[dict]:
        """Get all teams for a league."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM teams WHERE league_id = ? ORDER BY name
            """, (league_id,))
            return [dict(row) for row in cursor.fetchall()]

    def get_team_by_id(self, team_id: str) -> Optional[dict]:
        """Get a team by ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM teams WHERE id = ?", (team_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_team_by_identifier(self, league_id: str, identifier: str) -> Optional[dict]:
        """Get team by ID, name, or short name (case-insensitive partial match)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            # Try exact ID match first
            cursor.execute("SELECT * FROM teams WHERE id = ?", (identifier,))
            row = cursor.fetchone()
            if row:
                return dict(row)

            # Try case-insensitive partial match on name or short_name
            cursor.execute("""
                SELECT * FROM teams
                WHERE league_id = ? AND (
                    LOWER(name) LIKE LOWER(?) OR
                    LOWER(short_name) LIKE LOWER(?)
                )
            """, (league_id, f"%{identifier}%", f"%{identifier}%"))
            row = cursor.fetchone()
            return dict(row) if row else None

    # ==================== Standings ====================

    def save_standings(self, league_id: str, standings: list[dict]) -> None:
        """Save or update standings for a league."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            # Delete existing standings for this league
            cursor.execute("DELETE FROM standings WHERE league_id = ?", (league_id,))

            # Insert new standings
            for record in standings:
                cursor.execute("""
                    INSERT INTO standings
                    (league_id, team_id, rank, wins, losses, ties, points,
                     win_percentage, games_back, waiver_order, points_for,
                     points_against, streak, games_played, fpg, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    league_id,
                    record['team_id'],
                    record['rank'],
                    record.get('wins', 0),
                    record.get('losses', 0),
                    record.get('ties', 0),
                    record.get('points', 0),
                    record.get('win_percentage', 0),
                    record.get('games_back', 0),
                    record.get('waiver_order'),
                    record.get('points_for', 0),
                    record.get('points_against', 0),
                    record.get('streak'),
                    record.get('games_played', 0),
                    record.get('fpg', 0),
                    now
                ))

    def get_standings(self, league_id: str) -> list[dict]:
        """Get standings for a league, ordered by rank."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.*, t.name as team_name, t.short_name as team_short_name
                FROM standings s
                JOIN teams t ON s.team_id = t.id
                WHERE s.league_id = ?
                ORDER BY s.rank
            """, (league_id,))
            return [dict(row) for row in cursor.fetchall()]

    def get_teams_with_standings(self, league_id: str) -> list[dict]:
        """Get all teams with their standings info, ordered by rank."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT t.*, s.rank, s.wins, s.losses, s.ties, s.points,
                       s.win_percentage, s.games_back, s.waiver_order,
                       s.points_for, s.points_against, s.streak,
                       s.games_played, s.fpg
                FROM teams t
                LEFT JOIN standings s ON t.id = s.team_id AND t.league_id = s.league_id
                WHERE t.league_id = ?
                ORDER BY COALESCE(s.rank, 999), t.name
            """, (league_id,))
            return [dict(row) for row in cursor.fetchall()]

    # ==================== Players ====================

    def save_player(self, player: dict) -> None:
        """Save or update a single player."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO players
                (id, name, short_name, team_name, team_short_name,
                 position_short_names, day_to_day, out, injured_reserve,
                 suspended, last_sync_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                player['id'],
                player['name'],
                player.get('short_name'),
                player.get('team_name'),
                player.get('team_short_name'),
                player.get('position_short_names'),
                player.get('day_to_day', 0),
                player.get('out', 0),
                player.get('injured_reserve', 0),
                player.get('suspended', 0),
                datetime.now().isoformat()
            ))

    def save_players(self, players: list[dict]) -> None:
        """Save or update multiple players."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            for player in players:
                cursor.execute("""
                    INSERT OR REPLACE INTO players
                    (id, name, short_name, team_name, team_short_name,
                     position_short_names, day_to_day, out, injured_reserve,
                     suspended, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player['id'],
                    player['name'],
                    player.get('short_name'),
                    player.get('team_name'),
                    player.get('team_short_name'),
                    player.get('position_short_names'),
                    player.get('day_to_day', 0),
                    player.get('out', 0),
                    player.get('injured_reserve', 0),
                    player.get('suspended', 0),
                    now
                ))

    def get_player(self, player_id: str) -> Optional[dict]:
        """Get a player by ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM players WHERE id = ?", (player_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_players_by_ids(self, player_ids: list[str]) -> dict[str, dict]:
        """Get multiple players by their IDs."""
        if not player_ids:
            return {}
        with self.get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(player_ids))
            cursor.execute(f"""
                SELECT * FROM players WHERE id IN ({placeholders})
            """, player_ids)
            return {row['id']: dict(row) for row in cursor.fetchall()}

    # ==================== Roster Slots ====================

    def save_roster(self, team_id: str, roster_rows: list[dict]) -> None:
        """Save or update roster slots for a team (replaces existing)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            # Delete existing roster slots for this team
            cursor.execute("DELETE FROM roster_slots WHERE team_id = ?", (team_id,))

            # Insert new roster slots
            for row in roster_rows:
                cursor.execute("""
                    INSERT INTO roster_slots
                    (team_id, player_id, position_id, position_short, status_id,
                     salary, total_fantasy_points, fantasy_points_per_game, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    team_id,
                    row.get('player_id'),
                    row['position_id'],
                    row['position_short'],
                    row.get('status_id'),
                    row.get('salary'),
                    row.get('total_fantasy_points'),
                    row.get('fantasy_points_per_game'),
                    now
                ))

    def get_roster(self, team_id: str) -> list[dict]:
        """Get roster slots for a team."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT rs.*, p.name as player_name, p.team_name, p.team_short_name,
                       p.position_short_names, p.day_to_day, p.out,
                       p.injured_reserve, p.suspended
                FROM roster_slots rs
                LEFT JOIN players p ON rs.player_id = p.id
                WHERE rs.team_id = ?
                ORDER BY rs.id
            """, (team_id,))
            return [dict(row) for row in cursor.fetchall()]

    # ==================== Daily Scores ====================

    def save_daily_scores(
        self,
        team_id: str,
        scoring_date: date,
        scores: dict[str, float]
    ) -> None:
        """Save daily scores for players on a team for a specific date."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            date_str = scoring_date.isoformat()

            for player_id, points in scores.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_scores
                    (player_id, team_id, scoring_date, fantasy_points, last_sync_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (player_id, team_id, date_str, points, now))

    def get_daily_scores_for_team(
        self,
        team_id: str,
        start_date: date,
        end_date: date
    ) -> list[dict]:
        """Get daily scores for a team within a date range."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM daily_scores
                WHERE team_id = ? AND scoring_date >= ? AND scoring_date <= ?
                ORDER BY scoring_date, player_id
            """, (team_id, start_date.isoformat(), end_date.isoformat()))
            return [dict(row) for row in cursor.fetchall()]

    def get_daily_scores_for_player(
        self,
        player_id: str,
        start_date: date,
        end_date: date
    ) -> list[dict]:
        """Get daily scores for a player within a date range."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM daily_scores
                WHERE player_id = ? AND scoring_date >= ? AND scoring_date <= ?
                ORDER BY scoring_date
            """, (player_id, start_date.isoformat(), end_date.isoformat()))
            return [dict(row) for row in cursor.fetchall()]

    def get_daily_scores_date_range(self) -> Optional[tuple[str, str]]:
        """Get the min and max dates for which we have daily scores."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT MIN(scoring_date) as min_date, MAX(scoring_date) as max_date
                FROM daily_scores
            """)
            row = cursor.fetchone()
            if row and row['min_date']:
                return (row['min_date'], row['max_date'])
            return None

    # ==================== Player Trends ====================

    def save_player_trends(self, player_id: str, trends: dict) -> None:
        """
        Save player trends.

        Args:
            player_id: The player ID
            trends: Dict with keys like 'week1', 'week2', 'week3', '14', '30'
                    and values like {'total': float, 'games': int, 'fpg': float,
                                     'start': str, 'end': str}
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            for period_type, data in trends.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO player_trends
                    (player_id, period_type, period_start, period_end,
                     total_points, games_played, fpg, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player_id,
                    period_type,
                    data.get('start'),
                    data.get('end'),
                    data.get('total', 0),
                    data.get('games', 0),
                    data.get('fpg', 0),
                    now
                ))

    def get_player_trends(self, player_id: str) -> dict:
        """Get all trends for a player."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM player_trends WHERE player_id = ?
            """, (player_id,))
            result = {}
            for row in cursor.fetchall():
                result[row['period_type']] = {
                    'total': row['total_points'],
                    'games': row['games_played'],
                    'fpg': row['fpg'],
                    'start': row['period_start'],
                    'end': row['period_end']
                }
            return result

    def get_trends_for_players(self, player_ids: list[str]) -> dict[str, dict]:
        """Get trends for multiple players."""
        if not player_ids:
            return {}
        with self.get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(player_ids))
            cursor.execute(f"""
                SELECT * FROM player_trends WHERE player_id IN ({placeholders})
            """, player_ids)

            result = {}
            for row in cursor.fetchall():
                player_id = row['player_id']
                if player_id not in result:
                    result[player_id] = {}
                result[player_id][row['period_type']] = {
                    'total': row['total_points'],
                    'games': row['games_played'],
                    'fpg': row['fpg'],
                    'start': row['period_start'],
                    'end': row['period_end']
                }
            return result

    # ==================== Free Agents ====================

    def save_free_agents(
        self,
        players: list[dict],
        sort_key: str,
        position_filter: Optional[str] = None
    ) -> None:
        """Save free agent list for a specific sort/position combination."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            # Delete existing entries for this sort/position combination
            cursor.execute("""
                DELETE FROM free_agents
                WHERE sort_key = ? AND (position_filter = ? OR (position_filter IS NULL AND ? IS NULL))
            """, (sort_key, position_filter, position_filter))

            # Insert new entries
            for i, player in enumerate(players):
                cursor.execute("""
                    INSERT INTO free_agents
                    (player_id, rank, salary, total_fpts, fpg, age,
                     sort_key, position_filter, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    player['id'],
                    i + 1,
                    player.get('salary'),
                    player.get('total_fpts'),
                    player.get('fpg'),
                    player.get('age'),
                    sort_key,
                    position_filter,
                    now
                ))

    def get_free_agents(
        self,
        sort_key: str,
        position_filter: Optional[str] = None,
        limit: int = 25
    ) -> list[dict]:
        """Get free agents for a specific sort/position combination."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT fa.*, p.name, p.team_name, p.team_short_name,
                       p.position_short_names, p.day_to_day, p.out,
                       p.injured_reserve, p.suspended
                FROM free_agents fa
                JOIN players p ON fa.player_id = p.id
                WHERE fa.sort_key = ?
                  AND (fa.position_filter = ? OR (fa.position_filter IS NULL AND ? IS NULL))
                ORDER BY fa.rank
                LIMIT ?
            """, (sort_key, position_filter, position_filter, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_top_free_agent_ids(self, limit: int = 500) -> list[str]:
        """
        Get player IDs for top N free agents by rank.

        Args:
            limit: Maximum number of player IDs to return

        Returns:
            List of player IDs ordered by rank
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT player_id
                FROM free_agents
                ORDER BY rank
                LIMIT ?
            """, (limit,))
            return [row['player_id'] for row in cursor.fetchall()]

    # ==================== Player News ====================

    def save_player_news(
        self,
        player_id: str,
        news_items: list[dict],
        max_news_per_player: int = 30
    ) -> int:
        """
        Save news items for a player, keeping only the most recent N items.

        Args:
            player_id: The player ID
            news_items: List of news dicts with keys: news_date, headline, analysis
            max_news_per_player: Maximum news items to keep per player (default: 30)

        Returns:
            Number of news items saved
        """
        if not news_items:
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            saved_count = 0

            for item in news_items:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO player_news
                        (player_id, news_date, headline, analysis, last_sync_at)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        player_id,
                        item['news_date'],
                        item['headline'],
                        item.get('analysis'),
                        now
                    ))
                    saved_count += 1
                except Exception:
                    # Skip duplicates or invalid entries
                    pass

            # Keep only the most recent N news items per player
            cursor.execute("""
                DELETE FROM player_news
                WHERE player_id = ?
                AND id NOT IN (
                    SELECT id FROM player_news
                    WHERE player_id = ?
                    ORDER BY news_date DESC
                    LIMIT ?
                )
            """, (player_id, player_id, max_news_per_player))

            return saved_count

    def get_player_news(self, player_id: str, limit: int = 30) -> list[dict]:
        """
        Get news items for a player.

        Args:
            player_id: The player ID
            limit: Maximum number of news items to return

        Returns:
            List of news dicts ordered by date descending
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pn.*, p.name as player_name
                FROM player_news pn
                JOIN players p ON pn.player_id = p.id
                WHERE pn.player_id = ?
                ORDER BY pn.news_date DESC
                LIMIT ?
            """, (player_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_news_for_players(
        self,
        player_ids: list[str],
        limit_per_player: int = 5
    ) -> dict[str, list[dict]]:
        """
        Get news items for multiple players.

        Args:
            player_ids: List of player IDs
            limit_per_player: Maximum news items per player

        Returns:
            Dict mapping player_id to list of news items
        """
        if not player_ids:
            return {}

        result = {}
        with self.get_connection() as conn:
            cursor = conn.cursor()

            for player_id in player_ids:
                cursor.execute("""
                    SELECT pn.*, p.name as player_name
                    FROM player_news pn
                    JOIN players p ON pn.player_id = p.id
                    WHERE pn.player_id = ?
                    ORDER BY pn.news_date DESC
                    LIMIT ?
                """, (player_id, limit_per_player))
                news = [dict(row) for row in cursor.fetchall()]
                if news:
                    result[player_id] = news

        return result

    def get_all_player_news(self, limit: int = 100) -> list[dict]:
        """
        Get all recent player news across all players.

        Args:
            limit: Maximum total news items to return

        Returns:
            List of news dicts ordered by date descending
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pn.*, p.name as player_name
                FROM player_news pn
                JOIN players p ON pn.player_id = p.id
                ORDER BY pn.news_date DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    # ==================== Transactions ====================

    def save_transactions(self, league_id: str, transactions: list[dict]) -> int:
        """
        Save transactions and their associated players.

        Args:
            league_id: The league ID
            transactions: List of transaction dicts with keys:
                - id: Transaction ID (txSetId)
                - team_id: Team ID
                - transaction_date: ISO format date string
                - players: List of dicts with player_id and transaction_type

        Returns:
            Number of transactions saved
        """
        if not transactions:
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            saved_count = 0

            for tx in transactions:
                # Save transaction
                cursor.execute("""
                    INSERT OR REPLACE INTO transactions
                    (id, league_id, team_id, transaction_date, last_sync_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    tx['id'],
                    league_id,
                    tx['team_id'],
                    tx['transaction_date'],
                    now
                ))

                # Delete existing transaction players for this transaction
                cursor.execute(
                    "DELETE FROM transaction_players WHERE transaction_id = ?",
                    (tx['id'],)
                )

                # Save transaction players
                for player in tx.get('players', []):
                    cursor.execute("""
                        INSERT INTO transaction_players
                        (transaction_id, player_id, transaction_type, last_sync_at)
                        VALUES (?, ?, ?, ?)
                    """, (
                        tx['id'],
                        player['player_id'],
                        player['transaction_type'],
                        now
                    ))

                saved_count += 1

            return saved_count

    def get_transactions(self, league_id: str, limit: int = 100) -> list[dict]:
        """
        Get recent transactions for a league.

        Args:
            league_id: The league ID
            limit: Maximum number of transactions to return

        Returns:
            List of transaction dicts with nested players, ordered by date descending
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Get transactions
            cursor.execute("""
                SELECT t.*, tm.name as team_name, tm.short_name as team_short_name
                FROM transactions t
                JOIN teams tm ON t.team_id = tm.id
                WHERE t.league_id = ?
                ORDER BY t.transaction_date DESC
                LIMIT ?
            """, (league_id, limit))
            transactions = [dict(row) for row in cursor.fetchall()]

            # Get players for each transaction
            for tx in transactions:
                cursor.execute("""
                    SELECT tp.*, p.name as player_name, p.team_name as pro_team,
                           p.position_short_names
                    FROM transaction_players tp
                    JOIN players p ON tp.player_id = p.id
                    WHERE tp.transaction_id = ?
                """, (tx['id'],))
                tx['players'] = [dict(row) for row in cursor.fetchall()]

            return transactions

    def get_transaction_count(self, league_id: str) -> int:
        """Get total number of transactions for a league."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM transactions WHERE league_id = ?",
                (league_id,)
            )
            return cursor.fetchone()[0]

    # ==================== Scoring Periods & Matchups ====================

    def save_scoring_periods(self, league_id: str, periods: list[dict]) -> int:
        """
        Save scoring periods for a league.

        Args:
            league_id: The league ID
            periods: List of period dicts with keys:
                - period_number: Integer period number
                - start_date: ISO format date string
                - end_date: ISO format date string
                - is_playoffs: Boolean

        Returns:
            Number of periods saved
        """
        if not periods:
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            for period in periods:
                cursor.execute("""
                    INSERT OR REPLACE INTO scoring_periods
                    (league_id, period_number, start_date, end_date, is_playoffs, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    league_id,
                    period['period_number'],
                    period['start_date'],
                    period['end_date'],
                    1 if period.get('is_playoffs') else 0,
                    now
                ))

            return len(periods)

    def get_scoring_periods(self, league_id: str) -> list[dict]:
        """Get all scoring periods for a league, ordered by period number."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM scoring_periods
                WHERE league_id = ?
                ORDER BY period_number
            """, (league_id,))
            return [dict(row) for row in cursor.fetchall()]

    def save_matchups(self, league_id: str, matchups: list[dict]) -> int:
        """
        Save matchups for a league.

        Args:
            league_id: The league ID
            matchups: List of matchup dicts with keys:
                - period_number: Integer period number
                - matchup_key: Integer matchup identifier within period
                - away_team_id: Away team ID (optional)
                - away_team_name: Away team name (for display)
                - away_score: Away team score
                - home_team_id: Home team ID (optional)
                - home_team_name: Home team name (for display)
                - home_score: Home team score

        Returns:
            Number of matchups saved
        """
        if not matchups:
            return 0

        with self.get_connection() as conn:
            cursor = conn.cursor()
            now = datetime.now().isoformat()

            for matchup in matchups:
                cursor.execute("""
                    INSERT OR REPLACE INTO matchups
                    (league_id, period_number, matchup_key, away_team_id, away_team_name,
                     away_score, home_team_id, home_team_name, home_score, last_sync_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    league_id,
                    matchup['period_number'],
                    matchup['matchup_key'],
                    matchup.get('away_team_id'),
                    matchup.get('away_team_name'),
                    matchup.get('away_score', 0),
                    matchup.get('home_team_id'),
                    matchup.get('home_team_name'),
                    matchup.get('home_score', 0),
                    now
                ))

            return len(matchups)

    def get_matchups(
        self,
        league_id: str,
        period_number: Optional[int] = None
    ) -> list[dict]:
        """
        Get matchups for a league, optionally filtered by period.

        Args:
            league_id: The league ID
            period_number: Optional period number to filter by

        Returns:
            List of matchup dicts ordered by period and matchup key
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()

            if period_number is not None:
                cursor.execute("""
                    SELECT m.*, sp.start_date as period_start, sp.end_date as period_end,
                           sp.is_playoffs
                    FROM matchups m
                    LEFT JOIN scoring_periods sp ON m.league_id = sp.league_id
                        AND m.period_number = sp.period_number
                    WHERE m.league_id = ? AND m.period_number = ?
                    ORDER BY m.matchup_key
                """, (league_id, period_number))
            else:
                cursor.execute("""
                    SELECT m.*, sp.start_date as period_start, sp.end_date as period_end,
                           sp.is_playoffs
                    FROM matchups m
                    LEFT JOIN scoring_periods sp ON m.league_id = sp.league_id
                        AND m.period_number = sp.period_number
                    WHERE m.league_id = ?
                    ORDER BY m.period_number, m.matchup_key
                """, (league_id,))

            return [dict(row) for row in cursor.fetchall()]

    def get_matchup_count(self, league_id: str) -> int:
        """Get total number of matchups for a league."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM matchups WHERE league_id = ?",
                (league_id,)
            )
            return cursor.fetchone()[0]

    def get_scoring_period_count(self, league_id: str) -> int:
        """Get total number of scoring periods for a league."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM scoring_periods WHERE league_id = ?",
                (league_id,)
            )
            return cursor.fetchone()[0]

    # ==================== Sync Log ====================

    def log_sync_start(self, sync_type: str, league_id: str) -> int:
        """Log the start of a sync operation. Returns the sync log ID."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO sync_log (sync_type, league_id, started_at, status)
                VALUES (?, ?, ?, 'in_progress')
            """, (sync_type, league_id, datetime.now().isoformat()))
            return cursor.lastrowid

    def log_sync_complete(self, sync_id: int, api_calls: int = 0) -> None:
        """Log successful completion of a sync operation."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE sync_log
                SET completed_at = ?, status = 'completed', api_calls_made = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), api_calls, sync_id))

    def log_sync_failed(self, sync_id: int, error: str) -> None:
        """Log failed sync operation."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE sync_log
                SET completed_at = ?, status = 'failed', error_message = ?
                WHERE id = ?
            """, (datetime.now().isoformat(), error, sync_id))

    def get_last_sync(self, league_id: str, sync_type: str) -> Optional[dict]:
        """Get the most recent successful sync of a given type."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM sync_log
                WHERE league_id = ? AND sync_type = ? AND status = 'completed'
                ORDER BY completed_at DESC
                LIMIT 1
            """, (league_id, sync_type))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_sync_status(self, league_id: str) -> dict[str, Optional[dict]]:
        """Get the most recent sync status for all sync types."""
        sync_types = ['full', 'teams', 'standings', 'rosters', 'daily_scores', 'trends', 'free_agents', 'player_news', 'transactions', 'matchups']
        result = {}
        for sync_type in sync_types:
            result[sync_type] = self.get_last_sync(league_id, sync_type)
        return result


# ==================== Utility Functions ====================

def is_cache_fresh(last_sync_at: Optional[str], max_age_hours: float = 24.0) -> bool:
    """
    Check if cached data is fresh enough.

    Args:
        last_sync_at: ISO format datetime string of last sync
        max_age_hours: Maximum age in hours before cache is considered stale

    Returns:
        True if cache is fresh, False if stale or missing
    """
    if not last_sync_at:
        return False

    try:
        last_sync = datetime.fromisoformat(last_sync_at)
        age = datetime.now() - last_sync
        age_hours = age.total_seconds() / 3600
        return age_hours < max_age_hours
    except (ValueError, TypeError):
        return False


def get_cache_age_hours(last_sync_at: Optional[str]) -> Optional[float]:
    """
    Get the age of cached data in hours.

    Args:
        last_sync_at: ISO format datetime string of last sync

    Returns:
        Age in hours, or None if last_sync_at is invalid
    """
    if not last_sync_at:
        return None

    try:
        last_sync = datetime.fromisoformat(last_sync_at)
        age = datetime.now() - last_sync
        return age.total_seconds() / 3600
    except (ValueError, TypeError):
        return None
