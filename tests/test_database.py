"""Unit tests for database module."""

import pytest
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
import tempfile
import os

from fantrax_cli.database import (
    DatabaseManager,
    is_cache_fresh,
    get_cache_age_hours
)


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test_cache.db"


@pytest.fixture
def db_manager(temp_db_path):
    """Create a DatabaseManager with a temporary database."""
    return DatabaseManager(db_path=temp_db_path)


class TestDatabaseManager:
    """Tests for DatabaseManager class."""

    def test_init_creates_database_file(self, temp_db_path):
        """Test that initialization creates the database file."""
        assert not temp_db_path.exists()
        db = DatabaseManager(db_path=temp_db_path)
        assert temp_db_path.exists()

    def test_init_creates_tables(self, db_manager):
        """Test that initialization creates all required tables."""
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row['name'] for row in cursor.fetchall()]

        expected_tables = [
            'daily_scores', 'free_agents', 'league_metadata',
            'player_trends', 'players', 'roster_slots',
            'schema_version', 'standings', 'sync_log', 'teams'
        ]
        assert sorted(tables) == sorted(expected_tables)

    def test_init_creates_indexes(self, db_manager):
        """Test that initialization creates required indexes."""
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master WHERE type='index'
                AND name NOT LIKE 'sqlite_%'
            """)
            indexes = [row['name'] for row in cursor.fetchall()]

        assert 'idx_roster_slots_team_id' in indexes
        assert 'idx_daily_scores_player_date' in indexes
        assert 'idx_daily_scores_date' in indexes
        assert 'idx_player_trends_player' in indexes

    def test_clear_all(self, db_manager):
        """Test clearing all data from database."""
        # Add some data
        db_manager.save_league_metadata('league1', 'Test League')
        db_manager.save_teams('league1', [{'id': 'team1', 'name': 'Team 1', 'short': 'T1'}])

        # Verify data exists
        assert db_manager.get_league_metadata('league1') is not None
        assert len(db_manager.get_teams('league1')) == 1

        # Clear all
        db_manager.clear_all()

        # Verify data is gone
        assert db_manager.get_league_metadata('league1') is None
        assert len(db_manager.get_teams('league1')) == 0


class TestLeagueMetadata:
    """Tests for league metadata operations."""

    def test_save_and_get_league_metadata(self, db_manager):
        """Test saving and retrieving league metadata."""
        db_manager.save_league_metadata(
            league_id='test123',
            name='Test League',
            year='2024-25',
            start_date=date(2024, 10, 1),
            end_date=date(2025, 4, 30)
        )

        result = db_manager.get_league_metadata('test123')
        assert result is not None
        assert result['league_id'] == 'test123'
        assert result['name'] == 'Test League'
        assert result['year'] == '2024-25'
        assert result['start_date'] == '2024-10-01'
        assert result['end_date'] == '2025-04-30'
        assert result['last_sync_at'] is not None

    def test_get_nonexistent_league(self, db_manager):
        """Test getting metadata for nonexistent league."""
        result = db_manager.get_league_metadata('nonexistent')
        assert result is None

    def test_update_league_metadata(self, db_manager):
        """Test updating existing league metadata."""
        db_manager.save_league_metadata('test123', 'Original Name')
        db_manager.save_league_metadata('test123', 'Updated Name')

        result = db_manager.get_league_metadata('test123')
        assert result['name'] == 'Updated Name'


class TestTeams:
    """Tests for team operations."""

    def test_save_and_get_teams(self, db_manager):
        """Test saving and retrieving teams."""
        teams = [
            {'id': 'team1', 'name': 'Team Alpha', 'short': 'ALP'},
            {'id': 'team2', 'name': 'Team Beta', 'short': 'BET'},
        ]
        db_manager.save_teams('league1', teams)

        result = db_manager.get_teams('league1')
        assert len(result) == 2
        # Results are ordered by name
        assert result[0]['name'] == 'Team Alpha'
        assert result[1]['name'] == 'Team Beta'

    def test_get_team_by_id(self, db_manager):
        """Test getting team by ID."""
        teams = [{'id': 'abc123', 'name': 'Test Team', 'short': 'TST'}]
        db_manager.save_teams('league1', teams)

        result = db_manager.get_team_by_id('abc123')
        assert result is not None
        assert result['name'] == 'Test Team'

    def test_get_team_by_identifier_name(self, db_manager):
        """Test getting team by partial name match."""
        teams = [{'id': 'team1', 'name': 'Bois ton (dro)let', 'short': 'BOIS'}]
        db_manager.save_teams('league1', teams)

        result = db_manager.get_team_by_identifier('league1', 'Bois')
        assert result is not None
        assert result['id'] == 'team1'

    def test_get_team_by_identifier_short_name(self, db_manager):
        """Test getting team by short name."""
        teams = [{'id': 'team1', 'name': 'Test Team', 'short': 'BOIS'}]
        db_manager.save_teams('league1', teams)

        result = db_manager.get_team_by_identifier('league1', 'bois')
        assert result is not None
        assert result['id'] == 'team1'


class TestPlayers:
    """Tests for player operations."""

    def test_save_and_get_player(self, db_manager):
        """Test saving and retrieving a player."""
        player = {
            'id': 'player1',
            'name': 'John Smith',
            'short_name': 'J. Smith',
            'team_name': 'Boston Bruins',
            'team_short_name': 'BOS',
            'position_short_names': 'C,LW',
            'day_to_day': 0,
            'out': 0,
            'injured_reserve': 1,
            'suspended': 0
        }
        db_manager.save_player(player)

        result = db_manager.get_player('player1')
        assert result is not None
        assert result['name'] == 'John Smith'
        assert result['position_short_names'] == 'C,LW'
        assert result['injured_reserve'] == 1

    def test_save_multiple_players(self, db_manager):
        """Test saving multiple players at once."""
        players = [
            {'id': 'p1', 'name': 'Player 1'},
            {'id': 'p2', 'name': 'Player 2'},
            {'id': 'p3', 'name': 'Player 3'},
        ]
        db_manager.save_players(players)

        result = db_manager.get_players_by_ids(['p1', 'p2', 'p3'])
        assert len(result) == 3
        assert result['p1']['name'] == 'Player 1'
        assert result['p2']['name'] == 'Player 2'

    def test_get_players_by_ids_empty(self, db_manager):
        """Test getting players with empty ID list."""
        result = db_manager.get_players_by_ids([])
        assert result == {}


class TestRosterSlots:
    """Tests for roster slot operations."""

    def test_save_and_get_roster(self, db_manager):
        """Test saving and retrieving roster slots."""
        # First save the player
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})

        roster = [
            {
                'player_id': 'player1',
                'position_id': '2010',
                'position_short': 'C',
                'status_id': '1',
                'salary': 5.0,
                'total_fantasy_points': 100.5,
                'fantasy_points_per_game': 2.5
            },
            {
                'player_id': None,  # Empty slot
                'position_id': '2020',
                'position_short': 'LW',
                'status_id': '1',
                'salary': None,
                'total_fantasy_points': None,
                'fantasy_points_per_game': None
            }
        ]
        db_manager.save_roster('team1', roster)

        result = db_manager.get_roster('team1')
        assert len(result) == 2
        assert result[0]['player_id'] == 'player1'
        assert result[0]['player_name'] == 'Test Player'
        assert result[0]['salary'] == 5.0
        assert result[1]['player_id'] is None

    def test_save_roster_replaces_existing(self, db_manager):
        """Test that saving roster replaces existing slots."""
        roster1 = [{'player_id': None, 'position_id': '1', 'position_short': 'C'}]
        roster2 = [{'player_id': None, 'position_id': '2', 'position_short': 'LW'}]

        db_manager.save_roster('team1', roster1)
        assert len(db_manager.get_roster('team1')) == 1

        db_manager.save_roster('team1', roster2)
        result = db_manager.get_roster('team1')
        assert len(result) == 1
        assert result[0]['position_short'] == 'LW'


class TestDailyScores:
    """Tests for daily scores operations."""

    def test_save_and_get_daily_scores(self, db_manager):
        """Test saving and retrieving daily scores."""
        scores = {'player1': 5.5, 'player2': 3.0, 'player3': 0.0}
        scoring_date = date(2025, 1, 15)

        db_manager.save_daily_scores('team1', scoring_date, scores)

        result = db_manager.get_daily_scores_for_team(
            'team1',
            date(2025, 1, 1),
            date(2025, 1, 31)
        )
        assert len(result) == 3
        player_scores = {r['player_id']: r['fantasy_points'] for r in result}
        assert player_scores['player1'] == 5.5
        assert player_scores['player3'] == 0.0

    def test_get_daily_scores_for_player(self, db_manager):
        """Test getting scores for a specific player."""
        # Save scores for multiple days
        for day in range(1, 8):
            db_manager.save_daily_scores(
                'team1',
                date(2025, 1, day),
                {'player1': float(day)}
            )

        result = db_manager.get_daily_scores_for_player(
            'player1',
            date(2025, 1, 1),
            date(2025, 1, 7)
        )
        assert len(result) == 7
        assert result[0]['fantasy_points'] == 1.0
        assert result[6]['fantasy_points'] == 7.0

    def test_get_daily_scores_date_range(self, db_manager):
        """Test getting the date range of stored scores."""
        db_manager.save_daily_scores('team1', date(2025, 1, 5), {'p1': 1.0})
        db_manager.save_daily_scores('team1', date(2025, 1, 20), {'p1': 2.0})

        result = db_manager.get_daily_scores_date_range()
        assert result == ('2025-01-05', '2025-01-20')

    def test_get_daily_scores_date_range_empty(self, db_manager):
        """Test date range when no scores exist."""
        result = db_manager.get_daily_scores_date_range()
        assert result is None


class TestPlayerTrends:
    """Tests for player trends operations."""

    def test_save_and_get_player_trends(self, db_manager):
        """Test saving and retrieving player trends."""
        trends = {
            'week1': {'total': 10.5, 'games': 4, 'fpg': 2.625, 'start': '2025-01-18', 'end': '2025-01-24'},
            'week2': {'total': 8.0, 'games': 3, 'fpg': 2.67, 'start': '2025-01-11', 'end': '2025-01-17'},
            '14': {'total': 18.5, 'games': 7, 'fpg': 2.64, 'start': '2025-01-11', 'end': '2025-01-24'},
        }
        db_manager.save_player_trends('player1', trends)

        result = db_manager.get_player_trends('player1')
        assert len(result) == 3
        assert result['week1']['fpg'] == 2.625
        assert result['14']['games'] == 7

    def test_get_trends_for_multiple_players(self, db_manager):
        """Test getting trends for multiple players."""
        db_manager.save_player_trends('p1', {'week1': {'total': 10, 'games': 4, 'fpg': 2.5}})
        db_manager.save_player_trends('p2', {'week1': {'total': 8, 'games': 3, 'fpg': 2.67}})

        result = db_manager.get_trends_for_players(['p1', 'p2'])
        assert len(result) == 2
        assert result['p1']['week1']['fpg'] == 2.5
        assert result['p2']['week1']['fpg'] == 2.67


class TestFreeAgents:
    """Tests for free agents operations."""

    def test_save_and_get_free_agents(self, db_manager):
        """Test saving and retrieving free agents."""
        # First save players
        db_manager.save_players([
            {'id': 'fa1', 'name': 'Free Agent 1', 'position_short_names': 'C'},
            {'id': 'fa2', 'name': 'Free Agent 2', 'position_short_names': 'LW'},
        ])

        fa_list = [
            {'id': 'fa1', 'salary': '$1.5', 'total_fpts': 50.0, 'fpg': 2.5},
            {'id': 'fa2', 'salary': '$2.0', 'total_fpts': 45.0, 'fpg': 2.25},
        ]
        db_manager.save_free_agents(fa_list, 'SCORE', None)

        result = db_manager.get_free_agents('SCORE', None, limit=10)
        assert len(result) == 2
        assert result[0]['name'] == 'Free Agent 1'
        assert result[0]['rank'] == 1
        assert result[1]['rank'] == 2

    def test_get_free_agents_with_position_filter(self, db_manager):
        """Test getting free agents with position filter."""
        db_manager.save_players([
            {'id': 'fa1', 'name': 'Center', 'position_short_names': 'C'},
        ])

        db_manager.save_free_agents([{'id': 'fa1', 'fpg': 2.5}], 'SCORE', '2010')

        # Should find with correct filter
        result = db_manager.get_free_agents('SCORE', '2010', limit=10)
        assert len(result) == 1

        # Should not find with different filter
        result = db_manager.get_free_agents('SCORE', '2020', limit=10)
        assert len(result) == 0


class TestSyncLog:
    """Tests for sync log operations."""

    def test_log_sync_start_and_complete(self, db_manager):
        """Test logging sync start and completion."""
        sync_id = db_manager.log_sync_start('full', 'league1')
        assert sync_id > 0

        db_manager.log_sync_complete(sync_id, api_calls=53)

        result = db_manager.get_last_sync('league1', 'full')
        assert result is not None
        assert result['status'] == 'completed'
        assert result['api_calls_made'] == 53

    def test_log_sync_failed(self, db_manager):
        """Test logging failed sync."""
        sync_id = db_manager.log_sync_start('full', 'league1')
        db_manager.log_sync_failed(sync_id, 'Connection timeout')

        # get_last_sync only returns completed syncs
        result = db_manager.get_last_sync('league1', 'full')
        assert result is None

    def test_get_all_sync_status(self, db_manager):
        """Test getting status for all sync types."""
        sync_id = db_manager.log_sync_start('teams', 'league1')
        db_manager.log_sync_complete(sync_id, 6)

        result = db_manager.get_all_sync_status('league1')
        assert 'teams' in result
        assert result['teams']['status'] == 'completed'
        assert result['full'] is None  # No full sync done


class TestCacheFreshness:
    """Tests for cache freshness utility functions."""

    def test_is_cache_fresh_valid(self):
        """Test cache freshness with recent sync."""
        recent_time = datetime.now().isoformat()
        assert is_cache_fresh(recent_time, max_age_hours=24) is True

    def test_is_cache_fresh_stale(self):
        """Test cache freshness with old sync."""
        old_time = (datetime.now() - timedelta(hours=48)).isoformat()
        assert is_cache_fresh(old_time, max_age_hours=24) is False

    def test_is_cache_fresh_none(self):
        """Test cache freshness with None input."""
        assert is_cache_fresh(None) is False

    def test_is_cache_fresh_invalid_format(self):
        """Test cache freshness with invalid format."""
        assert is_cache_fresh('not-a-date') is False

    def test_get_cache_age_hours_valid(self):
        """Test getting cache age in hours."""
        two_hours_ago = (datetime.now() - timedelta(hours=2)).isoformat()
        age = get_cache_age_hours(two_hours_ago)
        assert age is not None
        assert 1.9 < age < 2.1  # Allow small tolerance

    def test_get_cache_age_hours_none(self):
        """Test getting cache age with None input."""
        assert get_cache_age_hours(None) is None

    def test_get_cache_age_hours_invalid(self):
        """Test getting cache age with invalid format."""
        assert get_cache_age_hours('invalid') is None


class TestDatabasePath:
    """Tests for database path handling."""

    def test_default_path_uses_platformdirs(self, monkeypatch):
        """Test that default path uses platformdirs."""
        import platformdirs
        with tempfile.TemporaryDirectory() as tmpdir:
            # Mock platformdirs to use our temp directory
            monkeypatch.setattr(
                platformdirs,
                'user_data_dir',
                lambda x: tmpdir
            )
            db = DatabaseManager(db_path=None)
            assert str(tmpdir) in str(db.db_path)
            assert 'fantrax_cache.db' in str(db.db_path)

    def test_custom_path(self, temp_db_path):
        """Test using custom database path."""
        db = DatabaseManager(db_path=temp_db_path)
        assert db.db_path == temp_db_path

    def test_creates_parent_directory(self):
        """Test that parent directories are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nested_path = Path(tmpdir) / "a" / "b" / "c" / "test.db"
            assert not nested_path.parent.exists()
            db = DatabaseManager(db_path=nested_path)
            assert nested_path.parent.exists()
