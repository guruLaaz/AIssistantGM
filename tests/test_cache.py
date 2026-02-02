"""Unit tests for cache module."""

import pytest
from datetime import datetime, date, timedelta
from pathlib import Path
from unittest.mock import Mock
import tempfile

from fantrax_cli.database import DatabaseManager
from fantrax_cli.cache import CacheManager, CacheResult, format_cache_age, CACHE_MAX_AGE
from fantrax_cli.config import Config


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test_cache.db"


@pytest.fixture
def db_manager(temp_db_path):
    """Create a DatabaseManager with a temporary database."""
    return DatabaseManager(db_path=temp_db_path)


@pytest.fixture
def config():
    """Create a test configuration."""
    return Config(
        username="test",
        password="test",
        league_id="test_league_123",
        cache_enabled=True,
        cache_max_age_hours=0.0  # 0 means use defaults
    )


@pytest.fixture
def cache_manager(db_manager, config):
    """Create a CacheManager with test dependencies."""
    return CacheManager(db_manager, config)


class TestCacheResult:
    """Tests for CacheResult dataclass."""

    def test_from_cache_true(self):
        """Test CacheResult with cached data."""
        result = CacheResult(
            data={"key": "value"},
            from_cache=True,
            cache_age_hours=2.5,
            stale=False
        )
        assert result.data == {"key": "value"}
        assert result.from_cache is True
        assert result.cache_age_hours == 2.5
        assert result.stale is False

    def test_from_cache_false(self):
        """Test CacheResult when cache miss."""
        result = CacheResult(data=None, from_cache=False)
        assert result.data is None
        assert result.from_cache is False
        assert result.cache_age_hours is None
        assert result.stale is False


class TestCacheManager:
    """Tests for CacheManager class."""

    def test_init(self, cache_manager, db_manager, config):
        """Test CacheManager initialization."""
        assert cache_manager.db == db_manager
        assert cache_manager.config == config
        assert cache_manager.is_cache_enabled() is True

    def test_cache_disabled(self, db_manager):
        """Test behavior when cache is disabled."""
        config = Config(
            username="test",
            password="test",
            league_id="test_league",
            cache_enabled=False
        )
        cache = CacheManager(db_manager, config)
        assert cache.is_cache_enabled() is False

        result = cache.get_teams()
        assert result.from_cache is False

    def test_get_max_age_default(self, cache_manager):
        """Test default max age values."""
        assert cache_manager.get_max_age('teams') == 168.0
        assert cache_manager.get_max_age('rosters') == 12.0
        assert cache_manager.get_max_age('trends') == 24.0

    def test_get_max_age_override(self, db_manager):
        """Test max age override from config."""
        config = Config(
            username="test",
            password="test",
            league_id="test_league",
            cache_enabled=True,
            cache_max_age_hours=6.0  # Override all to 6 hours
        )
        cache = CacheManager(db_manager, config)

        # Should use the minimum of default and override
        assert cache.get_max_age('rosters') == 6.0  # Uses override (6 < 12)
        assert cache.get_max_age('teams') == 6.0    # Uses override (6 < 168)


class TestCacheManagerTeams:
    """Tests for team caching."""

    def test_get_teams_empty_cache(self, cache_manager):
        """Test getting teams when cache is empty."""
        result = cache_manager.get_teams()
        assert result.from_cache is False
        assert result.data is None

    def test_get_teams_with_data(self, cache_manager, db_manager, config):
        """Test getting teams when data is cached."""
        # Add teams to cache
        db_manager.save_teams(config.league_id, [
            {'id': 'team1', 'name': 'Team 1', 'short': 'T1'},
            {'id': 'team2', 'name': 'Team 2', 'short': 'T2'},
        ])

        # Log a sync
        sync_id = db_manager.log_sync_start('teams', config.league_id)
        db_manager.log_sync_complete(sync_id, 6)

        result = cache_manager.get_teams()
        assert result.from_cache is True
        assert len(result.data) == 2
        assert result.stale is False

    def test_get_teams_stale_cache(self, cache_manager, db_manager, config):
        """Test getting teams when cache is stale."""
        # Add teams to cache
        db_manager.save_teams(config.league_id, [
            {'id': 'team1', 'name': 'Team 1', 'short': 'T1'},
        ])

        # Log an old sync (200 hours ago - past the 168 hour threshold)
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            old_time = (datetime.now() - timedelta(hours=200)).isoformat()
            cursor.execute("""
                INSERT INTO sync_log (sync_type, league_id, started_at, completed_at, status, api_calls_made)
                VALUES (?, ?, ?, ?, 'completed', 6)
            """, ('teams', config.league_id, old_time, old_time))

        result = cache_manager.get_teams()
        assert result.from_cache is True
        assert result.stale is True

    def test_get_teams_force_refresh(self, cache_manager, db_manager, config):
        """Test force refresh bypasses cache."""
        # Add teams to cache
        db_manager.save_teams(config.league_id, [
            {'id': 'team1', 'name': 'Team 1', 'short': 'T1'},
        ])

        result = cache_manager.get_teams(force_refresh=True)
        assert result.from_cache is False

    def test_get_team_by_identifier(self, cache_manager, db_manager, config):
        """Test getting team by identifier."""
        db_manager.save_teams(config.league_id, [
            {'id': 'team1', 'name': 'Test Team', 'short': 'TST'},
        ])

        # By ID
        team = cache_manager.get_team_by_identifier('team1')
        assert team is not None
        assert team['name'] == 'Test Team'

        # By name
        team = cache_manager.get_team_by_identifier('Test')
        assert team is not None

        # By short name
        team = cache_manager.get_team_by_identifier('TST')
        assert team is not None


class TestCacheManagerRosters:
    """Tests for roster caching."""

    def test_get_roster_empty_cache(self, cache_manager):
        """Test getting roster when cache is empty."""
        result = cache_manager.get_roster('team1')
        assert result.from_cache is False
        assert result.data is None

    def test_get_roster_with_data(self, cache_manager, db_manager, config):
        """Test getting roster when data is cached."""
        # Add roster to cache
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})
        db_manager.save_roster('team1', [
            {'player_id': 'player1', 'position_id': '2010', 'position_short': 'C'}
        ])

        # Log a sync
        sync_id = db_manager.log_sync_start('rosters', config.league_id)
        db_manager.log_sync_complete(sync_id, 12)

        result = cache_manager.get_roster('team1')
        assert result.from_cache is True
        assert len(result.data) == 1

    def test_get_player(self, cache_manager, db_manager):
        """Test getting a single player from cache."""
        db_manager.save_player({
            'id': 'player1',
            'name': 'Test Player',
            'position_short_names': 'C,LW'
        })

        player = cache_manager.get_player('player1')
        assert player is not None
        assert player['name'] == 'Test Player'

    def test_get_players_by_ids(self, cache_manager, db_manager):
        """Test getting multiple players from cache."""
        db_manager.save_players([
            {'id': 'p1', 'name': 'Player 1'},
            {'id': 'p2', 'name': 'Player 2'},
        ])

        players = cache_manager.get_players_by_ids(['p1', 'p2'])
        assert len(players) == 2


class TestCacheManagerTrends:
    """Tests for trends caching."""

    def test_get_player_trends_empty_cache(self, cache_manager):
        """Test getting trends when cache is empty."""
        result = cache_manager.get_player_trends('team1')
        assert result.from_cache is False

    def test_get_player_trends_with_data(self, cache_manager, db_manager, config):
        """Test getting trends when data is cached."""
        # Setup roster and trends
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})
        db_manager.save_roster('team1', [
            {'player_id': 'player1', 'position_id': '2010', 'position_short': 'C'}
        ])
        db_manager.save_player_trends('player1', {
            'week1': {'total': 10.0, 'games': 4, 'fpg': 2.5},
            '14': {'total': 20.0, 'games': 8, 'fpg': 2.5},
        })

        # Log a sync
        sync_id = db_manager.log_sync_start('trends', config.league_id)
        db_manager.log_sync_complete(sync_id, 0)

        result = cache_manager.get_player_trends('team1')
        assert result.from_cache is True
        assert 'player1' in result.data
        assert result.data['player1']['week1']['fpg'] == 2.5

    def test_get_trends_for_player(self, cache_manager, db_manager):
        """Test getting trends for a specific player."""
        db_manager.save_player_trends('player1', {
            'week1': {'total': 10.0, 'games': 4, 'fpg': 2.5},
        })

        trends = cache_manager.get_trends_for_player('player1')
        assert trends is not None
        assert 'week1' in trends


class TestCacheManagerFreeAgents:
    """Tests for free agent caching."""

    def test_get_free_agents_empty_cache(self, cache_manager):
        """Test getting free agents when cache is empty."""
        result = cache_manager.get_free_agents()
        assert result.from_cache is False

    def test_get_free_agents_with_data(self, cache_manager, db_manager, config):
        """Test getting free agents when data is cached."""
        # Add free agents
        db_manager.save_players([
            {'id': 'fa1', 'name': 'Free Agent 1'},
            {'id': 'fa2', 'name': 'Free Agent 2'},
        ])
        db_manager.save_free_agents([
            {'id': 'fa1', 'fpg': 2.5},
            {'id': 'fa2', 'fpg': 2.0},
        ], 'SCORE', None)

        # Log a sync
        sync_id = db_manager.log_sync_start('free_agents', config.league_id)
        db_manager.log_sync_complete(sync_id, 5)

        result = cache_manager.get_free_agents(sort_key='SCORE', limit=10)
        assert result.from_cache is True
        assert len(result.data) == 2


class TestCacheManagerDailyScores:
    """Tests for daily scores caching."""

    def test_get_daily_scores_for_team(self, cache_manager, db_manager):
        """Test getting daily scores for a team."""
        db_manager.save_daily_scores('team1', date(2025, 1, 15), {
            'player1': 5.5,
            'player2': 3.0,
        })

        scores = cache_manager.get_daily_scores_for_team(
            'team1',
            date(2025, 1, 1),
            date(2025, 1, 31)
        )
        assert len(scores) == 2

    def test_has_daily_scores_for_range(self, cache_manager, db_manager):
        """Test checking if daily scores cover a date range."""
        # Add scores for a range
        db_manager.save_daily_scores('team1', date(2025, 1, 1), {'p1': 1.0})
        db_manager.save_daily_scores('team1', date(2025, 1, 15), {'p1': 2.0})

        # Should have data for the covered range
        assert cache_manager.has_daily_scores_for_range(
            date(2025, 1, 5),
            date(2025, 1, 10)
        ) is True

        # Should not have data for range outside
        assert cache_manager.has_daily_scores_for_range(
            date(2025, 1, 16),
            date(2025, 1, 20)
        ) is False

    def test_has_daily_scores_empty_cache(self, cache_manager):
        """Test checking date range with empty cache."""
        assert cache_manager.has_daily_scores_for_range(
            date(2025, 1, 1),
            date(2025, 1, 15)
        ) is False


class TestCacheManagerLeagueMetadata:
    """Tests for league metadata caching."""

    def test_get_league_metadata(self, cache_manager, db_manager, config):
        """Test getting league metadata."""
        db_manager.save_league_metadata(
            config.league_id,
            "Test League",
            "2024-25"
        )

        meta = cache_manager.get_league_metadata()
        assert meta is not None
        assert meta['name'] == "Test League"

    def test_get_league_name(self, cache_manager, db_manager, config):
        """Test getting league name shortcut."""
        db_manager.save_league_metadata(
            config.league_id,
            "Test League",
            "2024-25"
        )

        name = cache_manager.get_league_name()
        assert name == "Test League"

    def test_get_league_name_empty(self, cache_manager):
        """Test getting league name when not cached."""
        name = cache_manager.get_league_name()
        assert name is None


class TestFormatCacheAge:
    """Tests for format_cache_age function."""

    def test_minutes(self):
        """Test formatting minutes."""
        assert format_cache_age(0.5) == "30 min ago"
        assert format_cache_age(0.1) == "6 min ago"

    def test_hours(self):
        """Test formatting hours."""
        assert format_cache_age(2.5) == "2.5 hours ago"
        assert format_cache_age(12.0) == "12.0 hours ago"

    def test_days(self):
        """Test formatting days."""
        assert format_cache_age(48.0) == "2.0 days ago"
        assert format_cache_age(72.0) == "3.0 days ago"

    def test_none(self):
        """Test formatting None."""
        assert format_cache_age(None) == "unknown"


class TestCacheMaxAgeDefaults:
    """Tests for default cache max age values."""

    def test_teams_max_age(self):
        """Teams should have long cache time (1 week)."""
        assert CACHE_MAX_AGE['teams'] == 168.0

    def test_rosters_max_age(self):
        """Rosters should have medium cache time (12 hours)."""
        assert CACHE_MAX_AGE['rosters'] == 12.0

    def test_trends_max_age(self):
        """Trends should have daily cache time (24 hours)."""
        assert CACHE_MAX_AGE['trends'] == 24.0

    def test_free_agents_max_age(self):
        """Free agents should have medium cache time (12 hours)."""
        assert CACHE_MAX_AGE['free_agents'] == 12.0

    def test_player_news_max_age(self):
        """Player news should have medium cache time (12 hours)."""
        assert CACHE_MAX_AGE['player_news'] == 12.0


class TestCacheManagerPlayerNews:
    """Tests for player news caching."""

    def test_get_player_news_empty_cache(self, cache_manager):
        """Test getting player news when cache is empty."""
        result = cache_manager.get_player_news('player1')
        assert result.from_cache is False

    def test_get_player_news_with_data(self, cache_manager, db_manager, config):
        """Test getting player news when data is cached."""
        # Add a player and news
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})
        db_manager.save_player_news('player1', [
            {'news_date': '2025-01-25T10:00:00', 'headline': 'Test news', 'analysis': 'Analysis text'}
        ])

        # Log a sync
        sync_id = db_manager.log_sync_start('player_news', config.league_id)
        db_manager.log_sync_complete(sync_id, 10)

        result = cache_manager.get_player_news('player1')
        assert result.from_cache is True
        assert len(result.data) == 1
        assert result.data[0]['headline'] == 'Test news'

    def test_get_player_news_stale_without_sync(self, cache_manager, db_manager):
        """Test that news is marked stale if no sync log exists."""
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})
        db_manager.save_player_news('player1', [
            {'news_date': '2025-01-25T10:00:00', 'headline': 'Test news'}
        ])

        result = cache_manager.get_player_news('player1')
        assert result.from_cache is True
        assert result.stale is True

    def test_get_player_news_force_refresh(self, cache_manager, db_manager, config):
        """Test force refresh bypasses cache."""
        db_manager.save_player({'id': 'player1', 'name': 'Test Player'})
        db_manager.save_player_news('player1', [
            {'news_date': '2025-01-25T10:00:00', 'headline': 'Test news'}
        ])

        sync_id = db_manager.log_sync_start('player_news', config.league_id)
        db_manager.log_sync_complete(sync_id, 10)

        result = cache_manager.get_player_news('player1', force_refresh=True)
        assert result.from_cache is False

    def test_get_news_for_roster_empty_cache(self, cache_manager):
        """Test getting roster news when cache is empty."""
        result = cache_manager.get_news_for_roster('team1')
        assert result.from_cache is False

    def test_get_news_for_roster_with_data(self, cache_manager, db_manager, config):
        """Test getting news for all players on a roster."""
        # Add team and players
        db_manager.save_league_metadata(config.league_id, 'Test League')
        db_manager.save_teams(config.league_id, [{'id': 'team1', 'name': 'Team 1', 'short': 'T1'}])
        db_manager.save_players([
            {'id': 'p1', 'name': 'Player 1'},
            {'id': 'p2', 'name': 'Player 2'},
        ])
        db_manager.save_roster('team1', [
            {'player_id': 'p1', 'position_id': '1', 'position_short': 'C'},
            {'player_id': 'p2', 'position_id': '2', 'position_short': 'LW'},
        ])

        # Add news for players
        db_manager.save_player_news('p1', [
            {'news_date': '2025-01-25T10:00:00', 'headline': 'P1 news'}
        ])
        db_manager.save_player_news('p2', [
            {'news_date': '2025-01-24T10:00:00', 'headline': 'P2 news'}
        ])

        # Log sync
        sync_id = db_manager.log_sync_start('player_news', config.league_id)
        db_manager.log_sync_complete(sync_id, 10)

        result = cache_manager.get_news_for_roster('team1', limit_per_player=5)
        assert result.from_cache is True
        assert len(result.data) == 2
        assert 'p1' in result.data
        assert 'p2' in result.data

    def test_get_all_player_news_empty_cache(self, cache_manager):
        """Test getting all player news when cache is empty."""
        result = cache_manager.get_all_player_news()
        assert result.from_cache is False

    def test_get_all_player_news_with_data(self, cache_manager, db_manager, config):
        """Test getting all player news."""
        db_manager.save_players([
            {'id': 'p1', 'name': 'Player 1'},
            {'id': 'p2', 'name': 'Player 2'},
        ])
        db_manager.save_player_news('p1', [
            {'news_date': '2025-01-25T10:00:00', 'headline': 'Latest news'}
        ])
        db_manager.save_player_news('p2', [
            {'news_date': '2025-01-24T10:00:00', 'headline': 'Older news'}
        ])

        sync_id = db_manager.log_sync_start('player_news', config.league_id)
        db_manager.log_sync_complete(sync_id, 10)

        result = cache_manager.get_all_player_news(limit=10)
        assert result.from_cache is True
        assert len(result.data) == 2
        # Should be sorted by date descending
        assert result.data[0]['headline'] == 'Latest news'
