"""Shared fixtures for fantrax unit tests."""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock
from typing import Any, Optional

from aissistant_gm.fantrax.config import Config
from aissistant_gm.fantrax.cache import CacheResult
from aissistant_gm.fantrax.database import DatabaseManager


# Test constants
TEST_USERNAME = "test@example.com"
TEST_PASSWORD = "testpass"
TEST_LEAGUE_ID = "test_league_123"
TEST_TEAM_ID = "team_001"
TEST_TEAM_NAME = "Test Team"
TEST_PLAYER_ID = "player_001"
TEST_PLAYER_NAME = "John Doe"


@pytest.fixture
def mock_config():
    """Create a Config instance with test values."""
    return Config(
        username=TEST_USERNAME,
        password=TEST_PASSWORD,
        league_id=TEST_LEAGUE_ID,
        cookie_file="test_cookie.cookie",
        min_request_interval=0.0,  # No delay in tests
        cache_enabled=True,
        cache_max_age_hours=24.0,
        fa_news_limit=100,
        fa_fetch_limit=100,
        sync_days_scores=7,
        max_news_per_player=10,
        news_days_back=7,
        selenium_timeout=1,
        login_wait_time=0,
        browser_window_size="800,600",
        user_agent="Test User Agent",
        scraper_max_retries=1,
        scraper_retry_delay=0.0,
        scraper_retry_backoff=1.0,
    )


@pytest.fixture
def mock_typer_context():
    """Create a mock typer.Context with obj dict."""
    import typer
    ctx = Mock(spec=typer.Context)
    ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}
    return ctx


@pytest.fixture
def mock_console():
    """Create a mock Rich Console."""
    console = Mock()
    console.print = Mock()
    console.status = MagicMock()
    console.status.return_value.__enter__ = Mock(return_value=None)
    console.status.return_value.__exit__ = Mock(return_value=None)
    return console


@pytest.fixture
def mock_team():
    """Create a mock team object."""
    team = Mock()
    team.id = TEST_TEAM_ID
    team.name = TEST_TEAM_NAME
    team.short_name = "TST"
    return team


@pytest.fixture
def mock_player():
    """Create a mock player object."""
    player = Mock()
    player.id = TEST_PLAYER_ID
    player.name = TEST_PLAYER_NAME
    player.team = Mock()
    player.team.name = "NHL Team"
    player.position = Mock()
    player.position.name = "C"
    player.points = 10.5
    player.status = Mock()
    player.status.id = 1
    player.status.name = "Active"
    return player


@pytest.fixture
def mock_league(mock_team):
    """Create a mock League object."""
    league = Mock()
    league.league_id = TEST_LEAGUE_ID
    league.name = "Test League"
    league.teams = [mock_team]
    league.my_team = mock_team
    league.session = Mock()
    league.scoring_dates = {}
    return league


@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database path."""
    return tmp_path / "test_fantrax.db"


@pytest.fixture
def mock_database_manager(temp_db_path):
    """Create a real DatabaseManager with temp database."""
    db = DatabaseManager(db_path=temp_db_path)
    return db


@pytest.fixture
def mock_cache_manager():
    """Create a mock CacheManager."""
    cache = Mock()
    cache.is_cache_enabled.return_value = True
    cache.is_fresh.return_value = True
    cache.get_cache_age.return_value = 1.0
    return cache


# Sample data fixtures
@pytest.fixture
def sample_team_data():
    """Sample team data as returned from database."""
    return {
        'id': TEST_TEAM_ID,
        'league_id': TEST_LEAGUE_ID,
        'name': TEST_TEAM_NAME,
        'short_name': 'TST',
        'logo_url': 'https://example.com/logo.png',
        'is_my_team': True,
    }


@pytest.fixture
def sample_player_data():
    """Sample player data as returned from database."""
    return {
        'id': TEST_PLAYER_ID,
        'name': TEST_PLAYER_NAME,
        'team': 'NHL Team',
        'position': 'C',
        'status_id': 1,
        'injury_status': None,
        'injury_details': None,
    }


@pytest.fixture
def sample_standings_data():
    """Sample standings data."""
    return {
        TEST_TEAM_ID: {
            'rank': 1,
            'wins': 10,
            'losses': 5,
            'ties': 2,
            'points_for': 150.5,
            'games_played': 17,
        }
    }


@pytest.fixture
def sample_roster_data(sample_player_data):
    """Sample roster data as returned from database."""
    return [{
        'team_id': TEST_TEAM_ID,
        'player_id': TEST_PLAYER_ID,
        'player_name': TEST_PLAYER_NAME,
        'roster_slot': 'C',
        'acquisition_type': 'Draft',
        **sample_player_data
    }]


@pytest.fixture
def sample_news_data():
    """Sample news data as returned from database."""
    return [{
        'id': 1,
        'player_id': TEST_PLAYER_ID,
        'player_name': TEST_PLAYER_NAME,
        'headline': 'Player signed new contract',
        'content': 'Full news content here...',
        'news_date': '2024-01-15T10:00:00Z',
        'source': 'NHL',
    }]


@pytest.fixture
def sample_trends_data():
    """Sample trends data."""
    return {
        TEST_PLAYER_ID: {
            'week1_fpts': 15.5,
            'week1_fpg': 2.2,
            'week2_fpts': 12.0,
            'week2_fpg': 2.0,
            'week3_fpts': 18.0,
            'week3_fpg': 2.6,
            '14d_fpg': 2.3,
            '30d_fpg': 2.1,
        }
    }


# Helper functions
def setup_cache_hit(mock_cache, method_name: str, data: Any,
                    cache_age_hours: float = 1.0, stale: bool = False):
    """Configure a cache manager mock to return a cache hit."""
    result = CacheResult(
        data=data,
        from_cache=True,
        cache_age_hours=cache_age_hours,
        stale=stale
    )
    getattr(mock_cache, method_name).return_value = result
    return result


def setup_cache_miss(mock_cache, method_name: str):
    """Configure a cache manager mock to return a cache miss."""
    result = CacheResult(
        data=None,
        from_cache=False,
        cache_age_hours=None,
        stale=False
    )
    getattr(mock_cache, method_name).return_value = result
    return result
