"""Tests for config module."""

import os
import pytest
from pathlib import Path
from fantrax_cli.config import Config, load_config


# Test constants
TEST_USERNAME = "test@example.com"
TEST_PASSWORD = "testpass"
TEST_LEAGUE_ID = "test123"
TEST_LEAGUE_ID_ENV = "env123"
TEST_LEAGUE_ID_OVERRIDE = "override456"
DEFAULT_COOKIE_FILE = "fantraxloggedin.cookie"
CUSTOM_COOKIE_FILE = "custom.cookie"
DEFAULT_MIN_REQUEST_INTERVAL = 1.0
CUSTOM_MIN_REQUEST_INTERVAL = 2.5


class TestConfig:
    """Test Config dataclass."""

    def test_config_creation(self):
        """Test creating a Config object."""
        config = Config(
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            league_id=TEST_LEAGUE_ID
        )
        assert config.username == TEST_USERNAME
        assert config.password == TEST_PASSWORD
        assert config.league_id == TEST_LEAGUE_ID
        assert config.cookie_file == DEFAULT_COOKIE_FILE
        assert config.min_request_interval == DEFAULT_MIN_REQUEST_INTERVAL

    def test_config_with_custom_values(self):
        """Test Config with custom cookie file and rate limit."""
        config = Config(
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            league_id=TEST_LEAGUE_ID,
            cookie_file=CUSTOM_COOKIE_FILE,
            min_request_interval=CUSTOM_MIN_REQUEST_INTERVAL
        )
        assert config.cookie_file == CUSTOM_COOKIE_FILE
        assert config.min_request_interval == CUSTOM_MIN_REQUEST_INTERVAL

    def test_cookie_path_property(self):
        """Test cookie_path property returns correct Path."""
        config = Config(
            username=TEST_USERNAME,
            password=TEST_PASSWORD,
            league_id=TEST_LEAGUE_ID
        )
        expected_path = Path.cwd() / DEFAULT_COOKIE_FILE
        assert config.cookie_path == expected_path


class TestLoadConfig:
    """Test load_config function."""

    def test_load_config_with_all_vars(self, monkeypatch):
        """Test loading config with all required environment variables."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.username == TEST_USERNAME
        assert config.password == TEST_PASSWORD
        assert config.league_id == TEST_LEAGUE_ID

    def test_load_config_with_optional_vars(self, monkeypatch):
        """Test loading config with optional environment variables."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_COOKIE_FILE", CUSTOM_COOKIE_FILE)
        monkeypatch.setenv("FANTRAX_MIN_REQUEST_INTERVAL", str(CUSTOM_MIN_REQUEST_INTERVAL))

        config = load_config()

        assert config.cookie_file == CUSTOM_COOKIE_FILE
        assert config.min_request_interval == CUSTOM_MIN_REQUEST_INTERVAL

    def test_load_config_with_league_id_override(self, monkeypatch):
        """Test that league_id parameter overrides environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID_ENV)

        config = load_config(league_id=TEST_LEAGUE_ID_OVERRIDE)

        assert config.league_id == TEST_LEAGUE_ID_OVERRIDE

    def test_load_config_missing_username(self, monkeypatch):
        """Test that missing username raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        # Clear and set environment variables
        monkeypatch.delenv("FANTRAX_USERNAME", raising=False)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "FANTRAX_USERNAME" in str(exc_info.value)

    def test_load_config_missing_password(self, monkeypatch):
        """Test that missing password raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.delenv("FANTRAX_PASSWORD", raising=False)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "FANTRAX_PASSWORD" in str(exc_info.value)

    def test_load_config_missing_league_id(self, monkeypatch):
        """Test that missing league_id raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.delenv("FANTRAX_LEAGUE_ID", raising=False)

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "FANTRAX_LEAGUE_ID" in str(exc_info.value)

    def test_load_config_missing_multiple_vars(self, monkeypatch):
        """Test error message lists all missing variables."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        # Clear all environment variables
        monkeypatch.delenv("FANTRAX_USERNAME", raising=False)
        monkeypatch.delenv("FANTRAX_PASSWORD", raising=False)
        monkeypatch.delenv("FANTRAX_LEAGUE_ID", raising=False)

        with pytest.raises(ValueError) as exc_info:
            load_config()

        error_msg = str(exc_info.value)
        assert "FANTRAX_USERNAME" in error_msg
        assert "FANTRAX_PASSWORD" in error_msg
        assert "FANTRAX_LEAGUE_ID" in error_msg

    def test_load_config_fa_news_limit_default(self, monkeypatch):
        """Test fa_news_limit has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.fa_news_limit == 500

    def test_load_config_fa_news_limit_custom(self, monkeypatch):
        """Test fa_news_limit can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_FA_NEWS_LIMIT", "100")

        config = load_config()

        assert config.fa_news_limit == 100

    def test_load_config_fa_news_limit_disabled(self, monkeypatch):
        """Test fa_news_limit can be disabled with 0."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_FA_NEWS_LIMIT", "0")

        config = load_config()

        assert config.fa_news_limit == 0

    # Tests for sync settings

    def test_load_config_fa_fetch_limit_default(self, monkeypatch):
        """Test fa_fetch_limit has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.fa_fetch_limit == 5000

    def test_load_config_fa_fetch_limit_custom(self, monkeypatch):
        """Test fa_fetch_limit can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_FA_FETCH_LIMIT", "1000")

        config = load_config()

        assert config.fa_fetch_limit == 1000

    def test_load_config_sync_days_scores_default(self, monkeypatch):
        """Test sync_days_scores has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.sync_days_scores == 35

    def test_load_config_sync_days_scores_custom(self, monkeypatch):
        """Test sync_days_scores can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_SYNC_DAYS_SCORES", "14")

        config = load_config()

        assert config.sync_days_scores == 14

    def test_load_config_max_news_per_player_default(self, monkeypatch):
        """Test max_news_per_player has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.max_news_per_player == 30

    def test_load_config_max_news_per_player_custom(self, monkeypatch):
        """Test max_news_per_player can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_MAX_NEWS_PER_PLAYER", "50")

        config = load_config()

        assert config.max_news_per_player == 50

    # Tests for browser automation settings

    def test_load_config_selenium_timeout_default(self, monkeypatch):
        """Test selenium_timeout has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.selenium_timeout == 10

    def test_load_config_selenium_timeout_custom(self, monkeypatch):
        """Test selenium_timeout can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_SELENIUM_TIMEOUT", "20")

        config = load_config()

        assert config.selenium_timeout == 20

    def test_load_config_login_wait_time_default(self, monkeypatch):
        """Test login_wait_time has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.login_wait_time == 5

    def test_load_config_login_wait_time_custom(self, monkeypatch):
        """Test login_wait_time can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_LOGIN_WAIT_TIME", "10")

        config = load_config()

        assert config.login_wait_time == 10

    def test_load_config_browser_window_size_default(self, monkeypatch):
        """Test browser_window_size has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert config.browser_window_size == "1920,1600"

    def test_load_config_browser_window_size_custom(self, monkeypatch):
        """Test browser_window_size can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_BROWSER_WINDOW_SIZE", "1280,720")

        config = load_config()

        assert config.browser_window_size == "1280,720"

    def test_load_config_user_agent_default(self, monkeypatch):
        """Test user_agent has correct default value."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)

        config = load_config()

        assert "Chrome" in config.user_agent
        assert "Mozilla" in config.user_agent

    def test_load_config_user_agent_custom(self, monkeypatch):
        """Test user_agent can be set via environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", TEST_USERNAME)
        monkeypatch.setenv("FANTRAX_PASSWORD", TEST_PASSWORD)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", TEST_LEAGUE_ID)
        monkeypatch.setenv("FANTRAX_USER_AGENT", "Custom User Agent")

        config = load_config()

        assert config.user_agent == "Custom User Agent"
