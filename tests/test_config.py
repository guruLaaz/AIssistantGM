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
