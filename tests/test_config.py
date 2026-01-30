"""Tests for config module."""

import os
import pytest
from pathlib import Path
from fantrax_cli.config import Config, load_config


class TestConfig:
    """Test Config dataclass."""

    def test_config_creation(self):
        """Test creating a Config object."""
        config = Config(
            username="test@example.com",
            password="testpass",
            league_id="test123"
        )
        assert config.username == "test@example.com"
        assert config.password == "testpass"
        assert config.league_id == "test123"
        assert config.cookie_file == "fantraxloggedin.cookie"
        assert config.min_request_interval == 1.0

    def test_config_with_custom_values(self):
        """Test Config with custom cookie file and rate limit."""
        config = Config(
            username="test@example.com",
            password="testpass",
            league_id="test123",
            cookie_file="custom.cookie",
            min_request_interval=2.5
        )
        assert config.cookie_file == "custom.cookie"
        assert config.min_request_interval == 2.5

    def test_cookie_path_property(self):
        """Test cookie_path property returns correct Path."""
        config = Config(
            username="test@example.com",
            password="testpass",
            league_id="test123"
        )
        expected_path = Path.cwd() / "fantraxloggedin.cookie"
        assert config.cookie_path == expected_path


class TestLoadConfig:
    """Test load_config function."""

    def test_load_config_with_all_vars(self, monkeypatch):
        """Test loading config with all required environment variables."""
        monkeypatch.setenv("FANTRAX_USERNAME", "test@example.com")
        monkeypatch.setenv("FANTRAX_PASSWORD", "testpass")
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "test123")

        config = load_config()

        assert config.username == "test@example.com"
        assert config.password == "testpass"
        assert config.league_id == "test123"

    def test_load_config_with_optional_vars(self, monkeypatch):
        """Test loading config with optional environment variables."""
        monkeypatch.setenv("FANTRAX_USERNAME", "test@example.com")
        monkeypatch.setenv("FANTRAX_PASSWORD", "testpass")
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "test123")
        monkeypatch.setenv("FANTRAX_COOKIE_FILE", "custom.cookie")
        monkeypatch.setenv("FANTRAX_MIN_REQUEST_INTERVAL", "2.5")

        config = load_config()

        assert config.cookie_file == "custom.cookie"
        assert config.min_request_interval == 2.5

    def test_load_config_with_league_id_override(self, monkeypatch):
        """Test that league_id parameter overrides environment variable."""
        monkeypatch.setenv("FANTRAX_USERNAME", "test@example.com")
        monkeypatch.setenv("FANTRAX_PASSWORD", "testpass")
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "env123")

        config = load_config(league_id="override456")

        assert config.league_id == "override456"

    def test_load_config_missing_username(self, monkeypatch):
        """Test that missing username raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        # Clear and set environment variables
        monkeypatch.delenv("FANTRAX_USERNAME", raising=False)
        monkeypatch.setenv("FANTRAX_PASSWORD", "testpass")
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "test123")

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "FANTRAX_USERNAME" in str(exc_info.value)

    def test_load_config_missing_password(self, monkeypatch):
        """Test that missing password raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        monkeypatch.setenv("FANTRAX_USERNAME", "test@example.com")
        monkeypatch.delenv("FANTRAX_PASSWORD", raising=False)
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "test123")

        with pytest.raises(ValueError) as exc_info:
            load_config()

        assert "FANTRAX_PASSWORD" in str(exc_info.value)

    def test_load_config_missing_league_id(self, monkeypatch):
        """Test that missing league_id raises ValueError."""
        # Mock load_dotenv to prevent loading from .env file
        monkeypatch.setattr("fantrax_cli.config.load_dotenv", lambda: None)
        monkeypatch.setenv("FANTRAX_USERNAME", "test@example.com")
        monkeypatch.setenv("FANTRAX_PASSWORD", "testpass")
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
