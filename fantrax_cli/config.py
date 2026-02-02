"""Configuration management for Fantrax CLI."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional

import platformdirs


@dataclass
class Config:
    """Configuration for Fantrax CLI."""

    username: str
    password: str
    league_id: str
    cookie_file: str = "fantraxloggedin.cookie"
    min_request_interval: float = 1.0  # Minimum seconds between API requests
    # Database/cache settings
    db_path: Optional[Path] = None  # None = use default location
    cache_enabled: bool = True
    cache_max_age_hours: float = 24.0

    @property
    def cookie_path(self) -> Path:
        """Get the full path to the cookie file."""
        return Path.cwd() / self.cookie_file

    @property
    def database_path(self) -> Path:
        """Get the full path to the database file."""
        if self.db_path:
            return self.db_path
        # Default: platform-specific data directory
        data_dir = Path(platformdirs.user_data_dir("fantrax-cli"))
        data_dir.mkdir(parents=True, exist_ok=True)
        return data_dir / "fantrax_cache.db"


def load_config(league_id: Optional[str] = None) -> Config:
    """
    Load configuration from environment variables.

    Args:
        league_id: Optional league ID override. If not provided, uses FANTRAX_LEAGUE_ID env var.

    Returns:
        Config object with loaded settings.

    Raises:
        ValueError: If required environment variables are missing.
    """
    # Load .env file if it exists
    load_dotenv()

    # Get required environment variables
    username = os.getenv("FANTRAX_USERNAME")
    password = os.getenv("FANTRAX_PASSWORD")
    env_league_id = os.getenv("FANTRAX_LEAGUE_ID")

    # Use provided league_id or fall back to environment variable
    final_league_id = league_id or env_league_id

    # Validate required fields
    missing_fields = []
    if not username:
        missing_fields.append("FANTRAX_USERNAME")
    if not password:
        missing_fields.append("FANTRAX_PASSWORD")
    if not final_league_id:
        missing_fields.append("FANTRAX_LEAGUE_ID")

    if missing_fields:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_fields)}\n"
            f"Please create a .env file with these variables or set them in your environment.\n"
            f"See .env.example for reference."
        )

    # Get optional configuration
    cookie_file = os.getenv("FANTRAX_COOKIE_FILE", "fantraxloggedin.cookie")
    min_request_interval = float(os.getenv("FANTRAX_MIN_REQUEST_INTERVAL", "1.0"))

    # Database/cache configuration
    db_path_str = os.getenv("FANTRAX_DB_PATH")
    db_path = Path(db_path_str) if db_path_str else None
    cache_enabled = os.getenv("FANTRAX_CACHE_ENABLED", "true").lower() in ("true", "1", "yes")
    cache_max_age_hours = float(os.getenv("FANTRAX_CACHE_MAX_AGE_HOURS", "24.0"))

    return Config(
        username=username,
        password=password,
        league_id=final_league_id,
        cookie_file=cookie_file,
        min_request_interval=min_request_interval,
        db_path=db_path,
        cache_enabled=cache_enabled,
        cache_max_age_hours=cache_max_age_hours
    )
