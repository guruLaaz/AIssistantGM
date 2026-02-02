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
    # News sync settings
    fa_news_limit: int = 500  # Max free agents to sync news for (0 = disabled)
    # Sync settings
    fa_fetch_limit: int = 5000  # Free agents to fetch
    sync_days_scores: int = 35  # Days of daily scores to sync
    max_news_per_player: int = 30  # Max news items kept per player
    # Browser automation settings
    selenium_timeout: int = 10  # Seconds to wait for page elements
    login_wait_time: int = 5  # Seconds to wait after login
    browser_window_size: str = "1920,1600"  # Chrome window dimensions
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"

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

    # News sync configuration
    fa_news_limit = int(os.getenv("FANTRAX_FA_NEWS_LIMIT", "500"))

    # Sync settings
    fa_fetch_limit = int(os.getenv("FANTRAX_FA_FETCH_LIMIT", "5000"))
    sync_days_scores = int(os.getenv("FANTRAX_SYNC_DAYS_SCORES", "35"))
    max_news_per_player = int(os.getenv("FANTRAX_MAX_NEWS_PER_PLAYER", "30"))

    # Browser automation settings
    selenium_timeout = int(os.getenv("FANTRAX_SELENIUM_TIMEOUT", "10"))
    login_wait_time = int(os.getenv("FANTRAX_LOGIN_WAIT_TIME", "5"))
    browser_window_size = os.getenv("FANTRAX_BROWSER_WINDOW_SIZE", "1920,1600")
    user_agent = os.getenv(
        "FANTRAX_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )

    return Config(
        username=username,
        password=password,
        league_id=final_league_id,
        cookie_file=cookie_file,
        min_request_interval=min_request_interval,
        db_path=db_path,
        cache_enabled=cache_enabled,
        cache_max_age_hours=cache_max_age_hours,
        fa_news_limit=fa_news_limit,
        fa_fetch_limit=fa_fetch_limit,
        sync_days_scores=sync_days_scores,
        max_news_per_player=max_news_per_player,
        selenium_timeout=selenium_timeout,
        login_wait_time=login_wait_time,
        browser_window_size=browser_window_size,
        user_agent=user_agent
    )
