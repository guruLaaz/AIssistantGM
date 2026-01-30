"""Configuration management for Fantrax CLI."""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional


@dataclass
class Config:
    """Configuration for Fantrax CLI."""

    username: str
    password: str
    league_id: str
    cookie_file: str = "fantraxloggedin.cookie"

    @property
    def cookie_path(self) -> Path:
        """Get the full path to the cookie file."""
        return Path.cwd() / self.cookie_file


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

    return Config(
        username=username,
        password=password,
        league_id=final_league_id,
        cookie_file=os.getenv("FANTRAX_COOKIE_FILE", "fantraxloggedin.cookie")
    )
