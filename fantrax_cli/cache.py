"""Cache manager for accessing cached Fantrax data with fallback logic."""

from datetime import date, timedelta
from typing import Optional, Any
from dataclasses import dataclass

from fantrax_cli.database import DatabaseManager, is_cache_fresh, get_cache_age_hours
from fantrax_cli.config import Config


# Default cache freshness thresholds (in hours)
CACHE_MAX_AGE = {
    'league_metadata': 168.0,   # 1 week (rarely changes)
    'teams': 168.0,             # 1 week (rarely changes)
    'standings': 12.0,          # 12 hours (changes after games)
    'rosters': 12.0,            # 12 hours
    'daily_scores': 24.0,       # 1 day
    'trends': 24.0,             # 1 day
    'free_agents': 12.0,        # 12 hours
}


@dataclass
class CacheResult:
    """Result from a cache lookup."""
    data: Any
    from_cache: bool
    cache_age_hours: Optional[float] = None
    stale: bool = False


class CacheManager:
    """
    Provides cached data access with fallback to API.

    This class serves as the high-level interface for accessing cached data.
    It checks cache freshness and provides appropriate data or flags when
    the cache is stale.
    """

    def __init__(self, db: DatabaseManager, config: Config):
        """
        Initialize cache manager.

        Args:
            db: DatabaseManager instance
            config: Application configuration
        """
        self.db = db
        self.config = config
        self._cache_enabled = config.cache_enabled
        self._max_age_override = config.cache_max_age_hours

    def is_cache_enabled(self) -> bool:
        """Check if caching is enabled."""
        return self._cache_enabled

    def get_max_age(self, cache_type: str) -> float:
        """Get the max age for a cache type, considering config overrides."""
        default = CACHE_MAX_AGE.get(cache_type, 24.0)
        # If user specified a global max age, use the minimum of default and override
        if self._max_age_override and self._max_age_override < default:
            return self._max_age_override
        return default

    def is_fresh(self, cache_type: str, last_sync_at: Optional[str] = None) -> bool:
        """
        Check if a specific cache type is fresh.

        Args:
            cache_type: Type of cache ('teams', 'rosters', 'trends', etc.)
            last_sync_at: Optional explicit last sync time. If not provided,
                          will look up from sync log.

        Returns:
            True if cache is fresh, False if stale or missing
        """
        if not self._cache_enabled:
            return False

        if last_sync_at:
            return is_cache_fresh(last_sync_at, self.get_max_age(cache_type))

        # Look up from sync log
        last_sync = self.db.get_last_sync(self.config.league_id, cache_type)
        if not last_sync:
            return False

        return is_cache_fresh(last_sync['completed_at'], self.get_max_age(cache_type))

    def get_cache_age(self, cache_type: str) -> Optional[float]:
        """
        Get the age of a cache type in hours.

        Args:
            cache_type: Type of cache

        Returns:
            Age in hours, or None if no cache exists
        """
        last_sync = self.db.get_last_sync(self.config.league_id, cache_type)
        if not last_sync:
            return None
        return get_cache_age_hours(last_sync['completed_at'])

    # ==================== Teams ====================

    def get_teams(self, force_refresh: bool = False) -> CacheResult:
        """
        Get teams from cache.

        Args:
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with teams data and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        teams = self.db.get_teams(self.config.league_id)
        if not teams:
            return CacheResult(data=None, from_cache=False)

        # Check freshness
        last_sync = self.db.get_last_sync(self.config.league_id, 'teams')
        if not last_sync:
            # Data exists but no sync record - use it but mark as potentially stale
            return CacheResult(
                data=teams,
                from_cache=True,
                stale=True
            )

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('teams'))

        return CacheResult(
            data=teams,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    def get_team_by_identifier(self, identifier: str) -> Optional[dict]:
        """
        Get a team by ID, name, or short name from cache.

        Args:
            identifier: Team ID, name, or short name

        Returns:
            Team dict or None if not found
        """
        return self.db.get_team_by_identifier(self.config.league_id, identifier)

    # ==================== Standings ====================

    def get_standings(self, force_refresh: bool = False) -> CacheResult:
        """
        Get standings from cache.

        Args:
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with standings data and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        standings = self.db.get_standings(self.config.league_id)
        if not standings:
            return CacheResult(data=None, from_cache=False)

        # Check freshness
        last_sync = self.db.get_last_sync(self.config.league_id, 'standings')
        if not last_sync:
            # Data exists but no sync record - use it but mark as potentially stale
            return CacheResult(
                data=standings,
                from_cache=True,
                stale=True
            )

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('standings'))

        return CacheResult(
            data=standings,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    def get_teams_with_standings(self, force_refresh: bool = False) -> CacheResult:
        """
        Get teams with their standings info from cache.

        Args:
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with teams+standings data ordered by rank
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        teams = self.db.get_teams_with_standings(self.config.league_id)
        if not teams:
            return CacheResult(data=None, from_cache=False)

        # Check freshness - use standings freshness
        last_sync = self.db.get_last_sync(self.config.league_id, 'standings')
        if not last_sync:
            # Check teams sync instead
            last_sync = self.db.get_last_sync(self.config.league_id, 'teams')

        if not last_sync:
            return CacheResult(data=teams, from_cache=True, stale=True)

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('standings'))

        return CacheResult(
            data=teams,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    # ==================== Rosters ====================

    def get_roster(self, team_id: str, force_refresh: bool = False) -> CacheResult:
        """
        Get roster for a team from cache.

        Args:
            team_id: Team ID
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with roster data and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        roster = self.db.get_roster(team_id)
        if not roster:
            return CacheResult(data=None, from_cache=False)

        # Check freshness based on roster sync
        last_sync = self.db.get_last_sync(self.config.league_id, 'rosters')
        if not last_sync:
            return CacheResult(data=roster, from_cache=True, stale=True)

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('rosters'))

        return CacheResult(
            data=roster,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    def get_player(self, player_id: str) -> Optional[dict]:
        """Get a player from cache."""
        return self.db.get_player(player_id)

    def get_players_by_ids(self, player_ids: list[str]) -> dict[str, dict]:
        """Get multiple players from cache."""
        return self.db.get_players_by_ids(player_ids)

    # ==================== Trends ====================

    def get_player_trends(
        self,
        team_id: str,
        force_refresh: bool = False
    ) -> CacheResult:
        """
        Get player trends for a team from cache.

        Args:
            team_id: Team ID
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with trends data (player_id -> trend dict) and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        # Get roster to find player IDs
        roster = self.db.get_roster(team_id)
        if not roster:
            return CacheResult(data=None, from_cache=False)

        player_ids = [r['player_id'] for r in roster if r.get('player_id')]
        if not player_ids:
            return CacheResult(data={}, from_cache=True)

        # Get trends for all players
        trends = self.db.get_trends_for_players(player_ids)

        # Check freshness
        last_sync = self.db.get_last_sync(self.config.league_id, 'trends')
        if not last_sync:
            # Check if we have daily_scores sync instead
            last_sync = self.db.get_last_sync(self.config.league_id, 'daily_scores')

        if not last_sync:
            return CacheResult(data=trends, from_cache=True, stale=True) if trends else CacheResult(data=None, from_cache=False)

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('trends'))

        return CacheResult(
            data=trends,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    def get_trends_for_player(self, player_id: str) -> Optional[dict]:
        """Get trends for a specific player from cache."""
        return self.db.get_player_trends(player_id)

    # ==================== Free Agents ====================

    def get_free_agents(
        self,
        sort_key: str = 'SCORE',
        position_filter: Optional[str] = None,
        limit: int = 25,
        force_refresh: bool = False
    ) -> CacheResult:
        """
        Get free agents from cache.

        Args:
            sort_key: Sort key used for the listing
            position_filter: Position filter used
            limit: Maximum number of results
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with free agent data and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        fa_list = self.db.get_free_agents(sort_key, position_filter, limit)
        if not fa_list:
            return CacheResult(data=None, from_cache=False)

        # Check freshness
        last_sync = self.db.get_last_sync(self.config.league_id, 'free_agents')
        if not last_sync:
            return CacheResult(data=fa_list, from_cache=True, stale=True)

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('free_agents'))

        return CacheResult(
            data=fa_list,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    def get_fa_trends(
        self,
        player_ids: list[str],
        force_refresh: bool = False
    ) -> CacheResult:
        """
        Get trends for free agent players from cache.

        Args:
            player_ids: List of player IDs to get trends for
            force_refresh: If True, indicates caller wants fresh data

        Returns:
            CacheResult with trends data and cache status
        """
        if not self._cache_enabled or force_refresh:
            return CacheResult(data=None, from_cache=False)

        if not player_ids:
            return CacheResult(data={}, from_cache=True)

        trends = self.db.get_trends_for_players(player_ids)
        if not trends:
            return CacheResult(data=None, from_cache=False)

        # Check freshness - FA trends would come from a free_agents sync
        last_sync = self.db.get_last_sync(self.config.league_id, 'free_agents')
        if not last_sync:
            return CacheResult(data=trends, from_cache=True, stale=True)

        age = get_cache_age_hours(last_sync['completed_at'])
        is_stale = not is_cache_fresh(last_sync['completed_at'], self.get_max_age('free_agents'))

        return CacheResult(
            data=trends,
            from_cache=True,
            cache_age_hours=age,
            stale=is_stale
        )

    # ==================== Daily Scores ====================

    def get_daily_scores_for_team(
        self,
        team_id: str,
        start_date: date,
        end_date: date
    ) -> list[dict]:
        """Get daily scores for a team from cache."""
        return self.db.get_daily_scores_for_team(team_id, start_date, end_date)

    def get_daily_scores_for_player(
        self,
        player_id: str,
        start_date: date,
        end_date: date
    ) -> list[dict]:
        """Get daily scores for a player from cache."""
        return self.db.get_daily_scores_for_player(player_id, start_date, end_date)

    def has_daily_scores_for_range(self, start_date: date, end_date: date) -> bool:
        """
        Check if we have daily scores cached for the given date range.

        Args:
            start_date: Start of range
            end_date: End of range

        Returns:
            True if cache covers the entire range
        """
        date_range = self.db.get_daily_scores_date_range()
        if not date_range:
            return False

        cached_start = date.fromisoformat(date_range[0])
        cached_end = date.fromisoformat(date_range[1])

        return cached_start <= start_date and cached_end >= end_date

    # ==================== League Metadata ====================

    def get_league_metadata(self) -> Optional[dict]:
        """Get league metadata from cache."""
        return self.db.get_league_metadata(self.config.league_id)

    def get_league_name(self) -> Optional[str]:
        """Get league name from cache."""
        meta = self.get_league_metadata()
        return meta['name'] if meta else None


def format_cache_age(age_hours: Optional[float]) -> str:
    """
    Format cache age as a human-readable string.

    Args:
        age_hours: Age in hours, or None

    Returns:
        Formatted string like "5 min ago", "2.5 hours ago", "3 days ago"
    """
    if age_hours is None:
        return "unknown"

    if age_hours < 1:
        minutes = int(age_hours * 60)
        return f"{minutes} min ago"
    elif age_hours < 24:
        return f"{age_hours:.1f} hours ago"
    else:
        days = age_hours / 24
        return f"{days:.1f} days ago"
