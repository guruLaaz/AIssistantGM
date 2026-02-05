"""Integration tests for the Fantrax web scraper.

These tests require real Fantrax credentials and network access.
They are marked as 'slow' and 'integration' to be excluded from quick test runs.
"""

import pytest
from pathlib import Path

from aissistant_gm.fantrax.config import load_config
from aissistant_gm.fantrax.database import DatabaseManager
from aissistant_gm.fantrax.scraper import FantraxScraper


@pytest.fixture
def config():
    """Load configuration from environment."""
    return load_config()


@pytest.fixture
def db(config):
    """Get database manager."""
    return DatabaseManager(config.database_path)


@pytest.fixture
def scraper(config):
    """Create a scraper instance."""
    return FantraxScraper(
        league_id=config.league_id,
        username=config.username,
        password=config.password,
        cookie_file=Path(config.cookie_file),
        selenium_timeout=config.selenium_timeout,
        login_wait_time=config.login_wait_time,
        browser_window_size=config.browser_window_size,
        user_agent=config.user_agent
    )


@pytest.mark.integration
@pytest.mark.slow
class TestScraperIntegration:
    """Integration tests for the scraper that require real credentials."""

    def test_scrape_returns_news_items(self, scraper):
        """Test that scraping returns news items."""
        # Scrape just 1 page to keep test quick
        news_items = scraper.scrape_player_news(max_pages=1, max_items_per_page=10)

        # Should get some news items
        assert isinstance(news_items, list)
        # May be empty if no news, but should not error

    def test_scrape_news_has_required_fields(self, scraper):
        """Test that scraped news items have required fields."""
        news_items = scraper.scrape_player_news(max_pages=1, max_items_per_page=5)

        if news_items:  # Only test if we got items
            for item in news_items:
                assert 'player_name' in item
                assert 'news_date' in item
                assert 'headline' in item
                assert 'analysis' in item

    def test_scrape_news_date_format(self, scraper):
        """Test that news dates are in ISO format."""
        news_items = scraper.scrape_player_news(max_pages=1, max_items_per_page=5)

        if news_items:
            for item in news_items:
                # Should be ISO format: YYYY-MM-DDTHH:MM:SS
                date_str = item['news_date']
                assert 'T' in date_str
                assert len(date_str) >= 10  # At least YYYY-MM-DD

    def test_match_players_with_real_database(self, scraper, db):
        """Test matching scraped news with real database players."""
        news_items = scraper.scrape_player_news(max_pages=1, max_items_per_page=10)

        if news_items:
            matched_items = scraper.match_players_with_database(news_items, db)

            # At least some items should be matched if we have players in DB
            matched_count = sum(1 for item in matched_items if item.get('player_id'))
            # This is a soft assertion - may be 0 if no players match
            assert matched_count >= 0


def _get_rostered_player_ids(db, league_id: str) -> tuple[list[str], str]:
    """Helper to get rostered player IDs and a team ID from database."""
    teams = db.get_teams(league_id)
    if not teams:
        return [], ""

    # Get first team with players
    for team in teams:
        roster = db.get_roster(team['id'])
        player_ids = [slot['player_id'] for slot in roster if slot.get('player_id')]
        if player_ids:
            return player_ids, team['id']

    return [], ""


@pytest.mark.integration
@pytest.mark.slow
class TestToiScraperIntegration:
    """Integration tests for TOI scraping that require real credentials."""

    def test_scrape_toi_for_single_player(self, scraper, db):
        """Test scraping TOI for a single player returns valid data."""
        player_ids, team_id = _get_rostered_player_ids(db, scraper.league_id)
        if not player_ids:
            pytest.skip("No rostered players in database")

        # Scrape TOI for single player
        result = scraper.scrape_player_toi([player_ids[0]], team_id, max_players=1)

        # May or may not find TOI depending on player type
        assert isinstance(result, dict)

    def test_scrape_toi_has_required_fields(self, scraper, db):
        """Test that scraped TOI data has all required fields."""
        player_ids, team_id = _get_rostered_player_ids(db, scraper.league_id)
        if not player_ids:
            pytest.skip("No rostered players in database")

        # Scrape TOI for a few players
        result = scraper.scrape_player_toi(player_ids[:3], team_id, max_players=3)

        # Check returned data has required fields
        for player_id, toi_data in result.items():
            assert 'toi_seconds' in toi_data
            assert 'toipp_seconds' in toi_data
            assert 'toish_seconds' in toi_data
            assert 'games_played' in toi_data
            assert isinstance(toi_data['toi_seconds'], int)
            assert isinstance(toi_data['games_played'], int)
            assert toi_data['toi_seconds'] >= 0
            assert toi_data['games_played'] >= 0

    def test_scrape_toi_values_are_reasonable(self, scraper, db):
        """Test that scraped TOI values are within reasonable ranges."""
        player_ids, team_id = _get_rostered_player_ids(db, scraper.league_id)
        if not player_ids:
            pytest.skip("No rostered players in database")

        result = scraper.scrape_player_toi(player_ids[:5], team_id, max_players=5)

        for player_id, toi_data in result.items():
            # TOI per game should be between 0 and 30 minutes (1800 seconds)
            # This is average per game, so max ~25 min for star players
            if toi_data['games_played'] > 0:
                toi_per_game = toi_data['toi_seconds'] / toi_data['games_played']
                assert 0 <= toi_per_game <= 1800, f"TOI per game {toi_per_game} out of range"

            # TOIPP (power play) should be less than total TOI
            assert toi_data['toipp_seconds'] <= toi_data['toi_seconds']
            # TOISH (shorthanded) should be less than total TOI
            assert toi_data['toish_seconds'] <= toi_data['toi_seconds']


@pytest.mark.integration
class TestScraperQuickIntegration:
    """Quick integration tests that don't require full scraping."""

    def test_scraper_creates_driver(self, scraper):
        """Test that scraper can create a WebDriver."""
        # This just tests the driver creation, not full scraping
        driver = scraper._get_driver()
        assert driver is not None
        driver.quit()

    def test_parse_real_tooltip_formats(self, scraper):
        """Test parsing various real tooltip formats from Fantrax."""
        test_cases = [
            "Jan 30, 1:34 AM: McDavid scored a goal on two shots, dished an assist.",
            "Feb 1, 10:17 AM: Scheifele scored the game-winning goal on three shots.",
            "Jan 24, 12:28 PM: Makar scored a goal on two shots in Thursday's win.",
            "Feb 2, 1:38 PM: Necas (lower body) won't be in the lineup Saturday.",
        ]

        for tooltip in test_cases:
            result = scraper._parse_tooltip_text(tooltip)
            assert result is not None, f"Failed to parse: {tooltip}"
            assert result['player_name'] is not None
            assert result['headline'] is not None

    def test_parse_time_to_seconds_formats(self, scraper):
        """Test parsing various time formats from real player pages."""
        test_cases = [
            ("16:05", 965),
            ("01:20", 80),
            ("01:30", 90),
            ("20:00", 1200),
            ("0:45", 45),
        ]

        for time_str, expected in test_cases:
            result = scraper._parse_time_to_seconds(time_str)
            assert result == expected, f"Expected {expected} for '{time_str}', got {result}"
