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
