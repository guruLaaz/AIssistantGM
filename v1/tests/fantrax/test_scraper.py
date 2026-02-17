"""Unit tests for the Fantrax web scraper module."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from aissistant_gm.fantrax.scraper import FantraxScraper


class TestParseTooltipText:
    """Tests for the _parse_tooltip_text method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock()
        )

    def test_parse_tooltip_with_time(self, scraper):
        """Test parsing tooltip with full date and time."""
        tooltip = "Jan 30, 1:34 AM: McDavid scored a goal on two shots, dished an assist and went plus-2 in Tuesday's 4-1 win over the Flames."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert result['player_name'] == 'McDavid'
        assert '01-30' in result['news_date']
        assert 'McDavid scored a goal' in result['headline']
        assert result['player_id'] is None

    def test_parse_tooltip_different_player(self, scraper):
        """Test parsing tooltip for different player."""
        tooltip = "Feb 1, 3:36 AM: Draisaitl scored a goal on five shots and added two assists in a 5-2 victory."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert result['player_name'] == 'Draisaitl'
        assert '02-01' in result['news_date']

    def test_parse_tooltip_with_logged_verb(self, scraper):
        """Test parsing tooltip with 'logged' verb."""
        tooltip = "Feb 1, 3:06 PM: Suzuki logged two assists, two shots on goal in Thursday's win."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert result['player_name'] == 'Suzuki'

    def test_parse_tooltip_with_notched_verb(self, scraper):
        """Test parsing tooltip with 'notched' verb."""
        tooltip = "Jan 28, 12:45 AM: Rantanen notched a power-play assist in Monday's game."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert result['player_name'] == 'Rantanen'

    def test_parse_tooltip_injury_news(self, scraper):
        """Test parsing tooltip with injury news."""
        tooltip = "Feb 2, 1:38 PM: Necas (lower body) won't be in the lineup Saturday."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert result['player_name'] == 'Necas'
        assert "won't be in the lineup" in result['headline']

    def test_parse_tooltip_with_hyphenated_name(self, scraper):
        """Test parsing tooltip with hyphenated player name."""
        tooltip = "Jan 25, 10:00 AM: Tkachuk-Brady registered two assists in the game."
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        # Should capture hyphenated name or first part
        assert result['player_name'] is not None

    def test_parse_tooltip_invalid_format(self, scraper):
        """Test parsing tooltip with invalid format returns None."""
        tooltip = "This is not a valid tooltip format"
        result = scraper._parse_tooltip_text(tooltip)

        assert result is None

    def test_parse_tooltip_empty_string(self, scraper):
        """Test parsing empty string returns None."""
        result = scraper._parse_tooltip_text("")
        assert result is None

    def test_parse_tooltip_truncates_long_headline(self, scraper):
        """Test that headline is truncated to 500 chars."""
        long_content = "McDavid scored " + "a" * 600
        tooltip = f"Jan 30, 1:34 AM: {long_content}"
        result = scraper._parse_tooltip_text(tooltip)

        assert result is not None
        assert len(result['headline']) <= 500

    def test_parse_tooltip_future_date_adjusted_to_last_year(self, scraper):
        """Test that future dates are adjusted to previous year."""
        # Create a date that would be in the future
        future_month = (datetime.now().month % 12) + 1
        month_name = datetime(2000, future_month, 1).strftime("%b")
        tooltip = f"{month_name} 15, 10:00 AM: TestPlayer scored a goal."

        result = scraper._parse_tooltip_text(tooltip)

        if result:
            news_date = datetime.fromisoformat(result['news_date'])
            assert news_date <= datetime.now()


class TestMatchPlayersWithDatabase:
    """Tests for the match_players_with_database method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock()
        )

    @pytest.fixture
    def mock_db(self):
        """Create a mock database with player data."""
        mock_db = Mock()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Simulate player data
        mock_cursor.fetchall.return_value = [
            {'id': 'player1', 'name': 'Connor McDavid'},
            {'id': 'player2', 'name': 'Leon Draisaitl'},
            {'id': 'player3', 'name': 'Nathan MacKinnon'},
            {'id': 'player4', 'name': 'Sidney Crosby'},
        ]

        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        mock_db.get_connection.return_value = mock_conn

        return mock_db

    def test_match_by_last_name(self, scraper, mock_db):
        """Test matching players by last name."""
        news_items = [
            {'player_name': 'McDavid', 'player_id': None, 'headline': 'Test'},
            {'player_name': 'Draisaitl', 'player_id': None, 'headline': 'Test'},
        ]

        result = scraper.match_players_with_database(news_items, mock_db)

        assert result[0]['player_id'] == 'player1'
        assert result[1]['player_id'] == 'player2'

    def test_match_by_full_name(self, scraper, mock_db):
        """Test matching players by full name."""
        news_items = [
            {'player_name': 'Connor McDavid', 'player_id': None, 'headline': 'Test'},
        ]

        result = scraper.match_players_with_database(news_items, mock_db)

        assert result[0]['player_id'] == 'player1'

    def test_no_match_for_unknown_player(self, scraper, mock_db):
        """Test that unknown players don't get matched."""
        news_items = [
            {'player_name': 'UnknownPlayer', 'player_id': None, 'headline': 'Test'},
        ]

        result = scraper.match_players_with_database(news_items, mock_db)

        assert result[0]['player_id'] is None

    def test_case_insensitive_matching(self, scraper, mock_db):
        """Test that matching is case insensitive."""
        news_items = [
            {'player_name': 'MCDAVID', 'player_id': None, 'headline': 'Test'},
            {'player_name': 'mcdavid', 'player_id': None, 'headline': 'Test'},
        ]

        result = scraper.match_players_with_database(news_items, mock_db)

        assert result[0]['player_id'] == 'player1'
        assert result[1]['player_id'] == 'player1'

    def test_skip_already_matched(self, scraper, mock_db):
        """Test that already matched items are skipped."""
        news_items = [
            {'player_name': 'McDavid', 'player_id': 'existing_id', 'headline': 'Test'},
        ]

        result = scraper.match_players_with_database(news_items, mock_db)

        # Should keep existing ID
        assert result[0]['player_id'] == 'existing_id'


class TestScraperInit:
    """Tests for scraper initialization."""

    def test_scraper_init_with_defaults(self):
        """Test scraper initialization with default values."""
        scraper = FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies")
        )

        assert scraper.league_id == "test_league"
        assert scraper.username == "test_user"
        assert scraper.password == "test_pass"
        assert scraper.selenium_timeout == 10
        assert scraper.login_wait_time == 5

    def test_scraper_init_with_custom_values(self):
        """Test scraper initialization with custom values."""
        scraper = FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            selenium_timeout=20,
            login_wait_time=10,
            browser_window_size="1280,800"
        )

        assert scraper.selenium_timeout == 20
        assert scraper.login_wait_time == 10
        assert scraper.browser_window_size == "1280,800"


class TestParseTimeToSeconds:
    """Tests for the _parse_time_to_seconds method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock()
        )

    def test_parse_standard_time(self, scraper):
        """Test parsing standard MM:SS format."""
        assert scraper._parse_time_to_seconds("16:05") == 965
        assert scraper._parse_time_to_seconds("01:30") == 90
        assert scraper._parse_time_to_seconds("00:45") == 45

    def test_parse_single_digit_minutes(self, scraper):
        """Test parsing with single digit minutes."""
        assert scraper._parse_time_to_seconds("5:30") == 330
        assert scraper._parse_time_to_seconds("0:15") == 15

    def test_parse_zero(self, scraper):
        """Test parsing zero time."""
        assert scraper._parse_time_to_seconds("00:00") == 0
        assert scraper._parse_time_to_seconds("0:00") == 0

    def test_parse_invalid_format(self, scraper):
        """Test parsing invalid format returns 0."""
        assert scraper._parse_time_to_seconds("invalid") == 0
        assert scraper._parse_time_to_seconds("") == 0
        assert scraper._parse_time_to_seconds("16") == 0

    def test_parse_no_colon(self, scraper):
        """Test parsing string without colon returns 0."""
        assert scraper._parse_time_to_seconds("1630") == 0

    def test_parse_large_minutes(self, scraper):
        """Test parsing with large minute values (like total TOI)."""
        assert scraper._parse_time_to_seconds("120:00") == 7200
        assert scraper._parse_time_to_seconds("999:59") == 59999


class TestExtractToiFromPage:
    """Tests for the _extract_toi_from_page method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock()
        )

    @pytest.mark.skip(reason="Page format parsing depends on actual Fantrax page structure; tested by integration tests")
    def test_extract_toi_valid_page_text(self, scraper):
        """Test extracting TOI from valid page text with standard layout.

        Note: This test is skipped because the actual Fantrax page format
        is complex and varies. The real page scraping is tested by integration
        tests in test_scraper_integration.py.
        """
        # The actual page format from Fantrax is difficult to mock correctly
        # as it depends on how Selenium extracts text from the HTML.
        # See integration tests for real page scraping validation.
        pass

    def test_extract_toi_no_toi_headers(self, scraper):
        """Test extracting TOI when page doesn't have TOI headers."""
        page_text = """
Player Stats
GP
G
A
10
5
3
"""
        mock_driver = MagicMock()
        mock_body = MagicMock()
        mock_body.text = page_text
        mock_driver.find_element.return_value = mock_body

        result = scraper._extract_toi_from_page(mock_driver)

        assert result is None

    def test_extract_toi_missing_toish(self, scraper):
        """Test extracting TOI when TOISH header is missing."""
        page_text = """
GP
TOI
TOIPP
57
16:05
01:20
"""
        mock_driver = MagicMock()
        mock_body = MagicMock()
        mock_body.text = page_text
        mock_driver.find_element.return_value = mock_body

        result = scraper._extract_toi_from_page(mock_driver)

        # Should return None because TOISH is required
        assert result is None

    def test_extract_toi_empty_page(self, scraper):
        """Test extracting TOI from empty page."""
        mock_driver = MagicMock()
        mock_body = MagicMock()
        mock_body.text = ""
        mock_driver.find_element.return_value = mock_body

        result = scraper._extract_toi_from_page(mock_driver)

        assert result is None

    def test_extract_toi_driver_exception(self, scraper):
        """Test extracting TOI when driver raises exception."""
        mock_driver = MagicMock()
        mock_driver.find_element.side_effect = Exception("Element not found")

        result = scraper._extract_toi_from_page(mock_driver)

        assert result is None


class TestScrapePlayerToi:
    """Tests for the scrape_player_toi method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock()
        )

    @patch.object(FantraxScraper, '_get_driver')
    @patch.object(FantraxScraper, '_login')
    @patch.object(FantraxScraper, '_extract_toi_from_page')
    def test_scrape_player_toi_success(self, mock_extract, mock_login, mock_get_driver, scraper):
        """Test successful TOI scraping for multiple players."""
        # Setup mocks - driver is now created directly without context manager
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_login.return_value = True
        mock_extract.return_value = {
            'toi_seconds': 965,
            'toipp_seconds': 80,
            'toish_seconds': 90,
            'games_played': 57
        }

        player_ids = ['player1', 'player2', 'player3']
        result = scraper.scrape_player_toi(player_ids, 'team123')

        assert len(result) == 3
        assert 'player1' in result
        assert result['player1']['toi_seconds'] == 965
        assert result['player1']['games_played'] == 57

    @patch.object(FantraxScraper, '_get_driver')
    @patch.object(FantraxScraper, '_login')
    def test_scrape_player_toi_login_failure(self, mock_login, mock_get_driver, scraper):
        """Test TOI scraping when login fails."""
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_login.return_value = False

        result = scraper.scrape_player_toi(['player1'], 'team123')

        assert result == {}

    @patch.object(FantraxScraper, '_get_driver')
    @patch.object(FantraxScraper, '_login')
    @patch.object(FantraxScraper, '_extract_toi_from_page')
    def test_scrape_player_toi_partial_success(self, mock_extract, mock_login, mock_get_driver, scraper):
        """Test TOI scraping when some players fail to extract."""
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_login.return_value = True
        # First player succeeds, second fails, third succeeds
        mock_extract.side_effect = [
            {'toi_seconds': 965, 'toipp_seconds': 80, 'toish_seconds': 90, 'games_played': 57},
            None,
            {'toi_seconds': 800, 'toipp_seconds': 60, 'toish_seconds': 70, 'games_played': 50}
        ]

        player_ids = ['player1', 'player2', 'player3']
        result = scraper.scrape_player_toi(player_ids, 'team123')

        assert len(result) == 2
        assert 'player1' in result
        assert 'player2' not in result
        assert 'player3' in result

    @patch.object(FantraxScraper, '_get_driver')
    @patch.object(FantraxScraper, '_login')
    @patch.object(FantraxScraper, '_extract_toi_from_page')
    def test_scrape_player_toi_respects_max_players(self, mock_extract, mock_login, mock_get_driver, scraper):
        """Test that max_players parameter is respected."""
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_login.return_value = True
        mock_extract.return_value = {
            'toi_seconds': 965, 'toipp_seconds': 80, 'toish_seconds': 90, 'games_played': 57
        }

        player_ids = ['p1', 'p2', 'p3', 'p4', 'p5']
        result = scraper.scrape_player_toi(player_ids, 'team123', max_players=3)

        # Should only scrape first 3 players
        assert len(result) == 3
        assert 'p1' in result
        assert 'p2' in result
        assert 'p3' in result
        assert 'p4' not in result

    def test_scrape_player_toi_empty_list(self, scraper):
        """Test TOI scraping with empty player list returns early."""
        # Empty list should return immediately without creating driver
        result = scraper.scrape_player_toi([], 'team123')

        assert result == {}

    @patch.object(FantraxScraper, '_get_driver')
    @patch.object(FantraxScraper, '_login')
    @patch.object(FantraxScraper, '_extract_toi_from_page')
    def test_scrape_player_toi_driver_exception(self, mock_extract, mock_login, mock_get_driver, scraper):
        """Test TOI scraping handles driver exceptions gracefully."""
        mock_driver = MagicMock()
        mock_get_driver.return_value = mock_driver
        mock_login.return_value = True
        # First player raises exception, second succeeds
        mock_driver.get.side_effect = [Exception("Network error"), None]
        mock_extract.return_value = {
            'toi_seconds': 965, 'toipp_seconds': 80, 'toish_seconds': 90, 'games_played': 57
        }

        player_ids = ['player1', 'player2']
        result = scraper.scrape_player_toi(player_ids, 'team123')

        # Should handle exception and continue with remaining players
        assert len(result) == 1
        assert 'player2' in result


class TestGetWithRetry:
    """Tests for the _get_with_retry method."""

    @pytest.fixture
    def scraper(self):
        """Create a scraper instance with custom retry settings for testing."""
        return FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            console=Mock(),
            max_retries=3,
            retry_delay=0.1,  # Short delay for faster tests
            retry_backoff=2.0
        )

    def test_retry_init_defaults(self):
        """Test that default retry settings are set correctly."""
        scraper = FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies")
        )
        assert scraper.max_retries == 3
        assert scraper.retry_delay == 2.0
        assert scraper.retry_backoff == 2.0

    def test_retry_init_custom(self):
        """Test that custom retry settings are set correctly."""
        scraper = FantraxScraper(
            league_id="test_league",
            username="test_user",
            password="test_pass",
            cookie_file=Path("/tmp/test_cookies"),
            max_retries=5,
            retry_delay=1.0,
            retry_backoff=1.5
        )
        assert scraper.max_retries == 5
        assert scraper.retry_delay == 1.0
        assert scraper.retry_backoff == 1.5

    def test_get_with_retry_success_first_attempt(self, scraper):
        """Test successful navigation on first attempt."""
        mock_driver = MagicMock()

        result = scraper._get_with_retry(mock_driver, "https://example.com")

        assert result is True
        mock_driver.get.assert_called_once_with("https://example.com")

    def test_get_with_retry_success_after_timeout(self, scraper):
        """Test successful navigation after timeout errors."""
        mock_driver = MagicMock()
        # First two calls fail with timeout, third succeeds
        mock_driver.get.side_effect = [
            Exception("Read timed out"),
            Exception("HTTPConnectionPool connection error"),
            None  # Success
        ]

        result = scraper._get_with_retry(mock_driver, "https://example.com")

        assert result is True
        assert mock_driver.get.call_count == 3

    def test_get_with_retry_max_retries_exceeded(self, scraper):
        """Test failure after max retries exceeded."""
        mock_driver = MagicMock()
        # All calls fail with timeout
        mock_driver.get.side_effect = Exception("Read timed out")

        result = scraper._get_with_retry(mock_driver, "https://example.com")

        assert result is False
        # Initial attempt + 3 retries = 4 total calls
        assert mock_driver.get.call_count == 4

    def test_get_with_retry_non_network_error_raises(self, scraper):
        """Test that non-network errors are raised immediately."""
        mock_driver = MagicMock()
        mock_driver.get.side_effect = ValueError("Some other error")

        with pytest.raises(ValueError, match="Some other error"):
            scraper._get_with_retry(mock_driver, "https://example.com")

        # Should not retry for non-network errors
        mock_driver.get.assert_called_once()

    def test_get_with_retry_various_timeout_messages(self, scraper):
        """Test that various timeout message formats are handled."""
        mock_driver = MagicMock()

        timeout_messages = [
            "urllib3.exceptions.ReadTimeoutError",
            "Connection timed out",
            "HTTPConnectionPool(host='localhost', port=54137): Read timed out",
        ]

        for msg in timeout_messages:
            mock_driver.reset_mock()
            mock_driver.get.side_effect = [Exception(msg), None]

            result = scraper._get_with_retry(mock_driver, "https://example.com")

            assert result is True, f"Failed for message: {msg}"
            assert mock_driver.get.call_count == 2
