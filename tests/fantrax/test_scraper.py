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
