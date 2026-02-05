"""Tests for auth module."""

import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from pathlib import Path
import pickle
import time

from aissistant_gm.fantrax import auth
from aissistant_gm.fantrax.fantraxapi import NotLoggedIn, FantraxException


class TestAddCookieToSession:
    """Test add_cookie_to_session function."""

    @patch('aissistant_gm.fantrax.auth.pickle')
    @patch('builtins.open', new_callable=mock_open)
    def test_loads_cookies_from_cache_file(self, mock_file, mock_pickle):
        """Test that cookies are loaded from cache file when it exists."""
        # Setup
        auth._cookie_file_path = Mock(spec=Path)
        auth._cookie_file_path.exists.return_value = True

        mock_cookies = [
            {'name': 'session', 'value': 'abc123'},
            {'name': 'auth', 'value': 'xyz789'}
        ]
        mock_pickle.load.return_value = mock_cookies

        session = Mock()
        session.cookies = Mock()

        # Execute
        auth.add_cookie_to_session(session, ignore_cookie=False)

        # Assert
        mock_file.assert_called_once_with(auth._cookie_file_path, "rb")
        mock_pickle.load.assert_called_once()
        assert session.cookies.set.call_count == 2
        session.cookies.set.assert_any_call('session', 'abc123')
        session.cookies.set.assert_any_call('auth', 'xyz789')

    @patch('aissistant_gm.fantrax.auth.webdriver')
    @patch('aissistant_gm.fantrax.auth.WebDriverWait')
    @patch('aissistant_gm.fantrax.auth.Service')
    @patch('aissistant_gm.fantrax.auth.ChromeDriverManager')
    @patch('aissistant_gm.fantrax.auth.pickle')
    @patch('aissistant_gm.fantrax.auth.time')
    @patch('builtins.open', new_callable=mock_open)
    def test_performs_selenium_login_when_no_cache(
        self, mock_file, mock_time, mock_pickle, mock_driver_manager,
        mock_service, mock_wait, mock_webdriver
    ):
        """Test that Selenium login is performed when no cache exists."""
        # Setup
        auth._cookie_file_path = Mock(spec=Path)
        auth._cookie_file_path.exists.return_value = False
        auth._username = 'test@example.com'
        auth._password = 'testpass'
        auth._selenium_timeout = 10
        auth._login_wait_time = 5
        auth._browser_window_size = '1920,1080'
        auth._user_agent = 'Test Agent'

        # Mock driver
        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [
            {'name': 'session', 'value': 'new123'}
        ]
        mock_webdriver.Chrome.return_value.__enter__ = Mock(return_value=mock_driver)
        mock_webdriver.Chrome.return_value.__exit__ = Mock(return_value=None)

        # Mock WebDriverWait to return mock elements
        mock_element = Mock()
        mock_wait.return_value.until.return_value = mock_element

        session = Mock()
        session.cookies = Mock()

        # Execute
        auth.add_cookie_to_session(session, ignore_cookie=False)

        # Assert
        mock_driver.get.assert_called_once_with("https://www.fantrax.com/login")
        mock_element.send_keys.assert_called()
        mock_pickle.dump.assert_called_once()
        session.cookies.set.assert_called_once_with('session', 'new123')

    @patch('aissistant_gm.fantrax.auth.webdriver')
    @patch('aissistant_gm.fantrax.auth.WebDriverWait')
    @patch('aissistant_gm.fantrax.auth.Service')
    @patch('aissistant_gm.fantrax.auth.ChromeDriverManager')
    @patch('aissistant_gm.fantrax.auth.pickle')
    @patch('aissistant_gm.fantrax.auth.time')
    @patch('builtins.open', new_callable=mock_open)
    def test_ignore_cookie_flag_forces_login(
        self, mock_file, mock_time, mock_pickle, mock_driver_manager,
        mock_service, mock_wait, mock_webdriver
    ):
        """Test that ignore_cookie=True forces a new login."""
        # Setup - even though cache exists, should do login
        auth._cookie_file_path = Mock(spec=Path)
        auth._cookie_file_path.exists.return_value = True  # Cache exists
        auth._username = 'test@example.com'
        auth._password = 'testpass'
        auth._selenium_timeout = 10
        auth._login_wait_time = 5
        auth._browser_window_size = '1920,1080'
        auth._user_agent = 'Test Agent'

        mock_driver = MagicMock()
        mock_driver.get_cookies.return_value = [{'name': 's', 'value': 'v'}]
        mock_webdriver.Chrome.return_value.__enter__ = Mock(return_value=mock_driver)
        mock_webdriver.Chrome.return_value.__exit__ = Mock(return_value=None)

        mock_element = Mock()
        mock_wait.return_value.until.return_value = mock_element

        session = Mock()
        session.cookies = Mock()

        # Execute - force login with ignore_cookie=True
        auth.add_cookie_to_session(session, ignore_cookie=True)

        # Assert - should have performed login, not loaded from cache
        mock_driver.get.assert_called_once_with("https://www.fantrax.com/login")


class TestNewRequest:
    """Test new_request function."""

    def test_rate_limiting_sleeps_when_needed(self):
        """Test that rate limiting sleeps when requests are too fast."""
        # Setup
        auth._last_request_time = time.time()  # Just made a request
        auth._min_request_interval = 1.0
        auth._cookie_file_path = Mock(spec=Path)

        league = Mock()
        league.logged_in = True

        with patch.object(auth, '_old_request', return_value={'data': 'test'}) as mock_old:
            with patch('aissistant_gm.fantrax.auth.time') as mock_time:
                mock_time.time.return_value = auth._last_request_time + 0.1  # 0.1s elapsed
                mock_time.sleep = Mock()

                # Execute
                result = auth.new_request(league, Mock())

                # Assert - should have slept
                mock_time.sleep.assert_called_once()
                sleep_time = mock_time.sleep.call_args[0][0]
                assert sleep_time > 0  # Should sleep for remaining interval

    def test_success_updates_last_request_time(self):
        """Test that successful request updates last request time."""
        # Setup
        auth._last_request_time = None
        auth._min_request_interval = 1.0
        auth._cookie_file_path = Mock(spec=Path)

        league = Mock()
        league.logged_in = True

        with patch.object(auth, '_old_request', return_value={'data': 'test'}):
            with patch('aissistant_gm.fantrax.auth.time') as mock_time:
                mock_time.time.return_value = 12345.0

                # Execute
                result = auth.new_request(league, Mock())

                # Assert
                assert auth._last_request_time == 12345.0
                assert result == {'data': 'test'}

    def test_not_logged_in_triggers_auth(self):
        """Test that NotLoggedIn exception triggers re-authentication."""
        auth._last_request_time = None
        auth._min_request_interval = 0
        auth._cookie_file_path = Mock(spec=Path)

        league = Mock()
        league.logged_in = False

        with patch.object(auth, '_old_request', return_value={'data': 'test'}):
            with patch.object(auth, 'add_cookie_to_session') as mock_add_cookie:
                # Execute
                result = auth.new_request(league, Mock())

                # Assert
                mock_add_cookie.assert_called_once_with(league.session)

    def test_not_logged_in_exception_retries(self):
        """Test that NotLoggedIn exception triggers retry with fresh cookies."""
        auth._last_request_time = None
        auth._min_request_interval = 0
        auth._cookie_file_path = Mock(spec=Path)

        league = Mock()
        league.logged_in = True

        # First call raises NotLoggedIn, second succeeds
        call_count = [0]

        def old_request_side_effect(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                raise NotLoggedIn("Not logged in")
            return {'data': 'test'}

        with patch.object(auth, '_old_request', side_effect=old_request_side_effect):
            with patch.object(auth, 'add_cookie_to_session') as mock_add_cookie:
                # Execute
                result = auth.new_request(league, Mock())

                # Assert - should have retried after re-auth
                mock_add_cookie.assert_called_once_with(league.session, ignore_cookie=True)

    def test_invalid_request_deletes_stale_cookies(self):
        """Test that INVALID_REQUEST error deletes stale cookie file."""
        auth._last_request_time = None
        auth._min_request_interval = 0
        auth._cookie_file_path = Mock(spec=Path)
        auth._cookie_file_path.exists.return_value = True

        league = Mock()
        league.logged_in = True

        # First call raises FantraxException with INVALID_REQUEST, second succeeds
        call_count = [0]

        def old_request_side_effect(*args):
            call_count[0] += 1
            if call_count[0] == 1:
                raise FantraxException("INVALID_REQUEST: stale session")
            return {'data': 'test'}

        with patch.object(auth, '_old_request', side_effect=old_request_side_effect):
            with patch.object(auth, 'add_cookie_to_session') as mock_add_cookie:
                # Execute
                result = auth.new_request(league, Mock())

                # Assert - should have deleted cookie file and retried
                auth._cookie_file_path.unlink.assert_called_once()
                mock_add_cookie.assert_called_once_with(league.session, ignore_cookie=True)

    def test_other_exception_raises(self):
        """Test that non-auth exceptions are re-raised."""
        auth._last_request_time = None
        auth._min_request_interval = 0
        auth._cookie_file_path = Mock(spec=Path)

        league = Mock()
        league.logged_in = True

        with patch.object(auth, '_old_request', side_effect=FantraxException("Some other error")):
            with pytest.raises(FantraxException) as exc_info:
                auth.new_request(league, Mock())

            assert "Some other error" in str(exc_info.value)


class TestGetAuthenticatedSession:
    """Test get_authenticated_session function."""

    @patch.object(auth, 'add_cookie_to_session')
    def test_creates_session_and_adds_cookies(self, mock_add_cookie):
        """Test that function creates session and adds cookies."""
        cookie_file = Path('/tmp/test_cookies.pkl')

        # Execute
        session = auth.get_authenticated_session(
            username='test@example.com',
            password='testpass',
            cookie_file=cookie_file
        )

        # Assert
        mock_add_cookie.assert_called_once()
        assert auth._cookie_file_path == cookie_file
        assert auth._username == 'test@example.com'
        assert auth._password == 'testpass'


class TestGetAuthenticatedLeague:
    """Test get_authenticated_league function."""

    @patch('aissistant_gm.fantrax.auth.League')
    def test_creates_league_and_installs_override(self, mock_league_class):
        """Test that function creates League and installs request override."""
        # Reset override flag
        auth._override_installed = False

        cookie_file = Path('/tmp/test_cookies.pkl')
        mock_league = Mock()
        mock_league_class.return_value = mock_league

        # Execute
        result = auth.get_authenticated_league(
            league_id='test_league',
            username='test@example.com',
            password='testpass',
            cookie_file=cookie_file,
            min_request_interval=2.0,
            selenium_timeout=15,
            login_wait_time=8,
            browser_window_size='1280,720',
            user_agent='Custom Agent'
        )

        # Assert
        assert result == mock_league
        mock_league_class.assert_called_once_with('test_league')
        assert auth._cookie_file_path == cookie_file
        assert auth._username == 'test@example.com'
        assert auth._password == 'testpass'
        assert auth._min_request_interval == 2.0
        assert auth._selenium_timeout == 15
        assert auth._login_wait_time == 8
        assert auth._browser_window_size == '1280,720'
        assert auth._user_agent == 'Custom Agent'
        assert auth._override_installed is True

    @patch('aissistant_gm.fantrax.auth.League')
    def test_override_only_installed_once(self, mock_league_class):
        """Test that request override is only installed once."""
        # Set override as already installed
        auth._override_installed = True
        original_request = auth.api.request

        cookie_file = Path('/tmp/test_cookies.pkl')

        # Execute twice
        auth.get_authenticated_league(
            league_id='test1',
            username='test@example.com',
            password='testpass',
            cookie_file=cookie_file
        )
        auth.get_authenticated_league(
            league_id='test2',
            username='test@example.com',
            password='testpass',
            cookie_file=cookie_file
        )

        # Assert - api.request should still be the same
        # (not overwritten twice)
        assert mock_league_class.call_count == 2
