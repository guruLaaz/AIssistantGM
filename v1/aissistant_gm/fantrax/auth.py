"""Authentication module for Fantrax private leagues using Selenium."""

import pickle
import time
from pathlib import Path
from typing import Optional

from requests import Session
from aissistant_gm.fantrax.fantraxapi import League, NotLoggedIn, FantraxException, api
from aissistant_gm.fantrax.fantraxapi.api import Method
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Global variables for authentication
_cookie_file_path: Optional[Path] = None
_username: Optional[str] = None
_password: Optional[str] = None
_old_request = api.request
_override_installed = False

# Rate limiting variables
_last_request_time: Optional[float] = None
_min_request_interval: float = 1.0  # Default minimum seconds between requests

# Browser automation settings
_selenium_timeout: int = 10  # Seconds to wait for page elements
_login_wait_time: int = 5  # Seconds to wait after login
_browser_window_size: str = "1920,1600"  # Chrome window dimensions
_user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def add_cookie_to_session(session: Session, ignore_cookie: bool = False) -> None:
    """
    Add authentication cookies to the session.

    Loads cookies from cache file or performs Selenium login if needed.

    Args:
        session: requests.Session object from League instance.
        ignore_cookie: If True, force a new login even if cached cookies exist.
    """
    global _cookie_file_path, _username, _password
    global _selenium_timeout, _login_wait_time, _browser_window_size, _user_agent

    if not ignore_cookie and _cookie_file_path.exists():
        # Load cached cookies
        with open(_cookie_file_path, "rb") as f:
            for cookie in pickle.load(f):
                session.cookies.set(cookie["name"], cookie["value"])
    else:
        # Perform Selenium login
        service = Service(ChromeDriverManager().install())
        options = Options()
        options.add_argument("--headless")
        options.add_argument(f"--window-size={_browser_window_size}")
        options.add_argument(f"user-agent={_user_agent}")

        with webdriver.Chrome(service=service, options=options) as driver:
            driver.get("https://www.fantrax.com/login")

            # Wait for and fill in username
            username_box = WebDriverWait(driver, _selenium_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//input[@formcontrolname='email']"))
            )
            username_box.send_keys(_username)

            # Wait for and fill in password
            password_box = WebDriverWait(driver, _selenium_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//input[@formcontrolname='password']"))
            )
            password_box.send_keys(_password)
            password_box.send_keys(Keys.ENTER)

            # Wait for login to complete
            time.sleep(_login_wait_time)

            # Save cookies
            cookies = driver.get_cookies()
            with open(_cookie_file_path, "wb") as cookie_file:
                pickle.dump(cookies, cookie_file)

            # Add cookies to session
            for cookie in cookies:
                session.cookies.set(cookie["name"], cookie["value"])


def new_request(league: League, methods: list[Method] | Method) -> dict:
    """
    Override for api.request that handles authentication and rate limiting.

    This function is installed as a replacement for the default api.request
    to automatically handle login, cookie management, and rate limiting.
    """
    global _cookie_file_path, _last_request_time, _min_request_interval

    # Rate limiting: ensure minimum interval between requests
    if _last_request_time is not None:
        elapsed = time.time() - _last_request_time
        if elapsed < _min_request_interval:
            sleep_time = _min_request_interval - elapsed
            time.sleep(sleep_time)

    try:
        if not league.logged_in:
            add_cookie_to_session(league.session)

        result = _old_request(league, methods)

        # Update last request time after successful request
        _last_request_time = time.time()

        return result
    except NotLoggedIn:
        add_cookie_to_session(league.session, ignore_cookie=True)
        return new_request(league, methods)
    except FantraxException as e:
        # Check if this is an INVALID_REQUEST error (stale cookies)
        error_str = str(e)
        if "INVALID_REQUEST" in error_str and _cookie_file_path and _cookie_file_path.exists():
            # Delete stale cookie file and re-authenticate
            _cookie_file_path.unlink()
            add_cookie_to_session(league.session, ignore_cookie=True)
            return _old_request(league, methods)
        else:
            # Re-raise if it's not a cookie issue
            raise


def get_authenticated_session(username: str, password: str, cookie_file: Path) -> Session:
    """
    Get an authenticated requests.Session for the Fantrax API.

    Args:
        username: Fantrax username/email.
        password: Fantrax password.
        cookie_file: Path to the cookie cache file.

    Returns:
        Authenticated requests.Session.
    """
    global _cookie_file_path, _username, _password

    # Set global variables for the authentication functions
    _cookie_file_path = cookie_file
    _username = username
    _password = password

    # Create session and add cookies
    session = Session()
    add_cookie_to_session(session)

    return session


def get_authenticated_league(
    league_id: str,
    username: str,
    password: str,
    cookie_file: Path,
    min_request_interval: float = 1.0,
    selenium_timeout: int = 10,
    login_wait_time: int = 5,
    browser_window_size: str = "1920,1600",
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
) -> League:
    """
    Get an authenticated League instance for a private league.

    This function handles cookie caching and Selenium authentication automatically
    by overriding the FantraxAPI's request mechanism.

    Args:
        league_id: Fantrax league ID.
        username: Fantrax username/email.
        password: Fantrax password.
        cookie_file: Path to the cookie cache file.
        min_request_interval: Minimum seconds between API requests (default: 1.0).
        selenium_timeout: Seconds to wait for page elements (default: 10).
        login_wait_time: Seconds to wait after login (default: 5).
        browser_window_size: Chrome window dimensions (default: "1920,1600").
        user_agent: Browser user-agent string.

    Returns:
        Authenticated League instance.

    Raises:
        Exception: If authentication fails.
    """
    global _cookie_file_path, _username, _password, _override_installed, _min_request_interval
    global _selenium_timeout, _login_wait_time, _browser_window_size, _user_agent

    # Set global variables for the authentication functions
    _cookie_file_path = cookie_file
    _username = username
    _password = password
    _min_request_interval = min_request_interval
    _selenium_timeout = selenium_timeout
    _login_wait_time = login_wait_time
    _browser_window_size = browser_window_size
    _user_agent = user_agent

    # Install the request override if not already installed
    if not _override_installed:
        api.request = new_request
        _override_installed = True

    # Create and return League instance
    # The override will handle authentication automatically
    league = League(league_id)

    return league
