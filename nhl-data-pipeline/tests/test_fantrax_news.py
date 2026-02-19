"""Tests for fetchers/fantrax_news.py — Fantrax player news fetcher."""

from __future__ import annotations

import hashlib
import os
import pickle
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from db.schema import get_db, init_db, upsert_player
from fetchers.fantrax_news import (
    FANTRAX_LOGIN_URL,
    FANTRAX_NEWS_URL,
    _dismiss_overlays,
    _load_cookies_for_playwright,
    _load_env,
    _login_fantrax,
    _parse_news_item,
    _save_cookies_from_playwright,
    backfill_fantrax_news,
    fetch_news_api,
    fetch_news_page,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Provide a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    """Provide an initialized database with known players."""
    init_db(db_path)
    conn = get_db(db_path)
    upsert_player(conn, {
        "id": 8478402,
        "full_name": "Connor McDavid",
        "first_name": "Connor",
        "last_name": "McDavid",
        "team_abbrev": "EDM",
        "position": "C",
    })
    upsert_player(conn, {
        "id": 8471679,
        "full_name": "Carey Price",
        "first_name": "Carey",
        "last_name": "Price",
        "team_abbrev": "MTL",
        "position": "G",
    })
    return conn


@pytest.fixture
def fantrax_config(tmp_path: Path) -> dict[str, Any]:
    """Provide a Fantrax config dict with temp cookie path."""
    return {
        "username": "testuser@example.com",
        "password": "testpass123",
        "league_id": "abc123",
        "cookie_file": tmp_path / "test_cookies.pkl",
    }


def _make_mock_element(
    player_name: str = "Connor McDavid",
    headline: str = "Scores hat trick",
    analysis: str = "McDavid scored three goals.",
    date_text: str = "Feb 18, 2026, 5:55 PM",
) -> MagicMock:
    """Create a mock DOM element that _parse_news_item can parse."""
    el = MagicMock()

    name_el = MagicMock()
    name_el.inner_text.return_value = player_name

    headline_el = MagicMock()
    headline_el.inner_text.return_value = headline

    analysis_el = MagicMock()
    analysis_el.inner_text.return_value = analysis

    date_el = MagicMock()
    date_el.inner_text.return_value = date_text

    def query_selector(selector: str):
        from fetchers.fantrax_news import (
            _ANALYSIS_SELECTOR,
            _DATE_SELECTOR,
            _HEADLINE_SELECTOR,
            _PLAYER_NAME_SELECTOR,
        )
        return {
            _PLAYER_NAME_SELECTOR: name_el,
            _HEADLINE_SELECTOR: headline_el,
            _ANALYSIS_SELECTOR: analysis_el,
            _DATE_SELECTOR: date_el,
        }.get(selector)

    el.query_selector = query_selector
    return el


def _setup_playwright_mocks(mock_sync_playwright: MagicMock) -> dict[str, MagicMock]:
    """Set up the full Playwright context manager mock chain.

    Returns a dict of mocks for easy access: pw, browser, context, page.
    """
    mock_pw = MagicMock()
    mock_browser = MagicMock()
    mock_context = MagicMock()
    mock_page = MagicMock()

    mock_sync_playwright.return_value.__enter__ = MagicMock(return_value=mock_pw)
    mock_sync_playwright.return_value.__exit__ = MagicMock(return_value=False)
    mock_pw.chromium.launch.return_value = mock_browser
    mock_browser.new_context.return_value = mock_context
    mock_context.new_page.return_value = mock_page

    # Default: not on login page after navigation
    mock_page.url = FANTRAX_NEWS_URL

    return {
        "pw": mock_pw,
        "browser": mock_browser,
        "context": mock_context,
        "page": mock_page,
    }


# =============================================================================
# Login Tests
# =============================================================================


class TestLoginFantrax:
    """Tests for _login_fantrax function."""

    def test_login_navigates_to_login_page(
        self, fantrax_config: dict[str, Any],
    ) -> None:
        """page.goto is called with the Fantrax login URL."""
        mock_page = MagicMock()
        mock_context = MagicMock()

        email_input = MagicMock()
        password_input = MagicMock()
        mock_page.wait_for_selector.side_effect = [email_input, password_input]
        mock_page.url = "https://www.fantrax.com/dashboard"
        mock_context.cookies.return_value = []

        _login_fantrax(mock_page, mock_context, fantrax_config)

        mock_page.goto.assert_called_once_with(
            FANTRAX_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000,
        )

    def test_login_fills_credentials(
        self, fantrax_config: dict[str, Any],
    ) -> None:
        """Email and password fields are filled with config values."""
        mock_page = MagicMock()
        mock_context = MagicMock()

        email_input = MagicMock()
        password_input = MagicMock()
        mock_page.wait_for_selector.side_effect = [email_input, password_input]
        mock_page.url = "https://www.fantrax.com/dashboard"
        mock_context.cookies.return_value = []

        _login_fantrax(mock_page, mock_context, fantrax_config)

        email_input.fill.assert_called_once_with("testuser@example.com")
        password_input.fill.assert_called_once_with("testpass123")
        password_input.press.assert_called_once_with("Enter")

    def test_login_saves_cookies(
        self, fantrax_config: dict[str, Any], tmp_path: Path,
    ) -> None:
        """After successful login, cookies are saved to pickle file."""
        mock_page = MagicMock()
        mock_context = MagicMock()

        email_input = MagicMock()
        password_input = MagicMock()
        mock_page.wait_for_selector.side_effect = [email_input, password_input]
        mock_page.url = "https://www.fantrax.com/dashboard"

        fake_cookies = [
            {"name": "session", "value": "abc123", "domain": ".fantrax.com",
             "path": "/", "secure": True, "httpOnly": True},
        ]
        mock_context.cookies.return_value = fake_cookies

        _login_fantrax(mock_page, mock_context, fantrax_config)

        assert fantrax_config["cookie_file"].exists()
        with open(fantrax_config["cookie_file"], "rb") as f:
            saved = pickle.load(f)
        assert len(saved) == 1
        assert saved[0]["name"] == "session"

    def test_login_raises_on_failure(
        self, fantrax_config: dict[str, Any],
    ) -> None:
        """RuntimeError raised when still on login page after submit."""
        mock_page = MagicMock()
        mock_context = MagicMock()

        email_input = MagicMock()
        password_input = MagicMock()
        mock_page.wait_for_selector.side_effect = [email_input, password_input]
        # Still on login page after submitting
        mock_page.url = "https://www.fantrax.com/login"

        with pytest.raises(RuntimeError, match="Login failed"):
            _login_fantrax(mock_page, mock_context, fantrax_config)


# =============================================================================
# Environment Loading Tests
# =============================================================================


class TestLoadEnv:
    """Tests for _load_env configuration loading."""

    ENV_VARS = (
        "FANTRAX_USERNAME",
        "FANTRAX_PASSWORD",
        "FANTRAX_LEAGUE_ID",
        "FANTRAX_COOKIE_FILE",
    )

    def _clear_fantrax_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Remove all FANTRAX_* env vars so tests start clean."""
        for var in self.ENV_VARS:
            monkeypatch.delenv(var, raising=False)

    @patch("fetchers.fantrax_news.load_dotenv")
    def test_loads_all_keys(
        self, _mock_dotenv: MagicMock, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Returned dict contains all expected keys when env is populated."""
        self._clear_fantrax_env(monkeypatch)
        monkeypatch.setenv("FANTRAX_USERNAME", "user@test.com")
        monkeypatch.setenv("FANTRAX_PASSWORD", "s3cret")
        monkeypatch.setenv("FANTRAX_LEAGUE_ID", "lg123")

        cfg = _load_env()

        assert cfg["username"] == "user@test.com"
        assert cfg["password"] == "s3cret"
        assert cfg["league_id"] == "lg123"
        assert isinstance(cfg["cookie_file"], Path)

    @patch("fetchers.fantrax_news.load_dotenv")
    def test_missing_env_returns_defaults(
        self, _mock_dotenv: MagicMock, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without env vars, username/password/league_id default to empty."""
        self._clear_fantrax_env(monkeypatch)

        cfg = _load_env()

        assert cfg["username"] == ""
        assert cfg["password"] == ""
        assert cfg["league_id"] == ""
        assert cfg["cookie_file"].name == "fantraxloggedin.cookie"

    @patch("fetchers.fantrax_news.load_dotenv")
    def test_partial_env(
        self, _mock_dotenv: MagicMock, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Setting only one var leaves others at defaults."""
        self._clear_fantrax_env(monkeypatch)
        monkeypatch.setenv("FANTRAX_USERNAME", "partial@test.com")

        cfg = _load_env()

        assert cfg["username"] == "partial@test.com"
        assert cfg["password"] == ""
        assert cfg["league_id"] == ""

    @patch("fetchers.fantrax_news.load_dotenv")
    def test_custom_cookie_file(
        self, _mock_dotenv: MagicMock, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FANTRAX_COOKIE_FILE overrides the default cookie filename."""
        self._clear_fantrax_env(monkeypatch)
        monkeypatch.setenv("FANTRAX_COOKIE_FILE", "my_cookies.pkl")

        cfg = _load_env()

        assert cfg["cookie_file"].name == "my_cookies.pkl"


# =============================================================================
# Cookie Helper Tests
# =============================================================================


class TestCookieHelpers:
    """Tests for _load_cookies_for_playwright and _save_cookies_from_playwright."""

    def test_load_playwright_valid_cookies(self, tmp_path: Path) -> None:
        """Valid pickle file loads cookies into browser context."""
        cookie_path = tmp_path / "cookies.pkl"
        cookies = [{"name": "sid", "value": "abc", "domain": ".fantrax.com"}]
        with open(cookie_path, "wb") as f:
            pickle.dump(cookies, f)

        mock_ctx = MagicMock()
        result = _load_cookies_for_playwright(mock_ctx, cookie_path)

        assert result is True
        mock_ctx.add_cookies.assert_called_once()
        added = mock_ctx.add_cookies.call_args[0][0]
        assert len(added) == 1
        assert added[0]["name"] == "sid"
        assert added[0]["domain"] == ".fantrax.com"
        assert added[0]["path"] == "/"

    def test_load_playwright_missing_file(self, tmp_path: Path) -> None:
        """Non-existent cookie file returns False."""
        mock_ctx = MagicMock()
        result = _load_cookies_for_playwright(mock_ctx, tmp_path / "nope.pkl")

        assert result is False
        mock_ctx.add_cookies.assert_not_called()

    def test_load_playwright_corrupt_pickle(self, tmp_path: Path) -> None:
        """Corrupt pickle file returns False (exception caught)."""
        cookie_path = tmp_path / "bad.pkl"
        cookie_path.write_bytes(b"not a pickle at all")

        mock_ctx = MagicMock()
        result = _load_cookies_for_playwright(mock_ctx, cookie_path)

        assert result is False

    def test_save_cookies_writes_pickle(self, tmp_path: Path) -> None:
        """Cookies from Playwright context are saved in Selenium format."""
        mock_ctx = MagicMock()
        mock_ctx.cookies.return_value = [
            {"name": "session", "value": "xyz", "domain": ".fantrax.com",
             "path": "/", "secure": True, "httpOnly": True},
        ]

        cookie_path = tmp_path / "saved.pkl"
        _save_cookies_from_playwright(mock_ctx, cookie_path)

        assert cookie_path.exists()
        with open(cookie_path, "rb") as f:
            saved = pickle.load(f)
        assert len(saved) == 1
        assert saved[0]["name"] == "session"
        assert "secure" in saved[0]
        assert "httpOnly" in saved[0]


# =============================================================================
# Overlay Dismissal Tests
# =============================================================================


class TestDismissOverlays:
    """Tests for _dismiss_overlays."""

    def test_overlay_present_clicked(self) -> None:
        """Visible overlay button is clicked and dismissed."""
        mock_page = MagicMock()
        mock_btn = MagicMock()
        mock_btn.is_visible.return_value = True

        # First query_selector call returns our visible button
        mock_page.query_selector.return_value = mock_btn

        _dismiss_overlays(mock_page)

        mock_btn.click.assert_called_once()
        mock_page.wait_for_timeout.assert_called_with(500)

    def test_no_overlay_no_error(self) -> None:
        """No overlay buttons present — completes without error."""
        mock_page = MagicMock()
        mock_page.query_selector.return_value = None
        mock_page.frames = []

        _dismiss_overlays(mock_page)

        # Should not raise; no click attempted
        mock_page.wait_for_timeout.assert_not_called()

    def test_overlay_click_raises_caught(self) -> None:
        """Exception during overlay click is caught gracefully."""
        mock_page = MagicMock()
        mock_page.query_selector.side_effect = Exception("click failed")
        mock_page.frames = []

        # Should not raise
        _dismiss_overlays(mock_page)


# =============================================================================
# Parse News Item Tests
# =============================================================================


class TestParseNewsItem:
    """Tests for _parse_news_item edge cases."""

    def test_missing_headline_returns_none(self) -> None:
        """Element with empty headline text returns None."""
        el = _make_mock_element(headline="")
        assert _parse_news_item(el) is None

    def test_missing_player_name_parses(self) -> None:
        """Element with empty player name still parses (headline only)."""
        el = _make_mock_element(player_name="", headline="Big trade rumor")
        result = _parse_news_item(el)

        assert result is not None
        assert result["player_name"] == ""
        # Headline should be headline_text only, no ": " prefix
        assert result["headline"] == "Big trade rumor"

    def test_missing_date_uses_raw_fallback(self) -> None:
        """Unparseable date string is kept as-is (ValueError branch)."""
        el = _make_mock_element(date_text="not a date")
        result = _parse_news_item(el)

        assert result is not None
        assert result["published_at"] == "not a date"

    def test_very_long_analysis(self) -> None:
        """Very long analysis text is preserved in full."""
        long_text = "x" * 10_000
        el = _make_mock_element(analysis=long_text)
        result = _parse_news_item(el)

        assert result is not None
        assert result["content"] == long_text

    def test_missing_headline_element(self) -> None:
        """query_selector returns None for headline selector → None."""
        from fetchers.fantrax_news import _HEADLINE_SELECTOR

        el = MagicMock()
        # Return None specifically for the headline selector
        def qs(selector: str):
            if selector == _HEADLINE_SELECTOR:
                return None
            mock_sub = MagicMock()
            mock_sub.inner_text.return_value = "some text"
            return mock_sub

        el.query_selector = qs
        assert _parse_news_item(el) is None


# =============================================================================
# Page Scraping Tests
# =============================================================================


class TestFetchNewsPage:
    """Tests for fetch_news_page (Playwright page scraping)."""

    @patch("fetchers.fantrax_news.sync_playwright")
    @patch("fetchers.fantrax_news._load_cookies_for_playwright", return_value=False)
    @patch("fetchers.fantrax_news._dismiss_overlays")
    @patch("fetchers.fantrax_news.time")
    def test_fetch_returns_list_of_dicts(
        self,
        mock_time: MagicMock,
        mock_dismiss: MagicMock,
        mock_load_cookies: MagicMock,
        mock_sync_playwright: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """Returns list of dicts with expected keys."""
        mocks = _setup_playwright_mocks(mock_sync_playwright)
        page = mocks["page"]

        mock_el = _make_mock_element()
        # Scrolling: return increasing counts then same to trigger stale break
        page.query_selector_all.side_effect = [
            [mock_el],       # scroll 1: count check
            [mock_el],       # scroll 2: count check (stale 1)
            [mock_el],       # scroll 3: count check (stale 2)
            [mock_el],       # scroll 4: count check (stale 3 → break)
            [mock_el],       # final extraction
        ]

        items = fetch_news_page(max_scrolls=10, config=fantrax_config)

        assert len(items) == 1
        item = items[0]
        assert "rotowire_news_id" in item
        assert "player_name" in item
        assert "headline" in item
        assert "content" in item
        assert "published_at" in item
        assert item["player_name"] == "Connor McDavid"
        assert item["rotowire_news_id"].startswith("fx_")

    @patch("fetchers.fantrax_news.sync_playwright")
    @patch("fetchers.fantrax_news._load_cookies_for_playwright", return_value=False)
    @patch("fetchers.fantrax_news._dismiss_overlays")
    @patch("fetchers.fantrax_news.time")
    def test_fetch_empty_response(
        self,
        mock_time: MagicMock,
        mock_dismiss: MagicMock,
        mock_load_cookies: MagicMock,
        mock_sync_playwright: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """No news elements returns empty list."""
        mocks = _setup_playwright_mocks(mock_sync_playwright)
        page = mocks["page"]

        # No items found — stale from the start
        page.query_selector_all.return_value = []

        items = fetch_news_page(max_scrolls=5, config=fantrax_config)

        assert items == []

    def test_fetch_playwright_missing(
        self, fantrax_config: dict[str, Any],
    ) -> None:
        """RuntimeError when Playwright is not installed."""
        with patch("fetchers.fantrax_news.HAS_PLAYWRIGHT", False):
            with pytest.raises(RuntimeError, match="Playwright required"):
                fetch_news_page(config=fantrax_config)

    @patch("fetchers.fantrax_news.sync_playwright")
    @patch("fetchers.fantrax_news._load_cookies_for_playwright", return_value=False)
    @patch("fetchers.fantrax_news._dismiss_overlays")
    @patch("fetchers.fantrax_news.time")
    def test_fetch_scroll_stops_on_no_new_items(
        self,
        mock_time: MagicMock,
        mock_dismiss: MagicMock,
        mock_load_cookies: MagicMock,
        mock_sync_playwright: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """3 consecutive scrolls with no new items breaks the loop."""
        mocks = _setup_playwright_mocks(mock_sync_playwright)
        page = mocks["page"]

        mock_el = _make_mock_element()
        # 2 items on every scroll (stale from scroll 1 onward)
        two_items = [mock_el, mock_el]
        page.query_selector_all.return_value = two_items

        items = fetch_news_page(max_scrolls=20, config=fantrax_config)

        # First scroll: count=2 vs prev=0 → not stale. Scrolls 2,3,4 are stale → break.
        assert page.evaluate.call_count == 4

    @patch("fetchers.fantrax_news.sync_playwright")
    @patch("fetchers.fantrax_news._load_cookies_for_playwright", return_value=False)
    @patch("fetchers.fantrax_news._dismiss_overlays")
    @patch("fetchers.fantrax_news.time")
    def test_fetch_max_scrolls_respected(
        self,
        mock_time: MagicMock,
        mock_dismiss: MagicMock,
        mock_load_cookies: MagicMock,
        mock_sync_playwright: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """max_scrolls=2 limits scroll iterations to 2."""
        mocks = _setup_playwright_mocks(mock_sync_playwright)
        page = mocks["page"]

        # Return increasing counts so stale_count never triggers
        call_count = 0

        def growing_items(_selector=None):
            nonlocal call_count
            call_count += 1
            return [MagicMock()] * call_count

        page.query_selector_all.side_effect = growing_items

        fetch_news_page(max_scrolls=2, config=fantrax_config)

        assert page.evaluate.call_count == 2

    @patch("fetchers.fantrax_news.sync_playwright")
    @patch("fetchers.fantrax_news._load_cookies_for_playwright", return_value=False)
    @patch("fetchers.fantrax_news._dismiss_overlays")
    @patch("fetchers.fantrax_news._parse_news_item")
    @patch("fetchers.fantrax_news.time")
    def test_fetch_stop_date_respected(
        self,
        mock_time: MagicMock,
        mock_parse: MagicMock,
        mock_dismiss: MagicMock,
        mock_load_cookies: MagicMock,
        mock_sync_playwright: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """Items older than stop_date cause scrolling to stop."""
        mocks = _setup_playwright_mocks(mock_sync_playwright)
        page = mocks["page"]

        # Return growing list so stale_count doesn't trigger
        mock_el = MagicMock()
        scroll_call = 0

        def growing_items(_selector=None):
            nonlocal scroll_call
            scroll_call += 1
            return [mock_el] * scroll_call

        page.query_selector_all.side_effect = growing_items

        # _parse_news_item returns item with old date on first scroll
        mock_parse.return_value = {
            "rotowire_news_id": "fx_abc",
            "player_name": "Test",
            "headline": "Test: Old news",
            "content": "Old content",
            "published_at": "2025-12-01T00:00:00",
        }

        items = fetch_news_page(
            max_scrolls=50, stop_date="2026-01-01", config=fantrax_config,
        )

        # Should stop early due to stop_date
        assert page.evaluate.call_count == 1


# =============================================================================
# API Fetcher Tests
# =============================================================================


class TestFetchNewsApi:
    """Tests for fetch_news_api (HTTP API approach)."""

    @patch("fetchers.fantrax_news.requests.Session")
    def test_api_returns_list_of_dicts(
        self,
        mock_session_cls: MagicMock,
        fantrax_config: dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """API returns list of dicts with correct keys from response."""
        # Create a cookie file so login isn't attempted
        cookie_file = fantrax_config["cookie_file"]
        with open(cookie_file, "wb") as f:
            pickle.dump([{"name": "sid", "value": "xyz"}], f)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "responses": [{
                "data": {
                    "stories": [{
                        "scorerFantasy": {"name": "Connor McDavid"},
                        "playerNews": {
                            "headlineNoBrief": "Scores hat trick",
                            "analysis": "Three goals in win.",
                            "newsDate": 1771200000000,
                        },
                    }],
                },
            }],
        }
        mock_session.post.return_value = mock_response

        items = fetch_news_api(config=fantrax_config)

        assert len(items) == 1
        item = items[0]
        assert item["player_name"] == "Connor McDavid"
        assert "Scores hat trick" in item["headline"]
        assert item["content"] == "Three goals in win."
        assert item["rotowire_news_id"].startswith("fx_")
        assert item["published_at"]  # non-empty

    @patch("fetchers.fantrax_news.requests.Session")
    def test_api_empty_stories(
        self,
        mock_session_cls: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """Empty stories list returns empty result."""
        with open(fantrax_config["cookie_file"], "wb") as f:
            pickle.dump([{"name": "sid", "value": "xyz"}], f)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "responses": [{"data": {"stories": []}}],
        }
        mock_session.post.return_value = mock_response

        items = fetch_news_api(config=fantrax_config)

        assert items == []

    @patch("fetchers.fantrax_news.requests.Session")
    def test_api_page_error_raises(
        self,
        mock_session_cls: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """Response with pageError raises RuntimeError."""
        with open(fantrax_config["cookie_file"], "wb") as f:
            pickle.dump([{"name": "sid", "value": "xyz"}], f)

        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "pageError": {"code": "NOT_AUTHORIZED"},
        }
        mock_session.post.return_value = mock_response

        with pytest.raises(RuntimeError, match="Fantrax API error"):
            fetch_news_api(config=fantrax_config)

    @patch("fetchers.fantrax_news.HAS_PLAYWRIGHT", False)
    @patch("fetchers.fantrax_news.requests.Session")
    def test_api_no_cookies_no_playwright(
        self,
        mock_session_cls: MagicMock,
        fantrax_config: dict[str, Any],
    ) -> None:
        """No cookies + no Playwright raises RuntimeError."""
        # Don't create cookie file — so _load_cookies_for_session returns False
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        with pytest.raises(RuntimeError, match="Playwright not available"):
            fetch_news_api(config=fantrax_config)


# =============================================================================
# Backfill Tests
# =============================================================================


class TestBackfillFantraxNews:
    """Tests for backfill_fantrax_news."""

    @patch("fetchers.fantrax_news.fetch_news_page")
    def test_backfill_saves_items(
        self,
        mock_fetch: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Mock fetch returns 5 items, all saved to player_news."""
        items = [
            {
                "rotowire_news_id": f"fx_item{i}",
                "player_name": "Connor McDavid",
                "headline": f"Connor McDavid: News {i}",
                "content": f"Content {i}",
                "published_at": f"2026-02-{10+i:02d}T12:00:00",
            }
            for i in range(5)
        ]
        mock_fetch.return_value = items

        summary = backfill_fantrax_news(db)

        cursor = db.execute("SELECT COUNT(*) as cnt FROM player_news")
        assert cursor.fetchone()["cnt"] == 5
        assert summary["total_fetched"] == 5
        assert summary["new_inserted"] == 5

    @patch("fetchers.fantrax_news.fetch_news_page")
    def test_backfill_returns_summary(
        self,
        mock_fetch: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Returns dict with total_fetched, new_inserted, duplicates_skipped."""
        mock_fetch.return_value = [
            {
                "rotowire_news_id": "fx_sum1",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: Goal",
                "content": "Scored.",
                "published_at": "2026-02-18T12:00:00",
            },
        ]

        summary = backfill_fantrax_news(db)

        assert "total_fetched" in summary
        assert "new_inserted" in summary
        assert "duplicates_skipped" in summary
        assert summary["total_fetched"] == 1
        assert summary["new_inserted"] == 1
        assert summary["duplicates_skipped"] == 0

    @patch("fetchers.fantrax_news.fetch_news_page")
    def test_backfill_dedup(
        self,
        mock_fetch: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Calling twice with same items produces no duplicates."""
        items = [
            {
                "rotowire_news_id": "fx_dedup1",
                "player_name": "Connor McDavid",
                "headline": "Connor McDavid: Assist",
                "content": "Helper.",
                "published_at": "2026-02-18T12:00:00",
            },
        ]
        mock_fetch.return_value = items

        first = backfill_fantrax_news(db)
        second = backfill_fantrax_news(db)

        assert first["new_inserted"] == 1
        assert second["new_inserted"] == 0
        assert second["duplicates_skipped"] == 1

        cursor = db.execute("SELECT COUNT(*) as cnt FROM player_news")
        assert cursor.fetchone()["cnt"] == 1

    @patch("fetchers.fantrax_news.save_news")
    @patch("fetchers.fantrax_news.fetch_news_page")
    def test_backfill_calls_save_news(
        self,
        mock_fetch: MagicMock,
        mock_save: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Verify save_news from rotowire module is called with items."""
        items = [
            {
                "rotowire_news_id": "fx_call1",
                "player_name": "Carey Price",
                "headline": "Carey Price: Save",
                "content": "Big save.",
                "published_at": "2026-02-18T12:00:00",
            },
        ]
        mock_fetch.return_value = items
        mock_save.return_value = 1

        backfill_fantrax_news(db)

        mock_save.assert_called_once_with(db, items)

    @patch("fetchers.fantrax_news.fetch_news_page")
    def test_backfill_empty_fetch(
        self,
        mock_fetch: MagicMock,
        db: sqlite3.Connection,
    ) -> None:
        """Empty fetch result returns summary with all zeros."""
        mock_fetch.return_value = []

        summary = backfill_fantrax_news(db)

        assert summary["total_fetched"] == 0
        assert summary["new_inserted"] == 0
        assert summary["duplicates_skipped"] == 0


# =============================================================================
# News ID Format Tests
# =============================================================================


class TestNewsIdFormat:
    """Tests for ID generation in _parse_news_item and API fetcher."""

    def test_fx_hash_format(self) -> None:
        """Fantrax IDs start with 'fx_' prefix."""
        el = _make_mock_element()
        result = _parse_news_item(el)

        assert result is not None
        assert result["rotowire_news_id"].startswith("fx_")

    def test_deterministic_hash(self) -> None:
        """Same input produces the same ID."""
        el1 = _make_mock_element(
            player_name="Test Player", headline="Scores goal",
            date_text="Feb 18, 2026, 5:55 PM",
        )
        el2 = _make_mock_element(
            player_name="Test Player", headline="Scores goal",
            date_text="Feb 18, 2026, 5:55 PM",
        )

        r1 = _parse_news_item(el1)
        r2 = _parse_news_item(el2)

        assert r1["rotowire_news_id"] == r2["rotowire_news_id"]

    def test_different_inputs_different_hashes(self) -> None:
        """Different player/headline/date produce different IDs."""
        el1 = _make_mock_element(player_name="Player A", headline="Goal")
        el2 = _make_mock_element(player_name="Player B", headline="Assist")

        r1 = _parse_news_item(el1)
        r2 = _parse_news_item(el2)

        assert r1["rotowire_news_id"] != r2["rotowire_news_id"]

    def test_no_collision_with_rss_ids(self) -> None:
        """Fantrax fx_ IDs never match the nhl* pattern used by RSS."""
        el = _make_mock_element()
        result = _parse_news_item(el)

        news_id = result["rotowire_news_id"]
        assert not news_id.startswith("nhl")
        assert news_id.startswith("fx_")

    def test_hash_length(self) -> None:
        """Hash portion is 12 hex characters (fx_ + 12 = 15 chars total)."""
        el = _make_mock_element()
        result = _parse_news_item(el)

        news_id = result["rotowire_news_id"]
        hash_part = news_id[3:]  # after "fx_"
        assert len(hash_part) == 12
        # Verify it's valid hex
        int(hash_part, 16)

    def test_parse_returns_none_for_no_headline(self) -> None:
        """Element with no headline text returns None."""
        el = _make_mock_element(headline="")
        result = _parse_news_item(el)

        assert result is None
