"""Fantrax player news fetcher.

Scrapes player news from the Fantrax news page, which aggregates
Rotowire content with infinite scrolling.  Uses the Fantrax API
as a lightweight fallback for daily sync.

Re-uses ``save_news()`` from :mod:`fetchers.rotowire` for storage.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import pickle
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a, **kw):  # type: ignore[misc]
        pass

from db.schema import get_db, init_db
from fetchers.rotowire import save_news

logger = logging.getLogger("pipeline.fantrax_news")

FANTRAX_NEWS_URL = "https://www.fantrax.com/news/nhl/player-news"
FANTRAX_LOGIN_URL = "https://www.fantrax.com/login"
FANTRAX_API_URL = "https://www.fantrax.com/fxpa/req"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Project root is two levels up from fetchers/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ENV_PATH = _PROJECT_ROOT / ".env"

# Selectors discovered via probe (Fantrax Angular app)
_ITEM_SELECTOR = ".fx-layout__pane.news-columns"
_PLAYER_NAME_SELECTOR = ".scorer__info__name a"
_HEADLINE_SELECTOR = ".news-columns__content h4"
_ANALYSIS_SELECTOR = ".news-columns__content p span"
_DATE_SELECTOR = ".news-columns__content .button-group h6"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _load_env() -> dict[str, Any]:
    """Load Fantrax credentials from .env file."""
    load_dotenv(_ENV_PATH)
    cookie_name = os.getenv("FANTRAX_COOKIE_FILE", "fantraxloggedin.cookie")
    return {
        "username": os.getenv("FANTRAX_USERNAME", ""),
        "password": os.getenv("FANTRAX_PASSWORD", ""),
        "league_id": os.getenv("FANTRAX_LEAGUE_ID", ""),
        "cookie_file": _PROJECT_ROOT / cookie_name,
    }


# ---------------------------------------------------------------------------
# Cookie management (shared between requests.Session and Playwright)
# ---------------------------------------------------------------------------

def _load_cookies_for_session(
    session: requests.Session, cookie_path: Path,
) -> bool:
    """Load pickle cookies into a requests.Session.

    Returns True if cookies were loaded.
    """
    if not cookie_path.exists():
        return False
    try:
        with open(cookie_path, "rb") as f:
            cookies = pickle.load(f)
        for cookie in cookies:
            session.cookies.set(cookie["name"], cookie["value"])
        logger.debug("Loaded %d cookies from %s", len(cookies), cookie_path)
        return True
    except Exception as exc:
        logger.warning("Failed to load cookies: %s", exc)
        return False


def _load_cookies_for_playwright(context, cookie_path: Path) -> bool:
    """Load pickle cookies into a Playwright browser context.

    Returns True if cookies were loaded.
    """
    if not cookie_path.exists():
        return False
    try:
        with open(cookie_path, "rb") as f:
            cookies = pickle.load(f)
        pw_cookies = [
            {
                "name": c["name"],
                "value": c["value"],
                "domain": c.get("domain", ".fantrax.com"),
                "path": c.get("path", "/"),
            }
            for c in cookies
        ]
        context.add_cookies(pw_cookies)
        logger.debug("Loaded %d cookies into browser context", len(cookies))
        return True
    except Exception as exc:
        logger.warning("Failed to load cookies for Playwright: %s", exc)
        return False


def _save_cookies_from_playwright(context, cookie_path: Path) -> None:
    """Save Playwright browser cookies to pickle in Selenium-compatible format."""
    pw_cookies = context.cookies()
    selenium_cookies = [
        {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ""),
            "path": c.get("path", "/"),
            "secure": c.get("secure", False),
            "httpOnly": c.get("httpOnly", False),
        }
        for c in pw_cookies
    ]
    with open(cookie_path, "wb") as f:
        pickle.dump(selenium_cookies, f)
    logger.info("Saved %d cookies to %s", len(selenium_cookies), cookie_path)


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

def _login_fantrax(page, context, config: dict) -> None:
    """Perform Fantrax login and save cookies.

    Replicates the v1 Selenium login flow using Playwright:
    1. Navigate to login page
    2. Fill email / password
    3. Press Enter, wait
    4. Save cookies to pickle
    """
    logger.info("Logging in to Fantrax...")
    page.goto(FANTRAX_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
    time.sleep(2)

    email_input = page.wait_for_selector(
        "//input[@formcontrolname='email']", timeout=10_000,
    )
    email_input.fill(config["username"])

    password_input = page.wait_for_selector(
        "//input[@formcontrolname='password']", timeout=10_000,
    )
    password_input.fill(config["password"])
    password_input.press("Enter")

    page.wait_for_timeout(5000)

    if "login" in page.url.lower():
        raise RuntimeError(f"Login failed — still on {page.url}")

    _save_cookies_from_playwright(context, config["cookie_file"])
    logger.info("Login successful")


def _dismiss_overlays(page) -> None:
    """Dismiss cookie-consent or notification overlays."""
    # Try main-frame buttons first
    for sel in [
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        ".cky-btn-accept",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                page.wait_for_timeout(500)
                logger.debug("Dismissed overlay: %s", sel)
                return
        except Exception:
            continue

    # Try inside iframes (Fantrax uses iframe consent)
    try:
        for frame in page.frames:
            for sel in ["button:has-text('Accept')", ".cky-btn-accept"]:
                try:
                    btn = frame.query_selector(sel)
                    if btn and btn.is_visible():
                        btn.click()
                        page.wait_for_timeout(500)
                        logger.debug("Dismissed overlay in iframe: %s", sel)
                        return
                except Exception:
                    continue
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Page scraping (primary approach for backfill)
# ---------------------------------------------------------------------------

def _parse_news_item(element) -> dict[str, Any] | None:
    """Extract a single news item from a ``news-columns`` DOM element.

    Returns a dict compatible with :func:`fetchers.rotowire.save_news`,
    or ``None`` if the element cannot be parsed.
    """
    try:
        # Player name
        name_el = element.query_selector(_PLAYER_NAME_SELECTOR)
        player_name = name_el.inner_text().strip() if name_el else ""

        # Headline
        headline_el = element.query_selector(_HEADLINE_SELECTOR)
        headline_text = headline_el.inner_text().strip() if headline_el else ""
        if not headline_text:
            return None

        # Combined headline in "Player: Headline" format (matches Rotowire RSS)
        headline = f"{player_name}: {headline_text}" if player_name else headline_text

        # Analysis text
        analysis_el = element.query_selector(_ANALYSIS_SELECTOR)
        analysis = analysis_el.inner_text().strip() if analysis_el else ""

        # Date (e.g. "Feb 18, 2026, 5:55 PM")
        date_el = element.query_selector(_DATE_SELECTOR)
        published_at = ""
        if date_el:
            raw_date = date_el.inner_text().strip()
            try:
                dt = datetime.strptime(raw_date, "%b %d, %Y, %I:%M %p")
                published_at = dt.isoformat()
            except ValueError:
                published_at = raw_date

        # Generate stable ID from player + headline + date
        hash_input = f"{player_name}|{headline_text}|{published_at}"
        fx_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]

        return {
            "rotowire_news_id": f"fx_{fx_hash}",
            "player_name": player_name,
            "headline": headline,
            "content": analysis,
            "published_at": published_at,
        }
    except Exception as exc:
        logger.warning("Failed to parse news element: %s", exc)
        return None


def fetch_news_page(
    max_scrolls: int = 50,
    scroll_delay: float = 2.0,
    stop_date: str | None = None,
    config: dict | None = None,
) -> list[dict[str, Any]]:
    """Fetch player news by scraping the Fantrax news page.

    Navigates to the Fantrax player news page (which aggregates
    Rotowire content) and scrolls to load historical items via
    infinite scroll.

    Args:
        max_scrolls: Maximum scroll iterations.
        scroll_delay: Seconds between scrolls.
        stop_date: ISO date (e.g. ``"2026-01-18"``).  Stop once the
            oldest visible item predates this date.
        config: Fantrax config dict.  Loaded from ``.env`` if None.

    Returns:
        List of dicts with keys: rotowire_news_id, player_name,
        headline, content, published_at.

    Raises:
        RuntimeError: If Playwright is not installed.
    """
    if not HAS_PLAYWRIGHT:
        raise RuntimeError(
            "Playwright required. Install with: "
            "pip install playwright && playwright install chromium"
        )

    if config is None:
        config = _load_env()

    logger.info(
        "Starting Fantrax page scrape (max_scrolls=%d, delay=%.1fs, stop=%s)",
        max_scrolls, scroll_delay, stop_date,
    )

    items: list[dict[str, Any]] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 900},
        )

        _load_cookies_for_playwright(context, config["cookie_file"])
        page = context.new_page()

        try:
            page.goto(
                FANTRAX_NEWS_URL, wait_until="domcontentloaded", timeout=60_000,
            )
            time.sleep(5)

            # If redirected to login, authenticate
            if "login" in page.url.lower():
                _login_fantrax(page, context, config)
                page.goto(
                    FANTRAX_NEWS_URL,
                    wait_until="domcontentloaded",
                    timeout=60_000,
                )
                time.sleep(5)

            _dismiss_overlays(page)

            # --- Infinite scroll loop ---
            stale_count = 0
            prev_count = 0
            for scroll_num in range(1, max_scrolls + 1):
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(int(scroll_delay * 1000))

                current_count = len(page.query_selector_all(_ITEM_SELECTOR))

                if current_count == prev_count:
                    stale_count += 1
                    if stale_count >= 3:
                        logger.info(
                            "No new items for 3 scrolls, stopping at %d items",
                            current_count,
                        )
                        break
                else:
                    stale_count = 0

                prev_count = current_count

                # Check stop_date on last visible item
                if stop_date:
                    all_els = page.query_selector_all(_ITEM_SELECTOR)
                    if all_els:
                        last = _parse_news_item(all_els[-1])
                        if (
                            last
                            and last.get("published_at", "")
                            and last["published_at"] < stop_date
                        ):
                            logger.info(
                                "Reached stop_date %s at scroll %d",
                                stop_date, scroll_num,
                            )
                            break

                if scroll_num % 5 == 0:
                    logger.info(
                        "Scroll %d/%d — %d items loaded",
                        scroll_num, max_scrolls, current_count,
                    )

            # --- Extract all items from final DOM ---
            elements = page.query_selector_all(_ITEM_SELECTOR)
            logger.info("Extracting news from %d DOM elements", len(elements))

            for el in elements:
                parsed = _parse_news_item(el)
                if parsed is not None:
                    items.append(parsed)

        except Exception:
            logger.exception("Fantrax page scrape failed")
            raise
        finally:
            browser.close()

    # Deduplicate (same item can appear if DOM shifted during scrolling)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in items:
        key = item["rotowire_news_id"]
        if key not in seen:
            seen.add(key)
            unique.append(item)

    logger.info(
        "Page scrape complete: %d items extracted, %d unique",
        len(items), len(unique),
    )
    return unique


# ---------------------------------------------------------------------------
# API fetcher (for daily sync — only returns ~1-2 days of news)
# ---------------------------------------------------------------------------

def fetch_news_api(
    config: dict | None = None,
) -> list[dict[str, Any]]:
    """Fetch player news via the Fantrax ``getPlayerNews`` API.

    This returns only the current day's news (~74 items spanning 1–2
    days).  Not suitable for historical backfill but useful for daily
    incremental syncs.

    Args:
        config: Fantrax config dict.  Loaded from ``.env`` if None.

    Returns:
        List of dicts in :func:`save_news` format.
    """
    if config is None:
        config = _load_env()

    session = requests.Session()
    cookie_loaded = _load_cookies_for_session(session, config["cookie_file"])

    if not cookie_loaded:
        logger.warning("No cookies for API call — need browser login first")
        if not HAS_PLAYWRIGHT:
            raise RuntimeError("No cookies and Playwright not available for login")
        with sync_playwright() as pw_inst:
            browser = pw_inst.chromium.launch(headless=True)
            ctx = browser.new_context(user_agent=USER_AGENT)
            pg = ctx.new_page()
            try:
                _login_fantrax(pg, ctx, config)
            finally:
                browser.close()
        _load_cookies_for_session(session, config["cookie_file"])

    payload = {
        "msgs": [{
            "method": "getPlayerNews",
            "data": {
                "leagueId": config["league_id"],
                "poolType": "ALL",
            },
        }],
    }

    resp = session.post(
        FANTRAX_API_URL,
        params={"leagueId": config["league_id"]},
        json=payload,
        timeout=30,
    )
    data = resp.json()

    if "pageError" in data:
        code = data["pageError"].get("code", "unknown")
        raise RuntimeError(f"Fantrax API error: {code}")

    stories = data["responses"][0]["data"].get("stories", [])
    items: list[dict[str, Any]] = []

    for story in stories:
        scorer = story.get("scorerFantasy", {})
        pn = story.get("playerNews", {})
        if not pn:
            continue

        player_name = scorer.get("name", "")

        raw_headline = pn.get("headlineNoBrief") or pn.get("content", "")
        headline = f"{player_name}: {raw_headline}" if player_name else raw_headline

        analysis = pn.get("analysis", "")

        news_date_ms = pn.get("newsDate")
        if news_date_ms:
            published_at = datetime.fromtimestamp(
                news_date_ms / 1000, tz=timezone.utc,
            ).isoformat()
        else:
            published_at = datetime.now(timezone.utc).isoformat()

        hash_input = f"{player_name}|{raw_headline}|{news_date_ms}"
        fx_hash = hashlib.md5(hash_input.encode()).hexdigest()[:12]

        items.append({
            "rotowire_news_id": f"fx_{fx_hash}",
            "player_name": player_name,
            "headline": headline,
            "content": analysis,
            "published_at": published_at,
        })

    logger.info("Fetched %d news items from Fantrax API", len(items))
    return items


# ---------------------------------------------------------------------------
# Backfill wrapper (called by pipeline)
# ---------------------------------------------------------------------------

def backfill_fantrax_news(
    conn: sqlite3.Connection,
    max_scrolls: int = 50,
    scroll_delay: float = 2.0,
    stop_date: str | None = None,
) -> dict[str, Any]:
    """Scrape Fantrax news page and save to database.

    Primary entry point for the pipeline.  Uses page scraping with
    infinite scroll to collect historical news items.

    Args:
        conn: Database connection.
        max_scrolls: Maximum scroll iterations.
        scroll_delay: Seconds between scrolls.
        stop_date: ISO date to stop at.

    Returns:
        Summary dict with total_fetched, new_inserted, duplicates_skipped.
    """
    items = fetch_news_page(
        max_scrolls=max_scrolls,
        scroll_delay=scroll_delay,
        stop_date=stop_date,
    )

    new_inserted = save_news(conn, items) if items else 0
    duplicates = len(items) - new_inserted

    summary = {
        "total_fetched": len(items),
        "new_inserted": new_inserted,
        "duplicates_skipped": duplicates,
    }

    logger.info(
        "Fantrax backfill: fetched=%d, inserted=%d, dupes=%d",
        summary["total_fetched"],
        summary["new_inserted"],
        summary["duplicates_skipped"],
    )
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for standalone usage."""
    parser = argparse.ArgumentParser(description="Fantrax news fetcher")
    parser.add_argument(
        "--max-scrolls", type=int, default=50,
        help="Max scroll iterations (default: 50)",
    )
    parser.add_argument(
        "--delay", type=float, default=2.0,
        help="Seconds between scrolls (default: 2.0)",
    )
    parser.add_argument(
        "--stop-date", type=str, default=None,
        help="Stop at this date (ISO format, e.g. 2026-01-18)",
    )
    parser.add_argument(
        "--api-only", action="store_true",
        help="Use API instead of page scraping (only ~1-2 days of data)",
    )
    parser.add_argument(
        "--db", type=str, default="db/nhl_data.db",
        help="Database path (default: db/nhl_data.db)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    db_path = Path(args.db)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)
    conn = get_db(db_path)

    try:
        if args.api_only:
            items = fetch_news_api()
            new_inserted = save_news(conn, items)
            print(f"API: fetched {len(items)}, inserted {new_inserted}")
        else:
            summary = backfill_fantrax_news(
                conn,
                max_scrolls=args.max_scrolls,
                scroll_delay=args.delay,
                stop_date=args.stop_date,
            )
            print(f"Backfill complete: {summary}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
