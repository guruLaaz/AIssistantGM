"""Web scraper for Fantrax data that's not available via API."""

import pickle
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import Keys
from webdriver_manager.chrome import ChromeDriverManager
from rich.console import Console


class FantraxScraper:
    """Scraper for Fantrax website content not available via API."""

    def __init__(
        self,
        league_id: str,
        username: str,
        password: str,
        cookie_file: Path,
        console: Optional[Console] = None,
        selenium_timeout: int = 10,
        login_wait_time: int = 5,
        browser_window_size: str = "1920,1600",
        user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        max_retries: int = 3,
        retry_delay: float = 2.0,
        retry_backoff: float = 2.0
    ):
        """Initialize the scraper.

        Args:
            league_id: Fantrax league ID
            username: Fantrax username/email
            password: Fantrax password
            cookie_file: Path to cookie cache file
            console: Rich console for output
            selenium_timeout: Seconds to wait for page elements
            login_wait_time: Seconds to wait after login
            browser_window_size: Chrome window dimensions
            user_agent: Browser user-agent string
            max_retries: Maximum number of retries for network requests
            retry_delay: Initial delay between retries in seconds
            retry_backoff: Multiplier for delay between retries (exponential backoff)
        """
        self.league_id = league_id
        self.username = username
        self.password = password
        self.cookie_file = cookie_file
        self.console = console or Console()
        self.selenium_timeout = selenium_timeout
        self.login_wait_time = login_wait_time
        self.browser_window_size = browser_window_size
        self.user_agent = user_agent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.retry_backoff = retry_backoff

    def _get_driver(self) -> webdriver.Chrome:
        """Create and configure Chrome WebDriver."""
        service = Service(ChromeDriverManager().install())
        options = Options()
        options.add_argument("--headless")
        options.add_argument(f"--window-size={self.browser_window_size}")
        options.add_argument(f"user-agent={self.user_agent}")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        return webdriver.Chrome(service=service, options=options)

    def _get_with_retry(self, driver: webdriver.Chrome, url: str) -> bool:
        """Navigate to URL with retry logic for network timeouts.

        Args:
            driver: Selenium WebDriver instance
            url: URL to navigate to

        Returns:
            True if navigation succeeded, False if all retries failed (network errors only)

        Raises:
            Exception: For non-network errors (immediately, without retry)
        """
        delay = self.retry_delay

        for attempt in range(self.max_retries + 1):
            try:
                driver.get(url)
                return True
            except Exception as e:
                error_str = str(e).lower()
                # Only retry on network-related errors
                if any(x in error_str for x in ['timeout', 'connection', 'httpconnectionpool', 'read timed out']):
                    if attempt < self.max_retries:
                        self.console.print(f"[dim]    Network error, retrying in {delay:.1f}s (attempt {attempt + 1}/{self.max_retries})...[/dim]")
                        time.sleep(delay)
                        delay *= self.retry_backoff
                    else:
                        self.console.print(f"[yellow]    Max retries reached for {url}[/yellow]")
                        return False
                else:
                    # Non-network error, don't retry
                    raise

        return False

    def _dismiss_cookie_popup(self, driver: webdriver.Chrome) -> None:
        """Try to dismiss cookie consent popup if present."""
        try:
            # Wait briefly for popup to appear
            time.sleep(1)

            # Try to find and click accept/close button in iframe
            iframes = driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                if "consent" in (iframe.get_attribute("id") or "").lower() or \
                   "privacy" in (iframe.get_attribute("src") or "").lower():
                    try:
                        driver.switch_to.frame(iframe)
                        # Try various accept button selectors
                        accept_selectors = [
                            "//button[contains(text(), 'Accept')]",
                            "//button[contains(text(), 'OK')]",
                            "//button[contains(text(), 'Agree')]",
                            "//button[contains(@class, 'accept')]",
                            "//button[@title='Accept']",
                        ]
                        for selector in accept_selectors:
                            try:
                                btn = driver.find_element(By.XPATH, selector)
                                btn.click()
                                self.console.print("[dim]Dismissed cookie popup[/dim]")
                                driver.switch_to.default_content()
                                time.sleep(1)
                                return
                            except Exception:
                                continue
                        driver.switch_to.default_content()
                    except Exception:
                        driver.switch_to.default_content()
        except Exception:
            pass

    def _login(self, driver: webdriver.Chrome) -> bool:
        """Login to Fantrax using stored cookies or credentials.

        Args:
            driver: Selenium WebDriver instance

        Returns:
            True if login successful, False otherwise
        """
        # Try to use cached cookies first
        if self.cookie_file.exists():
            if not self._get_with_retry(driver, "https://www.fantrax.com"):
                return False
            self._dismiss_cookie_popup(driver)

            with open(self.cookie_file, "rb") as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    try:
                        driver.add_cookie(cookie)
                    except Exception:
                        pass  # Some cookies may not be valid for current domain

            # Refresh to apply cookies
            if not self._get_with_retry(driver, f"https://www.fantrax.com/fantasy/league/{self.league_id}/home"):
                return False
            time.sleep(2)
            self._dismiss_cookie_popup(driver)

            # Check if we're logged in
            if "login" not in driver.current_url.lower():
                return True

        # Need to login with credentials
        self.console.print("[dim]Logging in to Fantrax...[/dim]")
        if not self._get_with_retry(driver, "https://www.fantrax.com/login"):
            return False
        self._dismiss_cookie_popup(driver)

        try:
            # Wait for and fill in username
            username_box = WebDriverWait(driver, self.selenium_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//input[@formcontrolname='email']"))
            )
            username_box.send_keys(self.username)

            # Wait for and fill in password
            password_box = WebDriverWait(driver, self.selenium_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//input[@formcontrolname='password']"))
            )
            password_box.send_keys(self.password)
            password_box.send_keys(Keys.ENTER)

            # Wait for login to complete
            time.sleep(self.login_wait_time)

            # Save cookies
            cookies = driver.get_cookies()
            with open(self.cookie_file, "wb") as cookie_file:
                pickle.dump(cookies, cookie_file)

            return "login" not in driver.current_url.lower()
        except Exception as e:
            self.console.print(f"[red]Login failed: {e}[/red]")
            return False

    def scrape_player_news(
        self,
        max_pages: int = 5,
        max_items_per_page: int = 50,
        scroll_pause_time: float = 1.5
    ) -> list[dict]:
        """Scrape player news from the Fantrax website.

        Args:
            max_pages: Maximum number of pages to scrape
            max_items_per_page: Maximum news items to collect per page
            scroll_pause_time: Seconds to wait after scrolling

        Returns:
            List of news items with keys: player_id, player_name, news_date, headline, analysis
        """
        news_items = []

        with self._get_driver() as driver:
            if not self._login(driver):
                self.console.print("[red]Failed to login to Fantrax[/red]")
                return news_items

            # Navigate to the players page with news view
            news_url = f"https://www.fantrax.com/fantasy/league/{self.league_id}/players;statusOrTeamFilter=ALL;pageNumber=1;positionOrGroup=ALL;miscDisplayType=1"
            if not self._get_with_retry(driver, news_url):
                self.console.print("[red]Failed to load news page[/red]")
                return news_items
            time.sleep(5)
            self._dismiss_cookie_popup(driver)
            time.sleep(3)

            # The news is displayed in tooltips when hovering over news icons
            # Each player with news has a news icon (PLAYER_NEWS_RECENT or PLAYER_NEWS_HOT)
            # We need to hover over each icon to get the tooltip content

            from selenium.webdriver.common.action_chains import ActionChains

            # Find all news icons (players with recent news have these icons)
            news_icons = driver.find_elements(
                By.XPATH,
                "//*[contains(@class, 'PLAYER_NEWS')]"
            )

            # Process each news icon
            actions = ActionChains(driver)
            processed_count = 0

            for i, icon in enumerate(news_icons):
                if processed_count >= 50:  # Limit to prevent too long scraping
                    break

                try:
                    # Scroll icon into view
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", icon)
                    time.sleep(0.3)

                    # Hover over the icon to trigger tooltip
                    actions.move_to_element(icon).perform()
                    time.sleep(0.5)  # Wait for tooltip to appear

                    # Look for tooltip content
                    # Angular Material tooltips typically have mat-tooltip or cdk-overlay class
                    tooltip_selectors = [
                        "//div[contains(@class, 'mat-mdc-tooltip')]",
                        "//div[contains(@class, 'cdk-overlay')]//div",
                        "//div[contains(@class, 'tooltip')]",
                        "//*[contains(@class, 'tooltip-content')]",
                    ]

                    tooltip_text = None
                    for sel in tooltip_selectors:
                        try:
                            tooltips = driver.find_elements(By.XPATH, sel)
                            for tt in tooltips:
                                text = tt.text.strip()
                                if text and len(text) > 10:
                                    tooltip_text = text
                                    break
                            if tooltip_text:
                                break
                        except Exception:
                            continue

                    if tooltip_text:
                        # Parse the tooltip text to extract date, player name, and headline
                        parsed = self._parse_tooltip_text(tooltip_text)
                        if parsed:
                            news_items.append(parsed)
                            processed_count += 1

                    # Move mouse away to close tooltip
                    actions.move_by_offset(100, 100).perform()
                    time.sleep(0.2)

                except Exception as e:
                    continue

            self.console.print(f"[dim]  Page 1: {processed_count} news items[/dim]")

            # Navigate to additional pages to get more news
            for page_num in range(2, max_pages + 1):
                page_url = f"https://www.fantrax.com/fantasy/league/{self.league_id}/players;statusOrTeamFilter=ALL;pageNumber={page_num};positionOrGroup=ALL;miscDisplayType=1"
                if not self._get_with_retry(driver, page_url):
                    self.console.print(f"[yellow]Failed to load page {page_num}, stopping pagination[/yellow]")
                    break
                time.sleep(3)

                # Find news icons on this page
                news_icons = driver.find_elements(
                    By.XPATH,
                    "//*[contains(@class, 'PLAYER_NEWS')]"
                )

                if not news_icons:
                    break  # No more players with news on this page

                page_count = 0
                for i, icon in enumerate(news_icons):
                    if page_count >= max_items_per_page:
                        break

                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", icon)
                        time.sleep(0.3)
                        actions.move_to_element(icon).perform()
                        time.sleep(0.5)

                        tooltip_text = None
                        for sel in tooltip_selectors:
                            try:
                                tooltips = driver.find_elements(By.XPATH, sel)
                                for tt in tooltips:
                                    text = tt.text.strip()
                                    if text and len(text) > 10:
                                        tooltip_text = text
                                        break
                                if tooltip_text:
                                    break
                            except Exception:
                                continue

                        if tooltip_text:
                            parsed = self._parse_tooltip_text(tooltip_text)
                            if parsed:
                                # Check for duplicates
                                is_duplicate = any(
                                    n['headline'] == parsed['headline'] and n['news_date'] == parsed['news_date']
                                    for n in news_items
                                )
                                if not is_duplicate:
                                    news_items.append(parsed)
                                    page_count += 1

                        actions.move_by_offset(100, 100).perform()
                        time.sleep(0.2)
                    except Exception:
                        continue

                self.console.print(f"[dim]  Page {page_num}: {page_count} news items[/dim]")

        # Show date range of scraped news
        if news_items:
            dates = [item['news_date'][:10] for item in news_items if item.get('news_date')]
            if dates:
                self.console.print(f"[dim]  Scraped {len(news_items)} news items ({min(dates)} to {max(dates)})[/dim]")
            else:
                self.console.print(f"[dim]  Scraped {len(news_items)} news items[/dim]")

        return news_items

    def match_players_with_database(
        self,
        news_items: list[dict],
        db: "DatabaseManager"
    ) -> list[dict]:
        """Match scraped player names with database player IDs.

        Args:
            news_items: List of news items with player_name
            db: DatabaseManager instance

        Returns:
            Updated news items with player_id filled in where possible
        """
        from aissistant_gm.fantrax.database import DatabaseManager

        # Build a lookup dict of player last names to IDs
        # This is a simple approach - we match by last name
        player_lookup = {}

        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name FROM players")
            for row in cursor.fetchall():
                player_id = row['id']
                full_name = row['name']
                if full_name:
                    # Extract last name (usually the last word)
                    parts = full_name.split()
                    if parts:
                        last_name = parts[-1]
                        # Store both full name and last name for matching
                        player_lookup[full_name.lower()] = player_id
                        player_lookup[last_name.lower()] = player_id

        # Match news items
        matched_count = 0
        for item in news_items:
            player_name = item.get('player_name')
            if player_name and not item.get('player_id'):
                # Try exact match first, then last name
                name_lower = player_name.lower()
                if name_lower in player_lookup:
                    item['player_id'] = player_lookup[name_lower]
                    matched_count += 1

        self.console.print(f"[dim]Matched {matched_count}/{len(news_items)} players with database[/dim]")
        return news_items

    def _parse_tooltip_text(self, tooltip_text: str) -> Optional[dict]:
        """Parse tooltip text to extract date, player name, and headline.

        Tooltip format: "Jan 30, 1:34 AM: McDavid scored a goal on two shots..."

        Args:
            tooltip_text: Raw tooltip text

        Returns:
            Dict with parsed news data or None if parsing fails
        """
        import re

        try:
            # Pattern: "Mon DD, H:MM AM/PM: PlayerName action..."
            # The date is at the start, followed by colon, then player name and action
            date_pattern = r'^([A-Z][a-z]{2}\s+\d{1,2},\s+\d{1,2}:\d{2}\s+[AP]M):\s*(.+)$'
            match = re.match(date_pattern, tooltip_text.strip())

            if not match:
                # Try without the time component
                date_pattern2 = r'^([A-Z][a-z]{2}\s+\d{1,2})[:,]\s*(.+)$'
                match = re.match(date_pattern2, tooltip_text.strip())

            if not match:
                return None

            date_str = match.group(1)
            content = match.group(2).strip()

            # Parse the date - add current year
            current_year = datetime.now().year
            try:
                # Try with time
                news_date = datetime.strptime(f"{date_str} {current_year}", "%b %d, %I:%M %p %Y")
            except ValueError:
                try:
                    # Try without time
                    news_date = datetime.strptime(f"{date_str} {current_year}", "%b %d %Y")
                except ValueError:
                    news_date = datetime.now()

            # Handle year boundary (if date is in future, it's probably last year)
            if news_date > datetime.now():
                news_date = news_date.replace(year=current_year - 1)

            # Extract player name from the content
            # The player name is typically the first word(s) before a verb
            # Common patterns: "McDavid scored...", "MacKinnon logged...", "Rantanen notched..."
            player_pattern = r'^([A-Z][a-zA-Z\'-]+(?:\s+[A-Z][a-zA-Z\'-]+)?)\s+(?:scored|logged|notched|registered|added|recorded|posted|collected|tallied|contributed|dished|fired|picked|won|had|will|is|was|isn\'t|won\'t)'
            player_match = re.match(player_pattern, content)

            player_name = None
            if player_match:
                player_name = player_match.group(1).strip()

            # If no player found with verb pattern, try to extract first capitalized word
            if not player_name:
                words = content.split()
                if words and words[0][0].isupper():
                    player_name = words[0]

            return {
                'player_id': None,  # Will be matched later with database
                'player_name': player_name,
                'news_date': news_date.isoformat(),
                'headline': content[:500],
                'analysis': ''
            }

        except Exception:
            return None

    def _parse_news_element(self, element) -> Optional[dict]:
        """Parse a news element into a structured dict.

        Args:
            element: Selenium WebElement containing news item

        Returns:
            Dict with news data or None if parsing fails
        """
        try:
            text = element.text
            if not text or len(text) < 20:
                return None

            # Try to extract player name, date, headline, analysis
            # This will need to be customized based on actual page structure
            lines = text.split('\n')

            news_item = {
                'player_id': None,  # Will need to match with database
                'player_name': None,
                'news_date': datetime.now().isoformat(),
                'headline': lines[0] if lines else text[:200],
                'analysis': '\n'.join(lines[1:]) if len(lines) > 1 else ''
            }

            # Try to find player name in the element
            try:
                player_elem = element.find_element(By.XPATH, ".//a[contains(@href, 'player')]")
                news_item['player_name'] = player_elem.text
                # Extract player ID from href if possible
                href = player_elem.get_attribute("href")
                if href and "playerId=" in href:
                    news_item['player_id'] = href.split("playerId=")[1].split("&")[0]
            except Exception:
                pass

            # Try to find date
            try:
                date_elem = element.find_element(By.XPATH, ".//*[contains(@class, 'date') or contains(@class, 'time')]")
                date_text = date_elem.text
                # Parse date - this will need to be customized
                news_item['news_date'] = date_text
            except Exception:
                pass

            return news_item
        except Exception:
            return None

    def scrape_player_toi(
        self,
        player_ids: list[str],
        team_id: str,
        max_players: int = 50,
        batch_size: int = 30
    ) -> dict[str, dict]:
        """
        Scrape TOI (Time On Ice) data for players from their profile pages.

        Args:
            player_ids: List of player IDs to scrape TOI for
            team_id: Fantasy team ID (used for player page URL)
            max_players: Maximum number of players to scrape
            batch_size: Number of players to scrape before restarting browser
                        (prevents WebDriver timeout issues)

        Returns:
            Dict mapping player_id to TOI data:
            {
                'player_id': {
                    'toi_seconds': int,
                    'toipp_seconds': int,
                    'toish_seconds': int,
                    'games_played': int
                }
            }
        """
        results = {}
        players_to_scrape = player_ids[:max_players]

        if not players_to_scrape:
            return results

        self.console.print(f"[dim]Scraping TOI for {len(players_to_scrape)} players (batch size: {batch_size})...[/dim]")

        # Process in batches to avoid WebDriver timeout
        for batch_start in range(0, len(players_to_scrape), batch_size):
            batch_end = min(batch_start + batch_size, len(players_to_scrape))
            batch = players_to_scrape[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(players_to_scrape) + batch_size - 1) // batch_size

            self.console.print(f"[dim]  Batch {batch_num}/{total_batches}: players {batch_start + 1}-{batch_end}[/dim]")

            driver = None
            try:
                driver = self._get_driver()
                if not self._login(driver):
                    self.console.print("[red]Failed to login to Fantrax[/red]")
                    continue

                for i, player_id in enumerate(batch):
                    try:
                        # Navigate to player page
                        # URL format: /player/{player_id}/{league_id}/{player_name}/{team_id}
                        # We don't have player_name but can use a placeholder - Fantrax redirects
                        player_url = f"https://www.fantrax.com/player/{player_id}/{self.league_id}/_/{team_id}"
                        if not self._get_with_retry(driver, player_url):
                            self.console.print(f"[dim]    Failed to load player {player_id} after retries[/dim]")
                            continue
                        time.sleep(2)  # Wait for page to load

                        # Extract TOI data from page text
                        toi_data = self._extract_toi_from_page(driver)

                        if toi_data:
                            results[player_id] = toi_data

                    except Exception as e:
                        self.console.print(f"[dim]    Error scraping player {player_id}: {e}[/dim]")
                        # If we get a connection error, break out of this batch
                        if "HTTPConnectionPool" in str(e) or "read timed out" in str(e).lower():
                            self.console.print("[dim]    Connection lost, ending batch early[/dim]")
                            break
                        continue

            except Exception as e:
                self.console.print(f"[dim]  Batch error: {e}[/dim]")
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass

            # Brief pause between batches
            if batch_end < len(players_to_scrape):
                time.sleep(2)

        self.console.print(f"[green]✓[/green] Scraped TOI for {len(results)} players")

        return results

    def _extract_toi_from_page(self, driver) -> Optional[dict]:
        """
        Extract TOI data from player page.

        The page shows season stats in text format like:
        GP G A Pt +/- PIM SOG Shot% PPG PPA SHG SHA GWG OG Hit Blk Gv Tk Faceoffs TOI TOIPP TOISH
        57 8 25 33 1 16 82 9.8 1 6 0 0 1 1 33 43 52 15 357-296-55% 16:05 01:20 01:30

        Returns:
            Dict with toi_seconds, toipp_seconds, toish_seconds, games_played or None
        """
        from selenium.webdriver.common.by import By

        try:
            body = driver.find_element(By.TAG_NAME, 'body')
            full_text = body.text
            lines = full_text.split('\n')

            # Find the line with TOI header
            toi_header_idx = None
            for i, line in enumerate(lines):
                if 'TOI' in line and 'TOIPP' in line and 'TOISH' in line:
                    toi_header_idx = i
                    break

            if toi_header_idx is None:
                return None

            # Get headers from the line containing TOI
            # The headers might be on one line or multiple lines before it
            # Look backwards for GP which should be the start
            header_start_idx = toi_header_idx
            for j in range(toi_header_idx, max(0, toi_header_idx - 30), -1):
                if lines[j].strip() == 'GP':
                    header_start_idx = j
                    break

            # Collect headers
            headers = []
            for j in range(header_start_idx, toi_header_idx + 1):
                line = lines[j].strip()
                if line:
                    headers.append(line)

            # Find indices for the stats we need
            try:
                gp_idx = headers.index('GP')
                toi_idx = headers.index('TOI')
                toipp_idx = headers.index('TOIPP')
                toish_idx = headers.index('TOISH')
            except ValueError:
                return None

            # Values should follow the headers
            values = []
            for j in range(toi_header_idx + 1, min(len(lines), toi_header_idx + len(headers) + 5)):
                line = lines[j].strip()
                if line and not any(x in line.lower() for x in ['recent', 'game', 'date', 'team']):
                    values.append(line)
                if len(values) >= len(headers):
                    break

            if len(values) < max(gp_idx, toi_idx, toipp_idx, toish_idx) + 1:
                return None

            # Parse values
            games_played = int(values[gp_idx])
            toi_str = values[toi_idx]
            toipp_str = values[toipp_idx]
            toish_str = values[toish_idx]

            return {
                'toi_seconds': self._parse_time_to_seconds(toi_str),
                'toipp_seconds': self._parse_time_to_seconds(toipp_str),
                'toish_seconds': self._parse_time_to_seconds(toish_str),
                'games_played': games_played
            }

        except Exception:
            return None

    def _parse_time_to_seconds(self, time_str: str) -> int:
        """
        Parse time string in MM:SS format to total seconds.

        Args:
            time_str: Time string like "16:05" or "01:30"

        Returns:
            Total seconds as integer
        """
        try:
            if ':' not in time_str:
                return 0
            parts = time_str.split(':')
            minutes = int(parts[0])
            seconds = int(parts[1])
            return minutes * 60 + seconds
        except (ValueError, IndexError):
            return 0
