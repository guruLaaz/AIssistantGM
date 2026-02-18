"""Rotowire news and injuries fetcher.

Fetches player news (RSS), injury reports (JSON), and Rotowire player IDs
from the Rotowire website.
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import time
import unicodedata
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests

from db.schema import get_db, init_db, upsert_player

logger = logging.getLogger(__name__)

RSS_URL = "https://www.rotowire.com/rss/news.php?sport=nhl"
INJURY_URL = "https://www.rotowire.com/hockey/tables/injury-report.php?team=ALL&pos=ALL"
SEARCH_URL = "https://www.rotowire.com/frontend/ajax/search-players.php"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.rotowire.com/hockey/injury-report.php",
    "X-Requested-With": "XMLHttpRequest",
}


def _strip_accents(text: str) -> str:
    """Remove accents from a string (e.g. 'Jose' from 'Jose')."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def match_player_name(conn: sqlite3.Connection, name: str | None) -> int | None:
    """Match a Rotowire player name to a player_id in our players table.

    Tries in order: exact match, case-insensitive, accent-normalized,
    first-initial + last-name pattern.

    Args:
        conn: Database connection.
        name: Player full name from Rotowire.

    Returns:
        player_id or None if no match found.
    """
    if not name:
        return None

    name = name.strip()
    if not name:
        return None

    # 1. Exact match
    cursor = conn.execute(
        "SELECT id FROM players WHERE full_name = ?", (name,)
    )
    row = cursor.fetchone()
    if row:
        return row["id"]

    # 2. Case-insensitive match
    cursor = conn.execute(
        "SELECT id FROM players WHERE LOWER(full_name) = LOWER(?)", (name,)
    )
    row = cursor.fetchone()
    if row:
        return row["id"]

    # 3. Accent-normalized match
    normalized_name = _strip_accents(name).lower()
    cursor = conn.execute("SELECT id, full_name FROM players")
    for row in cursor.fetchall():
        if row["full_name"] and _strip_accents(row["full_name"]).lower() == normalized_name:
            return row["id"]

    # 4. First initial + last name (e.g. "C. McDavid")
    match = re.match(r"^([A-Za-z])\.\s*(.+)$", name)
    if match:
        initial = match.group(1).upper()
        last_name = match.group(2).strip()
        cursor = conn.execute(
            "SELECT id, first_name FROM players WHERE LOWER(last_name) = LOWER(?)",
            (last_name,),
        )
        for row in cursor.fetchall():
            if row["first_name"] and row["first_name"][0].upper() == initial:
                return row["id"]

    return None


def fetch_news(
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch the NHL RSS news feed from Rotowire.

    Args:
        session: Optional requests session for connection pooling.

    Returns:
        List of dicts with keys: rotowire_news_id, player_name, headline,
        content, published_at.

    Raises:
        requests.HTTPError: On non-200 response.
        requests.ConnectionError: On network failure.
    """
    if session is None:
        session = requests.Session()

    response = session.get(RSS_URL, timeout=30)
    response.raise_for_status()

    try:
        root = ET.fromstring(response.text)
    except ET.ParseError:
        logger.warning("Failed to parse RSS XML")
        return []

    items: list[dict[str, Any]] = []
    for item in root.iter("item"):
        guid = item.findtext("guid", "")
        title = item.findtext("title", "")
        description = item.findtext("description", "")
        pub_date = item.findtext("pubDate", "")

        # Extract player name from title (text before the colon)
        player_name = ""
        if ":" in title:
            player_name = title.split(":", 1)[0].strip()

        items.append({
            "rotowire_news_id": guid,
            "player_name": player_name,
            "headline": title,
            "content": description.strip(),
            "published_at": pub_date,
        })

    return items


def save_news(
    conn: sqlite3.Connection,
    news_items: list[dict[str, Any]],
) -> int:
    """Insert news items into player_news table.

    Uses INSERT OR IGNORE to skip duplicates on rotowire_news_id.

    Args:
        conn: Database connection.
        news_items: List of dicts from fetch_news.

    Returns:
        Count of new rows inserted.
    """
    count = 0
    for item in news_items:
        player_id = match_player_name(conn, item.get("player_name"))
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO player_news
                (rotowire_news_id, player_id, headline, content, published_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                item["rotowire_news_id"],
                player_id,
                item.get("headline"),
                item.get("content"),
                item.get("published_at"),
            ),
        )
        count += cursor.rowcount
    conn.commit()
    return count


def fetch_injuries(
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch the injury report JSON from Rotowire.

    Args:
        session: Optional requests session for connection pooling.

    Returns:
        Raw list of injury dicts from the API.

    Raises:
        requests.HTTPError: On non-200 response.
        requests.ConnectionError: On network failure.
    """
    if session is None:
        session = requests.Session()

    response = session.get(INJURY_URL, headers=BROWSER_HEADERS, timeout=30)
    response.raise_for_status()

    return response.json()


def save_injuries(
    conn: sqlite3.Connection,
    injuries: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert injuries into player_injuries table.

    For matched players, also updates players.rotowire_id.
    Unmatched players stored with player_id=NULL.

    Args:
        conn: Database connection.
        injuries: List of injury dicts from fetch_injuries.

    Returns:
        Tuple of (upserted_count, unmatched_count).
    """
    upserted = 0
    unmatched = 0

    # Clear previous unmatched injuries from rotowire so re-runs don't
    # accumulate duplicates (NULL player_id bypasses UNIQUE constraint).
    conn.execute(
        "DELETE FROM player_injuries WHERE player_id IS NULL AND source = 'rotowire'"
    )

    for injury in injuries:
        player_name = injury.get("player", "")
        player_id = match_player_name(conn, player_name)

        if player_id is not None:
            # Update rotowire_id on matched player
            rotowire_id = int(injury["ID"])
            upsert_player(conn, {"id": player_id, "rotowire_id": rotowire_id})
        else:
            unmatched += 1

        conn.execute(
            """
            INSERT OR REPLACE INTO player_injuries
                (player_id, source, injury_type, status, updated_at)
            VALUES (?, 'rotowire', ?, ?, ?)
            """,
            (
                player_id,
                injury.get("injury"),
                injury.get("status"),
                injury.get("date"),
            ),
        )
        upserted += 1

    conn.commit()
    return upserted, unmatched


def search_rotowire_player(
    name: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Search the Rotowire player search API filtered to hockey.

    Args:
        name: Player name to search for.
        session: Optional requests session.

    Returns:
        List of dicts with keys: rotowire_id, name, team, position, link.

    Raises:
        requests.HTTPError: On non-200 response.
    """
    if session is None:
        session = requests.Session()

    response = session.get(
        SEARCH_URL, params={"searchTerm": name}, timeout=30
    )
    response.raise_for_status()

    data = response.json()
    results: list[dict[str, Any]] = []

    for player in data.get("players", []):
        link = player.get("link", "")
        if not link.startswith("/hockey/"):
            continue
        results.append({
            "rotowire_id": int(player["rotoPlayerID"]),
            "name": player.get("name", ""),
            "team": player.get("text", ""),
            "position": player.get("span", ""),
            "link": link,
        })

    return results


def discover_rotowire_ids(
    conn: sqlite3.Connection,
    session: requests.Session | None = None,
) -> int:
    """Look up Rotowire IDs for players missing them.

    For each player with rotowire_id IS NULL, searches Rotowire by name.
    If exactly one hockey result is found, updates the player's rotowire_id.

    Args:
        conn: Database connection.
        session: Optional requests session.

    Returns:
        Count of Rotowire IDs discovered.
    """
    if session is None:
        session = requests.Session()

    cursor = conn.execute(
        "SELECT id, full_name FROM players WHERE rotowire_id IS NULL"
    )
    players = cursor.fetchall()

    discovered = 0
    for i, player in enumerate(players):
        full_name = player["full_name"]
        if not full_name:
            continue

        if i > 0:
            time.sleep(0.5)

        try:
            results = search_rotowire_player(full_name, session)
        except (requests.HTTPError, requests.ConnectionError) as e:
            logger.warning("Search failed for %s: %s", full_name, e)
            continue

        if len(results) == 1:
            upsert_player(
                conn, {"id": player["id"], "rotowire_id": results[0]["rotowire_id"]}
            )
            discovered += 1

    return discovered


def sync_rotowire(
    conn: sqlite3.Connection,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """Run news and injury fetchers. Continues if one step fails.

    Args:
        conn: Database connection.
        session: Optional requests session.

    Returns:
        Summary dict with news_added, injuries_upserted, injuries_unmatched.
    """
    result: dict[str, Any] = {
        "news_added": 0,
        "injuries_upserted": 0,
        "injuries_unmatched": 0,
    }

    # News
    try:
        news_items = fetch_news(session)
        result["news_added"] = save_news(conn, news_items)
    except Exception as e:
        logger.warning("News fetch/save failed: %s", e)

    # Injuries
    try:
        injuries = fetch_injuries(session)
        upserted, unmatched = save_injuries(conn, injuries)
        result["injuries_upserted"] = upserted
        result["injuries_unmatched"] = unmatched
    except Exception as e:
        logger.warning("Injury fetch/save failed: %s", e)

    return result


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Rotowire news and injuries fetcher"
    )
    parser.add_argument(
        "--all", action="store_true", help="Fetch news and injuries"
    )
    parser.add_argument(
        "--news", action="store_true", help="Fetch RSS news only"
    )
    parser.add_argument(
        "--injuries", action="store_true", help="Fetch injury report only"
    )
    parser.add_argument(
        "--discover", action="store_true", help="Look up missing Rotowire IDs"
    )
    parser.add_argument(
        "--db", type=str, default="db/nhl_data.db", help="Database path"
    )

    args = parser.parse_args()

    db_path = Path(args.db)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)
    conn = get_db(db_path)

    session = requests.Session()

    if args.all:
        result = sync_rotowire(conn, session)
        print(f"Sync complete: {result}")

    elif args.news:
        items = fetch_news(session)
        count = save_news(conn, items)
        print(f"Added {count} news items")

    elif args.injuries:
        injuries = fetch_injuries(session)
        upserted, unmatched = save_injuries(conn, injuries)
        print(f"Upserted {upserted} injuries ({unmatched} unmatched)")

    elif args.discover:
        count = discover_rotowire_ids(conn, session)
        print(f"Discovered {count} Rotowire IDs")

    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
