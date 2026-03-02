"""Rotowire injuries fetcher.

Fetches injury reports (JSON), Rotowire player IDs, and provides news
storage utilities.
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

from db.schema import get_db, init_db, upsert_player

logger = logging.getLogger("pipeline.rotowire")

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


# Common first-name nickname mappings for player name matching.
_NICKNAMES: dict[str, list[str]] = {
    "Gabriel": ["Gabe"], "Gabe": ["Gabriel"],
    "Jacob": ["Jake"], "Jake": ["Jacob"],
    "Zachary": ["Zack", "Zach"], "Zack": ["Zachary"], "Zach": ["Zachary"],
    "Matthew": ["Matt"], "Matt": ["Matthew"],
    "Michael": ["Mike"], "Mike": ["Michael"],
    "Nicholas": ["Nick"], "Nick": ["Nicholas"],
    "Alexander": ["Alex"], "Alex": ["Alexander"],
    "William": ["Will", "Billy"], "Will": ["William"],
    "Robert": ["Bob", "Bobby"], "Bob": ["Robert"], "Bobby": ["Robert"],
    "Joseph": ["Joe"], "Joe": ["Joseph"],
    "Daniel": ["Dan", "Danny"], "Dan": ["Daniel"], "Danny": ["Daniel"],
    "Benjamin": ["Ben"], "Ben": ["Benjamin"],
    "Christopher": ["Chris"], "Chris": ["Christopher"],
    "Jonathan": ["Jon"], "Jon": ["Jonathan"],
    "Timothy": ["Tim"], "Tim": ["Timothy"],
    "Joshua": ["Josh"], "Josh": ["Joshua"],
    "Samuel": ["Sam"], "Sam": ["Samuel"],
    "Anthony": ["Tony"], "Tony": ["Anthony"],
    "Patrick": ["Pat"], "Pat": ["Patrick"],
    "Maxwell": ["Max"], "Max": ["Maxwell"],
    "Evgeni": ["Evgeny"], "Evgeny": ["Evgeni"],
    "Nikolai": ["Nikolas", "Nicolas"], "Nicolas": ["Nikolai"],
}


def _strip_accents(text: str) -> str:
    """Remove accents from a string (e.g. 'Jose' from 'Jose')."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _pick_by_team(
    rows: list[sqlite3.Row], team_abbrev: str | None
) -> int:
    """Return the best player id from *rows*, using *team_abbrev* to break ties."""
    if len(rows) == 1 or team_abbrev is None:
        return rows[0]["id"]
    for r in rows:
        if r["team_abbrev"] == team_abbrev:
            return r["id"]
    return rows[0]["id"]


def match_player_name(
    conn: sqlite3.Connection,
    name: str | None,
    team_abbrev: str | None = None,
) -> int | None:
    """Match a player name to a player_id in our players table.

    Tries in order: exact match, case-insensitive, hyphen-normalized,
    accent-normalized, drop-middle-names, nickname substitution,
    first-initial + last-name pattern.  When multiple players share the same name, *team_abbrev*
    is used to disambiguate.

    Args:
        conn: Database connection.
        name: Player full name from Rotowire.
        team_abbrev: Optional team abbreviation (e.g. "CAR") for
            disambiguation when multiple players share a name.

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
        "SELECT id, team_abbrev FROM players WHERE full_name = ?", (name,)
    )
    rows = cursor.fetchall()
    if rows:
        return _pick_by_team(rows, team_abbrev)

    # 2. Case-insensitive match
    cursor = conn.execute(
        "SELECT id, team_abbrev FROM players WHERE LOWER(full_name) = LOWER(?)",
        (name,),
    )
    rows = cursor.fetchall()
    if rows:
        return _pick_by_team(rows, team_abbrev)

    # 3. Hyphen-normalized match: "Fisker-Molgaard" → "Fisker Molgaard"
    dehyphenated = name.replace("-", " ")
    if dehyphenated != name:
        cursor = conn.execute(
            "SELECT id, team_abbrev FROM players WHERE LOWER(full_name) = LOWER(?)",
            (dehyphenated,),
        )
        rows = cursor.fetchall()
        if rows:
            return _pick_by_team(rows, team_abbrev)

    # 4. Accent-normalized match
    normalized_name = _strip_accents(name).lower()
    cursor = conn.execute("SELECT id, full_name, team_abbrev FROM players")
    matches = [
        row
        for row in cursor.fetchall()
        if row["full_name"]
        and _strip_accents(row["full_name"]).lower() == normalized_name
    ]
    if matches:
        return _pick_by_team(matches, team_abbrev)

    # 5. Drop middle names: "Elias Nils Pettersson" → "Elias Pettersson"
    parts = name.split()
    if len(parts) >= 3:
        short_name = f"{parts[0]} {parts[-1]}"
        cursor = conn.execute(
            "SELECT id, team_abbrev FROM players WHERE LOWER(full_name) = LOWER(?)",
            (short_name,),
        )
        rows = cursor.fetchall()
        if rows:
            return _pick_by_team(rows, team_abbrev)

    # 6. Nickname substitution: "Gabriel Perreault" → "Gabe Perreault"
    name_parts = name.split(maxsplit=1)
    if len(name_parts) == 2:
        first, rest = name_parts
        for alt in _NICKNAMES.get(first, []):
            alt_name = f"{alt} {rest}"
            cursor = conn.execute(
                "SELECT id, team_abbrev FROM players "
                "WHERE LOWER(full_name) = LOWER(?)",
                (alt_name,),
            )
            rows = cursor.fetchall()
            if rows:
                return _pick_by_team(rows, team_abbrev)

    # 7. First initial + last name (e.g. "C. McDavid")
    m = re.match(r"^([A-Za-z])\.\s*(.+)$", name)
    if m:
        initial = m.group(1).upper()
        last_name = m.group(2).strip()
        cursor = conn.execute(
            "SELECT id, first_name, team_abbrev FROM players "
            "WHERE LOWER(last_name) = LOWER(?)",
            (last_name,),
        )
        matches = [
            row
            for row in cursor.fetchall()
            if row["first_name"] and row["first_name"][0].upper() == initial
        ]
        if matches:
            return _pick_by_team(matches, team_abbrev)

    return None


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


def backfill_news_player_ids(conn: sqlite3.Connection) -> int:
    """Re-match unlinked news items against the current players table.

    Finds all player_news rows with player_id IS NULL, extracts the player
    name from the headline (text before the colon), and attempts to match
    using match_player_name().

    Args:
        conn: Database connection.

    Returns:
        Count of news items that were successfully matched.
    """
    cursor = conn.execute(
        "SELECT id, headline FROM player_news WHERE player_id IS NULL"
    )
    unmatched = cursor.fetchall()

    matched = 0
    for row in unmatched:
        headline = row["headline"] or ""
        if ":" not in headline:
            continue
        player_name = headline.split(":", 1)[0].strip()
        player_id = match_player_name(conn, player_name)
        if player_id is not None:
            conn.execute(
                "UPDATE player_news SET player_id = ? WHERE id = ?",
                (player_id, row["id"]),
            )
            matched += 1
    conn.commit()
    logger.info("Backfill matched %d news items", matched)
    return matched


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

    logger.debug("Fetching injuries from Rotowire")
    response = session.get(INJURY_URL, headers=BROWSER_HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()
    logger.info("Fetched %d injury records from Rotowire", len(data))
    return data


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

    # Full refresh: clear all rotowire injuries before re-inserting the
    # current report.  This removes players who have recovered and are no
    # longer on the Rotowire injury list.
    conn.execute("DELETE FROM player_injuries WHERE source = 'rotowire'")

    for injury in injuries:
        player_name = injury.get("player", "")
        player_id = match_player_name(
            conn, player_name, team_abbrev=injury.get("team")
        )

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
    """Run injury fetcher.

    Args:
        conn: Database connection.
        session: Optional requests session.

    Returns:
        Summary dict with injuries_upserted, injuries_unmatched.
    """
    result: dict[str, Any] = {
        "injuries_upserted": 0,
        "injuries_unmatched": 0,
    }

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
        description="Rotowire injuries fetcher"
    )
    parser.add_argument(
        "--all", action="store_true", help="Fetch injuries"
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
