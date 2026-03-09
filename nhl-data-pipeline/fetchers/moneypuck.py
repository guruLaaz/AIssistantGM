"""MoneyPuck injury data fetcher.

Fetches the current_injuries.csv from MoneyPuck, which includes expected
return dates for all injured NHL players.
"""

from __future__ import annotations

import csv
import io
import logging
import sqlite3
from typing import Any

import requests

from config.infra_constants import HTTP_TIMEOUT, MONEYPUCK_INJURIES_URL
from fetchers.rotowire import match_player_name

logger = logging.getLogger("pipeline.moneypuck")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

# MoneyPuck status codes → normalized status strings
_STATUS_MAP: dict[str, str] = {
    "IR": "IR",
    "IR-NR": "IR",
    "IR-LT": "IR",
    "DTD": "Day-To-Day",
    "O": "Out",
}


def fetch_injuries(
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch the current injuries CSV from MoneyPuck.

    Returns:
        List of dicts with CSV column names as keys.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching injuries from MoneyPuck")
    response = session.get(MONEYPUCK_INJURIES_URL, headers=_HEADERS, timeout=HTTP_TIMEOUT)
    response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))
    rows = list(reader)
    logger.info("Fetched %d injury records from MoneyPuck", len(rows))
    return rows


def save_injuries(
    conn: sqlite3.Connection,
    injuries: list[dict[str, Any]],
) -> tuple[int, int]:
    """Upsert MoneyPuck injuries into player_injuries table.

    Full refresh: clears all moneypuck-sourced rows first, then re-inserts.

    Returns:
        Tuple of (upserted_count, unmatched_count).
    """
    upserted = 0
    unmatched = 0

    conn.execute("DELETE FROM player_injuries WHERE source = 'moneypuck'")

    for row in injuries:
        player_name = row.get("playerName", "")
        team = row.get("teamCode", "")
        player_id = match_player_name(conn, player_name, team_abbrev=team)

        if player_id is None:
            unmatched += 1

        # Normalize status
        raw_status = row.get("playerInjuryStatus", "")
        status = _STATUS_MAP.get(raw_status, raw_status)

        # Normalize return date: keep 2099-12-31 as-is so the season-ending
        # filter catches indefinite injuries too.
        raw_return = row.get("dateOfReturn", "")
        expected_return = raw_return or None

        conn.execute(
            """
            INSERT OR REPLACE INTO player_injuries
                (player_id, source, injury_type, status, updated_at, expected_return)
            VALUES (?, 'moneypuck', ?, ?, date('now'), ?)
            """,
            (
                player_id,
                row.get("yahooInjuryDescription"),
                status,
                expected_return,
            ),
        )
        upserted += 1

    conn.commit()
    logger.info(
        "MoneyPuck injuries: %d upserted, %d unmatched", upserted, unmatched
    )
    return upserted, unmatched
