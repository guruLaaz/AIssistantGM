"""DailyFaceoff line combinations fetcher.

Scrapes current line combinations (EV, PP, PK) for all 32 NHL teams from
DailyFaceoff.  Each team page is Next.js SSG with structured JSON in a
``<script id="__NEXT_DATA__">`` tag.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from db.schema import get_db, init_db
from fetchers.rotowire import match_player_name

from config.infra_constants import DAILYFACEOFF_RATE_LIMIT

logger = logging.getLogger("pipeline.dailyfaceoff")

BASE_URL = "https://www.dailyfaceoff.com/teams"

# Map NHL 3-letter abbreviations to DailyFaceoff URL slugs.
TEAM_SLUGS: dict[str, str] = {
    "ANA": "anaheim-ducks",
    "BOS": "boston-bruins",
    "BUF": "buffalo-sabres",
    "CGY": "calgary-flames",
    "CAR": "carolina-hurricanes",
    "CHI": "chicago-blackhawks",
    "COL": "colorado-avalanche",
    "CBJ": "columbus-blue-jackets",
    "DAL": "dallas-stars",
    "DET": "detroit-red-wings",
    "EDM": "edmonton-oilers",
    "FLA": "florida-panthers",
    "LAK": "los-angeles-kings",
    "MIN": "minnesota-wild",
    "MTL": "montreal-canadiens",
    "NSH": "nashville-predators",
    "NJD": "new-jersey-devils",
    "NYI": "new-york-islanders",
    "NYR": "new-york-rangers",
    "OTT": "ottawa-senators",
    "PHI": "philadelphia-flyers",
    "PIT": "pittsburgh-penguins",
    "SJS": "san-jose-sharks",
    "SEA": "seattle-kraken",
    "STL": "st-louis-blues",
    "TBL": "tampa-bay-lightning",
    "TOR": "toronto-maple-leafs",
    "UTA": "utah-hockey-club",
    "VAN": "vancouver-canucks",
    "VGK": "vegas-golden-knights",
    "WSH": "washington-capitals",
    "WPG": "winnipeg-jets",
}

DEFAULT_RATE_LIMIT = DAILYFACEOFF_RATE_LIMIT

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# EV group identifier → line number
# ---------------------------------------------------------------------------
_EV_LINE_MAP: dict[str, int] = {
    "f1": 1, "f2": 2, "f3": 3, "f4": 4,
    "d1": 1, "d2": 2, "d3": 3,
}


def fetch_team_lines(
    slug: str,
    session: requests.Session | None = None,
) -> dict:
    """Fetch and extract the line combination JSON for one team.

    Args:
        slug: DailyFaceoff team slug (e.g. ``"toronto-maple-leafs"``).
        session: Optional requests session for connection pooling.

    Returns:
        The ``props.pageProps.combinations`` dict from __NEXT_DATA__.

    Raises:
        requests.HTTPError: On non-200 response.
        ValueError: If the page structure is unexpected.
    """
    if session is None:
        session = requests.Session()

    url = f"{BASE_URL}/{slug}/line-combinations"
    logger.debug("Fetching lines from %s", url)
    response = session.get(url, headers=BROWSER_HEADERS, timeout=30)
    response.raise_for_status()

    html = response.text
    match = re.search(
        r'<script\s+id="__NEXT_DATA__"\s+type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        raise ValueError(f"Could not find __NEXT_DATA__ in page for {slug}")

    data = json.loads(match.group(1))
    combinations = data["props"]["pageProps"]["combinations"]
    return combinations


def parse_team_lines(
    combinations: dict,
    team_abbrev: str,
) -> list[dict[str, Any]]:
    """Parse DailyFaceoff combinations JSON into flat player dicts.

    The ``combinations`` dict contains a flat ``players`` list where each
    player entry already carries its ``categoryIdentifier`` and
    ``groupIdentifier``.  The same player appears once per category
    (EV, PP, PK).  This function groups entries by name and merges them
    into a single dict per player.

    Args:
        combinations: The ``combinations`` dict from __NEXT_DATA__.
        team_abbrev: NHL team abbreviation (e.g. ``"TOR"``).

    Returns:
        List of player dicts with keys: player_name, position, ev_line,
        pp_unit, pk_unit, ev_group, pp_group, pk_group, ev_linemates,
        pp_linemates, rating.
    """
    raw_players = combinations.get("players", [])

    # Pre-build linemate lookup: (category, group) → [names]
    group_members: dict[tuple[str, str], list[str]] = {}
    for p in raw_players:
        cat = p.get("categoryIdentifier", "")
        gid = p.get("groupIdentifier", "")
        name = p.get("name", "").strip()
        if not name or cat == "oi" or gid in ("g", "ir"):
            continue
        group_members.setdefault((cat, gid), []).append(name)

    # Collect all player entries grouped by name
    player_entries: dict[str, list[dict]] = {}
    for p in raw_players:
        cat = p.get("categoryIdentifier", "")
        gid = p.get("groupIdentifier", "")
        name = p.get("name", "").strip()
        if not name or cat == "oi" or gid in ("g", "ir"):
            continue
        player_entries.setdefault(name, []).append({
            "category": cat,
            "group_id": gid,
            "position": p.get("positionIdentifier", ""),
            "rating": p.get("rating"),
        })

    # Build one record per player
    results: list[dict[str, Any]] = []
    for name, entries in player_entries.items():
        rec: dict[str, Any] = {
            "player_name": name,
            "position": None,
            "ev_line": None,
            "pp_unit": None,
            "pk_unit": None,
            "ev_group": None,
            "pp_group": None,
            "pk_group": None,
            "ev_linemates": [],
            "pp_linemates": [],
            "rating": None,
        }

        for entry in entries:
            cat = entry["category"]
            gid = entry["group_id"]

            if rec["position"] is None and entry["position"]:
                rec["position"] = entry["position"]

            if entry["rating"] is not None and rec["rating"] is None:
                rec["rating"] = entry["rating"]

            if cat == "ev" and gid in _EV_LINE_MAP:
                rec["ev_line"] = _EV_LINE_MAP[gid]
                rec["ev_group"] = gid
                rec["ev_linemates"] = [
                    n for n in group_members.get((cat, gid), []) if n != name
                ]

            elif cat == "pp" and gid in ("pp1", "pp2"):
                rec["pp_unit"] = int(gid[-1])
                rec["pp_group"] = gid
                rec["pp_linemates"] = [
                    n for n in group_members.get((cat, gid), []) if n != name
                ]

            elif cat == "pk" and gid in ("pk1", "pk2"):
                rec["pk_unit"] = int(gid[-1])
                rec["pk_group"] = gid

        results.append(rec)

    return results


def save_team_lines(
    conn: sqlite3.Connection,
    team_abbrev: str,
    players: list[dict[str, Any]],
) -> tuple[int, int]:
    """Save parsed line data for one team to the database.

    Deletes existing rows for the team, then inserts new ones.
    Attempts to resolve each player name to a player_id.

    Args:
        conn: Database connection.
        team_abbrev: NHL team abbreviation.
        players: List of player dicts from ``parse_team_lines``.

    Returns:
        Tuple of (saved_count, unmatched_count).
    """
    now = datetime.now(timezone.utc).isoformat()

    # Clear previous data for this team
    conn.execute(
        "DELETE FROM line_combinations WHERE team_abbrev = ?",
        (team_abbrev,),
    )

    saved = 0
    unmatched = 0

    for p in players:
        player_id = match_player_name(conn, p["player_name"], team_abbrev)
        if player_id is None:
            unmatched += 1

        conn.execute(
            """
            INSERT INTO line_combinations
                (player_id, team_abbrev, player_name, position,
                 ev_line, pp_unit, pk_unit, ev_group, pp_group, pk_group,
                 ev_linemates, pp_linemates, rating, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                team_abbrev,
                p["player_name"],
                p["position"],
                p["ev_line"],
                p["pp_unit"],
                p["pk_unit"],
                p["ev_group"],
                p["pp_group"],
                p["pk_group"],
                json.dumps(p["ev_linemates"]) if p["ev_linemates"] else None,
                json.dumps(p["pp_linemates"]) if p["pp_linemates"] else None,
                p["rating"],
                now,
            ),
        )
        saved += 1

    conn.commit()
    return saved, unmatched


def fetch_all_lines(
    conn: sqlite3.Connection,
    session: requests.Session | None = None,
    rate_limit: float = DEFAULT_RATE_LIMIT,
) -> dict[str, Any]:
    """Fetch line combinations for all 32 NHL teams.

    Continues on per-team failure so one broken page doesn't block others.

    Args:
        conn: Database connection.
        session: Optional requests session.
        rate_limit: Seconds to sleep between teams.

    Returns:
        Summary dict with players_saved, unmatched, teams_failed.
    """
    if session is None:
        session = requests.Session()

    total_saved = 0
    total_unmatched = 0
    teams_failed = 0

    for i, (abbrev, slug) in enumerate(TEAM_SLUGS.items()):
        try:
            combinations = fetch_team_lines(slug, session)
            players = parse_team_lines(combinations, abbrev)
            saved, unmatched = save_team_lines(conn, abbrev, players)
            total_saved += saved
            total_unmatched += unmatched
            logger.info(
                "%s: %d players saved (%d unmatched)", abbrev, saved, unmatched
            )
        except Exception as e:
            logger.warning("Failed fetching lines for %s: %s", abbrev, e)
            teams_failed += 1

        if i < len(TEAM_SLUGS) - 1:
            time.sleep(rate_limit)

    logger.info(
        "Line combos complete: %d saved, %d unmatched, %d teams failed",
        total_saved, total_unmatched, teams_failed,
    )
    return {
        "players_saved": total_saved,
        "unmatched": total_unmatched,
        "teams_failed": teams_failed,
    }


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DailyFaceoff line combinations fetcher",
    )
    parser.add_argument(
        "--team",
        type=str,
        help="Fetch lines for a single team (3-letter abbreviation, e.g. TOR)",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="db/nhl_data.db",
        help="Database path",
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
    session = requests.Session()

    if args.team:
        abbrev = args.team.upper()
        slug = TEAM_SLUGS.get(abbrev)
        if not slug:
            print(f"Unknown team: {abbrev}")
            conn.close()
            return
        combinations = fetch_team_lines(slug, session)
        players = parse_team_lines(combinations, abbrev)
        saved, unmatched = save_team_lines(conn, abbrev, players)
        print(f"{abbrev}: {saved} players saved ({unmatched} unmatched)")
    else:
        result = fetch_all_lines(conn, session)
        print(
            f"Done: {result['players_saved']} saved, "
            f"{result['unmatched']} unmatched, "
            f"{result['teams_failed']} teams failed"
        )

    conn.close()


if __name__ == "__main__":
    main()
