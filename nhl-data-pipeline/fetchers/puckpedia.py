"""PuckPedia line combinations fetcher.

Scrapes current line combinations (EV, PP, PK) for all 32 NHL teams from
PuckPedia's depth-chart app at ``depth-charts.puckpedia.com/{ABBREV}``.

The page uses React SSR streaming, so Playwright is required to render the
DOM before extracting lineup data via JavaScript DOM queries.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from db.schema import get_db, init_db
from fetchers.rotowire import match_player_name

from config.infra_constants import PUCKPEDIA_RATE_JITTER, PUCKPEDIA_RATE_LIMIT

logger = logging.getLogger("pipeline.puckpedia")

BASE_URL = "https://depth-charts.puckpedia.com"

# All 32 NHL team abbreviations (standard NHL codes used throughout the DB).
TEAM_ABBREVS: list[str] = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ",
    "DAL", "DET", "EDM", "FLA", "LAK", "MIN", "MTL", "NSH",
    "NJD", "NYI", "NYR", "OTT", "PHI", "PIT", "SJS", "SEA",
    "STL", "TBL", "TOR", "UTA", "VAN", "VGK", "WSH", "WPG",
]

# PuckPedia uses non-standard abbreviations for two teams.
_PUCKPEDIA_ABBREV: dict[str, str] = {
    "NSH": "NAS",
    "WSH": "WAS",
}

DEFAULT_RATE_LIMIT = PUCKPEDIA_RATE_LIMIT

# ---------------------------------------------------------------------------
# JavaScript executed inside the Playwright page to extract lineup data.
# Returns a dict with keys: lw, centers, rw, ld, rd, goalies,
#                            pp1, pp2, pk1, pk2
# Each value is a list of {id, name} dicts.
# ---------------------------------------------------------------------------
_EXTRACT_JS = """() => {
    function getPlayers(container) {
        if (!container) return [];
        const els = container.querySelectorAll('[id^="player-name-"]');
        return [...els].map(el => ({
            id: el.id.replace('player-name-', ''),
            name: el.querySelector('div')?.textContent?.trim()
                  || el.textContent?.trim()
        }));
    }

    // Forwards: #nhl-forward-tiles > 2nd child (grid-cols-3)
    //   children[0]=LW, children[1]=C, children[2]=RW
    const fwd = document.getElementById('nhl-forward-tiles');
    const fwdGrid = fwd?.children?.[1];
    const lw = fwdGrid ? getPlayers(fwdGrid.children[0]) : [];
    const centers = fwdGrid ? getPlayers(fwdGrid.children[1]) : [];
    const rw = fwdGrid ? getPlayers(fwdGrid.children[2]) : [];

    // Defense + Goalies: #nhl-nonforward-tiles > 2nd child (grid-cols-3)
    //   children[0]=LD, children[1]=RD, children[2]=Goalies
    const nfw = document.getElementById('nhl-nonforward-tiles');
    const nfwGrid = nfw?.children?.[1];
    const ld = nfwGrid ? getPlayers(nfwGrid.children[0]) : [];
    const rd = nfwGrid ? getPlayers(nfwGrid.children[1]) : [];
    const goalies = nfwGrid ? getPlayers(nfwGrid.children[2]) : [];

    // Special teams: H3-based parsing (PK2 has a duplicate PK1-tiles ID
    // bug on PuckPedia, so we must use headings instead of element IDs).
    const st = document.getElementById('special-teams');
    const units = { pp1: [], pp2: [], pk1: [], pk2: [] };
    if (st) {
        for (const h of st.querySelectorAll('h3')) {
            const text = h.textContent.trim();
            const players = getPlayers(h.parentElement);
            if (text === 'Powerplay 1') units.pp1 = players;
            else if (text === 'Powerplay 2') units.pp2 = players;
            else if (text === 'Penalty Kill 1') units.pk1 = players;
            else if (text === 'Penalty Kill 2') units.pk2 = players;
        }
    }

    return { lw, centers, rw, ld, rd, goalies, ...units };
}"""


# ---------------------------------------------------------------------------
# Playwright browser helpers
# ---------------------------------------------------------------------------

def _launch_browser():
    """Launch a stealth Playwright Chromium browser and return (browser, page)."""
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
    )
    page = context.new_page()
    Stealth().apply_stealth_sync(page)
    return pw, browser, page


def _close_browser(pw, browser):
    """Cleanly shut down the Playwright browser."""
    try:
        browser.close()
    finally:
        pw.stop()


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def fetch_team_lines(page: Any, team_abbrev: str) -> dict:
    """Navigate to a team's depth chart and extract raw lineup data.

    Args:
        page: Playwright Page object (reused across teams).
        team_abbrev: NHL 3-letter abbreviation (e.g. ``"TOR"``).

    Returns:
        Dict with keys ``lw``, ``centers``, ``rw``, ``ld``, ``rd``,
        ``goalies``, ``pp1``, ``pp2``, ``pk1``, ``pk2``.
        Each value is a list of ``{"id": str, "name": str}`` dicts.

    Raises:
        Exception: On navigation failure or missing page structure.
    """
    pp_abbrev = _PUCKPEDIA_ABBREV.get(team_abbrev, team_abbrev)
    url = f"{BASE_URL}/{pp_abbrev}"
    logger.debug("Fetching lines from %s", url)
    page.goto(url, wait_until="networkidle", timeout=30_000)
    page.wait_for_selector("#nhl-forward-tiles", timeout=10_000)
    page.wait_for_timeout(1000)  # extra buffer for React hydration

    raw = page.evaluate(_EXTRACT_JS)

    # Sanity check — at least some forwards should be present
    if not raw.get("lw") and not raw.get("centers"):
        raise ValueError(
            f"No forward data extracted for {team_abbrev} — "
            "page structure may have changed"
        )

    return raw


def parse_team_lines(
    raw: dict,
    team_abbrev: str,
) -> list[dict[str, Any]]:
    """Convert raw Playwright extraction to flat player dicts.

    Merges EV, PP, and PK appearances for the same player into one record.

    Args:
        raw: Dict returned by ``fetch_team_lines``.
        team_abbrev: NHL team abbreviation.

    Returns:
        List of player dicts with keys: player_name, position, ev_line,
        pp_unit, pk_unit, ev_group, pp_group, pk_group, ev_linemates,
        pp_linemates, rating.
    """
    # --- Build EV forward records (by column position) ---
    lw = raw.get("lw", [])
    centers = raw.get("centers", [])
    rw_list = raw.get("rw", [])

    # Keyed by player name → partial record
    players: dict[str, dict[str, Any]] = {}

    max_fwd_lines = max(len(lw), len(centers), len(rw_list))
    for line_idx in range(max_fwd_lines):
        line_num = line_idx + 1
        row_names: list[str] = []
        row_entries: list[tuple[str, str]] = []  # (name, position)

        for col, pos in [(lw, "lw"), (centers, "c"), (rw_list, "rw")]:
            if line_idx < len(col):
                name = col[line_idx]["name"]
                row_names.append(name)
                row_entries.append((name, pos))

        for name, pos in row_entries:
            rec = players.setdefault(name, _empty_record(name))
            rec["position"] = pos
            rec["ev_line"] = line_num
            rec["ev_group"] = f"f{line_num}"
            rec["ev_linemates"] = [n for n in row_names if n != name]

    # --- Build EV defense records ---
    ld = raw.get("ld", [])
    rd = raw.get("rd", [])

    max_def_pairs = max(len(ld), len(rd))
    for pair_idx in range(max_def_pairs):
        pair_num = pair_idx + 1
        pair_names: list[str] = []
        pair_entries: list[str] = []

        for col in [ld, rd]:
            if pair_idx < len(col):
                name = col[pair_idx]["name"]
                pair_names.append(name)
                pair_entries.append(name)

        for name in pair_entries:
            rec = players.setdefault(name, _empty_record(name))
            rec["position"] = "d"
            rec["ev_line"] = pair_num
            rec["ev_group"] = f"d{pair_num}"
            rec["ev_linemates"] = [n for n in pair_names if n != name]

    # --- PP units ---
    for unit_num, key in [(1, "pp1"), (2, "pp2")]:
        unit_players = raw.get(key, [])
        unit_names = [p["name"] for p in unit_players]
        for p in unit_players:
            rec = players.setdefault(p["name"], _empty_record(p["name"]))
            rec["pp_unit"] = unit_num
            rec["pp_group"] = f"pp{unit_num}"
            rec["pp_linemates"] = [n for n in unit_names if n != p["name"]]

    # --- PK units ---
    for unit_num, key in [(1, "pk1"), (2, "pk2")]:
        unit_players = raw.get(key, [])
        for p in unit_players:
            rec = players.setdefault(p["name"], _empty_record(p["name"]))
            rec["pk_unit"] = unit_num
            rec["pk_group"] = f"pk{unit_num}"

    return list(players.values())


def _empty_record(name: str) -> dict[str, Any]:
    """Return a blank player record with all expected keys."""
    return {
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
        player_id = match_player_name(
            conn, p["player_name"], team_abbrev, position=p["position"],
        )
        if player_id is None:
            unmatched += 1
            logger.warning(
                "Unmatched: %s (%s) — pos=%s ev_line=%s pp=%s pk=%s",
                p["player_name"], team_abbrev,
                p["position"], p["ev_line"], p["pp_unit"], p["pk_unit"],
            )

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
    session: Any = None,
    rate_limit: float = DEFAULT_RATE_LIMIT,
) -> dict[str, Any]:
    """Fetch line combinations for all 32 NHL teams.

    Launches a single Playwright browser, navigates to each team's
    depth chart page, and extracts lineup data.  Continues on per-team
    failure so one broken page doesn't block others.

    Args:
        conn: Database connection.
        session: Unused (kept for pipeline API compatibility).
        rate_limit: Seconds to sleep between teams.

    Returns:
        Summary dict with players_saved, unmatched, teams_failed.
    """
    pw, browser, page = _launch_browser()

    total_saved = 0
    total_unmatched = 0
    teams_failed = 0

    try:
        for i, abbrev in enumerate(TEAM_ABBREVS):
            try:
                raw = fetch_team_lines(page, abbrev)
                players = parse_team_lines(raw, abbrev)
                saved, unmatched = save_team_lines(conn, abbrev, players)
                total_saved += saved
                total_unmatched += unmatched
                logger.info(
                    "%s: %d players saved (%d unmatched)",
                    abbrev, saved, unmatched,
                )
            except Exception as e:
                logger.warning("Failed fetching lines for %s: %s", abbrev, e)
                teams_failed += 1

            if i < len(TEAM_ABBREVS) - 1:
                time.sleep(rate_limit + random.uniform(0, PUCKPEDIA_RATE_JITTER))
    finally:
        _close_browser(pw, browser)

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
        description="PuckPedia line combinations fetcher",
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

    if args.team:
        abbrev = args.team.upper()
        if abbrev not in TEAM_ABBREVS:
            print(f"Unknown team: {abbrev}")
            conn.close()
            return
        pw, browser, page = _launch_browser()
        try:
            raw = fetch_team_lines(page, abbrev)
            players = parse_team_lines(raw, abbrev)
            saved, unmatched = save_team_lines(conn, abbrev, players)
            print(f"{abbrev}: {saved} players saved ({unmatched} unmatched)")
        finally:
            _close_browser(pw, browser)
    else:
        result = fetch_all_lines(conn)
        print(
            f"Done: {result['players_saved']} saved, "
            f"{result['unmatched']} unmatched, "
            f"{result['teams_failed']} teams failed"
        )

    conn.close()


if __name__ == "__main__":
    main()
