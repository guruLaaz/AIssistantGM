"""Fantrax fantasy league data fetcher.

Syncs teams, standings, and rosters from the Fantrax API to the
local database.  Re-uses authentication infrastructure from
:mod:`fetchers.fantrax_news`.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path
from typing import Any

import requests

from db.schema import get_db, init_db
from fetchers.fantrax_news import (
    FANTRAX_API_URL,
    _load_env,
    _load_cookies_for_session,
)

logger = logging.getLogger("pipeline.fantrax_league")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fantrax_api_call(
    session: requests.Session,
    league_id: str,
    method: str,
    extra_data: dict[str, Any] | None = None,
) -> dict:
    """POST to the Fantrax API and return the response data.

    Args:
        session: Authenticated requests session with cookies.
        league_id: Fantrax league ID.
        method: API method name (e.g. ``"getStandings"``).
        extra_data: Additional keys merged into the request data dict.

    Returns:
        The ``responses[0]["data"]`` dict from the API response.

    Raises:
        RuntimeError: If the response contains a ``pageError``.
        requests.HTTPError: On non-2xx HTTP status.
    """
    data: dict[str, Any] = {"leagueId": league_id}
    if extra_data:
        data.update(extra_data)

    payload = {
        "msgs": [{"method": method, "data": data}],
    }

    resp = session.post(
        FANTRAX_API_URL,
        params={"leagueId": league_id},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()

    body = resp.json()

    if "pageError" in body:
        code = body["pageError"].get("code", "unknown")
        raise RuntimeError(f"Fantrax API error: {code}")

    return body["responses"][0]["data"]


def _get_authenticated_session(
    config: dict[str, Any] | None = None,
) -> tuple[requests.Session, str]:
    """Create an authenticated requests session.

    Args:
        config: Fantrax config dict.  Loaded from ``.env`` if None.

    Returns:
        Tuple of (session, league_id).

    Raises:
        RuntimeError: If no cookies are available.
    """
    if config is None:
        config = _load_env()

    session = requests.Session()
    loaded = _load_cookies_for_session(session, config["cookie_file"])

    if not loaded:
        raise RuntimeError(
            "No Fantrax cookies found. Run fantrax_news first to "
            "authenticate via browser login."
        )

    return session, config["league_id"]


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------

def _extract_teams_from_roster_data(
    roster_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract the fantasy teams list from a getTeamRosterInfo response.

    The ``fantasyTeams`` key in the roster API response contains a list
    of all league teams with id, name, shortName, and logo URLs.

    Args:
        roster_data: The ``data`` dict from a getTeamRosterInfo call.

    Returns:
        List of team dicts with id, name, short_name, logo_url.
    """
    teams_list = roster_data.get("fantasyTeams", [])
    teams: list[dict[str, Any]] = []
    for info in teams_list:
        teams.append({
            "id": info.get("id", ""),
            "name": info.get("name", ""),
            "short_name": info.get("shortName", ""),
            "logo_url": info.get("logoUrl256") or info.get("logoUrl128"),
        })
    logger.info("Extracted %d teams from roster data", len(teams))
    return teams


def fetch_teams(
    session: requests.Session,
    league_id: str,
    _roster_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch fantasy team info from the league.

    Extracts the full team list from a ``getTeamRosterInfo`` API
    response (the ``fantasyTeams`` key).  If *_roster_data* is provided
    (already fetched by the orchestrator), it is used directly to avoid
    a redundant API call.

    Args:
        session: Authenticated requests session.
        league_id: Fantrax league ID.
        _roster_data: Optional pre-fetched roster API response dict.

    Returns:
        List of team dicts with id, name, short_name, logo_url.
    """
    if _roster_data is not None:
        return _extract_teams_from_roster_data(_roster_data)

    # Discover a team_id from standings, then call roster API
    standings_data = _fantrax_api_call(session, league_id, "getStandings")
    table_list = standings_data.get("tableList", [])
    team_id = ""
    if table_list:
        rows = table_list[0].get("rows", [])
        if rows:
            fixed = rows[0].get("fixedCells", [])
            if len(fixed) > 1:
                team_id = fixed[1].get("teamId", "")
    if not team_id:
        logger.warning("Could not discover a team ID for fetching teams")
        return []

    data = _fantrax_api_call(
        session, league_id, "getTeamRosterInfo",
        extra_data={"teamId": team_id, "view": "STATS"},
    )
    return _extract_teams_from_roster_data(data)


def fetch_standings(
    session: requests.Session,
    league_id: str,
) -> list[dict[str, Any]]:
    """Fetch league standings.

    Args:
        session: Authenticated requests session.
        league_id: Fantrax league ID.

    Returns:
        List of standing dicts for each team.
    """
    data = _fantrax_api_call(session, league_id, "getStandings")

    table_list = data.get("tableList", [])
    if not table_list:
        logger.warning("No standings tables in response")
        return []

    table = table_list[0]

    # Build field-name-to-column-index map from header
    header_cells = table.get("header", {}).get("cells", [])
    fields: dict[str, int] = {c["key"]: i for i, c in enumerate(header_cells)}

    logger.debug("Standings header fields: %s", list(fields.keys()))

    rows = table.get("rows", [])
    standings: list[dict[str, Any]] = []

    for row in rows:
        fixed_cells = row.get("fixedCells", [])
        cells = row.get("cells", [])

        team_id = fixed_cells[1]["teamId"] if len(fixed_cells) > 1 else ""
        rank = int(fixed_cells[0].get("content", 0)) if fixed_cells else 0

        def get_cell(field_name: str, default: Any = 0) -> Any:
            """Look up a cell value by field key."""
            if field_name not in fields:
                return default
            idx = fields[field_name]
            if idx >= len(cells):
                return default
            content = cells[idx].get("content", "")
            if content == "" or content == "-":
                return default
            try:
                return float(str(content).replace(",", ""))
            except (ValueError, TypeError):
                return default

        # Fantasy points: try multiple field name variants
        fpts = (
            get_cell("fantasyPoints", 0)
            or get_cell("fPts", 0)
            or get_cell("pointsFor", 0)
        )
        # Games played
        gp = get_cell("sc", 0) or get_cell("gp", 0)
        # FP/G
        fpg = (
            get_cell("FPtsPerGame", 0)
            or get_cell("fpGp", 0)
            or get_cell("fPtsPerGp", 0)
        )
        # Claims remaining
        cr = (
            get_cell("maxClaimsSeason", None)
            or get_cell("cr", None)
            or get_cell("claimsRemaining", None)
        )

        # Waiver order
        ww = get_cell("wwOrder", None)

        # Streak (string value)
        streak = ""
        if "streak" in fields:
            idx = fields["streak"]
            if idx < len(cells):
                streak = cells[idx].get("content", "")

        standings.append({
            "team_id": team_id,
            "rank": rank,
            "wins": int(get_cell("win", 0)),
            "losses": int(get_cell("loss", 0)),
            "ties": int(get_cell("tie", 0)),
            "points": int(get_cell("points", 0)),
            "win_percentage": get_cell("winpc", 0),
            "games_back": (
                get_cell("pointsBehindLeader", 0)
                or get_cell("gamesback", 0)
            ),
            "waiver_order": int(ww) if ww is not None else None,
            "claims_remaining": int(cr) if cr is not None else None,
            "points_for": fpts,
            "points_against": get_cell("pointsAgainst", 0),
            "streak": streak,
            "games_played": int(gp),
            "fantasy_points_per_game": fpg,
        })

    logger.info("Fetched standings for %d teams", len(standings))
    return standings


def _parse_roster_slots(roster_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse roster slots from a getTeamRosterInfo response.

    Extracts player info from ``scorer`` objects and stats from
    ``cells`` using the table header for column mapping.

    Args:
        roster_data: The ``data`` dict from a getTeamRosterInfo call.

    Returns:
        List of roster slot dicts.
    """
    slots: list[dict[str, Any]] = []

    tables = roster_data.get("tables", [])
    for table in tables:
        # Build header-based column index map
        header_cells = table.get("header", {}).get("cells", [])
        fields: dict[str, int] = {
            c["key"]: i for i, c in enumerate(header_cells)
        }

        for row in table.get("rows", []):
            scorer = row.get("scorer", {})
            if not scorer:
                continue

            player_id = scorer.get("scorerId", "")
            player_name = scorer.get("name", "")
            pos_short = scorer.get("posShortNames", "")
            status_id = row.get("statusId", scorer.get("statusId", None))

            cells = row.get("cells", [])

            def _cell_float(key: str) -> float | None:
                """Extract a float from cells by header key."""
                idx = fields.get(key)
                if idx is None or idx >= len(cells):
                    return None
                raw = cells[idx].get("content", "")
                if raw == "" or raw == "-":
                    return None
                try:
                    return float(
                        str(raw).replace(",", "").replace("$", "")
                    )
                except (ValueError, TypeError):
                    return None

            salary = _cell_float("salary")
            total_fpts = _cell_float("fpts")
            fpg = _cell_float("fptsPerGame")

            position_id = str(row.get("posId", ""))

            slots.append({
                "player_id": player_id,
                "player_name": player_name,
                "position_id": position_id,
                "position_short": pos_short,
                "status_id": status_id,
                "salary": salary,
                "total_fantasy_points": total_fpts,
                "fantasy_points_per_game": fpg,
            })

    return slots


def fetch_roster(
    session: requests.Session,
    league_id: str,
    team_id: str,
    _roster_data: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Fetch roster slots for a single team.

    Args:
        session: Authenticated requests session.
        league_id: Fantrax league ID.
        team_id: The fantasy team ID.
        _roster_data: Optional pre-fetched roster API response dict.

    Returns:
        List of roster slot dicts.
    """
    if _roster_data is None:
        _roster_data = _fantrax_api_call(
            session,
            league_id,
            "getTeamRosterInfo",
            extra_data={"teamId": team_id, "view": "STATS"},
        )

    slots = _parse_roster_slots(_roster_data)
    logger.info("Fetched %d roster slots for team %s", len(slots), team_id)
    return slots


# ---------------------------------------------------------------------------
# Save functions
# ---------------------------------------------------------------------------

def save_teams(
    conn: sqlite3.Connection,
    league_id: str,
    teams: list[dict[str, Any]],
) -> int:
    """Save teams to fantasy_teams table.

    Args:
        conn: Database connection.
        league_id: Fantrax league ID.
        teams: List of team dicts from fetch_teams.

    Returns:
        Number of teams saved.
    """
    for team in teams:
        conn.execute(
            """
            INSERT OR REPLACE INTO fantasy_teams
                (id, league_id, name, short_name, logo_url)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                team["id"],
                league_id,
                team["name"],
                team.get("short_name", ""),
                team.get("logo_url"),
            ),
        )
    conn.commit()
    return len(teams)


def save_standings(
    conn: sqlite3.Connection,
    league_id: str,
    standings: list[dict[str, Any]],
) -> int:
    """Save standings to fantasy_standings table.

    Performs a full replace: deletes existing rows for the league
    before inserting.

    Args:
        conn: Database connection.
        league_id: Fantrax league ID.
        standings: List of standing dicts from fetch_standings.

    Returns:
        Number of standings rows saved.
    """
    conn.execute(
        "DELETE FROM fantasy_standings WHERE league_id = ?", (league_id,)
    )
    for s in standings:
        conn.execute(
            """
            INSERT INTO fantasy_standings
                (league_id, team_id, rank, wins, losses, ties, points,
                 win_percentage, games_back, waiver_order, claims_remaining,
                 points_for, points_against, streak, games_played,
                 fantasy_points_per_game)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                league_id,
                s["team_id"],
                s["rank"],
                s.get("wins", 0),
                s.get("losses", 0),
                s.get("ties", 0),
                s.get("points", 0),
                s.get("win_percentage", 0),
                s.get("games_back", 0),
                s.get("waiver_order"),
                s.get("claims_remaining"),
                s.get("points_for", 0),
                s.get("points_against", 0),
                s.get("streak", ""),
                s.get("games_played", 0),
                s.get("fantasy_points_per_game", 0),
            ),
        )
    conn.commit()
    return len(standings)


def save_roster(
    conn: sqlite3.Connection,
    team_id: str,
    slots: list[dict[str, Any]],
) -> int:
    """Save roster slots to fantasy_roster_slots table.

    Performs a full replace: deletes existing rows for the team
    before inserting.

    Args:
        conn: Database connection.
        team_id: Fantasy team ID.
        slots: List of roster slot dicts from fetch_roster.

    Returns:
        Number of roster slots saved.
    """
    conn.execute(
        "DELETE FROM fantasy_roster_slots WHERE team_id = ?", (team_id,)
    )
    for slot in slots:
        conn.execute(
            """
            INSERT INTO fantasy_roster_slots
                (team_id, player_id, player_name, position_id,
                 position_short, status_id, salary,
                 total_fantasy_points, fantasy_points_per_game)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                team_id,
                slot.get("player_id"),
                slot.get("player_name"),
                slot.get("position_id"),
                slot.get("position_short"),
                slot.get("status_id"),
                slot.get("salary"),
                slot.get("total_fantasy_points"),
                slot.get("fantasy_points_per_game"),
            ),
        )
    conn.commit()
    return len(slots)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def sync_fantrax_league(
    conn: sqlite3.Connection,
    config: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Sync all Fantrax league data to the database.

    Main pipeline entry point.  Fetches and saves teams, standings,
    and rosters for every team.

    Args:
        conn: Database connection.
        config: Fantrax config dict.  Loaded from ``.env`` if None.

    Returns:
        Summary dict with teams_synced, standings_synced,
        roster_slots_synced counts.
    """
    session, league_id = _get_authenticated_session(config)

    # Standings first — gives us team_ids
    standings = fetch_standings(session, league_id)
    standings_synced = save_standings(conn, league_id, standings)
    logger.info("Saved %d standings rows", standings_synced)

    team_ids = [s["team_id"] for s in standings if s["team_id"]]

    # Fetch roster for each team; extract team list from first call
    teams: list[dict[str, Any]] = []
    teams_synced = 0
    total_slots = 0

    for i, tid in enumerate(team_ids):
        roster_data = _fantrax_api_call(
            session, league_id, "getTeamRosterInfo",
            extra_data={"teamId": tid, "view": "STATS"},
        )

        # First roster response contains the full fantasyTeams list
        if i == 0:
            teams = fetch_teams(session, league_id, _roster_data=roster_data)
            teams_synced = save_teams(conn, league_id, teams)
            logger.info("Saved %d teams", teams_synced)

        roster = fetch_roster(session, league_id, tid, _roster_data=roster_data)
        total_slots += save_roster(conn, tid, roster)

    logger.info("Saved %d total roster slots", total_slots)

    return {
        "teams_synced": teams_synced,
        "standings_synced": standings_synced,
        "roster_slots_synced": total_slots,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for standalone usage."""
    parser = argparse.ArgumentParser(
        description="Fantrax league data fetcher",
    )
    parser.add_argument(
        "--db", type=str, default="db/nhl_data.db",
        help="Database path (default: db/nhl_data.db)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    db_path = Path(args.db)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)
    conn = get_db(db_path)

    try:
        summary = sync_fantrax_league(conn)
        print(f"Sync complete: {summary}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
