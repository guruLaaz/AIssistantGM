"""NHL stats fetcher.

Fetches player rosters, game logs, and team schedules from the NHL APIs:
- Web API (api-web.nhle.com/v1) — rosters, schedules
- Stats API (api.nhle.com/stats/rest/en) — game logs, season totals (bulk)
"""

from __future__ import annotations

import argparse
import calendar
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests

from db.schema import PlayerDict, get_db, init_db, upsert_player
from utils.time import toi_to_seconds

logger = logging.getLogger("pipeline.nhl_api")

NHL_API_BASE = "https://api-web.nhle.com/v1"
NHL_STATS_API_BASE = "https://api.nhle.com/stats/rest/en"

ALL_TEAMS = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET",
    "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT",
    "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "UTA", "VAN", "VGK",
    "WSH", "WPG"
]

DEFAULT_RATE_LIMIT = 0.2  # seconds between requests

_BACKOFF_MAX_RETRIES = 4
_BACKOFF_BASE = 1  # seconds; doubles each retry: 1, 2, 4, 8


def _api_get(
    session: requests.Session,
    url: str,
    timeout: int = 30,
) -> requests.Response:
    """GET with adaptive retry on HTTP 429.

    Retries up to ``_BACKOFF_MAX_RETRIES`` times with exponential backoff.
    Respects the ``Retry-After`` header when present.  On success the caller
    can assume the API is no longer rate-limiting.

    Raises:
        requests.HTTPError: On non-429 failures or after retries exhausted.
    """
    for attempt in range(_BACKOFF_MAX_RETRIES + 1):
        response = session.get(url, timeout=timeout)
        if response.status_code != 429:
            response.raise_for_status()
            return response

        if attempt == _BACKOFF_MAX_RETRIES:
            response.raise_for_status()  # raises HTTPError

        retry_after = response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                wait = float(retry_after)
            except ValueError:
                wait = _BACKOFF_BASE * (2 ** attempt)
        else:
            wait = _BACKOFF_BASE * (2 ** attempt)

        logger.warning(
            "429 rate-limited on %s — retrying in %.1fs (attempt %d/%d)",
            url, wait, attempt + 1, _BACKOFF_MAX_RETRIES,
        )
        time.sleep(wait)

    # Unreachable, but keeps type checkers happy
    raise requests.HTTPError("Max retries exhausted", response=response)  # pragma: no cover


def _paginate_stats_api(
    session: requests.Session,
    url: str,
    params: dict[str, str],
    page_size: int = 100,
) -> list[dict[str, Any]]:
    """Paginate through a Stats API endpoint, collecting all rows.

    Args:
        session: Requests session.
        url: Full endpoint URL (without query params).
        params: Base query parameters (cayenneExp, isAggregate, etc.).
        page_size: Rows per page.

    Returns:
        List of all data dicts from all pages.
    """
    all_rows: list[dict[str, Any]] = []
    start = 0

    while True:
        page_params = {**params, "start": str(start), "limit": str(page_size)}
        query = "&".join(f"{k}={v}" for k, v in page_params.items())
        full_url = f"{url}?{query}"

        response = _api_get(session, full_url)
        data = response.json()

        rows = data.get("data", [])
        all_rows.extend(rows)

        logger.debug(
            "Stats API page: start=%d, got=%d, total=%s",
            start, len(rows), data.get("total"),
        )

        if len(rows) < page_size:
            break

        start += page_size
        time.sleep(DEFAULT_RATE_LIMIT)

    return all_rows


# ---------------------------------------------------------------------------
# Stats API bulk fetchers
# ---------------------------------------------------------------------------


def _season_month_ranges(season: str) -> list[tuple[str, str]]:
    """Return (start_date, end_date) pairs covering each month of a season.

    An NHL regular season runs Oct through Apr.  For season '20252026',
    this returns 7 tuples covering 2025-10 through 2026-04.

    Args:
        season: 8-digit season string (e.g. '20252026').

    Returns:
        List of (start, end) ISO-date strings.
    """
    start_year = int(season[:4])
    end_year = int(season[4:])

    ranges: list[tuple[str, str]] = []

    # Oct, Nov, Dec of start_year
    for month in (10, 11, 12):
        last_day = calendar.monthrange(start_year, month)[1]
        ranges.append((
            f"{start_year}-{month:02d}-01",
            f"{start_year}-{month:02d}-{last_day:02d}",
        ))

    # Jan, Feb, Mar, Apr of end_year
    for month in (1, 2, 3, 4):
        last_day = calendar.monthrange(end_year, month)[1]
        ranges.append((
            f"{end_year}-{month:02d}-01",
            f"{end_year}-{month:02d}-{last_day:02d}",
        ))

    return ranges


def fetch_all_skater_gamelogs_bulk(
    season: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch all skater per-game stats in bulk from the Stats API.

    Fetches data month-by-month (Oct-Apr) to stay under the API's 10K-row
    cap.  Each month yields ~6,300 rows (867 skaters * ~7 GP/month).
    Paginates skater/summary and skater/realtime per month, then joins on
    (playerId, gameId) to get complete per-game stats including hits
    and blocks.

    Args:
        season: Season string (e.g. '20252026').
        session: Optional requests session.

    Returns:
        List of stat dicts matching the format expected by save_skater_stats.
    """
    if session is None:
        session = requests.Session()

    month_ranges = _season_month_ranges(season)
    all_summary_rows: list[dict[str, Any]] = []
    all_realtime_rows: list[dict[str, Any]] = []

    for start_date, end_date in month_ranges:
        base_params = {
            "isAggregate": "false",
            "isGame": "true",
            "cayenneExp": (
                f'seasonId={season} and gameTypeId=2'
                f' and gameDate>="{start_date}"'
                f' and gameDate<="{end_date}"'
            ),
            "sort": '[{"property":"gameDate","direction":"DESC"}]',
        }

        logger.info(
            "Fetching skater gamelogs (summary) for %s to %s...",
            start_date, end_date,
        )
        summary_rows = _paginate_stats_api(
            session, f"{NHL_STATS_API_BASE}/skater/summary", base_params,
        )
        logger.info("  Got %d summary rows", len(summary_rows))
        all_summary_rows.extend(summary_rows)

        logger.info(
            "Fetching skater gamelogs (realtime) for %s to %s...",
            start_date, end_date,
        )
        realtime_rows = _paginate_stats_api(
            session, f"{NHL_STATS_API_BASE}/skater/realtime", base_params,
        )
        logger.info("  Got %d realtime rows", len(realtime_rows))
        all_realtime_rows.extend(realtime_rows)

    # Index realtime by (playerId, gameId) for fast lookup
    realtime_index: dict[tuple[int, int], dict[str, Any]] = {}
    for row in all_realtime_rows:
        key = (row["playerId"], row["gameId"])
        realtime_index[key] = row

    # Join and convert to internal format
    stats: list[dict[str, Any]] = []
    for row in all_summary_rows:
        pid = row["playerId"]
        gid = row["gameId"]
        rt = realtime_index.get((pid, gid), {})

        stats.append({
            "player_id": pid,
            "game_date": row.get("gameDate"),
            "toi": int(row.get("timeOnIcePerGame", 0)),
            "pp_toi": 0,
            "goals": int(row.get("goals", 0)),
            "assists": int(row.get("assists", 0)),
            "points": int(row.get("points", 0)),
            "plus_minus": int(row.get("plusMinus", 0)),
            "pim": int(row.get("penaltyMinutes", 0)),
            "shots": int(row.get("shots", 0)),
            "hits": int(rt.get("hits", 0)),
            "blocks": int(rt.get("blockedShots", 0)),
            "powerplay_goals": int(row.get("ppGoals", 0)),
            "powerplay_points": int(row.get("ppPoints", 0)),
            "shorthanded_goals": int(row.get("shGoals", 0)),
            "shorthanded_points": int(row.get("shPoints", 0)),
        })

    logger.info("Combined %d skater game log rows", len(stats))
    return stats


def fetch_all_skater_seasontotals_bulk(
    season: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch all skater season totals in bulk from the Stats API.

    Args:
        season: Season string (e.g. '20252026').
        session: Optional requests session.

    Returns:
        List of stat dicts with player_id and season aggregate stats.
    """
    if session is None:
        session = requests.Session()

    base_params = {
        "isAggregate": "true",
        "isGame": "false",
        "cayenneExp": f"seasonId={season} and gameTypeId=2",
        "sort": '[{"property":"points","direction":"DESC"}]',
    }

    logger.info("Fetching all skater season totals (summary) from Stats API...")
    summary_rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/skater/summary", base_params,
    )
    logger.info("  Got %d summary rows", len(summary_rows))

    # Realtime endpoint doesn't have "points" — sort by "hits" instead
    realtime_params = {
        **base_params,
        "sort": '[{"property":"hits","direction":"DESC"}]',
    }

    logger.info("Fetching all skater season totals (realtime) from Stats API...")
    realtime_rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/skater/realtime", realtime_params,
    )
    logger.info("  Got %d realtime rows", len(realtime_rows))

    # Index realtime by playerId
    realtime_index: dict[int, dict[str, Any]] = {}
    for row in realtime_rows:
        realtime_index[row["playerId"]] = row

    stats: list[dict[str, Any]] = []
    for row in summary_rows:
        pid = row["playerId"]
        rt = realtime_index.get(pid, {})

        stats.append({
            "player_id": pid,
            "game_date": None,
            "toi": int(row.get("timeOnIcePerGame", 0)) * int(row.get("gamesPlayed", 0)),
            "pp_toi": 0,
            "goals": int(row.get("goals", 0)),
            "assists": int(row.get("assists", 0)),
            "points": int(row.get("points", 0)),
            "plus_minus": int(row.get("plusMinus", 0)),
            "pim": int(row.get("penaltyMinutes", 0)),
            "shots": int(row.get("shots", 0)),
            "hits": int(rt.get("hits", 0)),
            "blocks": int(rt.get("blockedShots", 0)),
            "powerplay_goals": int(row.get("ppGoals", 0)),
            "powerplay_points": int(row.get("ppPoints", 0)),
            "shorthanded_goals": int(row.get("shGoals", 0)),
            "shorthanded_points": int(row.get("shPoints", 0)),
        })

    logger.info("Combined %d skater season total rows", len(stats))
    return stats


def fetch_all_goalie_gamelogs_bulk(
    season: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch all goalie per-game stats in bulk from the Stats API.

    Args:
        season: Season string (e.g. '20252026').
        session: Optional requests session.

    Returns:
        List of stat dicts matching the format expected by save_goalie_stats.
    """
    if session is None:
        session = requests.Session()

    base_params = {
        "isAggregate": "false",
        "isGame": "true",
        "cayenneExp": f"seasonId={season} and gameTypeId=2",
        "sort": '[{"property":"gameDate","direction":"DESC"}]',
    }

    logger.info("Fetching all goalie gamelogs from Stats API...")
    rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/goalie/summary", base_params,
    )
    logger.info("  Got %d goalie game log rows", len(rows))

    stats: list[dict[str, Any]] = []
    for row in rows:
        stats.append({
            "player_id": row["playerId"],
            "game_date": row.get("gameDate"),
            "toi": int(row.get("timeOnIce", 0)),
            "saves": int(row.get("saves", 0)),
            "goals_against": int(row.get("goalsAgainst", 0)),
            "shots_against": int(row.get("shotsAgainst", 0)),
            "wins": int(row.get("wins", 0)),
            "losses": int(row.get("losses", 0)),
            "ot_losses": int(row.get("otLosses", 0)),
            "shutouts": int(row.get("shutouts", 0)),
        })

    return stats


def fetch_all_goalie_seasontotals_bulk(
    season: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """Fetch all goalie season totals in bulk from the Stats API.

    Args:
        season: Season string (e.g. '20252026').
        session: Optional requests session.

    Returns:
        List of stat dicts with player_id and season aggregate stats.
    """
    if session is None:
        session = requests.Session()

    base_params = {
        "isAggregate": "true",
        "isGame": "false",
        "cayenneExp": f"seasonId={season} and gameTypeId=2",
        "sort": '[{"property":"wins","direction":"DESC"}]',
    }

    logger.info("Fetching all goalie season totals from Stats API...")
    rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/goalie/summary", base_params,
    )
    logger.info("  Got %d goalie season total rows", len(rows))

    stats: list[dict[str, Any]] = []
    for row in rows:
        stats.append({
            "player_id": row["playerId"],
            "game_date": None,
            "toi": int(row.get("timeOnIce", 0)),
            "saves": int(row.get("saves", 0)),
            "goals_against": int(row.get("goalsAgainst", 0)),
            "shots_against": int(row.get("shotsAgainst", 0)),
            "wins": int(row.get("wins", 0)),
            "losses": int(row.get("losses", 0)),
            "ot_losses": int(row.get("otLosses", 0)),
            "shutouts": int(row.get("shutouts", 0)),
        })

    return stats


def fetch_roster(
    team_abbrev: str,
    session: requests.Session | None = None
) -> list[PlayerDict]:
    """Fetch the current roster for a single team.

    Args:
        team_abbrev: Three-letter team abbreviation (e.g., "EDM").
        session: Optional requests session for connection pooling.

    Returns:
        List of PlayerDict with id, full_name, first_name, last_name,
        team_abbrev, and position.

    Raises:
        requests.HTTPError: On API failure.
        requests.Timeout: On request timeout.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching roster for %s", team_abbrev)
    url = f"{NHL_API_BASE}/roster/{team_abbrev}/current"
    response = _api_get(session, url)

    data = response.json()
    players: list[PlayerDict] = []

    for position_group in ["forwards", "defensemen", "goalies"]:
        for player_data in data.get(position_group, []):
            first_name = player_data.get("firstName", {}).get("default")
            last_name = player_data.get("lastName", {}).get("default")

            # Handle null/None name fields
            first_name = first_name if first_name else ""
            last_name = last_name if last_name else ""

            full_name = f"{first_name} {last_name}".strip()

            players.append({
                "id": player_data["id"],
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "team_abbrev": team_abbrev,
                "position": player_data.get("positionCode", ""),
            })

    return players


def fetch_all_rosters(
    conn: sqlite3.Connection,
    session: requests.Session | None = None,
    rate_limit: float = DEFAULT_RATE_LIMIT
) -> tuple[int, list[str]]:
    """Fetch rosters for all 32 teams and upsert to database.

    Continues on individual team failures.

    Args:
        conn: Database connection.
        session: Optional requests session.
        rate_limit: Seconds to sleep between requests.

    Returns:
        Tuple of (total players upserted, list of failed team abbreviations).
    """
    if session is None:
        session = requests.Session()

    total_count = 0
    failed_teams: list[str] = []

    for i, team in enumerate(ALL_TEAMS):
        try:
            players = fetch_roster(team, session)
            for player in players:
                upsert_player(conn, player)
            total_count += len(players)
        except (requests.HTTPError, requests.Timeout) as e:
            logger.warning(f"Failed to fetch roster for {team}: {e}")
            failed_teams.append(team)

        # Rate limit (except after last team)
        if i < len(ALL_TEAMS) - 1 and rate_limit > 0:
            time.sleep(rate_limit)

    logger.info("Fetched rosters: %d players (%d teams failed)", total_count, len(failed_teams))
    return total_count, failed_teams


def discover_missing_players(
    conn: sqlite3.Connection,
    season: str,
    session: requests.Session | None = None,
    rate_limit: float = DEFAULT_RATE_LIMIT,
) -> int:
    """Discover players who played this season but aren't in the players table.

    Queries the Stats API aggregate endpoints to find all player IDs, then
    fetches details for any missing ones from the player landing page.

    Args:
        conn: Database connection.
        season: Season string (e.g. '20252026').
        session: Optional requests session.
        rate_limit: Seconds to sleep between individual lookups.

    Returns:
        Number of newly discovered players.
    """
    if session is None:
        session = requests.Session()

    known_ids = {
        row["id"] for row in conn.execute("SELECT id FROM players").fetchall()
    }

    # Fetch all player IDs from Stats API aggregate endpoints
    skater_rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/skater/summary", {
            "isAggregate": "true",
            "isGame": "false",
            "cayenneExp": f"seasonId={season} and gameTypeId=2",
            "sort": '[{"property":"points","direction":"DESC"}]',
        },
    )
    goalie_rows = _paginate_stats_api(
        session, f"{NHL_STATS_API_BASE}/goalie/summary", {
            "isAggregate": "true",
            "isGame": "false",
            "cayenneExp": f"seasonId={season} and gameTypeId=2",
            "sort": '[{"property":"wins","direction":"DESC"}]',
        },
    )

    all_ids = {r["playerId"] for r in skater_rows} | {r["playerId"] for r in goalie_rows}
    missing = all_ids - known_ids

    if not missing:
        logger.info("No missing players to discover")
        return 0

    logger.info("Discovering %d players not on current rosters...", len(missing))

    discovered = 0
    for i, pid in enumerate(sorted(missing)):
        try:
            url = f"{NHL_API_BASE}/player/{pid}/landing"
            data = _api_get(session, url).json()
            first = data.get("firstName", {}).get("default", "")
            last = data.get("lastName", {}).get("default", "")
            upsert_player(conn, {
                "id": pid,
                "full_name": f"{first} {last}".strip(),
                "first_name": first,
                "last_name": last,
                "team_abbrev": data.get("currentTeamAbbrev", ""),
                "position": data.get("position", ""),
            })
            discovered += 1
        except Exception as e:
            logger.warning("Failed to discover player %d: %s", pid, e)

        if i < len(missing) - 1 and rate_limit > 0:
            time.sleep(rate_limit)

    conn.commit()
    logger.info("Discovered %d new players", discovered)
    return discovered


def fetch_skater_game_log(
    player_id: int,
    season: str,
    session: requests.Session | None = None
) -> list[dict[str, Any]]:
    """Fetch per-game stats for a skater.

    Args:
        player_id: NHL API player ID.
        season: Season in 8-digit format (e.g., "20232024").
        session: Optional requests session.

    Returns:
        List of stat dicts with game_date, toi (in seconds), goals, assists,
        hits, blocks, etc. Missing hits/blocks default to 0.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching skater game log for player %d", player_id)
    url = f"{NHL_API_BASE}/player/{player_id}/game-log/{season}/2"
    response = _api_get(session, url)

    data = response.json()
    game_log = data.get("gameLog", [])

    stats: list[dict[str, Any]] = []
    for game in game_log:
        stats.append({
            "game_date": game.get("gameDate"),
            "toi": toi_to_seconds(game.get("toi")),
            "pp_toi": toi_to_seconds(game.get("powerPlayToi")),
            "goals": int(game.get("goals", 0)),
            "assists": int(game.get("assists", 0)),
            "points": int(game.get("points", 0)),
            "plus_minus": int(game.get("plusMinus", 0)),
            "pim": int(game.get("pim", 0)),
            "shots": int(game.get("shots", 0)),
            "hits": int(game.get("hits", 0)),
            "blocks": int(game.get("blockedShots", 0)),
            "powerplay_goals": int(game.get("powerPlayGoals", 0)),
            "powerplay_points": int(game.get("powerPlayPoints", 0)),
            "shorthanded_goals": int(game.get("shorthandedGoals", 0)),
            "shorthanded_points": int(game.get("shorthandedPoints", 0)),
        })

    return stats


def fetch_goalie_game_log(
    player_id: int,
    season: str,
    session: requests.Session | None = None
) -> list[dict[str, Any]]:
    """Fetch per-game stats for a goalie.

    Args:
        player_id: NHL API player ID.
        season: Season in 8-digit format (e.g., "20232024").
        session: Optional requests session.

    Returns:
        List of stat dicts with game_date, toi (in seconds), wins, losses,
        shutouts, etc.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching goalie game log for player %d", player_id)
    url = f"{NHL_API_BASE}/player/{player_id}/game-log/{season}/2"
    response = _api_get(session, url)

    data = response.json()
    game_log = data.get("gameLog", [])

    stats: list[dict[str, Any]] = []
    for game in game_log:
        decision = game.get("decision", "")
        wins = 1 if decision == "W" else 0
        losses = 1 if decision == "L" else 0
        ot_losses = 1 if decision == "O" else 0

        shots_against = int(game.get("shotsAgainst", 0))
        goals_against = int(game.get("goalsAgainst", 0))
        saves = shots_against - goals_against

        stats.append({
            "game_date": game.get("gameDate"),
            "toi": toi_to_seconds(game.get("toi")),
            "saves": saves,
            "goals_against": goals_against,
            "shots_against": shots_against,
            "wins": wins,
            "losses": losses,
            "ot_losses": ot_losses,
            "shutouts": int(game.get("shutouts", 0)),
        })

    return stats


def fetch_player_landing(
    player_id: int,
    session: requests.Session | None = None
) -> dict[str, Any]:
    """Fetch season totals from the player landing page.

    Args:
        player_id: NHL API player ID.
        session: Optional requests session.

    Returns:
        Dict with season summary stats.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching player landing for %d", player_id)
    url = f"{NHL_API_BASE}/player/{player_id}/landing"
    response = _api_get(session, url)

    return response.json()


def fetch_team_schedule(
    team_abbrev: str,
    season: str,
    session: requests.Session | None = None
) -> list[dict[str, Any]]:
    """Fetch all games for a team in a season.

    Args:
        team_abbrev: Three-letter team abbreviation.
        season: Season in 8-digit format (e.g., "20232024").
        session: Optional requests session.

    Returns:
        List of game dicts with game_date, opponent, home_away, result.
    """
    if session is None:
        session = requests.Session()

    logger.debug("Fetching schedule for %s", team_abbrev)
    url = f"{NHL_API_BASE}/club-schedule-season/{team_abbrev}/{season}"
    response = _api_get(session, url)

    data = response.json()
    games_data = data.get("games", [])

    games: list[dict[str, Any]] = []
    for game in games_data:
        home_team = game.get("homeTeam", {}).get("abbrev", "")
        away_team = game.get("awayTeam", {}).get("abbrev", "")

        if home_team == team_abbrev:
            home_away = "home"
            opponent = away_team
        else:
            home_away = "away"
            opponent = home_team

        games.append({
            "game_date": game.get("gameDate"),
            "opponent": opponent,
            "home_away": home_away,
            "result": None,  # Can be populated from game outcome if needed
        })

    return games


def save_skater_stats(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    stats: list[dict[str, Any]],
    is_season_total: bool = False
) -> int:
    """Insert skater game log rows into skater_stats table.

    Args:
        conn: Database connection.
        player_id: NHL API player ID.
        season: Season in 8-digit format.
        stats: List of stat dicts from fetch_skater_game_log.
        is_season_total: True for season total row (NULL game_date).

    Returns:
        Number of rows inserted/updated.
    """
    # For season totals, delete existing row first (NULL game_date doesn't work
    # with UNIQUE constraint for INSERT OR REPLACE)
    if is_season_total:
        conn.execute(
            """
            DELETE FROM skater_stats
            WHERE player_id = ? AND season = ? AND is_season_total = 1
            """,
            (player_id, season),
        )

    count = 0
    for stat in stats:
        conn.execute(
            """
            INSERT OR REPLACE INTO skater_stats (
                player_id, game_date, season, is_season_total, toi, pp_toi,
                goals, assists, points, plus_minus, pim, shots,
                hits, blocks, powerplay_goals, powerplay_points,
                shorthanded_goals, shorthanded_points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                stat.get("game_date"),
                season,
                1 if is_season_total else 0,
                stat.get("toi", 0),
                stat.get("pp_toi", 0),
                stat.get("goals", 0),
                stat.get("assists", 0),
                stat.get("points", 0),
                stat.get("plus_minus", 0),
                stat.get("pim", 0),
                stat.get("shots", 0),
                stat.get("hits", 0),
                stat.get("blocks", 0),
                stat.get("powerplay_goals", 0),
                stat.get("powerplay_points", 0),
                stat.get("shorthanded_goals", 0),
                stat.get("shorthanded_points", 0),
            ),
        )
        count += 1
    conn.commit()
    return count


def save_goalie_stats(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    stats: list[dict[str, Any]],
    is_season_total: bool = False
) -> int:
    """Insert goalie game log rows into goalie_stats table.

    Args:
        conn: Database connection.
        player_id: NHL API player ID.
        season: Season in 8-digit format.
        stats: List of stat dicts from fetch_goalie_game_log.
        is_season_total: True for season total row (NULL game_date).

    Returns:
        Number of rows inserted/updated.
    """
    # For season totals, delete existing row first (NULL game_date doesn't work
    # with UNIQUE constraint for INSERT OR REPLACE)
    if is_season_total:
        conn.execute(
            """
            DELETE FROM goalie_stats
            WHERE player_id = ? AND season = ? AND is_season_total = 1
            """,
            (player_id, season),
        )

    count = 0
    for stat in stats:
        conn.execute(
            """
            INSERT OR REPLACE INTO goalie_stats (
                player_id, game_date, season, is_season_total, toi,
                saves, goals_against, shots_against,
                wins, losses, ot_losses, shutouts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                stat.get("game_date"),
                season,
                1 if is_season_total else 0,
                stat.get("toi", 0),
                stat.get("saves", 0),
                stat.get("goals_against", 0),
                stat.get("shots_against", 0),
                stat.get("wins", 0),
                stat.get("losses", 0),
                stat.get("ot_losses", 0),
                stat.get("shutouts", 0),
            ),
        )
        count += 1
    conn.commit()
    return count


def save_team_schedule(
    conn: sqlite3.Connection,
    team_abbrev: str,
    season: str,
    games: list[dict[str, Any]]
) -> int:
    """Insert team games into team_games table.

    Args:
        conn: Database connection.
        team_abbrev: Three-letter team abbreviation.
        season: Season in 8-digit format.
        games: List of game dicts from fetch_team_schedule.

    Returns:
        Number of rows inserted/updated.
    """
    count = 0
    for game in games:
        conn.execute(
            """
            INSERT OR REPLACE INTO team_games (
                team, season, game_date, opponent, home_away, result
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                team_abbrev,
                season,
                game.get("game_date"),
                game.get("opponent"),
                game.get("home_away"),
                game.get("result"),
            ),
        )
        count += 1
    conn.commit()
    return count


def sync_all(
    conn: sqlite3.Connection,
    season: str,
    rate_limit: float = DEFAULT_RATE_LIMIT,
    session: requests.Session | None = None
) -> dict[str, int]:
    """Run the full pipeline: rosters → schedules → game logs → season totals.

    Args:
        conn: Database connection.
        season: Season in 8-digit format (e.g., "20232024").
        rate_limit: Seconds to sleep between API calls.
        session: Optional requests session.

    Returns:
        Summary dict with counts.
    """
    if session is None:
        session = requests.Session()

    result = {
        "players": 0,
        "skater_games": 0,
        "goalie_games": 0,
        "team_games": 0,
    }

    # Fetch rosters
    player_count, failed = fetch_all_rosters(conn, session, rate_limit)
    result["players"] = player_count

    # Fetch schedules for all teams
    for i, team in enumerate(ALL_TEAMS):
        try:
            games = fetch_team_schedule(team, season, session)
            save_team_schedule(conn, team, season, games)
            result["team_games"] += len(games)
        except (requests.HTTPError, requests.Timeout) as e:
            logger.warning(f"Failed to fetch schedule for {team}: {e}")

        if rate_limit > 0:
            time.sleep(rate_limit)

    # Fetch game logs for all players
    cursor = conn.execute("SELECT id, position FROM players")
    players = cursor.fetchall()

    for player in players:
        player_id = player["id"]
        position = player["position"]

        try:
            if position == "G":
                stats = fetch_goalie_game_log(player_id, season, session)
                save_goalie_stats(conn, player_id, season, stats)
                result["goalie_games"] += len(stats)
            else:
                stats = fetch_skater_game_log(player_id, season, session)
                save_skater_stats(conn, player_id, season, stats)
                result["skater_games"] += len(stats)
        except (requests.HTTPError, requests.Timeout) as e:
            logger.warning(f"Failed to fetch game log for player {player_id}: {e}")

        if rate_limit > 0:
            time.sleep(rate_limit)

    return result


def calculate_games_benched(
    conn: sqlite3.Connection,
    player_id: int,
    season: str
) -> int | None:
    """Calculate games a player was benched.

    Args:
        conn: Database connection.
        player_id: NHL API player ID.
        season: Season in 8-digit format.

    Returns:
        Number of games benched, or None if player not in DB.
        Returns 0 if team has no games or player GP exceeds team games.
    """
    # Check if player exists
    cursor = conn.execute(
        "SELECT team_abbrev FROM players WHERE id = ?",
        (player_id,)
    )
    player = cursor.fetchone()
    if player is None:
        return None

    team_abbrev = player["team_abbrev"]

    # Get team game count
    cursor = conn.execute(
        """
        SELECT COUNT(*) as cnt FROM team_games
        WHERE team = ? AND season = ?
        """,
        (team_abbrev, season)
    )
    team_games = cursor.fetchone()["cnt"]

    if team_games == 0:
        return 0

    # Get player game count (exclude season totals)
    cursor = conn.execute(
        """
        SELECT COUNT(*) as cnt FROM skater_stats
        WHERE player_id = ? AND season = ? AND is_season_total = 0
        """,
        (player_id, season)
    )
    player_games = cursor.fetchone()["cnt"]

    # If player is goalie, check goalie_stats instead
    if player_games == 0:
        cursor = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM goalie_stats
            WHERE player_id = ? AND season = ? AND is_season_total = 0
            """,
            (player_id, season)
        )
        player_games = cursor.fetchone()["cnt"]

    benched = team_games - player_games
    return max(0, benched)  # Floor at 0


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="NHL Web API stats fetcher"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run full sync"
    )
    parser.add_argument(
        "--rosters",
        action="store_true",
        help="Fetch rosters only"
    )
    parser.add_argument(
        "--player",
        type=int,
        metavar="ID",
        help="Fetch game log for a specific player"
    )
    parser.add_argument(
        "--position",
        choices=["skater", "goalie"],
        help="Player position (required if player not in DB)"
    )
    parser.add_argument(
        "--schedules",
        action="store_true",
        help="Fetch team schedules only"
    )
    parser.add_argument(
        "--season",
        type=str,
        default="20242025",
        help="Season in 8-digit format (e.g., 20242025)"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="db/nhl_data.db",
        help="Database path"
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help="Seconds between API requests"
    )

    args = parser.parse_args()

    # Initialize DB
    db_path = Path(args.db)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    init_db(db_path)
    conn = get_db(db_path)

    session = requests.Session()

    if args.all:
        result = sync_all(conn, args.season, args.rate_limit, session)
        print(f"Sync complete: {result}")

    elif args.rosters:
        count, failed = fetch_all_rosters(conn, session, args.rate_limit)
        print(f"Fetched {count} players")
        if failed:
            print(f"Failed teams: {failed}")

    elif args.player:
        # Determine position
        position = args.position
        if position is None:
            cursor = conn.execute(
                "SELECT position FROM players WHERE id = ?",
                (args.player,)
            )
            row = cursor.fetchone()
            if row is None:
                print(f"Player {args.player} not in DB. Use --position to specify.")
                return
            position = "goalie" if row["position"] == "G" else "skater"

        if position == "goalie":
            stats = fetch_goalie_game_log(args.player, args.season, session)
            save_goalie_stats(conn, args.player, args.season, stats)
        else:
            stats = fetch_skater_game_log(args.player, args.season, session)
            save_skater_stats(conn, args.player, args.season, stats)

        print(f"Fetched {len(stats)} games for player {args.player}")

    elif args.schedules:
        total = 0
        for team in ALL_TEAMS:
            try:
                games = fetch_team_schedule(team, args.season, session)
                save_team_schedule(conn, team, args.season, games)
                total += len(games)
                print(f"{team}: {len(games)} games")
            except requests.HTTPError as e:
                print(f"{team}: Error - {e}")
            time.sleep(args.rate_limit)
        print(f"Total: {total} games")

    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
