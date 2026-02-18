"""NHL Web API stats fetcher.

Fetches player rosters, game logs, and team schedules from the NHL Web API.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests

from db.schema import PlayerDict, get_db, init_db, upsert_player
from utils.time import toi_to_seconds

logger = logging.getLogger(__name__)

NHL_API_BASE = "https://api-web.nhle.com/v1"

ALL_TEAMS = [
    "ANA", "BOS", "BUF", "CGY", "CAR", "CHI", "COL", "CBJ", "DAL", "DET",
    "EDM", "FLA", "LAK", "MIN", "MTL", "NSH", "NJD", "NYI", "NYR", "OTT",
    "PHI", "PIT", "SJS", "SEA", "STL", "TBL", "TOR", "UTA", "VAN", "VGK",
    "WSH", "WPG"
]

DEFAULT_RATE_LIMIT = 0.5  # seconds between requests


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

    url = f"{NHL_API_BASE}/roster/{team_abbrev}/current"
    response = session.get(url, timeout=30)
    response.raise_for_status()

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

    return total_count, failed_teams


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

    url = f"{NHL_API_BASE}/player/{player_id}/game-log/{season}/2"
    response = session.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    game_log = data.get("gameLog", [])

    stats: list[dict[str, Any]] = []
    for game in game_log:
        stats.append({
            "game_date": game.get("gameDate"),
            "toi": toi_to_seconds(game.get("toi")),
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

    url = f"{NHL_API_BASE}/player/{player_id}/game-log/{season}/2"
    response = session.get(url, timeout=30)
    response.raise_for_status()

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

    url = f"{NHL_API_BASE}/player/{player_id}/landing"
    response = session.get(url, timeout=30)
    response.raise_for_status()

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

    url = f"{NHL_API_BASE}/club-schedule-season/{team_abbrev}/{season}"
    response = session.get(url, timeout=30)
    response.raise_for_status()

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
                player_id, game_date, season, is_season_total, toi,
                goals, assists, points, plus_minus, pim, shots,
                hits, blocks, powerplay_goals, powerplay_points,
                shorthanded_goals, shorthanded_points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                player_id,
                stat.get("game_date"),
                season,
                1 if is_season_total else 0,
                stat.get("toi", 0),
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
