"""Data query layer for the NHL fantasy assistant.

All public functions take a sqlite3.Connection as first arg and return
dicts or lists suitable for the formatters module.

Season totals (is_season_total=1) include correct hits and blocks from
the Stats API bulk endpoints.  Per-game rows may be incomplete due to
the Stats API 10K-row cap, so season-level hits/blocks come from the
season totals row.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from datetime import date, timedelta

from assistant.scoring import calc_skater_fpts, calc_goalie_fpts
from assistant.player_match import (
    resolve_player,
    resolve_fantrax_to_nhl,
    get_rostered_nhl_ids,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FORWARD_POSITIONS = {"C", "L", "R"}


def _is_goalie(conn: sqlite3.Connection, player_id: int) -> bool:
    """Check if a player is a goalie."""
    row = conn.execute(
        "SELECT position FROM players WHERE id = ?", (player_id,)
    ).fetchone()
    return row is not None and row["position"] == "G"


def _position_group(position: str | None) -> str:
    """Map a position code to F/D/G."""
    if not position:
        return "F"
    if position == "G":
        return "G"
    if position == "D":
        return "D"
    return "F"


def _get_recent_fpts_list(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    is_goalie: bool,
) -> list[float]:
    """Get per-game fantasy points ordered by game_date DESC.

    Returns a list of floats where index 0 is the most recent game.
    Empty list if no per-game rows exist.
    """
    if is_goalie:
        rows = conn.execute(
            "SELECT wins, losses, ot_losses, shutouts "
            "FROM goalie_stats "
            "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
            "ORDER BY game_date DESC",
            (player_id, season),
        ).fetchall()
        return [
            calc_goalie_fpts(
                goals=0, assists=0, wins=r["wins"], shutouts=r["shutouts"],
                ot_losses=r["ot_losses"], losses=r["losses"],
            )
            for r in rows
        ]
    else:
        rows = conn.execute(
            "SELECT goals, assists, hits, blocks "
            "FROM skater_stats "
            "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
            "ORDER BY game_date DESC",
            (player_id, season),
        ).fetchall()
        return [
            calc_skater_fpts(
                goals=r["goals"], assists=r["assists"],
                blocks=r["blocks"], hits=r["hits"],
            )
            for r in rows
        ]


def _get_skater_season_stats(
    conn: sqlite3.Connection, player_id: int, season: str
) -> dict:
    """Get skater season stats with hits/blocks from season totals row.

    Season totals (is_season_total=1) have correct hits/blocks from the
    Stats API realtime endpoint.  Games played comes from per-game row count.
    """
    # Season totals row
    totals = conn.execute(
        "SELECT goals, assists, points, shots, plus_minus, pim, toi, "
        "pp_toi, powerplay_goals, powerplay_points, "
        "shorthanded_goals, shorthanded_points, "
        "hits, blocks "
        "FROM skater_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 1",
        (player_id, season),
    ).fetchone()

    # Per-game row count for games played
    agg = conn.execute(
        "SELECT COUNT(*) AS games_played "
        "FROM skater_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 0",
        (player_id, season),
    ).fetchone()

    if totals is None and agg["games_played"] == 0:
        return {}

    goals = totals["goals"] if totals else 0
    assists = totals["assists"] if totals else 0
    points = totals["points"] if totals else 0
    hits = totals["hits"] if totals else 0
    blocks = totals["blocks"] if totals else 0
    gp = agg["games_played"]

    toi = totals["toi"] if totals else 0
    fpts = calc_skater_fpts(goals=goals, assists=assists, blocks=blocks, hits=hits)
    fpts_per_game = round(fpts / gp, 2) if gp > 0 else 0.0
    toi_per_game = round(toi / gp) if gp > 0 else 0

    result = {
        "goals": goals,
        "assists": assists,
        "points": points,
        "hits": hits,
        "blocks": blocks,
        "shots": totals["shots"] if totals else 0,
        "plus_minus": totals["plus_minus"] if totals else 0,
        "pim": totals["pim"] if totals else 0,
        "toi": totals["toi"] if totals else 0,
        "pp_toi": totals["pp_toi"] if totals else 0,
        "powerplay_goals": totals["powerplay_goals"] if totals else 0,
        "powerplay_points": totals["powerplay_points"] if totals else 0,
        "shorthanded_goals": totals["shorthanded_goals"] if totals else 0,
        "shorthanded_points": totals["shorthanded_points"] if totals else 0,
        "games_played": gp,
        "fantasy_points": round(fpts, 2),
        "fpts_per_game": fpts_per_game,
        "toi_per_game": toi_per_game,
    }
    return result


def _get_goalie_season_stats(
    conn: sqlite3.Connection, player_id: int, season: str
) -> dict:
    """Get goalie season stats."""
    totals = conn.execute(
        "SELECT wins, losses, ot_losses, shutouts, saves, "
        "goals_against, shots_against, toi "
        "FROM goalie_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 1",
        (player_id, season),
    ).fetchone()

    gp_row = conn.execute(
        "SELECT COUNT(*) AS games_played "
        "FROM goalie_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 0",
        (player_id, season),
    ).fetchone()

    gp = gp_row["games_played"] if gp_row else 0

    if totals is None and gp == 0:
        return {}

    wins = totals["wins"] if totals else 0
    losses = totals["losses"] if totals else 0
    ot_losses = totals["ot_losses"] if totals else 0
    shutouts = totals["shutouts"] if totals else 0
    saves = totals["saves"] if totals else 0
    goals_against = totals["goals_against"] if totals else 0
    shots_against = totals["shots_against"] if totals else 0
    toi = totals["toi"] if totals else 0

    fpts = calc_goalie_fpts(
        goals=0, assists=0, wins=wins, shutouts=shutouts,
        ot_losses=ot_losses, losses=losses,
    )
    fpts_per_game = round(fpts / gp, 2) if gp > 0 else 0.0
    gaa = round(goals_against * 3600 / toi, 2) if toi > 0 else 0.0
    sv_pct = round(saves / shots_against, 3) if shots_against > 0 else 0.0

    return {
        "wins": wins,
        "losses": losses,
        "ot_losses": ot_losses,
        "shutouts": shutouts,
        "saves": saves,
        "goals_against": goals_against,
        "shots_against": shots_against,
        "toi": toi,
        "gaa": gaa,
        "sv_pct": sv_pct,
        "games_played": gp,
        "fantasy_points": round(fpts, 2),
        "fpts_per_game": fpts_per_game,
    }


def _get_injury_status(conn: sqlite3.Connection, player_id: int) -> dict | None:
    """Get current injury info for a player, or None if healthy."""
    row = conn.execute(
        "SELECT injury_type, status, updated_at "
        "FROM player_injuries WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "injury_type": row["injury_type"],
        "status": row["status"],
        "updated_at": row["updated_at"],
    }


def _get_line_context(
    conn: sqlite3.Connection, player_id: int
) -> dict | None:
    """Get current line combination info for a player, or None if unavailable."""
    row = conn.execute(
        "SELECT ev_line, pp_unit, pk_unit, ev_group, pp_group, "
        "ev_linemates, pp_linemates, position, rating, updated_at "
        "FROM line_combinations WHERE player_id = ?",
        (player_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "ev_line": row["ev_line"],
        "pp_unit": row["pp_unit"],
        "pk_unit": row["pk_unit"],
        "ev_linemates": json.loads(row["ev_linemates"]) if row["ev_linemates"] else [],
        "pp_linemates": json.loads(row["pp_linemates"]) if row["pp_linemates"] else [],
        "rating": row["rating"],
    }


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


def get_my_roster(
    conn: sqlite3.Connection, team_id: str, season: str
) -> list[dict]:
    """Get the user's fantasy roster with NHL stats and calculated FP.

    Args:
        conn: Database connection.
        team_id: Fantasy team ID.
        season: Season string (e.g. '20252026').

    Returns:
        List of player dicts with stats, FP, injury info.
    """
    slots = conn.execute(
        "SELECT player_name, position_short, status_id, salary "
        "FROM fantasy_roster_slots WHERE team_id = ?",
        (team_id,),
    ).fetchall()

    roster = []
    for slot in slots:
        name = slot["player_name"]
        if not name:
            continue
        resolved = resolve_player(conn, name)
        if resolved is None:
            roster.append({
                "player_name": name,
                "position": slot["position_short"],
                "nhl_id": None,
                "team": None,
                "games_played": 0,
                "fantasy_points": 0.0,
                "fpts_per_game": 0.0,
                "injury": None,
                "salary": slot["salary"],
                "roster_status": slot["status_id"],
            })
            continue

        nhl_id = resolved["id"]
        position = resolved["position"]
        goalie = position == "G"

        if goalie:
            stats = _get_goalie_season_stats(conn, nhl_id, season)
        else:
            stats = _get_skater_season_stats(conn, nhl_id, season)

        injury = _get_injury_status(conn, nhl_id)

        entry = {
            "player_name": resolved["full_name"],
            "position": position,
            "nhl_id": nhl_id,
            "team": resolved["team_abbrev"],
            "games_played": stats.get("games_played", 0),
            "fantasy_points": stats.get("fantasy_points", 0.0),
            "fpts_per_game": stats.get("fpts_per_game", 0.0),
            "injury": injury,
            "salary": slot["salary"],
            "roster_status": slot["status_id"],
        }

        if goalie:
            entry.update({
                "wins": stats.get("wins", 0),
                "losses": stats.get("losses", 0),
                "ot_losses": stats.get("ot_losses", 0),
                "shutouts": stats.get("shutouts", 0),
                "gaa": stats.get("gaa", 0.0),
                "sv_pct": stats.get("sv_pct", 0.0),
            })
        else:
            entry.update({
                "goals": stats.get("goals", 0),
                "assists": stats.get("assists", 0),
                "points": stats.get("points", 0),
                "hits": stats.get("hits", 0),
                "blocks": stats.get("blocks", 0),
                "toi_per_game": stats.get("toi_per_game", 0),
            })

        roster.append(entry)

    return roster


def get_roster_analysis(
    conn: sqlite3.Connection, team_id: str, season: str
) -> dict:
    """Analyze roster by position breakdown, bottom performers, injuries.

    Args:
        conn: Database connection.
        team_id: Fantasy team ID.
        season: Season string.

    Returns:
        Dict with position_counts, avg_fpts_by_position,
        bottom_performers, injured_players.
    """
    roster = get_my_roster(conn, team_id, season)

    counts: dict[str, int] = {"F": 0, "D": 0, "G": 0}
    fpts_sums: dict[str, float] = {"F": 0.0, "D": 0.0, "G": 0.0}
    injured = []

    for p in roster:
        group = _position_group(p.get("position"))
        counts[group] = counts.get(group, 0) + 1
        fpts_sums[group] = fpts_sums.get(group, 0.0) + p["fpts_per_game"]
        if p.get("injury"):
            injured.append(p)

    avg_fpts = {}
    for g in ("F", "D", "G"):
        avg_fpts[g] = round(fpts_sums[g] / counts[g], 2) if counts[g] > 0 else 0.0

    # Bottom 3 by FP/G (players with at least 1 game)
    with_games = [p for p in roster if p["games_played"] > 0]
    with_games.sort(key=lambda p: p["fpts_per_game"])
    bottom = with_games[:3]

    return {
        "position_counts": counts,
        "avg_fpts_by_position": avg_fpts,
        "bottom_performers": bottom,
        "injured_players": injured,
    }


def search_free_agents(
    conn: sqlite3.Connection,
    season: str,
    position: str = "any",
    sort_by: str = "fpts_per_game",
    min_games: int = 10,
    limit: int = 20,
) -> list[dict]:
    """Find the best available free agents (not on any fantasy roster).

    Args:
        conn: Database connection.
        season: Season string.
        position: Filter by position ('any', 'F', 'D', 'G', or specific like 'C').
        sort_by: Sort key ('fpts_per_game' or 'fantasy_points').
        min_games: Minimum games played.
        limit: Max results.

    Returns:
        List of player dicts with stats and FP/G.
    """
    rostered = get_rostered_nhl_ids(conn)

    # Build position filter
    if position == "any":
        pos_filter = None
    elif position == "F":
        pos_filter = _FORWARD_POSITIONS
    else:
        pos_filter = {position}

    all_players = conn.execute(
        "SELECT id, full_name, team_abbrev, position FROM players"
    ).fetchall()

    results = []
    for p in all_players:
        pid = p["id"]
        if pid in rostered:
            continue

        pos = p["position"]
        if pos_filter and pos not in pos_filter:
            continue

        goalie = pos == "G"
        if goalie:
            stats = _get_goalie_season_stats(conn, pid, season)
        else:
            stats = _get_skater_season_stats(conn, pid, season)

        if not stats:
            continue

        gp = stats.get("games_played", 0)
        if gp < min_games:
            continue

        entry = {
            "player_name": p["full_name"],
            "team": p["team_abbrev"],
            "position": pos,
            "games_played": gp,
            "fantasy_points": stats.get("fantasy_points", 0.0),
            "fpts_per_game": stats.get("fpts_per_game", 0.0),
        }

        if goalie:
            entry.update({
                "wins": stats.get("wins", 0),
                "losses": stats.get("losses", 0),
                "shutouts": stats.get("shutouts", 0),
                "gaa": stats.get("gaa", 0.0),
                "sv_pct": stats.get("sv_pct", 0.0),
            })
        else:
            entry.update({
                "goals": stats.get("goals", 0),
                "assists": stats.get("assists", 0),
                "points": stats.get("points", 0),
                "hits": stats.get("hits", 0),
                "blocks": stats.get("blocks", 0),
                "toi_per_game": stats.get("toi_per_game", 0),
            })

        # Hot/cold trend based on last 14 games vs season average
        fpts_list = _get_recent_fpts_list(conn, pid, season, is_goalie=goalie)
        recent_14 = fpts_list[:14]
        recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0
        entry["recent_14_fpg"] = recent_14_fpg
        season_fpg = entry["fpts_per_game"]
        if season_fpg > 0 and recent_14_fpg > season_fpg * 1.15:
            entry["trend"] = "hot"
        elif season_fpg > 0 and recent_14_fpg < season_fpg * 0.85:
            entry["trend"] = "cold"
        else:
            entry["trend"] = "neutral"

        entry["injury"] = _get_injury_status(conn, pid)

        line_ctx = _get_line_context(conn, pid)
        entry["ev_line"] = line_ctx["ev_line"] if line_ctx else None
        entry["pp_unit"] = line_ctx["pp_unit"] if line_ctx else None

        recent_news_row = conn.execute(
            "SELECT headline FROM player_news WHERE player_id = ? "
            "AND published_at >= date('now', '-42 days') "
            "ORDER BY published_at DESC LIMIT 1",
            (pid,),
        ).fetchone()
        entry["recent_news"] = recent_news_row["headline"] if recent_news_row else None

        results.append(entry)

    results.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
    return results[:limit]


def get_player_stats(
    conn: sqlite3.Connection,
    player_name: str,
    season: str,
    recent_games: int = 5,
) -> dict | None:
    """Get detailed stats for a single player.

    Args:
        conn: Database connection.
        player_name: Player name to search.
        season: Season string.
        recent_games: Number of recent games for the log.

    Returns:
        Dict with player info, season stats, game log, injury, news.
        None if player not found.
    """
    resolved = resolve_player(conn, player_name)
    if resolved is None:
        return None

    nhl_id = resolved["id"]
    goalie = resolved["position"] == "G"

    # Season stats
    if goalie:
        stats = _get_goalie_season_stats(conn, nhl_id, season)
    else:
        stats = _get_skater_season_stats(conn, nhl_id, season)

    # Recent game log
    if goalie:
        log_rows = conn.execute(
            "SELECT game_date, wins, losses, ot_losses, shutouts, "
            "saves, goals_against, shots_against, toi "
            "FROM goalie_stats "
            "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
            "ORDER BY game_date DESC LIMIT ?",
            (nhl_id, season, recent_games),
        ).fetchall()
        game_log = []
        for r in log_rows:
            fpts = calc_goalie_fpts(
                goals=0, assists=0, wins=r["wins"], shutouts=r["shutouts"],
                ot_losses=r["ot_losses"], losses=r["losses"],
            )
            game_log.append({
                "game_date": r["game_date"],
                "wins": r["wins"],
                "losses": r["losses"],
                "ot_losses": r["ot_losses"],
                "shutouts": r["shutouts"],
                "saves": r["saves"],
                "goals_against": r["goals_against"],
                "fantasy_points": round(fpts, 2),
            })
    else:
        log_rows = conn.execute(
            "SELECT game_date, goals, assists, points, hits, blocks, "
            "shots, plus_minus, toi "
            "FROM skater_stats "
            "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
            "ORDER BY game_date DESC LIMIT ?",
            (nhl_id, season, recent_games),
        ).fetchall()
        game_log = []
        for r in log_rows:
            fpts = calc_skater_fpts(
                goals=r["goals"], assists=r["assists"],
                blocks=r["blocks"], hits=r["hits"],
            )
            game_log.append({
                "game_date": r["game_date"],
                "goals": r["goals"],
                "assists": r["assists"],
                "points": r["points"],
                "hits": r["hits"],
                "blocks": r["blocks"],
                "shots": r["shots"],
                "toi": r["toi"],
                "fantasy_points": round(fpts, 2),
            })

    # Injury
    injury = _get_injury_status(conn, nhl_id)

    # News
    news_rows = conn.execute(
        "SELECT headline, content, published_at "
        "FROM player_news WHERE player_id = ? "
        "ORDER BY published_at DESC LIMIT 5",
        (nhl_id,),
    ).fetchall()
    news = [dict(r) for r in news_rows]

    line_context = _get_line_context(conn, nhl_id)

    return {
        "player": resolved,
        "is_goalie": goalie,
        "season_stats": stats,
        "game_log": game_log,
        "injury": injury,
        "news": news,
        "line_context": line_context,
    }


def compare_players(
    conn: sqlite3.Connection,
    player_names: list[str],
    season: str,
) -> list[dict]:
    """Side-by-side comparison of 2-5 players.

    Args:
        conn: Database connection.
        player_names: List of player names to compare.
        season: Season string.

    Returns:
        List of player stat dicts (one per player).
    """
    results = []
    for name in player_names:
        data = get_player_stats(conn, name, season)
        if data:
            entry = {"player": data["player"], "is_goalie": data["is_goalie"]}
            entry.update(data["season_stats"])
            entry["line_context"] = data.get("line_context")
            results.append(entry)
    return results


def get_player_trends(
    conn: sqlite3.Connection,
    player_name: str,
    season: str,
) -> dict | None:
    """Analyze player trends over recent windows.

    Calculates averages for last 7, last 14, and full season games.
    Flags hot (>15% above season avg) or cold (>15% below).

    Args:
        conn: Database connection.
        player_name: Player name to search.
        season: Season string.

    Returns:
        Dict with windows and trend flag. None if player not found.
    """
    resolved = resolve_player(conn, player_name)
    if resolved is None:
        return None

    nhl_id = resolved["id"]
    goalie = resolved["position"] == "G"
    all_fpts = _get_recent_fpts_list(conn, nhl_id, season, is_goalie=goalie)

    if not all_fpts:
        return {"player": resolved, "windows": {}, "trend": "neutral"}

    def avg(lst: list[float]) -> float:
        return round(sum(lst) / len(lst), 2) if lst else 0.0

    season_avg = avg(all_fpts)
    last_7_avg = avg(all_fpts[:7])
    last_14_avg = avg(all_fpts[:14])
    last_30_avg = avg(all_fpts[:30])

    if season_avg > 0 and last_7_avg > season_avg * 1.15:
        trend = "hot"
    elif season_avg > 0 and last_7_avg < season_avg * 0.85:
        trend = "cold"
    else:
        trend = "neutral"

    return {
        "player": resolved,
        "windows": {
            "last_7": {"fpts_per_game": last_7_avg, "games": min(7, len(all_fpts))},
            "last_14": {"fpts_per_game": last_14_avg, "games": min(14, len(all_fpts))},
            "last_30": {"fpts_per_game": last_30_avg, "games": min(30, len(all_fpts))},
            "season": {"fpts_per_game": season_avg, "games": len(all_fpts)},
        },
        "trend": trend,
    }


def get_recent_news(
    conn: sqlite3.Connection,
    player_name: str | None = None,
    team_id: str | None = None,
    limit: int = 15,
) -> list[dict]:
    """Get recent player news.

    Args:
        conn: Database connection.
        player_name: If given, news for that specific player.
        team_id: If given, news for all players on that fantasy roster.
        limit: Max news items.

    Returns:
        List of news dicts with player_name, headline, content, published_at.
    """
    if player_name:
        resolved = resolve_player(conn, player_name)
        if resolved is None:
            return []
        rows = conn.execute(
            "SELECT pn.headline, pn.content, pn.published_at, p.full_name "
            "FROM player_news pn "
            "JOIN players p ON pn.player_id = p.id "
            "WHERE pn.player_id = ? "
            "ORDER BY pn.published_at DESC LIMIT ?",
            (resolved["id"], limit),
        ).fetchall()
    elif team_id:
        # Get all player names on the roster, resolve to NHL IDs
        slots = conn.execute(
            "SELECT player_name FROM fantasy_roster_slots WHERE team_id = ?",
            (team_id,),
        ).fetchall()
        nhl_ids = []
        for s in slots:
            if s["player_name"]:
                nid = resolve_fantrax_to_nhl(conn, s["player_name"])
                if nid is not None:
                    nhl_ids.append(nid)
        if not nhl_ids:
            return []
        placeholders = ",".join("?" * len(nhl_ids))
        rows = conn.execute(
            f"SELECT pn.headline, pn.content, pn.published_at, p.full_name "
            f"FROM player_news pn "
            f"JOIN players p ON pn.player_id = p.id "
            f"WHERE pn.player_id IN ({placeholders}) "
            f"ORDER BY pn.published_at DESC LIMIT ?",
            (*nhl_ids, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT pn.headline, pn.content, pn.published_at, p.full_name "
            "FROM player_news pn "
            "LEFT JOIN players p ON pn.player_id = p.id "
            "ORDER BY pn.published_at DESC LIMIT ?",
            (limit,),
        ).fetchall()

    return [
        {
            "player_name": r["full_name"],
            "headline": r["headline"],
            "content": r["content"],
            "published_at": r["published_at"],
        }
        for r in rows
    ]


def get_schedule_analysis(
    conn: sqlite3.Connection,
    team_or_player: str,
    season: str,
    days_ahead: int = 14,
) -> dict | None:
    """Analyze upcoming schedule for a team or player.

    Args:
        conn: Database connection.
        team_or_player: 3-letter team abbreviation or player name.
        season: Season string.
        days_ahead: Number of days to look ahead.

    Returns:
        Dict with team, games list, game_count, back_to_backs.
        None if team/player not found.
    """
    # Detect team abbrev (3-letter uppercase) vs player name
    if len(team_or_player) <= 3 and team_or_player.isupper():
        team_abbrev = team_or_player
    else:
        resolved = resolve_player(conn, team_or_player)
        if resolved is None:
            return None
        team_abbrev = resolved["team_abbrev"]

    today = date.today().isoformat()
    end_date = (date.today() + timedelta(days=days_ahead)).isoformat()

    rows = conn.execute(
        "SELECT game_date, opponent, home_away, result "
        "FROM team_games "
        "WHERE team = ? AND season = ? AND game_date > ? AND game_date <= ? "
        "ORDER BY game_date",
        (team_abbrev, season, today, end_date),
    ).fetchall()

    games = [
        {
            "game_date": r["game_date"],
            "opponent": r["opponent"],
            "home_away": r["home_away"],
            "result": r["result"],
        }
        for r in rows
    ]

    # Detect back-to-backs
    back_to_backs = []
    dates = [g["game_date"] for g in games]
    for i in range(len(dates) - 1):
        d1 = date.fromisoformat(dates[i])
        d2 = date.fromisoformat(dates[i + 1])
        if (d2 - d1).days == 1:
            back_to_backs.append((dates[i], dates[i + 1]))

    return {
        "team": team_abbrev,
        "games": games,
        "game_count": len(games),
        "back_to_backs": back_to_backs,
    }


def get_league_standings(conn: sqlite3.Connection) -> list[dict]:
    """Get fantasy league standings with team names.

    Args:
        conn: Database connection.

    Returns:
        List of standing dicts ordered by rank.
    """
    rows = conn.execute(
        "SELECT fs.rank, ft.name AS team_name, ft.short_name, "
        "fs.wins, fs.losses, fs.ties, fs.points, "
        "fs.win_percentage, fs.games_back, fs.waiver_order, "
        "fs.claims_remaining, fs.points_for, fs.points_against, "
        "fs.streak, fs.games_played, fs.fantasy_points_per_game "
        "FROM fantasy_standings fs "
        "JOIN fantasy_teams ft ON fs.team_id = ft.id "
        "ORDER BY fs.rank"
    ).fetchall()

    return [dict(r) for r in rows]


def get_injuries(
    conn: sqlite3.Connection,
    scope: str = "my_roster",
    team_id: str | None = None,
) -> list[dict]:
    """Get injury report filtered by scope.

    Args:
        conn: Database connection.
        scope: 'my_roster', 'all', or 'team'.
        team_id: Required for 'my_roster' scope; for 'team' scope
                 pass an NHL team abbreviation.

    Returns:
        List of injury dicts with player info.
    """
    if scope == "all":
        rows = conn.execute(
            "SELECT p.full_name, p.team_abbrev, p.position, "
            "pi.injury_type, pi.status, pi.updated_at "
            "FROM player_injuries pi "
            "JOIN players p ON pi.player_id = p.id "
            "ORDER BY pi.updated_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    if scope == "team":
        rows = conn.execute(
            "SELECT p.full_name, p.team_abbrev, p.position, "
            "pi.injury_type, pi.status, pi.updated_at "
            "FROM player_injuries pi "
            "JOIN players p ON pi.player_id = p.id "
            "WHERE p.team_abbrev = ? "
            "ORDER BY pi.updated_at DESC",
            (team_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    # scope == "my_roster"
    if not team_id:
        return []
    slots = conn.execute(
        "SELECT player_name FROM fantasy_roster_slots WHERE team_id = ?",
        (team_id,),
    ).fetchall()
    results = []
    for s in slots:
        if not s["player_name"]:
            continue
        nhl_id = resolve_fantrax_to_nhl(conn, s["player_name"])
        if nhl_id is None:
            continue
        injury = _get_injury_status(conn, nhl_id)
        if injury:
            player = conn.execute(
                "SELECT full_name, team_abbrev, position FROM players WHERE id = ?",
                (nhl_id,),
            ).fetchone()
            results.append({
                "full_name": player["full_name"],
                "team_abbrev": player["team_abbrev"],
                "position": player["position"],
                **injury,
            })
    return results


# ---------------------------------------------------------------------------
# Advanced analysis
# ---------------------------------------------------------------------------


def get_trade_candidates(
    conn: sqlite3.Connection,
    team_id: str,
    season: str,
    limit: int = 15,
) -> list[dict]:
    """Find buy-low trade targets on other fantasy teams.

    Identifies rostered players (not on the user's team) whose last-7-game
    FP/G exceeds their season FP/G by more than 20%.  Includes the owner
    team name and standings rank to help gauge willingness to sell.

    Args:
        conn: Database connection.
        team_id: User's fantasy team ID (excluded from results).
        season: Season string.
        limit: Max results to return.

    Returns:
        List of dicts sorted by trend_pct descending.
    """
    rows = conn.execute(
        "SELECT frs.player_name, frs.position_short, frs.team_id, "
        "ft.name AS owner_team_name, fs.rank AS owner_rank "
        "FROM fantasy_roster_slots frs "
        "JOIN fantasy_teams ft ON frs.team_id = ft.id "
        "LEFT JOIN fantasy_standings fs ON frs.team_id = fs.team_id "
        "WHERE frs.team_id != ? "
        "AND frs.player_name IS NOT NULL AND frs.player_name != ''",
        (team_id,),
    ).fetchall()

    candidates = []
    for row in rows:
        resolved = resolve_player(conn, row["player_name"])
        if resolved is None:
            continue

        nhl_id = resolved["id"]
        goalie = resolved["position"] == "G"

        if goalie:
            stats = _get_goalie_season_stats(conn, nhl_id, season)
        else:
            stats = _get_skater_season_stats(conn, nhl_id, season)

        if not stats or stats.get("games_played", 0) < 7:
            continue

        season_fpg = stats.get("fpts_per_game", 0.0)
        if season_fpg <= 0:
            continue

        fpts_list = _get_recent_fpts_list(conn, nhl_id, season, is_goalie=goalie)
        if len(fpts_list) < 7:
            continue

        last_7_fpg = round(sum(fpts_list[:7]) / 7, 2)

        if last_7_fpg <= season_fpg * 1.20:
            continue

        trend_pct = round((last_7_fpg / season_fpg - 1) * 100, 1)

        recent_14 = fpts_list[:14]
        recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0
        if season_fpg > 0 and recent_14_fpg > season_fpg * 1.15:
            trend = "hot"
        elif season_fpg > 0 and recent_14_fpg < season_fpg * 0.85:
            trend = "cold"
        else:
            trend = "neutral"

        injury = _get_injury_status(conn, nhl_id)

        line_ctx = _get_line_context(conn, nhl_id)

        news_row = conn.execute(
            "SELECT headline FROM player_news WHERE player_id = ? "
            "AND published_at >= date('now', '-42 days') "
            "ORDER BY published_at DESC LIMIT 1",
            (nhl_id,),
        ).fetchone()

        candidates.append({
            "player_name": resolved["full_name"],
            "owner_team_name": row["owner_team_name"],
            "position": resolved["position"],
            "season_fpg": season_fpg,
            "recent_7_fpg": last_7_fpg,
            "recent_14_fpg": recent_14_fpg,
            "trend": trend,
            "trend_pct": trend_pct,
            "games_played": stats["games_played"],
            "owner_rank": row["owner_rank"],
            "toi_per_game": stats.get("toi_per_game", 0),
            "pp_toi": stats.get("pp_toi", 0),
            "signal": "trending_up",
            "injury": injury,
            "recent_news": news_row["headline"] if news_row else None,
            "line_info": {
                "ev_line": line_ctx["ev_line"] if line_ctx else None,
                "pp_unit": line_ctx["pp_unit"] if line_ctx else None,
            },
        })

    # --- High-TOI underperformer detection ---
    trending_names = {c["player_name"] for c in candidates}
    all_fpg_values = [c["season_fpg"] for c in candidates]

    # Compute median from ALL rostered skaters (not just trending candidates)
    all_season_fpg: list[float] = []
    high_toi_candidates = []
    for row in rows:
        resolved = resolve_player(conn, row["player_name"])
        if resolved is None or resolved["position"] == "G":
            continue
        if resolved["full_name"] in trending_names:
            continue
        stats = _get_skater_season_stats(conn, resolved["id"], season)
        if not stats or stats.get("games_played", 0) < 7:
            continue
        sfpg = stats.get("fpts_per_game", 0.0)
        if sfpg <= 0:
            continue
        all_season_fpg.append(sfpg)
        all_fpg_values.append(sfpg)

    if all_fpg_values:
        median_fpg = statistics.median(all_fpg_values)
    else:
        median_fpg = 0.0

    # Second pass: find high-TOI players below median FP/G
    for row in rows:
        resolved = resolve_player(conn, row["player_name"])
        if resolved is None or resolved["position"] == "G":
            continue
        if resolved["full_name"] in trending_names:
            continue
        stats = _get_skater_season_stats(conn, resolved["id"], season)
        if not stats or stats.get("games_played", 0) < 7:
            continue
        sfpg = stats.get("fpts_per_game", 0.0)
        if sfpg <= 0 or sfpg >= median_fpg:
            continue
        tpg = stats.get("toi_per_game", 0)
        pos = resolved["position"]
        # Forwards > 960s (16 min/game), Defensemen > 1200s (20 min/game)
        if pos in ("C", "L", "R") and tpg > 960:
            pass  # qualifies
        elif pos == "D" and tpg > 1200:
            pass  # qualifies
        else:
            continue

        all_fpts = _get_recent_fpts_list(conn, resolved["id"], season, False)
        last_7 = all_fpts[:7]
        last_7_fpg = round(sum(last_7) / len(last_7), 2) if last_7 else 0.0
        recent_14 = all_fpts[:14]
        recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0
        if sfpg > 0 and recent_14_fpg > sfpg * 1.15:
            trend = "hot"
        elif sfpg > 0 and recent_14_fpg < sfpg * 0.85:
            trend = "cold"
        else:
            trend = "neutral"

        injury = _get_injury_status(conn, resolved["id"])

        line_ctx = _get_line_context(conn, resolved["id"])

        news_row = conn.execute(
            "SELECT headline FROM player_news WHERE player_id = ? "
            "AND published_at >= date('now', '-42 days') "
            "ORDER BY published_at DESC LIMIT 1",
            (resolved["id"],),
        ).fetchone()

        high_toi_candidates.append({
            "player_name": resolved["full_name"],
            "owner_team_name": row["owner_team_name"],
            "position": pos,
            "season_fpg": sfpg,
            "recent_7_fpg": last_7_fpg,
            "recent_14_fpg": recent_14_fpg,
            "trend": trend,
            "trend_pct": 0.0,
            "games_played": stats["games_played"],
            "owner_rank": row["owner_rank"],
            "toi_per_game": tpg,
            "pp_toi": stats.get("pp_toi", 0),
            "signal": "high_toi_underperformer",
            "injury": injury,
            "recent_news": news_row["headline"] if news_row else None,
            "line_info": {
                "ev_line": line_ctx["ev_line"] if line_ctx else None,
                "pp_unit": line_ctx["pp_unit"] if line_ctx else None,
            },
        })

    candidates.sort(key=lambda c: c["trend_pct"], reverse=True)
    # Reserve up to 5 slots for high-TOI underperformers so they aren't
    # pushed out by trending-up candidates filling the limit.
    high_toi_candidates.sort(key=lambda c: c["toi_per_game"], reverse=True)
    reserved = min(5, len(high_toi_candidates))
    trending_limit = max(limit - reserved, 0)
    result = candidates[:trending_limit]
    result.extend(high_toi_candidates[:limit - len(result)])
    return result


def get_drop_candidates(
    conn: sqlite3.Connection,
    team_id: str,
    season: str,
) -> list[dict]:
    """Identify the weakest players on the user's roster.

    For each rostered player, calculates season FP/G and last-14-game FP/G,
    flags trend direction, and returns the bottom 5 sorted by recent
    performance ascending.

    Args:
        conn: Database connection.
        team_id: User's fantasy team ID.
        season: Season string.

    Returns:
        List of up to 5 dicts sorted by recent_14_fpg ascending.
    """
    slots = conn.execute(
        "SELECT player_name, position_short, status_id, salary "
        "FROM fantasy_roster_slots "
        "WHERE team_id = ? "
        "AND player_name IS NOT NULL AND player_name != ''",
        (team_id,),
    ).fetchall()

    players = []
    for slot in slots:
        resolved = resolve_player(conn, slot["player_name"])
        if resolved is None:
            continue

        nhl_id = resolved["id"]
        goalie = resolved["position"] == "G"

        if goalie:
            stats = _get_goalie_season_stats(conn, nhl_id, season)
        else:
            stats = _get_skater_season_stats(conn, nhl_id, season)

        season_fpg = stats.get("fpts_per_game", 0.0) if stats else 0.0
        gp = stats.get("games_played", 0) if stats else 0

        fpts_list = _get_recent_fpts_list(conn, nhl_id, season, is_goalie=goalie)
        recent_14 = fpts_list[:14]
        recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0

        # Trend: same 15% threshold as get_player_trends
        if season_fpg > 0 and recent_14_fpg > season_fpg * 1.15:
            trend = "hot"
        elif season_fpg > 0 and recent_14_fpg < season_fpg * 0.85:
            trend = "cold"
        else:
            trend = "neutral"

        injury = _get_injury_status(conn, nhl_id)

        recent_news_row = conn.execute(
            "SELECT headline FROM player_news WHERE player_id = ? "
            "AND published_at >= date('now', '-42 days') "
            "ORDER BY published_at DESC LIMIT 1",
            (nhl_id,),
        ).fetchone()

        line_ctx = _get_line_context(conn, nhl_id)
        players.append({
            "player_name": resolved["full_name"],
            "position": resolved["position"],
            "season_fpg": season_fpg,
            "recent_14_fpg": recent_14_fpg,
            "trend": trend,
            "injury": injury,
            "games_played": gp,
            "salary": slot["salary"],
            "roster_status": slot["status_id"],
            "recent_news": recent_news_row["headline"] if recent_news_row else None,
            "line_info": {
                "ev_line": line_ctx["ev_line"] if line_ctx else None,
                "pp_unit": line_ctx["pp_unit"] if line_ctx else None,
            },
        })

    players.sort(key=lambda p: p["recent_14_fpg"])
    return players[:5]


def get_team_roster(
    conn: sqlite3.Connection,
    team_name: str,
    season: str,
) -> dict | None:
    """Get any fantasy team's roster with NHL stats and FP.

    Looks up the team by name or short_name (fuzzy match), then returns
    the full roster with the same stats as get_my_roster().

    Args:
        conn: Database connection.
        team_name: Fantasy team name or short name to look up.
        season: Season string.

    Returns:
        Dict with team_info and roster list, or None if team not found.
    """
    # Try exact match first, then LIKE
    row = conn.execute(
        "SELECT id, name, short_name FROM fantasy_teams "
        "WHERE LOWER(name) = LOWER(?) OR LOWER(short_name) = LOWER(?)",
        (team_name, team_name),
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT id, name, short_name FROM fantasy_teams "
            "WHERE name LIKE ? OR short_name LIKE ?",
            (f"%{team_name}%", f"%{team_name}%"),
        ).fetchone()
    if not row:
        return None

    team_id = row["id"]

    # Get standing info
    standing = conn.execute(
        "SELECT rank, points_for, fantasy_points_per_game "
        "FROM fantasy_standings WHERE team_id = ?",
        (team_id,),
    ).fetchone()

    roster = get_my_roster(conn, team_id, season)

    return {
        "team_info": {
            "team_id": team_id,
            "team_name": row["name"],
            "short_name": row["short_name"],
            "rank": standing["rank"] if standing else None,
            "points_for": standing["points_for"] if standing else 0.0,
            "fpg": standing["fantasy_points_per_game"] if standing else 0.0,
        },
        "roster": roster,
    }


def suggest_trades(
    conn: sqlite3.Connection,
    my_team_id: str,
    opponent_team_name: str,
    season: str,
    limit: int = 5,
) -> dict | None:
    """Suggest player-for-player trades with another GM.

    Analyzes both rosters to find swaps that improve both teams —
    where each team trades away a surplus and fills a need.

    Logic:
    1. Identify each team's position-group FP/G averages.
    2. Find players on one team who are below that team's avg at
       their position but above the other team's avg (and vice versa).
    3. Rank swaps by combined upgrade value.

    Args:
        conn: Database connection.
        my_team_id: User's fantasy team ID.
        opponent_team_name: Opponent's team name or short name.
        season: Season string.
        limit: Max trade suggestions.

    Returns:
        Dict with my_team info, opponent info, and list of trade
        suggestions. None if opponent not found.
    """
    opponent_data = get_team_roster(conn, opponent_team_name, season)
    if opponent_data is None:
        return None

    my_roster = get_my_roster(conn, my_team_id, season)

    def roster_by_group(roster: list[dict]) -> dict[str, list[dict]]:
        groups: dict[str, list[dict]] = {"F": [], "D": [], "G": []}
        for p in roster:
            if p.get("games_played", 0) < 5:
                continue
            g = _position_group(p.get("position"))
            groups[g].append(p)
        return groups

    def avg_fpg(players: list[dict]) -> float:
        vals = [p["fpts_per_game"] for p in players if p["fpts_per_game"] > 0]
        return round(sum(vals) / len(vals), 2) if vals else 0.0

    my_groups = roster_by_group(my_roster)
    opp_groups = roster_by_group(opponent_data["roster"])

    my_avgs = {g: avg_fpg(ps) for g, ps in my_groups.items()}
    opp_avgs = {g: avg_fpg(ps) for g, ps in opp_groups.items()}

    # Helper to enrich a player with trend, injury, news, and line context
    def _enrich(player: dict) -> dict:
        resolved = resolve_player(conn, player["player_name"])
        if resolved is None:
            return {"trend": "neutral", "recent_14_fpg": 0.0,
                    "injury": None, "recent_news": None,
                    "ev_line": None, "pp_unit": None}
        nhl_id = resolved["id"]
        goalie = resolved["position"] == "G"
        fpts_list = _get_recent_fpts_list(conn, nhl_id, season, is_goalie=goalie)
        recent_14 = fpts_list[:14]
        recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0
        season_fpg = player.get("fpts_per_game", 0.0)
        if season_fpg > 0 and recent_14_fpg > season_fpg * 1.15:
            trend = "hot"
        elif season_fpg > 0 and recent_14_fpg < season_fpg * 0.85:
            trend = "cold"
        else:
            trend = "neutral"
        injury = _get_injury_status(conn, nhl_id)
        line_ctx = _get_line_context(conn, nhl_id)
        news_row = conn.execute(
            "SELECT headline FROM player_news WHERE player_id = ? "
            "AND published_at >= date('now', '-42 days') "
            "ORDER BY published_at DESC LIMIT 1",
            (nhl_id,),
        ).fetchone()
        return {
            "trend": trend,
            "recent_14_fpg": recent_14_fpg,
            "injury": injury,
            "recent_news": news_row["headline"] if news_row else None,
            "ev_line": line_ctx["ev_line"] if line_ctx else None,
            "pp_unit": line_ctx["pp_unit"] if line_ctx else None,
        }

    suggestions = []

    # For each position group, look for players I could send that
    # would help the opponent, and players they could send that help me
    for group in ("F", "D", "G"):
        # My players that are below my team avg but above opponent avg
        # (I'm upgrading by dropping them, opponent is upgrading by gaining)
        my_sendable = [
            p for p in my_groups[group]
            if p["fpts_per_game"] < my_avgs[group]
            and p["fpts_per_game"] > opp_avgs[group]
        ]
        # Opponent players below their avg but above my avg
        opp_sendable = [
            p for p in opp_groups[group]
            if p["fpts_per_game"] < opp_avgs[group]
            and p["fpts_per_game"] > my_avgs[group]
        ]

        # Build cross-group and same-group swaps
        for my_p in my_sendable:
            send_extra = _enrich(my_p)
            for opp_p in opp_sendable:
                my_upgrade = round(opp_p["fpts_per_game"] - my_p["fpts_per_game"], 2)
                opp_upgrade = round(my_p["fpts_per_game"] - opp_p["fpts_per_game"], 2)
                if my_upgrade > 0:
                    recv_extra = _enrich(opp_p)
                    suggestions.append({
                        "send_player": my_p["player_name"],
                        "send_position": my_p.get("position", ""),
                        "send_fpg": my_p["fpts_per_game"],
                        "send_recent_14_fpg": send_extra["recent_14_fpg"],
                        "send_trend": send_extra["trend"],
                        "send_injury": send_extra["injury"],
                        "send_news": send_extra["recent_news"],
                        "receive_player": opp_p["player_name"],
                        "receive_position": opp_p.get("position", ""),
                        "receive_fpg": opp_p["fpts_per_game"],
                        "receive_recent_14_fpg": recv_extra["recent_14_fpg"],
                        "receive_trend": recv_extra["trend"],
                        "receive_injury": recv_extra["injury"],
                        "receive_news": recv_extra["recent_news"],
                        "receive_ev_line": recv_extra["ev_line"],
                        "receive_pp_unit": recv_extra["pp_unit"],
                        "my_upgrade": my_upgrade,
                        "opp_upgrade": opp_upgrade,
                        "position_group": group,
                    })

    # Also look for cross-position-group swaps where one team is deep
    # at a position and weak at another
    for send_group in ("F", "D", "G"):
        for recv_group in ("F", "D", "G"):
            if send_group == recv_group:
                continue
            my_surplus = [
                p for p in my_groups[send_group]
                if p["fpts_per_game"] >= my_avgs[send_group]
                and p["fpts_per_game"] > opp_avgs[send_group]
            ]
            my_surplus.sort(key=lambda p: p["fpts_per_game"])
            my_surplus = my_surplus[:3]

            opp_surplus = [
                p for p in opp_groups[recv_group]
                if p["fpts_per_game"] >= opp_avgs[recv_group]
                and p["fpts_per_game"] > my_avgs[recv_group]
            ]
            opp_surplus.sort(key=lambda p: p["fpts_per_game"])
            opp_surplus = opp_surplus[:3]

            for my_p in my_surplus:
                send_extra = _enrich(my_p)
                for opp_p in opp_surplus:
                    fpg_diff = abs(my_p["fpts_per_game"] - opp_p["fpts_per_game"])
                    if fpg_diff > 0.3:
                        continue
                    recv_extra = _enrich(opp_p)
                    suggestions.append({
                        "send_player": my_p["player_name"],
                        "send_position": my_p.get("position", ""),
                        "send_fpg": my_p["fpts_per_game"],
                        "send_recent_14_fpg": send_extra["recent_14_fpg"],
                        "send_trend": send_extra["trend"],
                        "send_injury": send_extra["injury"],
                        "send_news": send_extra["recent_news"],
                        "receive_player": opp_p["player_name"],
                        "receive_position": opp_p.get("position", ""),
                        "receive_fpg": opp_p["fpts_per_game"],
                        "receive_recent_14_fpg": recv_extra["recent_14_fpg"],
                        "receive_trend": recv_extra["trend"],
                        "receive_injury": recv_extra["injury"],
                        "receive_news": recv_extra["recent_news"],
                        "receive_ev_line": recv_extra["ev_line"],
                        "receive_pp_unit": recv_extra["pp_unit"],
                        "my_upgrade": round(opp_p["fpts_per_game"] - my_avgs.get(recv_group, 0), 2),
                        "opp_upgrade": round(my_p["fpts_per_game"] - opp_avgs.get(send_group, 0), 2),
                        "position_group": f"{send_group}->{recv_group}",
                    })

    # Sort by my upgrade desc, deduplicate
    suggestions.sort(key=lambda s: s["my_upgrade"], reverse=True)
    seen = set()
    deduped = []
    for s in suggestions:
        key = (s["send_player"], s["receive_player"])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    return {
        "my_team": {
            "name": "My Team",
            "avg_fpg": my_avgs,
        },
        "opponent": {
            "name": opponent_data["team_info"]["team_name"],
            "short_name": opponent_data["team_info"]["short_name"],
            "rank": opponent_data["team_info"]["rank"],
            "avg_fpg": opp_avgs,
        },
        "suggestions": deduped[:limit],
    }


def get_pickup_recommendations(
    conn: sqlite3.Connection,
    team_id: str,
    season: str,
    limit: int = 10,
) -> list[dict]:
    """Recommend free agent pickups paired with drop candidates.

    Matches available free agents to the user's weakest rostered players
    by position group (F/D/G), showing the FP/G upgrade for each swap.

    Args:
        conn: Database connection.
        team_id: User's fantasy team ID.
        season: Season string.
        limit: Max recommendations to return.

    Returns:
        List of dicts sorted by fpg_upgrade descending.
    """
    drops = get_drop_candidates(conn, team_id, season)
    if not drops:
        return []

    free_agents = search_free_agents(
        conn, season, position="any", sort_by="fpts_per_game",
        min_games=10, limit=50,
    )
    if not free_agents:
        return []

    # Group by position group
    fa_by_pos: dict[str, list[dict]] = {}
    for fa in free_agents:
        pg = _position_group(fa.get("position"))
        fa_by_pos.setdefault(pg, []).append(fa)

    # Pre-build news lookup for free agents
    fa_news: dict[str, str] = {}
    for fa in free_agents:
        resolved_fa = resolve_player(conn, fa["player_name"])
        if resolved_fa:
            news_row = conn.execute(
                "SELECT headline FROM player_news WHERE player_id = ? "
                "AND published_at >= date('now', '-42 days') "
                "ORDER BY published_at DESC LIMIT 1",
                (resolved_fa["id"],),
            ).fetchone()
            if news_row:
                fa_news[fa["player_name"]] = news_row["headline"]

    used_pickups: set[str] = set()
    used_drops: set[str] = set()
    recommendations = []

    # Build all possible (drop, pickup) pairs sorted by upgrade.
    # Use recent 14-game FP/G for both sides so the comparison reflects
    # current production, not season-long averages that mask role changes.
    pairs = []
    for drop in drops:
        drop_pg = _position_group(drop["position"])
        drop_recent_fpg = drop["recent_14_fpg"]
        drop_season_fpg = drop["season_fpg"]
        for fa in fa_by_pos.get(drop_pg, []):
            fa_recent_fpg = fa.get("recent_14_fpg", 0.0)
            fa_season_fpg = fa.get("fpts_per_game", 0.0)
            upgrade = round(fa_recent_fpg - drop_recent_fpg, 2)
            if upgrade <= 0:
                continue

            # Determine reason
            fa_injury = fa.get("injury")
            fa_trend = fa.get("trend", "neutral")
            if fa_injury:
                reason = "IR stash candidate"
            elif fa_trend == "hot":
                reason = f"Trending up, +{upgrade:.2f} recent FP/G"
            elif drop["trend"] == "cold":
                reason = f"Cold streak, +{upgrade:.2f} recent FP/G"
            else:
                reason = f"+{upgrade:.2f} recent FP/G upgrade"

            # Add line context for FA
            fa_pp = fa.get("pp_unit")
            fa_ev = fa.get("ev_line")
            if fa_pp == 1:
                reason += " | PP1"
            elif fa_pp == 2:
                reason += " | PP2"
            if fa_ev and fa_ev <= 2:
                reason += f" | Line {fa_ev}"

            recent_news = fa_news.get(fa["player_name"])
            if recent_news:
                reason += f" | News: {recent_news}"

            pairs.append({
                "pickup_name": fa["player_name"],
                "pickup_position": fa.get("position", ""),
                "pickup_season_fpg": round(fa_season_fpg, 2),
                "pickup_recent_fpg": round(fa_recent_fpg, 2),
                "pickup_trend": fa_trend,
                "pickup_team": fa.get("team", ""),
                "drop_name": drop["player_name"],
                "drop_position": drop["position"],
                "drop_season_fpg": drop_season_fpg,
                "drop_recent_fpg": drop_recent_fpg,
                "fpg_upgrade": upgrade,
                "reason": reason,
            })

    # Greedy assignment: best upgrade first, no reuse
    pairs.sort(key=lambda p: p["fpg_upgrade"], reverse=True)
    for pair in pairs:
        if pair["pickup_name"] in used_pickups:
            continue
        if pair["drop_name"] in used_drops:
            continue
        used_pickups.add(pair["pickup_name"])
        used_drops.add(pair["drop_name"])
        recommendations.append(pair)

    return recommendations[:limit]
