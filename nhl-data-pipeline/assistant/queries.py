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
from datetime import date, timedelta

from assistant.scoring import calc_skater_fpts, calc_goalie_fpts
from assistant.player_match import (
    resolve_player,
    resolve_fantrax_to_nhl,
    get_rostered_nhl_ids,
)
from config.fantasy_constants import (
    FORWARD_POSITIONS as _FORWARD_POSITIONS,
    GP_LIMITS,
    SALARY_CAP,
    IR_SLOT_STATUS,
    RECENT_GAMES_WINDOW,
    PERIPHERAL_STATS_WINDOW,
    HOT_THRESHOLD_MULTIPLIER,
    COLD_THRESHOLD_MULTIPLIER,
    TREND_HOT_THRESHOLD_7_DAY,
    TREND_COLD_THRESHOLD_7_DAY,
    GOALIE_MAX_GAP_GAMES,
    TRADE_TARGET_ELIGIBLE_EV_LINES,
    TRADE_TARGET_ELIGIBLE_PP_UNITS,
    FA_MAX_IR_RESULTS,
    DROP_CANDIDATES_COUNT,
    DROP_FPG_CEILING,
    DEFAULT_MIN_GAMES,
    VERDICT_STRONG_THRESHOLD,
    VERDICT_MARGINAL_THRESHOLD,
    NEWS_RECENCY_DAYS,
    IR_SEASON_CUTOFF_DATE,
    IR_MAX_DAYS_OUT,
)

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


def _build_team_remaining(conn: sqlite3.Connection) -> dict[str, int]:
    """Return remaining games per NHL team from the schedule.

    Falls back to 0 for any team not in the schedule.
    """
    rows = conn.execute(
        "SELECT team, COUNT(*) as cnt FROM team_games"
        " WHERE game_date > date('now') GROUP BY team"
    ).fetchall()
    return {r["team"]: r["cnt"] for r in rows}


def _gp_capacity(
    roster: list[dict],
    pos_group: str,
    team_remaining: dict[str, int],
) -> int:
    """Sum remaining team games for all roster players in *pos_group*."""
    total = 0
    for p in roster:
        if _position_group(p.get("position")) == pos_group:
            total += team_remaining.get(p.get("team", ""), 0)
    return total


def _effective_fpg(player: dict) -> float:
    """Get the best available FP/G for a player (L14, fallback to season)."""
    r14 = player.get("recent_14_fpg", 0.0)
    if r14 > 0:
        return r14
    return player.get("fpts_per_game", 0.0)


def _calc_trend(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    is_goalie: bool,
    season_fpg: float,
) -> tuple[float, str]:
    """Compute L14 FP/G and hot/cold/neutral trend label."""
    fpts_list = _get_recent_fpts_list(conn, player_id, season, is_goalie=is_goalie)
    recent_14 = fpts_list[:RECENT_GAMES_WINDOW]
    recent_14_fpg = round(sum(recent_14) / len(recent_14), 2) if recent_14 else 0.0
    if season_fpg > 0 and recent_14_fpg > season_fpg * HOT_THRESHOLD_MULTIPLIER:
        trend = "hot"
    elif season_fpg > 0 and recent_14_fpg < season_fpg * COLD_THRESHOLD_MULTIPLIER:
        trend = "cold"
    else:
        trend = "neutral"
    return recent_14_fpg, trend


def _get_player_news(
    conn: sqlite3.Connection,
    player_id: int,
    limit: int = 5,
    recency_days: int | None = NEWS_RECENCY_DAYS,
    include_content: bool = True,
) -> list[dict]:
    """Fetch recent news for a player.

    Returns list of dicts with keys: headline, date, and optionally content.
    """
    cols = "headline, content, published_at" if include_content else "headline, published_at"
    recency = (
        f"AND published_at >= date('now', '-{recency_days} days') "
        if recency_days is not None
        else ""
    )
    rows = conn.execute(
        f"SELECT {cols} FROM player_news WHERE player_id = ? "
        f"{recency}"
        "ORDER BY published_at DESC LIMIT ?",
        (player_id, limit),
    ).fetchall()
    result = []
    for r in rows:
        item: dict = {"headline": r["headline"], "date": r["published_at"]}
        if include_content:
            item["content"] = r["content"]
        result.append(item)
    return result


def _get_season_stats(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    is_goalie: bool,
) -> dict | None:
    """Dispatch to goalie or skater season stats."""
    if is_goalie:
        return _get_goalie_season_stats(conn, player_id, season)
    return _get_skater_season_stats(conn, player_id, season)


def _find_drop_candidates(
    roster: list[dict],
    fa_position_group: str,
    remaining_gp: dict[str, int],
    team_remaining: dict[str, int],
    fa_team: str = "",
    count: int = DROP_CANDIDATES_COUNT,
) -> list[dict]:
    """Find the best drop candidates for a given FA pickup.

    Walks the roster sorted by effective FP/G ascending (worst first) and
    returns up to *count* players whose removal keeps every position group
    with enough GP capacity to cover remaining fantasy GP.

    Args:
        roster: Full roster list from get_my_roster().
        fa_position_group: Position group of the FA being picked up ('F','D','G').
        remaining_gp: Remaining fantasy GP per position {'F': 232, 'D': 105, 'G': 13}.
        team_remaining: NHL team → remaining games lookup.
        fa_team: NHL team abbreviation of the FA (for capacity calculation).
        count: Number of drop candidates to return.

    Returns:
        List of drop-candidate dicts (worst-first), each with:
        player_name, position, fpts_per_game, recent_14_fpg.
    """
    # Pre-compute current GP capacity per position group
    capacity: dict[str, int] = {}
    for grp in ("F", "D", "G"):
        capacity[grp] = _gp_capacity(roster, grp, team_remaining)

    sorted_roster = sorted(roster, key=_effective_fpg)

    candidates: list[dict] = []
    for player in sorted_roster:
        if len(candidates) >= count:
            break

        # --- Filters ---
        # 1. Exclude IR-slotted players
        if player.get("roster_status") == IR_SLOT_STATUS:
            continue

        # 2. Exclude players below minimum games
        if player.get("games_played", 0) < DEFAULT_MIN_GAMES:
            continue

        # 3. FP/G ceiling by position
        eff = _effective_fpg(player)
        drop_group = _position_group(player.get("position"))
        ceiling = DROP_FPG_CEILING.get(drop_group)
        if ceiling is not None and eff >= ceiling:
            continue

        # 4. GP capacity viability: simulate swap
        drop_team_games = team_remaining.get(player.get("team", ""), 0)
        fa_team_games = team_remaining.get(fa_team, 0)
        for grp in ("F", "D", "G"):
            new_cap = capacity[grp]
            if grp == drop_group:
                new_cap -= drop_team_games
            if grp == fa_position_group:
                new_cap += fa_team_games
            if new_cap < remaining_gp.get(grp, 0):
                break
        else:
            candidates.append({
                "player_name": player["player_name"],
                "position": player.get("position", ""),
                "fpts_per_game": player.get("fpts_per_game", 0.0),
                "recent_14_fpg": player.get("recent_14_fpg", 0.0),
                "trend": player.get("trend", "neutral"),
                "nhl_id": player.get("nhl_id"),
                "team": player.get("team", ""),
            })
    return candidates


def _claim_verdict(net_fpg: float) -> str:
    """Classify a net FP/G into a human-readable verdict."""
    if net_fpg >= VERDICT_STRONG_THRESHOLD:
        return "strong"
    if net_fpg >= VERDICT_MARGINAL_THRESHOLD:
        return "marginal"
    return "not worth a claim"


def _is_season_ending_ir(injury: dict | None) -> bool:
    """Return True if the player is on IR with a season-ending timeline.

    Filters on two independent criteria (either disqualifies):
    1. Expected return after IR_SEASON_CUTOFF_DATE (hard date)
    2. Expected return more than IR_MAX_DAYS_OUT days from today
    """
    if not injury or injury.get("status") != "IR":
        return False
    ret = injury.get("expected_return")
    if not ret:
        return False  # no return date data → let the AI decide
    ret_date = date.fromisoformat(ret)
    cutoff = date.fromisoformat(IR_SEASON_CUTOFF_DATE)
    if ret_date > cutoff:
        return True
    if (ret_date - date.today()).days > IR_MAX_DAYS_OUT:
        return True
    return False


def _get_fantasy_gp(
    conn: sqlite3.Connection,
    team_id: str,
    roster: list[dict] | None = None,
) -> dict[str, dict]:
    """Get real fantasy GP per position from Fantrax data.

    Falls back to summing NHL games_played if the
    fantasy_gp_per_position table has no data for this team.
    """
    rows = conn.execute(
        "SELECT position, gp_used, gp_limit, gp_remaining "
        "FROM fantasy_gp_per_position WHERE team_id = ?",
        (team_id,),
    ).fetchall()

    if rows:
        gp_limits: dict[str, dict] = {}
        for row in rows:
            pos = row["position"]
            gp_limits[pos] = {
                "used": row["gp_used"],
                "limit": row["gp_limit"],
                "remaining": row["gp_remaining"],
                "pct": round(row["gp_used"] / row["gp_limit"] * 100, 1)
                if row["gp_limit"] > 0 else 0.0,
            }
        return gp_limits

    # Fallback: sum NHL GP (inaccurate but better than nothing)
    gp_used: dict[str, int] = {"F": 0, "D": 0, "G": 0}
    for p in (roster or []):
        group = _position_group(p.get("position"))
        gp_used[group] += p.get("games_played", 0)

    gp_limits = {}
    for g in ("F", "D", "G"):
        limit = GP_LIMITS[g]
        used = gp_used[g]
        gp_limits[g] = {
            "used": used,
            "limit": limit,
            "remaining": limit - used,
            "pct": round(used / limit * 100, 1) if limit > 0 else 0.0,
        }
    return gp_limits


def _get_recent_pp_toi(
    conn: sqlite3.Connection,
    player_id: int,
    season: str,
    n: int = PERIPHERAL_STATS_WINDOW,
) -> list[int]:
    """Return per-game PP TOI (seconds) for the last *n* games, newest first."""
    rows = conn.execute(
        "SELECT pp_toi FROM skater_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
        "ORDER BY game_date DESC LIMIT ?",
        (player_id, season, n),
    ).fetchall()
    return [r["pp_toi"] for r in rows]


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


def _calc_goalie_start_rates(
    conn: sqlite3.Connection, player_id: int, season: str
) -> tuple[float, float]:
    """Calculate goalie start rates using dressed games as denominator.

    A goalie is "dressed" for a team game if they played OR were healthy
    backup (i.e. gaps between appearances are <= MAX_GAP team games).
    Gaps longer than MAX_GAP are treated as injury absences.

    Returns:
        (season_start_rate, l14_start_rate) as percentages.
    """
    MAX_GAP = GOALIE_MAX_GAP_GAMES

    # Get goalie's team
    player = conn.execute(
        "SELECT team_abbrev FROM players WHERE id = ?", (player_id,),
    ).fetchone()
    if not player:
        return 0.0, 0.0

    team = player["team_abbrev"]

    # Get goalie's per-game entries with decision flag
    goalie_rows = conn.execute(
        "SELECT game_date, "
        "  CASE WHEN wins + losses + ot_losses > 0 THEN 1 ELSE 0 END "
        "    AS has_decision "
        "FROM goalie_stats "
        "WHERE player_id = ? AND season = ? AND is_season_total = 0 "
        "ORDER BY game_date",
        (player_id, season),
    ).fetchall()
    if not goalie_rows:
        return 0.0, 0.0

    goalie_dates = {r["game_date"] for r in goalie_rows}
    decision_dates = {r["game_date"] for r in goalie_rows if r["has_decision"]}
    first_game = goalie_rows[0]["game_date"]
    last_game = goalie_rows[-1]["game_date"]

    # Get past team game dates between goalie's first and last appearance
    today = date.today().isoformat()
    upper = min(last_game, today)
    team_dates = [
        r["game_date"]
        for r in conn.execute(
            "SELECT game_date FROM team_games "
            "WHERE team = ? AND season = ? "
            "  AND game_date >= ? AND game_date <= ? "
            "ORDER BY game_date",
            (team, season, first_game, upper),
        ).fetchall()
    ]
    if not team_dates:
        return 0.0, 0.0

    # Build list of dressed games: goalie played + backup gaps <= MAX_GAP
    dressed: list[str] = []
    gap: list[str] = []
    for td in team_dates:
        if td in goalie_dates:
            # Goalie played — flush any accumulated backup gap
            if gap and len(gap) <= MAX_GAP:
                dressed.extend(gap)
            gap = []
            dressed.append(td)
        else:
            gap.append(td)
    # Trailing gap (after last goalie game within range) is not counted

    if not dressed:
        return 0.0, 0.0

    # Season start rate
    total_decisions = sum(1 for d in dressed if d in decision_dates)
    sr = round(total_decisions / len(dressed) * 100, 1)

    # L14: last 14 dressed games for this goalie
    last_14 = dressed[-RECENT_GAMES_WINDOW:]
    l14_decisions = sum(1 for d in last_14 if d in decision_dates)
    sr14 = round(l14_decisions / len(last_14) * 100, 1) if last_14 else 0.0

    return sr, sr14


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

    # Start rate: decisions / dressed games (games as starter or backup)
    start_rate, start_rate_l14 = _calc_goalie_start_rates(
        conn, player_id, season
    )

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
        "start_rate": start_rate,
        "start_rate_l14": start_rate_l14,
    }


def _get_injury_status(conn: sqlite3.Connection, player_id: int) -> dict | None:
    """Get current injury info for a player, or None if healthy.

    Prefers the moneypuck source (has expected_return) over rotowire.
    """
    row = conn.execute(
        "SELECT injury_type, status, updated_at, expected_return "
        "FROM player_injuries WHERE player_id = ? "
        "ORDER BY expected_return IS NOT NULL DESC, source = 'moneypuck' DESC "
        "LIMIT 1",
        (player_id,),
    ).fetchone()
    if row is None:
        return None
    result: dict = {
        "injury_type": row["injury_type"],
        "status": row["status"],
        "updated_at": row["updated_at"],
    }
    if row["expected_return"]:
        result["expected_return"] = row["expected_return"]
    return result


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
        "deployed_position": row["position"],
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
                "recent_14_fpg": 0.0,
                "trend": "neutral",
                "ev_line": None,
                "pp_unit": None,
            })
            continue

        nhl_id = resolved["id"]
        position = resolved["position"]
        goalie = position == "G"

        stats = _get_season_stats(conn, nhl_id, season, goalie)

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
                "start_rate": stats.get("start_rate", 0.0),
                "start_rate_l14": stats.get("start_rate_l14", 0.0),
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

        # Recent trend and line context
        r14_fpg, trend = _calc_trend(conn, nhl_id, season, goalie, entry["fpts_per_game"])
        entry["recent_14_fpg"] = r14_fpg
        entry["trend"] = trend

        line_ctx = _get_line_context(conn, nhl_id)
        entry["ev_line"] = line_ctx["ev_line"] if line_ctx else None
        entry["pp_unit"] = line_ctx["pp_unit"] if line_ctx else None

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
        bottom_performers, injured_players, gp_limits.
    """
    roster = get_my_roster(conn, team_id, season)

    counts: dict[str, int] = {"F": 0, "D": 0, "G": 0}
    fpts_sums: dict[str, float] = {"F": 0.0, "D": 0.0, "G": 0.0}
    total_salary = 0.0
    injured = []

    for p in roster:
        group = _position_group(p.get("position"))
        counts[group] = counts.get(group, 0) + 1
        fpts_sums[group] = fpts_sums.get(group, 0.0) + p["fpts_per_game"]
        total_salary += p.get("salary", 0) or 0
        if p.get("injury"):
            injured.append(p)

    avg_fpts = {}
    for g in ("F", "D", "G"):
        avg_fpts[g] = round(fpts_sums[g] / counts[g], 2) if counts[g] > 0 else 0.0

    # GP limits per position group — use real Fantrax data
    gp_limits = _get_fantasy_gp(conn, team_id, roster)

    # Bottom 3 by FP/G (players with at least 1 game)
    with_games = [p for p in roster if p["games_played"] > 0]
    with_games.sort(key=lambda p: p["fpts_per_game"])
    bottom = with_games[:3]

    return {
        "position_counts": counts,
        "avg_fpts_by_position": avg_fpts,
        "bottom_performers": bottom,
        "injured_players": injured,
        "gp_limits": gp_limits,
        "salary": {
            "total": total_salary,
            "cap": SALARY_CAP,
            "space": SALARY_CAP - total_salary,
        },
    }


def search_free_agents(
    conn: sqlite3.Connection,
    season: str,
    position: str = "any",
    sort_by: str = "fpts_per_game",
    min_games: int = 10,
    limit: int = 20,
    team_id: str | None = None,
) -> list[dict]:
    """Find the best available free agents (not on any fantasy roster).

    Args:
        conn: Database connection.
        season: Season string.
        position: Filter by position ('any', 'F', 'D', 'G', or specific like 'C').
        sort_by: Sort key ('fpts_per_game' or 'fantasy_points').
        min_games: Minimum games played.
        limit: Max results.
        team_id: If provided, enrich results with drop candidates and net value.

    Returns:
        List of player dicts with stats and FP/G.
    """
    rostered = get_rostered_nhl_ids(conn)

    # Build salary lookup from fantrax_players table (name → salary)
    salary_lookup: dict[str, int] = {}
    try:
        for row in conn.execute(
            "SELECT player_name, salary FROM fantrax_players WHERE salary > 0"
        ).fetchall():
            salary_lookup[row["player_name"].lower()] = row["salary"]
    except Exception:
        pass  # Table may not exist yet

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
        stats = _get_season_stats(conn, pid, season, goalie)

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
            "salary": salary_lookup.get(p["full_name"].lower(), 0),
        }

        if goalie:
            entry.update({
                "wins": stats.get("wins", 0),
                "losses": stats.get("losses", 0),
                "shutouts": stats.get("shutouts", 0),
                "gaa": stats.get("gaa", 0.0),
                "sv_pct": stats.get("sv_pct", 0.0),
                "peripheral_fpg": 0.0,
                "start_rate": stats.get("start_rate", 0.0),
                "start_rate_l14": stats.get("start_rate_l14", 0.0),
            })
        else:
            hits = stats.get("hits", 0)
            blocks = stats.get("blocks", 0)
            peripheral_fpg = round((hits + blocks) * 0.1 / gp, 2) if gp > 0 else 0.0
            entry.update({
                "goals": stats.get("goals", 0),
                "assists": stats.get("assists", 0),
                "points": stats.get("points", 0),
                "hits": hits,
                "blocks": blocks,
                "toi_per_game": stats.get("toi_per_game", 0),
                "pp_toi": stats.get("pp_toi", 0),
                "peripheral_fpg": peripheral_fpg,
            })

        # Hot/cold trend based on last 14 games vs season average
        r14_fpg, trend = _calc_trend(conn, pid, season, goalie, entry["fpts_per_game"])
        entry["recent_14_fpg"] = r14_fpg
        entry["trend"] = trend

        entry["injury"] = _get_injury_status(conn, pid)

        line_ctx = _get_line_context(conn, pid)
        entry["ev_line"] = line_ctx["ev_line"] if line_ctx else None
        entry["pp_unit"] = line_ctx["pp_unit"] if line_ctx else None

        # Per-game PP TOI for recent games (AI can reason about actual usage)
        if not goalie:
            pp_toi_recent = _get_recent_pp_toi(conn, pid, season)
            entry["pp_toi_recent"] = pp_toi_recent
            entry["pp_toi_per_game"] = (
                round(sum(pp_toi_recent) / len(pp_toi_recent))
                if pp_toi_recent else 0
            )
        else:
            entry["pp_toi_recent"] = []
            entry["pp_toi_per_game"] = 0

        entry["recent_news"] = _get_player_news(conn, pid)

        results.append(entry)

    # When searching all positions, cap goalies to prevent them from
    # dominating results due to inflated FP/G from 2pts/win scoring.
    if pos_filter is None:
        skaters = [r for r in results if r["position"] != "G"]
        goalies = [r for r in results if r["position"] == "G"]
        skaters.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        goalies.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        goalie_cap = min(3, limit)
        skater_cap = limit - goalie_cap
        merged = skaters[:skater_cap] + goalies[:goalie_cap]
        merged.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        final = merged[:limit]
    else:
        results.sort(key=lambda x: x.get(sort_by, 0), reverse=True)
        final = results[:limit]

    # --- Filter: eligible line deployment (skip AHL/scratched skaters) ---
    final = [
        fa for fa in final
        if fa.get("position") == "G"
        or (fa.get("ev_line") is not None and fa["ev_line"] in TRADE_TARGET_ELIGIBLE_EV_LINES)
        or (fa.get("pp_unit") is not None and fa["pp_unit"] in TRADE_TARGET_ELIGIBLE_PP_UNITS)
    ]

    # --- Filter season-ending IR and cap remaining IR players ---
    ir_count = 0
    capped: list[dict] = []
    for fa in final:
        inj = fa.get("injury")
        if _is_season_ending_ir(inj):
            continue
        if isinstance(inj, dict) and inj.get("status") == "IR":
            ir_count += 1
            if ir_count > FA_MAX_IR_RESULTS:
                continue
        capped.append(fa)
    final = capped

    # --- Enrich with drop candidates and net value ---
    if team_id is not None:
        roster = get_my_roster(conn, team_id, season)
        team_remaining = _build_team_remaining(conn)
        gp_data = _get_fantasy_gp(conn, team_id, roster)
        remaining_gp = {
            pos: gp_data.get(pos, {}).get("remaining", 0)
            for pos in ("F", "D", "G")
        }
        for fa in final:
            fa_pos_group = _position_group(fa["position"])
            fa_eff = _effective_fpg(fa)
            candidates = _find_drop_candidates(
                roster, fa_pos_group, remaining_gp, team_remaining,
                fa_team=fa.get("team", ""),
            )
            drop_list = []
            for c in candidates:
                c_eff = _effective_fpg(c)
                net = round(fa_eff - c_eff, 2)
                drop_entry: dict = {
                    "player_name": c["player_name"],
                    "position": c["position"],
                    "fpts_per_game": round(c["fpts_per_game"], 2),
                    "recent_14_fpg": round(c["recent_14_fpg"], 2),
                    "net_fpg": net,
                }
                # Attach recent news for the drop candidate
                nhl_id = c.get("nhl_id")
                if nhl_id:
                    drop_news = _get_player_news(conn, nhl_id, limit=3, include_content=False)
                    if drop_news:
                        drop_entry["news"] = [
                            {"date": n["date"][:10], "hl": n["headline"]}
                            for n in drop_news
                        ]
                drop_entry["verdict"] = _claim_verdict(net)
                drop_list.append(drop_entry)
            fa["drop_candidates"] = drop_list
            if not drop_list:
                fa["verdict"] = "no room"

    return final


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
    stats = _get_season_stats(conn, nhl_id, season, goalie)

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

    # News (no recency filter for player card — show all recent news)
    news = _get_player_news(conn, nhl_id, recency_days=None)

    line_context = _get_line_context(conn, nhl_id)

    # Salary lookup from fantrax_players
    salary = 0
    try:
        sal_row = conn.execute(
            "SELECT salary FROM fantrax_players WHERE LOWER(player_name) = LOWER(?)",
            (resolved["full_name"],),
        ).fetchone()
        if sal_row:
            salary = sal_row["salary"]
    except Exception:
        pass

    return {
        "player": resolved,
        "is_goalie": goalie,
        "season_stats": stats,
        "game_log": game_log,
        "injury": injury,
        "news": news,
        "line_context": line_context,
        "salary": salary,
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
    Flags hot (>25% above season avg) or cold (>25% below).

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
    last_14_avg = avg(all_fpts[:RECENT_GAMES_WINDOW])
    last_30_avg = avg(all_fpts[:30])

    if season_avg > 0 and last_7_avg > season_avg * TREND_HOT_THRESHOLD_7_DAY:
        trend = "hot"
    elif season_avg > 0 and last_7_avg < season_avg * TREND_COLD_THRESHOLD_7_DAY:
        trend = "cold"
    else:
        trend = "neutral"

    return {
        "player": resolved,
        "windows": {
            "last_7": {"fpts_per_game": last_7_avg, "games": min(7, len(all_fpts))},
            "last_14": {"fpts_per_game": last_14_avg, "games": min(RECENT_GAMES_WINDOW, len(all_fpts))},
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

    # Enrich games with opponent team stats
    opp_abbrevs = list({g["opponent"] for g in games if g["opponent"]})
    if opp_abbrevs:
        placeholders = ",".join("?" for _ in opp_abbrevs)
        opp_rows = conn.execute(
            f"SELECT team, wins, losses, ot_losses, points, "
            f"goals_for_per_game, goals_against_per_game, l10_record, l14_record, streak "
            f"FROM nhl_team_stats WHERE season = ? AND team IN ({placeholders})",
            (season, *opp_abbrevs),
        ).fetchall()
        opp_stats = {
            r["team"]: {
                "rec": f"{r['wins']}-{r['losses']}-{r['ot_losses']}",
                "pts": r["points"],
                "gf_g": round(r["goals_for_per_game"], 2),
                "ga_g": round(r["goals_against_per_game"], 2),
                "l10": r["l10_record"],
                "l14": r["l14_record"],
                "streak": r["streak"],
            }
            for r in opp_rows
        }
        for g in games:
            if g["opponent"] in opp_stats:
                g["opp"] = opp_stats[g["opponent"]]

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
        "SELECT fs.team_id, fs.rank, ft.name AS team_name, ft.short_name, "
        "fs.wins, fs.losses, fs.ties, fs.points, "
        "fs.win_percentage, fs.games_back, fs.waiver_order, "
        "fs.claims_remaining, fs.points_for, fs.points_against, "
        "fs.streak, fs.games_played, fs.fantasy_points_per_game "
        "FROM fantasy_standings fs "
        "JOIN fantasy_teams ft ON fs.team_id = ft.id "
        "ORDER BY fs.rank"
    ).fetchall()

    # Build GP-per-position lookup for all teams
    gp_rows = conn.execute(
        "SELECT team_id, position, gp_used, gp_limit, gp_remaining "
        "FROM fantasy_gp_per_position"
    ).fetchall()
    gp_by_team: dict[str, dict[str, dict]] = {}
    for gr in gp_rows:
        tid = gr["team_id"]
        gp_by_team.setdefault(tid, {})[gr["position"]] = {
            "used": gr["gp_used"],
            "limit": gr["gp_limit"],
            "remaining": gr["gp_remaining"],
        }

    results = []
    for r in rows:
        entry = dict(r)
        team_id = entry.pop("team_id")
        entry["gp_remaining"] = gp_by_team.get(team_id, {})
        results.append(entry)

    return results


def get_nhl_standings(
    conn: sqlite3.Connection,
    season: str,
    team: str | None = None,
) -> list[dict]:
    """Get NHL team standings and performance stats.

    Args:
        conn: Database connection.
        season: Season string.
        team: Optional 3-letter abbreviation to filter to one team.

    Returns:
        List of team stat dicts ordered by points descending.
    """
    if team:
        rows = conn.execute(
            "SELECT * FROM nhl_team_stats WHERE season = ? AND team = ?",
            (season, team.upper()),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM nhl_team_stats WHERE season = ? ORDER BY points DESC",
            (season,),
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

