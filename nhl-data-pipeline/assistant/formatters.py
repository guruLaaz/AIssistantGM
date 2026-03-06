"""Compact JSON formatters for query results.

Each function takes the output of the corresponding queries.py function
and returns a compact JSON string.  This minimizes token usage while
keeping every stat explicitly labeled so the AI never has to guess.
"""

from __future__ import annotations

import json


def _json(obj) -> str:
    """Compact JSON with no extra whitespace."""
    return json.dumps(obj, separators=(",", ":"))


def _fpos(position: str | None) -> str:
    """Map NHL position (C/L/R/D/G) to fantasy position (F/D/G)."""
    if not position:
        return "F"
    if position in ("G", "D"):
        return position
    return "F"


def _deploy_tag(ev_line: int | None, pp_unit: int | None, position: str = "") -> str:
    """Compact deployment string like 'L1/PP2', 'D1/PP1', or '-'."""
    parts: list[str] = []
    if ev_line is not None:
        prefix = "D" if position == "D" else "L"
        parts.append(f"{prefix}{ev_line}")
    if pp_unit is not None:
        parts.append(f"PP{pp_unit}")
    return "/".join(parts) if parts else "-"


def _line_tag(line_info: dict | None, position: str = "") -> str:
    """Build a compact line descriptor like 'L1/PP1' from line info dict."""
    if not line_info:
        return ""
    parts = []
    ev = line_info.get("ev_line")
    if ev:
        prefix = "D" if position == "D" else "L"
        parts.append(f"{prefix}{ev}")
    pp = line_info.get("pp_unit")
    if pp:
        parts.append(f"PP{pp}")
    return "/".join(parts)


def _toi_str(seconds: int | None) -> str:
    """Format TOI seconds as M:SS."""
    if not seconds:
        return "0:00"
    return f"{seconds // 60}:{seconds % 60:02d}"


def _inj_str(injury: dict | None) -> str | None:
    if not injury:
        return None
    return injury.get("status", "INJ")


# ---------------------------------------------------------------------------
# Roster
# ---------------------------------------------------------------------------


def format_roster(data: list[dict]) -> str:
    if not data:
        return "No players on roster."
    rows = []
    for p in data:
        raw_pos = p.get("position", "")
        inj = p.get("injury")
        row: dict = {
            "name": p.get("player_name") or "",
            "pos": _fpos(raw_pos),
            "gp": p.get("games_played", 0),
            "fp": round(p.get("fantasy_points", 0.0), 1),
            "fpg": round(p.get("fpts_per_game", 0.0), 2),
            "r14": round(p.get("recent_14_fpg", 0.0), 2),
            "trend": p.get("trend", "neutral"),
            "line": _line_tag({"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}, raw_pos) or "-",
        }
        sal = p.get("salary", 0) or 0
        if sal > 0:
            row["sal"] = round(sal / 1e6, 1)
        if raw_pos == "G":
            row["w"] = p.get("wins", 0)
            row["l"] = p.get("losses", 0)
            row["so"] = p.get("shutouts", 0)
            row["sr"] = p.get("start_rate", 0.0)
            row["sr14"] = p.get("start_rate_l14", 0.0)
        else:
            row["g"] = p.get("goals", 0)
            row["a"] = p.get("assists", 0)
            row["h"] = p.get("hits", 0)
            row["b"] = p.get("blocks", 0)
        if inj:
            row["inj"] = _inj_str(inj)
        rows.append(row)
    return _json(rows)


# ---------------------------------------------------------------------------
# Free agents
# ---------------------------------------------------------------------------


def format_free_agents(data: list[dict], claims_remaining: int | None = None) -> str:
    if not data:
        return "No free agents found."
    result: dict = {}
    if claims_remaining is not None:
        result["claims"] = claims_remaining
        if claims_remaining <= 2:
            result["WARNING"] = f"ONLY {claims_remaining} CLAIM(S) LEFT — choose wisely"
    rows = []
    for p in data:
        raw_pos = p.get("position", "")
        row: dict = {
            "name": p.get("player_name", ""),
            "pos": _fpos(raw_pos),
            "team": p.get("team", ""),
            "gp": p.get("games_played", 0),
            "fpg": round(p.get("fpts_per_game", 0.0), 2),
            "line": _line_tag({"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}, raw_pos) or "-",
        }
        if raw_pos == "G":
            row["w"] = p.get("wins", 0)
            row["so"] = p.get("shutouts", 0)
            row["gaa"] = round(p.get("gaa", 0.0), 2)
            row["sr"] = p.get("start_rate", 0.0)
            row["sr14"] = p.get("start_rate_l14", 0.0)
        else:
            row["g"] = p.get("goals", 0)
            row["a"] = p.get("assists", 0)
            row["h"] = p.get("hits", 0)
            row["b"] = p.get("blocks", 0)
            peri = p.get("peripheral_fpg", 0.0)
            if peri:
                row["peri_fpg"] = round(peri, 2)
        inj = p.get("injury")
        if inj:
            row["inj"] = _inj_str(inj)
        sal = p.get("salary", 0) or 0
        if sal > 0:
            row["sal"] = round(sal / 1e6, 1)
        rows.append(row)
    if result:
        result["players"] = rows
        return _json(result)
    return _json(rows)


# ---------------------------------------------------------------------------
# Player card
# ---------------------------------------------------------------------------


def format_player_card(data: dict) -> str:
    if not data:
        return "Player not found."
    p = data["player"]
    stats = data.get("season_stats", {})
    result: dict = {
        "name": p["full_name"],
        "team": p["team_abbrev"],
        "pos": _fpos(p.get("position")),
        "gp": stats.get("games_played", 0),
        "fp": round(stats.get("fantasy_points", 0.0), 1),
        "fpg": round(stats.get("fpts_per_game", 0.0), 2),
    }
    # Injury
    if data.get("injury"):
        inj = data["injury"]
        result["inj"] = f"{inj['injury_type']} - {inj['status']}"
    # Line context
    line_ctx = data.get("line_context")
    if line_ctx:
        result["line"] = _line_tag(line_ctx, p.get("position", ""))
        ev_mates = line_ctx.get("ev_linemates", [])
        if ev_mates:
            result["ev_mates"] = ev_mates
        pp_mates = line_ctx.get("pp_linemates", [])
        if pp_mates:
            result["pp_mates"] = pp_mates
    # Stats
    if data["is_goalie"]:
        result.update({
            "w": stats.get("wins", 0), "l": stats.get("losses", 0),
            "otl": stats.get("ot_losses", 0), "so": stats.get("shutouts", 0),
            "gaa": round(stats.get("gaa", 0.0), 2),
            "svp": round(stats.get("sv_pct", 0.0), 3),
            "sr": stats.get("start_rate", 0.0),
            "sr14": stats.get("start_rate_l14", 0.0),
        })
    else:
        result.update({
            "g": stats.get("goals", 0), "a": stats.get("assists", 0),
            "pts": stats.get("points", 0), "h": stats.get("hits", 0),
            "b": stats.get("blocks", 0), "sog": stats.get("shots", 0),
            "pm": stats.get("plus_minus", 0),
            "toi": _toi_str(stats.get("toi_per_game", 0)),
            "ppg": stats.get("powerplay_goals", 0),
            "ppp": stats.get("powerplay_points", 0),
        })
        pp_toi = stats.get("pp_toi", 0)
        if pp_toi:
            result["pp_toi"] = _toi_str(pp_toi)
        shg = stats.get("shorthanded_goals", 0)
        shp = stats.get("shorthanded_points", 0)
        if shg or shp:
            result["shg"] = shg
            result["shp"] = shp
    # Salary
    sal = data.get("salary", 0) or stats.get("salary", 0) or 0
    if sal > 0:
        result["sal"] = round(sal / 1e6, 1)
    # Game log
    log = data.get("game_log", [])
    if log:
        gl = []
        for g in log:
            entry: dict = {"date": g.get("game_date", ""), "fp": round(g.get("fantasy_points", 0.0), 1)}
            if data["is_goalie"]:
                entry.update({"w": g["wins"], "l": g["losses"], "so": g["shutouts"], "sv": g["saves"], "ga": g["goals_against"]})
            else:
                entry.update({"g": g["goals"], "a": g["assists"], "h": g["hits"], "b": g["blocks"], "sog": g["shots"], "toi": _toi_str(g.get("toi", 0))})
            gl.append(entry)
        result["log"] = gl
    # News
    news = data.get("news", [])
    if news:
        result["news"] = [
            {"date": (n.get("published_at") or "")[:10], "hl": n.get("headline", "")}
            for n in news
        ]
    return _json(result)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def format_comparison(data: list[dict]) -> str:
    if not data:
        return "No players to compare."
    rows = []
    for p in data:
        player = p["player"]
        row: dict = {
            "name": player["full_name"],
            "pos": _fpos(player.get("position")),
            "gp": p.get("games_played", "-"),
            "fp": p.get("fantasy_points", "-"),
            "fpg": p.get("fpts_per_game", "-"),
        }
        if p.get("is_goalie"):
            row.update({"w": p.get("wins", "-"), "l": p.get("losses", "-"), "so": p.get("shutouts", "-"), "gaa": p.get("gaa", "-"), "sr": p.get("start_rate", "-"), "sr14": p.get("start_rate_l14", "-")})
        else:
            row.update({"g": p.get("goals", "-"), "a": p.get("assists", "-"), "pts": p.get("points", "-"), "h": p.get("hits", "-"), "b": p.get("blocks", "-")})
            toi = p.get("toi_per_game")
            if toi and toi != "-":
                row["toi"] = _toi_str(int(toi))
        lt = _line_tag(p.get("line_context"), player.get("position", ""))
        if lt:
            row["line"] = lt
        rows.append(row)
    return _json(rows)


# ---------------------------------------------------------------------------
# Trends
# ---------------------------------------------------------------------------


def format_trends(data: dict) -> str:
    if not data:
        return "Player not found."
    p = data["player"]
    windows = data.get("windows", {})
    result: dict = {"name": p["full_name"], "trend": data.get("trend", "neutral")}
    for key in ("last_7", "last_14", "last_30", "season"):
        w = windows.get(key, {})
        result[key] = {"fpg": round(w.get("fpts_per_game", 0.0), 2), "gp": w.get("games", 0)}
    return _json(result)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def format_standings(data: list[dict]) -> str:
    if not data:
        return "No standings data."
    rows = []
    for s in data:
        row: dict = {
            "rank": s.get("rank", 0),
            "team": s.get("team_name") or s.get("short_name", ""),
            "gp": s.get("games_played", 0),
            "pf": round(s.get("points_for", 0.0), 1),
            "fpg": round(s.get("fantasy_points_per_game", 0.0), 2),
            "gb": s.get("games_back", 0.0),
            "streak": s.get("streak", ""),
        }
        claims = s.get("claims_remaining")
        if claims is not None:
            row["claims"] = claims
            if claims <= 2:
                row["WARNING"] = f"ONLY {claims} CLAIM(S) LEFT"
        gp_rem = s.get("gp_remaining", {})
        if gp_rem:
            row["gp_rem"] = {g: gp_rem[g].get("remaining", "?") for g in ("F", "D", "G") if g in gp_rem}
        rows.append(row)
    return _json(rows)


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------


def format_schedule(data: dict) -> str:
    if not data:
        return "No schedule data."
    b2b_dates = set()
    for d1, d2 in data.get("back_to_backs", []):
        b2b_dates.add(d1)
        b2b_dates.add(d2)
    games = []
    for g in data.get("games", []):
        entry: dict = {"date": g["game_date"], "vs": g.get("opponent", ""), "ha": g.get("home_away", "")}
        if g["game_date"] in b2b_dates:
            entry["b2b"] = True
        if "opp" in g:
            entry["opp"] = g["opp"]
        games.append(entry)
    result = {
        "team": data.get("team", ""),
        "count": data.get("game_count", 0),
        "b2b_count": len(data.get("back_to_backs", [])),
        "games": games,
    }
    return _json(result)


# ---------------------------------------------------------------------------
# NHL team standings
# ---------------------------------------------------------------------------


def format_nhl_standings(data: list[dict]) -> str:
    if not data:
        return "No NHL team stats available."
    rows = []
    for t in data:
        entry = {
            "team": t["team"],
            "rec": f"{t['wins']}-{t['losses']}-{t['ot_losses']}",
            "pts": t["points"],
            "gf_g": round(t["goals_for_per_game"], 2),
            "ga_g": round(t["goals_against_per_game"], 2),
            "l10": t.get("l10_record", ""),
            "l14": t.get("l14_record", ""),
            "streak": t.get("streak", ""),
            "div": t.get("division", ""),
        }
        rows.append(entry)
    return _json(rows)


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


def format_news(data: list[dict]) -> str:
    if not data:
        return "No recent news."
    rows = []
    for n in data:
        player = n.get("player_name") or "Unknown"
        headline = n.get("headline") or ""
        if headline.startswith(f"{player}: "):
            headline = headline[len(player) + 2:]
        rows.append({"date": (n.get("published_at") or "")[:10], "player": player, "hl": headline})
    return _json(rows)


# ---------------------------------------------------------------------------
# Injuries
# ---------------------------------------------------------------------------


def format_injuries(data: list[dict]) -> str:
    if not data:
        return "No injuries to report."
    rows = []
    for i in data:
        rows.append({
            "name": i.get("full_name", ""),
            "pos": _fpos(i.get("position")),
            "team": i.get("team_abbrev", ""),
            "injury": i.get("injury_type", "Unknown"),
            "status": i.get("status", "Unknown"),
            "updated": i.get("updated_at", ""),
        })
    return _json(rows)


# ---------------------------------------------------------------------------
# Team roster (opponent scouting)
# ---------------------------------------------------------------------------


def format_team_roster(data: dict) -> str:
    if not data:
        return "Team not found."
    info = data["team_info"]
    result = {
        "team": info["team_name"],
        "short": info["short_name"],
        "rank": info.get("rank", "?"),
        "pf": round(info.get("points_for", 0), 1),
        "fpg": round(info.get("fpg", 0), 2),
    }
    # Inline roster as JSON array instead of calling format_roster
    # to avoid double-encoding
    roster = data["roster"]
    players = []
    for p in roster:
        raw_pos = p.get("position", "")
        row: dict = {
            "name": p.get("player_name", ""),
            "pos": _fpos(raw_pos),
            "gp": p.get("games_played", 0),
            "fp": round(p.get("fantasy_points", 0.0), 1),
            "fpg": round(p.get("fpts_per_game", 0.0), 2),
            "r14": round(p.get("recent_14_fpg", 0.0), 2),
            "trend": p.get("trend", "neutral"),
            "line": _line_tag({"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}, raw_pos) or "-",
        }
        sal = p.get("salary", 0) or 0
        if sal > 0:
            row["sal"] = round(sal / 1e6, 1)
        if raw_pos == "G":
            row["w"] = p.get("wins", 0)
            row["l"] = p.get("losses", 0)
            row["so"] = p.get("shutouts", 0)
            row["sr"] = p.get("start_rate", 0.0)
            row["sr14"] = p.get("start_rate_l14", 0.0)
        else:
            row["g"] = p.get("goals", 0)
            row["a"] = p.get("assists", 0)
            row["h"] = p.get("hits", 0)
            row["b"] = p.get("blocks", 0)
        inj = p.get("injury")
        if inj:
            row["inj"] = _inj_str(inj)
        players.append(row)
    result["roster"] = players
    return _json(result)


# ---------------------------------------------------------------------------
# Trade suggestions (kept for potential re-enable)
# ---------------------------------------------------------------------------


def format_trade_suggestions(data: dict) -> str:
    if not data:
        return "Could not generate trade suggestions."
    opp = data["opponent"]
    result = {
        "opponent": opp["name"],
        "opp_rank": opp.get("rank", "?"),
        "my_avg": data["my_team"]["avg_fpg"],
        "opp_avg": opp["avg_fpg"],
        "trades": [],
    }
    for s in data.get("suggestions", []):
        result["trades"].append({
            "send": s["send_player"], "send_pos": _fpos(s.get("send_position")),
            "send_r14": round(s.get("send_recent_14_fpg", s["send_fpg"]), 2),
            "recv": s["receive_player"], "recv_pos": _fpos(s.get("receive_position")),
            "recv_r14": round(s.get("receive_recent_14_fpg", s["receive_fpg"]), 2),
            "upgrade": round(s["my_upgrade"], 2),
        })
    return _json(result)


def format_trade_targets(data: list[dict]) -> str:
    if not data:
        return "No buy-low trade targets found matching criteria."
    rows = []
    for p in data:
        raw_pos = p.get("position", "")
        row: dict = {
            "name": p.get("player_name", ""),
            "pos": _fpos(raw_pos),
            "owner": p.get("owner_team_name", ""),
            "rank": p.get("owner_rank"),
            "gp": p.get("games_played", 0),
            "fpg": round(p.get("season_fpg", 0.0), 2),
            "r7_fpg": round(p.get("recent_7_fpg", 0.0), 2),
            "line": _line_tag(p.get("line_info"), raw_pos) or "-",
            "signal": p.get("signal", "trending_up"),
            "trend_pct": p.get("trend_pct", 0),
        }
        tpg = p.get("toi_per_game", 0)
        if tpg and raw_pos != "G":
            row["toi"] = _toi_str(tpg)
        rows.append(row)
    return _json(rows)


def format_drop_candidates(data: list[dict]) -> str:
    if not data:
        return "No drop candidates identified."
    rows = []
    for p in data:
        raw_pos = p.get("position", "")
        row: dict = {
            "name": p.get("player_name", ""),
            "pos": _fpos(raw_pos),
            "gp": p.get("games_played", 0),
            "fpg": round(p.get("season_fpg", 0.0), 2),
            "r14_fpg": round(p.get("recent_14_fpg", 0.0), 2),
            "line": _line_tag(p.get("line_info"), raw_pos) or "-",
            "trend": p.get("trend", "neutral"),
        }
        inj = p.get("injury")
        if inj:
            row["inj"] = _inj_str(inj)
        rows.append(row)
    return _json(rows)


def format_roster_moves(drops: list[dict], pickups: dict | list[dict]) -> str:
    if isinstance(pickups, dict):
        claims_remaining = pickups.get("claims_remaining")
        gp_remaining = pickups.get("gp_remaining")
        pickup_list = pickups.get("recommendations", [])
        ir_stash = pickups.get("ir_stash", [])
        ir_slot_open = pickups.get("ir_slot_open", False)
    else:
        claims_remaining = None
        gp_remaining = None
        pickup_list = pickups
        ir_stash = []
        ir_slot_open = False

    result: dict = {}
    if claims_remaining is not None:
        result["claims"] = claims_remaining
        if claims_remaining <= 2:
            result["WARNING"] = f"ONLY {claims_remaining} CLAIM(S) LEFT — choose wisely"
    if gp_remaining:
        result["gp_rem"] = gp_remaining

    # Drops
    drop_rows = []
    for p in drops:
        raw_pos = p.get("position", "")
        row: dict = {
            "name": p.get("player_name", ""),
            "pos": _fpos(raw_pos),
            "fpg": round(p.get("season_fpg", 0.0), 2),
            "r14": round(p.get("recent_14_fpg", 0.0), 2),
            "trend": p.get("trend", "neutral"),
        }
        inj = p.get("injury")
        if inj:
            row["inj"] = _inj_str(inj)
        drop_rows.append(row)
    result["drops"] = drop_rows

    # Pickups
    pick_rows = []
    for r in pickup_list:
        raw_pos = r.get("pickup_position", "")
        row: dict = {
            "name": r.get("pickup_name", ""),
            "pos": _fpos(raw_pos),
            "team": r.get("pickup_team", "")[:3],
            "gp": r.get("pickup_games_played", 0),
            "fpg": round(r.get("pickup_season_fpg", 0.0), 2),
            "r14": round(r.get("pickup_recent_fpg", 0.0), 2),
            "reg_fpg": round(r.get("pickup_regressed_fpg", r.get("pickup_recent_fpg", 0.0)), 2),
            "drop": r.get("drop_name", ""),
            "upgrade": round(r.get("fpg_upgrade", 0.0), 2),
            "est_gp": r.get("est_games", 0),
            "value": round(r.get("total_value", 0.0), 1),
        }
        if r.get("pickup_injury"):
            row["inj"] = _inj_str(r["pickup_injury"])
        pick_rows.append(row)
    result["pickups"] = pick_rows

    # IR stash
    if ir_slot_open and ir_stash:
        stash_rows = []
        for r in ir_stash:
            stash_rows.append({
                "name": r.get("pickup_name", ""),
                "pos": _fpos(r.get("pickup_position", "")),
                "team": r.get("pickup_team", "")[:3],
                "gp": r.get("pickup_games_played", 0),
                "fpg": round(r.get("pickup_season_fpg", 0.0), 2),
                "reg_fpg": round(r.get("pickup_regressed_fpg", 0.0), 2),
                "est_gp": r.get("est_games", 0),
                "value": round(r.get("total_value", 0.0), 1),
            })
        result["ir_stash"] = stash_rows

    return _json(result)


# ---------------------------------------------------------------------------
# Web search results
# ---------------------------------------------------------------------------


def format_web_search_results(data: dict, query: str) -> str:
    results = data.get("web", {}).get("results", [])
    if not results:
        return f"No web results found for: '{query}'"
    rows = []
    for r in results:
        entry: dict = {"title": r.get("title", ""), "url": r.get("url", "")}
        snippet = r.get("description", "")
        if snippet:
            snippet = snippet.replace("&amp;", "&").replace("&#x27;", "'")
            if len(snippet) > 200:
                snippet = snippet[:197].rsplit(" ", 1)[0] + "..."
            entry["snippet"] = snippet
        age = r.get("age", "")
        if age:
            entry["age"] = age
        rows.append(entry)
    return _json({"query": query, "results": rows})
