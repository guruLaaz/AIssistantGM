"""Labeled key-value formatters for query results.

Each function takes the output of the corresponding queries.py function
and returns an explicitly labeled string.  Every stat carries its name
so the AI never has to guess column positions.
"""

from __future__ import annotations


def _divider(width: int = 80) -> str:
    return "-" * width


def _fpos(position: str | None) -> str:
    """Map NHL position (C/L/R/D/G) to fantasy position (F/D/G)."""
    if not position:
        return "F"
    if position in ("G", "D"):
        return position
    return "F"


def _injury_tag(injury: dict | None) -> str:
    if not injury:
        return ""
    return f"[{injury.get('status', 'INJ')}]"


def _news_lines(news: list | str | None, player_name: str = "",
                 indent: int = 2) -> list[str]:
    """Render recent news as indented lines with date and content snippet.

    Accepts a list of dicts ``{"headline", "content", "date"}`` (preferred),
    a list of plain headline strings, or a single headline string (legacy).
    """
    if not news:
        return []
    if isinstance(news, str):
        news = [{"headline": news}]
    result: list[str] = []
    pad = " " * indent
    for item in news:
        if isinstance(item, str):
            item = {"headline": item}
        headline = item.get("headline", "")
        content = item.get("content", "")
        date_raw = item.get("date", "")
        # Format date as compact "2026-03-02"
        date_str = ""
        if date_raw:
            try:
                date_str = date_raw[:10]
            except (ValueError, TypeError):
                pass
        # Strip redundant "Player Name: " prefix from headline
        if player_name and headline.startswith(f"{player_name}: "):
            headline = headline[len(player_name) + 2:]
        # Truncate long headlines
        if len(headline) > 90:
            headline = headline[:87].rsplit(" ", 1)[0] + "..."
        prefix = f"[{date_str}] " if date_str else ""
        result.append(f"{pad}^ {prefix}{headline}")
        # Add truncated content snippet if available
        if content:
            if "Visit RotoWire" in content:
                content = content[:content.index("Visit RotoWire")].strip()
            if content:
                if len(content) > 120:
                    content = content[:117].rsplit(" ", 1)[0] + "..."
                result.append(f"{pad}  {content}")
    return result


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
    """Build a compact line descriptor like 'L1/PP1' or 'D1/PP1' from line info dict."""
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


# ---------------------------------------------------------------------------
# Roster
# ---------------------------------------------------------------------------


def format_roster(data: list[dict]) -> str:
    """Format fantasy roster with labeled stats per player."""
    if not data:
        return "No players on roster."

    lines = []
    for p in data:
        name = p.get("player_name") or ""
        pos = _fpos(p.get("position"))
        raw_pos = p.get("position", "")
        gp = p.get("games_played", 0)
        fp = p.get("fantasy_points", 0.0)
        fpg = p.get("fpts_per_game", 0.0)
        r14 = p.get("recent_14_fpg", 0.0)
        salary = p.get("salary", 0) or 0
        sal_str = f"${salary / 1_000_000:.1f}M" if salary > 0 else "-"
        inj = _injury_tag(p.get("injury"))
        trend = p.get("trend", "neutral")
        trend_str = ("HOT" if trend == "hot"
                     else "COLD" if trend == "cold"
                     else "neutral")
        line_info = {"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}
        lt = _line_tag(line_info, raw_pos) or "-"

        if raw_pos == "G":
            w = p.get("wins", 0)
            l = p.get("losses", 0)
            so = p.get("shutouts", 0)
            stats_str = f"W:{w} L:{l} SO:{so}"
        else:
            g = p.get("goals", 0)
            a = p.get("assists", 0)
            h = p.get("hits", 0)
            b = p.get("blocks", 0)
            stats_str = f"G:{g} A:{a} H:{h} B:{b}"

        lines.append(f"--- {name} ({pos}) ---")
        lines.append(f"GP:{gp} | {stats_str} | FP:{fp:.1f} | FP/G:{fpg:.2f} | Last 14 games FP/G:{r14:.2f}")
        parts = [f"Salary:{sal_str}", f"Trend:{trend_str}", f"Line:{lt}"]
        if inj:
            parts.append(f"Injury:{inj}")
        lines.append(" | ".join(parts))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Free agents
# ---------------------------------------------------------------------------


def format_free_agents(data: list[dict]) -> str:
    """Format free agent search results with labeled stats."""
    if not data:
        return "No free agents found."

    lines = []
    for p in data:
        name = p.get("player_name") or ""
        raw_pos = p.get("position", "")
        pos = _fpos(raw_pos)
        team = p.get("team", "") or ""
        gp = p.get("games_played", 0)
        fpg = p.get("fpts_per_game", 0.0)
        peri = p.get("peripheral_fpg", 0.0)
        inj = _injury_tag(p.get("injury"))

        if raw_pos == "G":
            w = p.get("wins", 0)
            so = p.get("shutouts", 0)
            gaa = p.get("gaa", 0.0)
            stats_str = f"W:{w} SO:{so} GAA:{gaa:.2f}"
            peri_str = ""
        else:
            g = p.get("goals", 0)
            a = p.get("assists", 0)
            h = p.get("hits", 0)
            b = p.get("blocks", 0)
            stats_str = f"G:{g} A:{a} H:{h} B:{b}"
            peri_str = f" | Peripheral FP/G:{peri:.2f}"

        line_info = {"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}
        lt = _line_tag(line_info, raw_pos) or "-"

        header = f"--- {name} ({pos}, {team}) ---"
        if inj:
            header = f"--- {name} ({pos}, {team}) {inj} ---"
        lines.append(header)
        lines.append(f"GP:{gp} | {stats_str} | FP/G:{fpg:.2f}{peri_str} | Line:{lt}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Player card
# ---------------------------------------------------------------------------


def format_player_card(data: dict) -> str:
    """Format detailed player stats card (already key-value)."""
    if not data:
        return "Player not found."

    p = data["player"]
    lines = []
    lines.append(f"=== {p['full_name']} ({p['team_abbrev']} - {_fpos(p.get('position'))}) ===")

    # Injury banner
    if data.get("injury"):
        inj = data["injury"]
        lines.append(f"  INJURY: {inj['injury_type']} - {inj['status']}")

    # Line combination info
    line_ctx = data.get("line_context")
    if line_ctx:
        ev = line_ctx.get("ev_line")
        pp = line_ctx.get("pp_unit")
        line_parts = []
        deployed_pos = line_ctx.get("deployed_position", "")
        if ev:
            pos_label = deployed_pos.upper() if deployed_pos else ""
            if pos_label:
                line_parts.append(f"Line {ev} ({pos_label})")
            else:
                pos = p.get("position", "")
                label = "Forward" if pos in ("C", "L", "R") else "Defense"
                line_parts.append(f"{label} Line {ev}")
        if pp:
            line_parts.append(f"PP{pp}")
        if line_parts:
            lines.append(f"  Lines: {' | '.join(line_parts)}")
        ev_mates = line_ctx.get("ev_linemates", [])
        if ev_mates:
            lines.append(f"  EV Linemates: {', '.join(ev_mates)}")
        pp_mates = line_ctx.get("pp_linemates", [])
        if pp_mates:
            lines.append(f"  PP Linemates: {', '.join(pp_mates)}")

    # Season stats
    stats = data.get("season_stats", {})
    gp = stats.get("games_played", 0)
    fp = stats.get("fantasy_points", 0.0)
    fpg = stats.get("fpts_per_game", 0.0)

    lines.append("")
    lines.append(f"Season: {gp} GP | {fp:.1f} FP | {fpg:.2f} FP/G")

    if data["is_goalie"]:
        lines.append(
            f"  W:{stats.get('wins',0)} L:{stats.get('losses',0)} "
            f"OTL:{stats.get('ot_losses',0)} SO:{stats.get('shutouts',0)} "
            f"GAA:{stats.get('gaa',0.0):.2f} SV%:{stats.get('sv_pct',0.0):.3f}"
        )
    else:
        lines.append(
            f"  G:{stats.get('goals',0)} A:{stats.get('assists',0)} "
            f"Pts:{stats.get('points',0)} H:{stats.get('hits',0)} "
            f"B:{stats.get('blocks',0)} SOG:{stats.get('shots',0)} "
            f"+/-:{stats.get('plus_minus',0)}"
        )
        toi_pg = stats.get("toi_per_game", 0)
        ppg = stats.get("powerplay_goals", 0)
        ppp = stats.get("powerplay_points", 0)
        shg = stats.get("shorthanded_goals", 0)
        shp = stats.get("shorthanded_points", 0)
        parts = [f"TOI/G: {_toi_str(toi_pg)}"]
        pp_toi = stats.get("pp_toi", 0)
        if pp_toi:
            parts.append(f"PP TOI: {_toi_str(pp_toi)}")
        parts.append(f"PPG:{ppg} PPP:{ppp}")
        if shg or shp:
            parts.append(f"SHG:{shg} SHP:{shp}")
        lines.append("  " + "  ".join(parts))

    # Game log
    log = data.get("game_log", [])
    if log:
        lines.append("")
        lines.append("Recent Games:")
        for g in log:
            date = g.get("game_date", "")
            fp_val = g.get("fantasy_points", 0.0)
            if data["is_goalie"]:
                lines.append(
                    f"  {date} | W:{g['wins']} L:{g['losses']} OTL:{g['ot_losses']} "
                    f"SO:{g['shutouts']} SV:{g['saves']} GA:{g['goals_against']} | FP:{fp_val:.1f}"
                )
            else:
                toi = _toi_str(g.get("toi", 0))
                lines.append(
                    f"  {date} | G:{g['goals']} A:{g['assists']} Pts:{g['points']} "
                    f"H:{g['hits']} B:{g['blocks']} SOG:{g['shots']} TOI:{toi} | FP:{fp_val:.1f}"
                )

    # News
    news = data.get("news", [])
    if news:
        lines.append("")
        lines.append("Recent News:")
        player_name = p["full_name"]
        for n in news:
            date_str = (n.get("published_at") or "")[:10]
            headline = n.get("headline", "")
            if headline.startswith(f"{player_name}: "):
                headline = headline[len(player_name) + 2:]
            lines.append(f"  [{date_str}] {headline}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def format_comparison(data: list[dict]) -> str:
    """Format side-by-side player comparison with labeled rows."""
    if not data:
        return "No players to compare."

    lines = []

    # Header row with player names
    label_width = 14
    col_width = 18
    header = f"{'Stat':<{label_width}}"
    for p in data:
        name = p["player"]["full_name"][:col_width - 1]
        header += f" {name:>{col_width}}"
    lines.append(header)
    lines.append(_divider(label_width + col_width * len(data) + len(data)))

    # Position row
    pos_row = f"{'Position':<{label_width}}"
    for p in data:
        pos_row += f" {_fpos(p['player'].get('position')):>{col_width}}"
    lines.append(pos_row)

    # Determine which stats to show based on whether any are goalies
    has_goalie = any(p.get("is_goalie") for p in data)

    stat_rows = [
        ("GP", "games_played"),
        ("FP", "fantasy_points"),
        ("FP/G", "fpts_per_game"),
    ]

    if has_goalie:
        stat_rows += [
            ("Wins", "wins"),
            ("Losses", "losses"),
            ("Shutouts", "shutouts"),
            ("GAA", "gaa"),
        ]
    else:
        stat_rows += [
            ("Goals", "goals"),
            ("Assists", "assists"),
            ("Points", "points"),
            ("Hits", "hits"),
            ("Blocks", "blocks"),
            ("TOI/G", "toi_per_game"),
        ]

    for label, key in stat_rows:
        row = f"{label:<{label_width}}"
        for p in data:
            val = p.get(key, "-")
            if key == "toi_per_game" and isinstance(val, (int, float)) and val != "-":
                val_int = int(val)
                row += f" {_toi_str(val_int):>{col_width}}"
            elif isinstance(val, float):
                row += f" {val:>{col_width}.2f}"
            else:
                row += f" {str(val):>{col_width}}"
        lines.append(row)

    # Line combination row
    line_row = f"{'Line':<{label_width}}"
    has_any_line = False
    for p in data:
        tag = _line_tag(p.get("line_context"), p.get("position", ""))
        if tag:
            has_any_line = True
        line_row += f" {(tag or '-'):>{col_width}}"
    if has_any_line:
        lines.append(line_row)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Trends
# ---------------------------------------------------------------------------


def format_trends(data: dict) -> str:
    """Format player trend analysis."""
    if not data:
        return "Player not found."

    p = data["player"]
    lines = []
    lines.append(f"=== Trends: {p['full_name']} ===")

    windows = data.get("windows", {})
    for label, key in [("Last 7", "last_7"), ("Last 14", "last_14"), ("Last 30", "last_30"), ("Season", "season")]:
        w = windows.get(key, {})
        fpg = w.get("fpts_per_game", 0.0)
        games = w.get("games", 0)
        lines.append(f"  {label:<10} {fpg:>5.2f} FP/G  ({games} games)")

    trend = data.get("trend", "neutral")
    if trend == "hot":
        arrow = "HOT"
    elif trend == "cold":
        arrow = "COLD"
    else:
        arrow = "neutral"

    lines.append(f"\n  Trend: {arrow}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def format_standings(data: list[dict]) -> str:
    """Format fantasy league standings with labeled values."""
    if not data:
        return "No standings data."

    lines = []
    for s in data:
        name = s.get("team_name") or s.get("short_name") or ""
        rank = s.get("rank", 0)
        gp = s.get("games_played", 0)
        pf = s.get("points_for", 0.0)
        fpg = s.get("fantasy_points_per_game", 0.0)
        gb = s.get("games_back", 0.0)
        gb_str = "-" if gb == 0.0 else f"{gb:.2f}"
        streak = s.get("streak", "")
        lines.append(f"#{rank} {name} | GP:{gp} | PF:{pf:.1f} | FP/G:{fpg:.2f} | GB:{gb_str} | Streak:{streak}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------


def format_schedule(data: dict) -> str:
    """Format schedule analysis."""
    if not data:
        return "No schedule data."

    lines = []
    team = data.get("team", "")
    count = data.get("game_count", 0)
    lines.append(f"=== Schedule: {team} ({count} games) ===")

    b2b_dates = set()
    for d1, d2 in data.get("back_to_backs", []):
        b2b_dates.add(d1)
        b2b_dates.add(d2)

    for g in data.get("games", []):
        gd = g["game_date"]
        opp = g.get("opponent", "")
        ha = g.get("home_away", "")
        b2b = " [B2B]" if gd in b2b_dates else ""
        lines.append(f"  {gd} | vs:{opp} | {ha}{b2b}")

    b2b_count = len(data.get("back_to_backs", []))
    if b2b_count:
        lines.append(f"\n  Back-to-backs: {b2b_count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


def format_news(data: list[dict]) -> str:
    """Format recent news items."""
    if not data:
        return "No recent news."

    lines = []
    for n in data:
        date_str = (n.get("published_at") or "")[:10]
        player = n.get("player_name") or "Unknown"
        headline = n.get("headline") or ""
        if headline.startswith(f"{player}: "):
            headline = headline[len(player) + 2:]
        lines.append(f"[{date_str}] {player}: {headline}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Injuries
# ---------------------------------------------------------------------------


def format_injuries(data: list[dict]) -> str:
    """Format injury report with labeled fields."""
    if not data:
        return "No injuries to report."

    lines = []
    for i in data:
        name = i.get("full_name") or ""
        team = i.get("team_abbrev") or ""
        pos = _fpos(i.get("position"))
        injury_type = i.get("injury_type") or "Unknown"
        status = i.get("status") or "Unknown"
        updated = i.get("updated_at") or ""
        lines.append(
            f"{name} ({pos}, {team}) | Injury:{injury_type} | Status:{status} | Updated:{updated}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Advanced analysis formatters
# ---------------------------------------------------------------------------


def format_team_roster(data: dict) -> str:
    """Format another team's roster for trade analysis."""
    if not data:
        return "Team not found."

    info = data["team_info"]
    roster = data["roster"]

    lines = []
    lines.append(f"=== {info['team_name']} ({info['short_name']}) ===")
    lines.append(f"Rank:#{info.get('rank', '?')} | PF:{info.get('points_for', 0):.1f} | "
                 f"FP/G:{info.get('fpg', 0):.2f}")
    lines.append("")
    lines.append(format_roster(roster))

    return "\n".join(lines)


def format_trade_suggestions(data: dict) -> str:
    """Format trade suggestions with labeled fields."""
    if not data:
        return "Could not generate trade suggestions."

    opp = data["opponent"]
    suggestions = data.get("suggestions", [])

    lines = []
    lines.append(f"=== Trade Suggestions vs {opp['name']} "
                 f"(#{opp.get('rank', '?')}) ===")
    lines.append("")

    # Position avg comparison
    my_avgs = data["my_team"]["avg_fpg"]
    opp_avgs = opp["avg_fpg"]
    for g in ("F", "D", "G"):
        lines.append(f"  {g}: My avg FP/G:{my_avgs.get(g, 0):.2f} | Their avg FP/G:{opp_avgs.get(g, 0):.2f}")
    lines.append("")

    if not suggestions:
        lines.append("No mutually beneficial trades identified.")
        return "\n".join(lines)

    for idx, s in enumerate(suggestions, 1):
        send_r14 = s.get('send_recent_14_fpg', s['send_fpg'])
        recv_r14 = s.get('receive_recent_14_fpg', s['receive_fpg'])
        lines.append(f"Trade #{idx}:")
        lines.append(f"  Send: {s['send_player']} ({_fpos(s.get('send_position'))}) | Last 14 games FP/G:{send_r14:.2f}")
        lines.append(f"  Receive: {s['receive_player']} ({_fpos(s.get('receive_position'))}) | Last 14 games FP/G:{recv_r14:.2f}")
        lines.append(f"  My FP/G upgrade: {s['my_upgrade']:+.2f}")
        send_news = _news_lines(s.get("send_news"), s.get("send_player", ""), indent=4)
        recv_news = _news_lines(s.get("receive_news"), s.get("receive_player", ""), indent=4)
        if send_news:
            lines.append(f"  Send news:")
            lines.extend(send_news)
        if recv_news:
            lines.append(f"  Receive news:")
            lines.extend(recv_news)

    return "\n".join(lines)


def format_trade_targets(data: list[dict]) -> str:
    """Format buy-low trade target candidates with labeled fields."""
    if not data:
        return "No buy-low trade targets found matching criteria."

    lines = []
    lines.append("=== Buy-Low Trade Targets ===")
    lines.append("")

    for p in data:
        name = p.get("player_name") or ""
        raw_pos = p.get("position", "")
        owner = p.get("owner_team_name") or ""
        rank = p.get("owner_rank")
        rank_str = f" (#{rank})" if rank else ""
        tpg = p.get("toi_per_game", 0)
        toi = _toi_str(tpg) if tpg and raw_pos != "G" else "-"
        lt = _line_tag(p.get("line_info"), raw_pos) or "-"
        signal = p.get("signal", "trending_up")
        trend_pct = p.get("trend_pct", 0)
        if signal == "high_toi_underperformer":
            trend_str = f"high-TOI underperformer ({trend_pct:+.0f}%)"
        else:
            trend_str = f"trending up ({trend_pct:+.0f}%)"

        lines.append(f"--- {name} ({_fpos(raw_pos)}) ---")
        lines.append(f"Owner:{owner}{rank_str} | GP:{p.get('games_played', 0)} | "
                     f"Season FP/G:{p.get('season_fpg', 0.0):.2f} | Last 7 games FP/G:{p.get('recent_7_fpg', 0.0):.2f}")
        lines.append(f"TOI/G:{toi} | Line:{lt} | Signal:{trend_str}")
        lines.extend(_news_lines(p.get("recent_news"), name))

    return "\n".join(lines)


def format_drop_candidates(data: list[dict]) -> str:
    """Format drop candidate analysis with labeled fields."""
    if not data:
        return "No drop candidates identified."

    lines = []
    lines.append("=== Drop Candidates (Weakest Roster Players) ===")
    lines.append("")

    for p in data:
        name = p.get("player_name") or ""
        raw_pos = p.get("position", "")
        trend = p.get("trend", "neutral")
        trend_str = ("HOT" if trend == "hot"
                     else "COLD" if trend == "cold"
                     else "neutral")
        inj = _injury_tag(p.get("injury"))
        lt = _line_tag(p.get("line_info"), raw_pos) or "-"

        lines.append(f"--- {name} ({_fpos(raw_pos)}) ---")
        parts = [
            f"GP:{p.get('games_played', 0)}",
            f"Season FP/G:{p.get('season_fpg', 0.0):.2f}",
            f"Last 14 games FP/G:{p.get('recent_14_fpg', 0.0):.2f}",
            f"Line:{lt}",
            f"Trend:{trend_str}",
        ]
        if inj:
            parts.append(f"Injury:{inj}")
        lines.append(" | ".join(parts))
        lines.extend(_news_lines(p.get("recent_news"), name))

    return "\n".join(lines)


def format_roster_moves(drops: list[dict], pickups: dict | list[dict]) -> str:
    """Format combined drop and pickup recommendations with labeled fields."""
    # Unwrap new dict format
    if isinstance(pickups, dict):
        claims_remaining = pickups.get("claims_remaining")
        gp_remaining = pickups.get("gp_remaining")
        pickup_list = pickups.get("recommendations", [])
    else:
        claims_remaining = None
        gp_remaining = None
        pickup_list = pickups

    lines = []

    # Claims banner
    if claims_remaining is not None:
        lines.append(f"=== CLAIMS REMAINING: {claims_remaining}/10 ===")
        if claims_remaining <= 2:
            lines.append("*** CLAIMS ARE SCARCE -- only use on high-impact pickups! ***")
        lines.append("")

    # GP remaining banner
    if gp_remaining:
        parts = [f"{g}={gp_remaining[g]}" for g in ("F", "D", "G")]
        lines.append(f"GP Remaining: {' | '.join(parts)}")
        lines.append("")

    # Section 1: Drop Candidates
    lines.append("=== RECOMMENDED DROPS ===")
    lines.append("")
    if drops:
        for p in drops:
            name = p.get("player_name") or ""
            raw_pos = p.get("position", "")
            trend = p.get("trend", "neutral")
            trend_str = ("HOT" if trend == "hot"
                         else "COLD" if trend == "cold"
                         else "neutral")
            inj = _injury_tag(p.get("injury"))
            header = f"--- {name} ({_fpos(raw_pos)}) ---"
            if inj:
                header = f"--- {name} ({_fpos(raw_pos)}) {inj} ---"
            lines.append(header)
            lines.append(f"Season FP/G:{p.get('season_fpg', 0.0):.2f} | "
                         f"Last 14 games FP/G:{p.get('recent_14_fpg', 0.0):.2f} | "
                         f"Trend:{trend_str}")
    else:
        lines.append("  No clear drop candidates.")

    lines.append("")

    # Section 2: Pickup Recommendations
    lines.append("=== RECOMMENDED PICKUPS ===")
    lines.append("")
    if pickup_list:
        for r in pickup_list:
            pickup_name = r.get("pickup_name") or ""
            raw_pos = r.get("pickup_position", "")
            inj_tag = ""
            if r.get("pickup_injury"):
                inj_tag = f" {_injury_tag(r['pickup_injury'])}"
            team = r.get("pickup_team", "")[:3]
            ev_line = r.get("pickup_ev_line")
            prefix = "D" if raw_pos == "D" else "L"
            line_str = f"{prefix}{ev_line}" if ev_line is not None else "-"
            pp_pg = r.get("pickup_pp_toi_per_game", 0)
            pp_str = _toi_str(pp_pg)
            regressed = r.get("pickup_regressed_fpg",
                              r.get("pickup_recent_fpg", 0.0))
            drop_name = r.get("drop_name") or ""

            lines.append(f"--- Pickup: {pickup_name} ({_fpos(raw_pos)}, {team}){inj_tag} ---")
            lines.append(
                f"GP:{r.get('pickup_games_played', 0)} | "
                f"Season FP/G:{r.get('pickup_season_fpg', 0.0):.2f} | "
                f"Last 14 games FP/G:{r.get('pickup_recent_fpg', 0.0):.2f} | "
                f"Regressed FP/G:{regressed:.2f}"
            )
            lines.append(
                f"Line:{line_str} | PP/G:{pp_str} | "
                f"Drop:{drop_name} (Season FP/G:{r.get('drop_season_fpg', 0.0):.2f}, "
                f"Last 14 games FP/G:{r.get('drop_recent_fpg', 0.0):.2f})"
            )
            lines.append(
                f"FP/G upgrade:{r.get('fpg_upgrade', 0.0):+.2f} | "
                f"Est games:{r.get('est_games', 0)} | "
                f"Total value:{r.get('total_value', 0.0):+.1f}"
            )
            reason = r.get('reason') or ''
            if reason:
                lines.append(f"Reason: {reason}")
            lines.extend(
                _news_lines(r.get("pickup_recent_news"),
                            pickup_name, indent=2)
            )
    else:
        lines.append("  No clear pickup recommendations.")

    # Section 3: IR Stash Candidates
    ir_stash: list[dict] = []
    ir_slot_open = False
    if isinstance(pickups, dict):
        ir_stash = pickups.get("ir_stash", [])
        ir_slot_open = pickups.get("ir_slot_open", False)

    if ir_slot_open:
        lines.append("")
        lines.append("=== IR STASH CANDIDATES (no drop needed) ===")
        lines.append("")
        if ir_stash:
            for r in ir_stash:
                pickup_name = r.get("pickup_name") or ""
                raw_pos = r.get("pickup_position", "")
                inj_tag = ""
                if r.get("pickup_injury"):
                    inj_tag = f" {_injury_tag(r['pickup_injury'])}"
                team = r.get("pickup_team", "")[:3]
                regressed = r.get("pickup_regressed_fpg", 0.0)

                lines.append(f"--- IR Stash: {pickup_name} ({_fpos(raw_pos)}, {team}){inj_tag} ---")
                lines.append(
                    f"GP:{r.get('pickup_games_played', 0)} | "
                    f"Season FP/G:{r.get('pickup_season_fpg', 0.0):.2f} | "
                    f"Last 14 games FP/G:{r.get('pickup_recent_fpg', 0.0):.2f} | "
                    f"Regressed FP/G:{regressed:.2f}"
                )
                lines.append(
                    f"Est games:{r.get('est_games', 0)} | "
                    f"Total value:{r.get('total_value', 0.0):+.1f} | "
                    f"Reason: {r.get('reason', '')}"
                )
                lines.extend(
                    _news_lines(r.get("pickup_recent_news"),
                                pickup_name, indent=2)
                )
        else:
            lines.append("  No IR-eligible free agents worth stashing.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Web search results
# ---------------------------------------------------------------------------


def format_web_search_results(data: dict, query: str) -> str:
    """Format Brave Search API response as readable text."""
    results = data.get("web", {}).get("results", [])

    if not results:
        return f"No web results found for: '{query}'"

    lines = []
    lines.append(f"=== Web Search: {query} ===")
    lines.append("")

    for i, r in enumerate(results, 1):
        title = r.get("title", "Untitled")
        url = r.get("url", "")
        snippet = r.get("description", "")
        snippet = snippet.replace("&amp;", "&").replace("&#x27;", "'")
        age = r.get("age", "")

        lines.append(f"{i}. {title}")
        if age:
            lines.append(f"   [{age}] {url}")
        else:
            lines.append(f"   {url}")
        if snippet:
            if len(snippet) > 200:
                snippet = snippet[:197].rsplit(" ", 1)[0] + "..."
            lines.append(f"   {snippet}")
        lines.append("")

    return "\n".join(lines)
