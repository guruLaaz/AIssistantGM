"""Terminal-friendly formatters for query results.

Each function takes the output of the corresponding queries.py function
and returns an aligned, human-readable string.
"""

from __future__ import annotations


def _divider(width: int = 80) -> str:
    return "-" * width


def _injury_tag(injury: dict | None) -> str:
    if not injury:
        return ""
    return f"[{injury.get('status', 'INJ')}]"


def _line_tag(line_info: dict | None) -> str:
    """Build a compact line descriptor like 'L1/PP1' from line info dict."""
    if not line_info:
        return ""
    parts = []
    ev = line_info.get("ev_line")
    if ev:
        parts.append(f"L{ev}")
    pp = line_info.get("pp_unit")
    if pp:
        parts.append(f"PP{pp}")
    return "/".join(parts)


# ---------------------------------------------------------------------------
# Roster
# ---------------------------------------------------------------------------


def format_roster(data: list[dict]) -> str:
    """Format fantasy roster with stats and fantasy points.

    Args:
        data: Output from get_my_roster().

    Returns:
        Formatted table string.
    """
    if not data:
        return "No players on roster."

    lines = []
    lines.append(f"{'Player':<22} {'Pos':>3} {'GP':>3} {'Key Stats':<20} "
                 f"{'FP':>6} {'FP/G':>5} {'Injury':<12}")
    lines.append(_divider(76))

    for p in data:
        name = (p["player_name"] or "")[:21]
        pos = p.get("position", "")
        gp = p.get("games_played", 0)
        fp = p.get("fantasy_points", 0.0)
        fpg = p.get("fpts_per_game", 0.0)
        inj = _injury_tag(p.get("injury"))

        if pos == "G":
            w = p.get("wins", 0)
            l = p.get("losses", 0)
            so = p.get("shutouts", 0)
            key_stats = f"{w}W {l}L {so}SO"
        else:
            g = p.get("goals", 0)
            a = p.get("assists", 0)
            h = p.get("hits", 0)
            b = p.get("blocks", 0)
            key_stats = f"{g}G {a}A {h}H {b}B"

        lines.append(f"{name:<22} {pos:>3} {gp:>3} {key_stats:<20} "
                     f"{fp:>6.1f} {fpg:>5.2f} {inj:<12}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Free agents
# ---------------------------------------------------------------------------


def format_free_agents(data: list[dict]) -> str:
    """Format free agent search results.

    Args:
        data: Output from search_free_agents().

    Returns:
        Formatted table string.
    """
    if not data:
        return "No free agents found."

    lines = []
    lines.append(f"{'Player':<22} {'Team':>4} {'Pos':>3} {'GP':>3} "
                 f"{'Key Stats':<20} {'FP/G':>5} {'Line':>7}")
    lines.append(_divider(70))

    for p in data:
        tag = _injury_tag(p.get("injury"))
        name = ((p["player_name"] or "")[:21] + f" {tag}").rstrip()
        team = p.get("team", "") or ""
        pos = p.get("position", "")
        gp = p.get("games_played", 0)
        fpg = p.get("fpts_per_game", 0.0)

        if pos == "G":
            w = p.get("wins", 0)
            so = p.get("shutouts", 0)
            gaa = p.get("gaa", 0.0)
            key_stats = f"{w}W {so}SO {gaa:.2f}GAA"
        else:
            g = p.get("goals", 0)
            a = p.get("assists", 0)
            h = p.get("hits", 0)
            b = p.get("blocks", 0)
            key_stats = f"{g}G {a}A {h}H {b}B"

        line_info = {"ev_line": p.get("ev_line"), "pp_unit": p.get("pp_unit")}
        lt = _line_tag(line_info) if line_info.get("ev_line") or line_info.get("pp_unit") else ""

        lines.append(f"{name:<22} {team:>4} {pos:>3} {gp:>3} "
                     f"{key_stats:<20} {fpg:>5.2f} {lt:>7}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Player card
# ---------------------------------------------------------------------------


def format_player_card(data: dict) -> str:
    """Format detailed player stats card.

    Args:
        data: Output from get_player_stats().

    Returns:
        Formatted player card string.
    """
    if not data:
        return "Player not found."

    p = data["player"]
    lines = []
    lines.append(f"=== {p['full_name']} ({p['team_abbrev']} - {p['position']}) ===")

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
        pos = p.get("position", "")
        if ev:
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
        toi_str = f"{toi_pg // 60}:{toi_pg % 60:02d}" if toi_pg else "0:00"
        ppg = stats.get("powerplay_goals", 0)
        ppp = stats.get("powerplay_points", 0)
        shg = stats.get("shorthanded_goals", 0)
        shp = stats.get("shorthanded_points", 0)
        parts = [f"TOI/G: {toi_str}"]
        pp_toi = stats.get("pp_toi", 0)
        if pp_toi:
            pp_min = f"{pp_toi // 60}:{pp_toi % 60:02d}"
            parts.append(f"PP TOI: {pp_min}")
        parts.append(f"PPG:{ppg} PPP:{ppp}")
        if shg or shp:
            parts.append(f"SHG:{shg} SHP:{shp}")
        lines.append("  " + "  ".join(parts))

    # Game log
    log = data.get("game_log", [])
    if log:
        lines.append("")
        lines.append("Recent Games:")
        if data["is_goalie"]:
            lines.append(f"  {'Date':<12} {'W':>2} {'L':>2} {'OTL':>3} "
                         f"{'SO':>2} {'SV':>3} {'GA':>2} {'FP':>5}")
            for g in log:
                lines.append(
                    f"  {g['game_date']:<12} {g['wins']:>2} {g['losses']:>2} "
                    f"{g['ot_losses']:>3} {g['shutouts']:>2} {g['saves']:>3} "
                    f"{g['goals_against']:>2} {g['fantasy_points']:>5.1f}"
                )
        else:
            lines.append(f"  {'Date':<12} {'G':>2} {'A':>2} {'Pts':>3} "
                         f"{'H':>3} {'B':>3} {'SOG':>3} {'TOI':>6} {'FP':>5}")
            for g in log:
                g_toi = g.get("toi", 0)
                toi_str = f"{g_toi // 60}:{g_toi % 60:02d}" if g_toi else "0:00"
                lines.append(
                    f"  {g['game_date']:<12} {g['goals']:>2} {g['assists']:>2} "
                    f"{g['points']:>3} {g['hits']:>3} {g['blocks']:>3} "
                    f"{g['shots']:>3} {toi_str:>6} {g['fantasy_points']:>5.1f}"
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
            # Strip redundant "Player Name: " prefix since we're already
            # in the player's card context
            if headline.startswith(f"{player_name}: "):
                headline = headline[len(player_name) + 2:]
            lines.append(f"  [{date_str}] {headline}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------


def format_comparison(data: list[dict]) -> str:
    """Format side-by-side player comparison.

    Args:
        data: Output from compare_players().

    Returns:
        Formatted comparison string.
    """
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
                row += f" {f'{val_int // 60}:{val_int % 60:02d}':>{col_width}}"
            elif isinstance(val, float):
                row += f" {val:>{col_width}.2f}"
            else:
                row += f" {str(val):>{col_width}}"
        lines.append(row)

    # Line combination row
    line_row = f"{'Line':<{label_width}}"
    has_any_line = False
    for p in data:
        tag = _line_tag(p.get("line_context"))
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
    """Format player trend analysis.

    Args:
        data: Output from get_player_trends().

    Returns:
        Formatted trend string.
    """
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
        arrow = "^^ HOT"
    elif trend == "cold":
        arrow = "vv COLD"
    else:
        arrow = "-- Neutral"

    lines.append(f"\n  Trend: {arrow}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def format_standings(data: list[dict]) -> str:
    """Format fantasy league standings.

    Args:
        data: Output from get_league_standings().

    Returns:
        Formatted standings table.
    """
    if not data:
        return "No standings data."

    lines = []
    lines.append(f"{'Rk':>3} {'Team':<22} {'GP':>4} {'PF':>8} "
                 f"{'FP/G':>6} {'GB':>6} {'Streak':<6}")
    lines.append(_divider(60))

    for s in data:
        name = (s.get("team_name") or s.get("short_name") or "")[:21]
        gb = s.get("games_back", 0.0)
        gb_str = "  -" if gb == 0.0 else f"{gb:.1f}"
        lines.append(
            f"{s.get('rank', 0):>3} {name:<22} "
            f"{s.get('games_played', 0):>4} "
            f"{s.get('points_for', 0.0):>8.1f} "
            f"{s.get('fantasy_points_per_game', 0.0):>6.2f} "
            f"{gb_str:>6} "
            f"{s.get('streak', ''):>6}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------


def format_schedule(data: dict) -> str:
    """Format schedule analysis.

    Args:
        data: Output from get_schedule_analysis().

    Returns:
        Formatted schedule string.
    """
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

    lines.append(f"  {'Date':<12} {'Opp':>4} {'H/A':>4} {'B2B':>4}")
    lines.append("  " + _divider(28))

    for g in data.get("games", []):
        gd = g["game_date"]
        b2b = " *" if gd in b2b_dates else ""
        lines.append(
            f"  {gd:<12} {g.get('opponent', ''):>4} "
            f"{g.get('home_away', ''):>4}{b2b}"
        )

    b2b_count = len(data.get("back_to_backs", []))
    if b2b_count:
        lines.append(f"\n  * {b2b_count} back-to-back(s)")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# News
# ---------------------------------------------------------------------------


def format_news(data: list[dict]) -> str:
    """Format recent news items.

    Args:
        data: Output from get_recent_news().

    Returns:
        Formatted news list.
    """
    if not data:
        return "No recent news."

    lines = []
    for n in data:
        date_str = (n.get("published_at") or "")[:10]
        player = n.get("player_name") or "Unknown"
        headline = n.get("headline") or ""
        # Headlines are stored as "Player Name: headline text" — strip the
        # prefix to avoid duplication since we already show the player name.
        if headline.startswith(f"{player}: "):
            headline = headline[len(player) + 2:]
        lines.append(f"[{date_str}] {player}: {headline}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Injuries
# ---------------------------------------------------------------------------


def format_injuries(data: list[dict]) -> str:
    """Format injury report.

    Args:
        data: Output from get_injuries().

    Returns:
        Formatted injury table.
    """
    if not data:
        return "No injuries to report."

    lines = []
    lines.append(f"{'Player':<22} {'Team':>4} {'Pos':>3} {'Injury':<16} "
                 f"{'Status':<12} {'Updated':<10}")
    lines.append(_divider(72))

    for i in data:
        name = (i.get("full_name") or "")[:21]
        lines.append(
            f"{name:<22} {(i.get('team_abbrev') or ''):>4} "
            f"{(i.get('position') or ''):>3} "
            f"{(i.get('injury_type') or ''):>16} "
            f"{(i.get('status') or ''):<12} "
            f"{(i.get('updated_at') or ''):<10}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Advanced analysis formatters
# ---------------------------------------------------------------------------


def format_team_roster(data: dict) -> str:
    """Format another team's roster for trade analysis.

    Args:
        data: Output from get_team_roster().

    Returns:
        Formatted team roster string.
    """
    if not data:
        return "Team not found."

    info = data["team_info"]
    roster = data["roster"]

    lines = []
    lines.append(f"=== {info['team_name']} ({info['short_name']}) ===")
    lines.append(f"  Rank: #{info.get('rank', '?')} | "
                 f"PF: {info.get('points_for', 0):.1f} | "
                 f"FP/G: {info.get('fpg', 0):.2f}")
    lines.append("")
    lines.append(format_roster(roster))

    return "\n".join(lines)


def format_trade_suggestions(data: dict) -> str:
    """Format trade suggestions between two teams.

    Args:
        data: Output from suggest_trades().

    Returns:
        Formatted trade suggestion string.
    """
    if not data:
        return "Could not generate trade suggestions."

    opp = data["opponent"]
    suggestions = data.get("suggestions", [])

    lines = []
    lines.append(f"=== Trade Suggestions vs {opp['name']} "
                 f"(#{opp.get('rank', '?')}) ===")
    lines.append("")

    # Show position avg comparison
    lines.append(f"{'Position':<10} {'My Avg FP/G':>11} {'Their Avg FP/G':>14}")
    lines.append(_divider(38))
    my_avgs = data["my_team"]["avg_fpg"]
    opp_avgs = opp["avg_fpg"]
    for g in ("F", "D", "G"):
        lines.append(f"{g:<10} {my_avgs.get(g, 0):>11.2f} {opp_avgs.get(g, 0):>14.2f}")
    lines.append("")

    if not suggestions:
        lines.append("No mutually beneficial trades identified.")
        return "\n".join(lines)

    lines.append(f"{'Send':<22} {'Pos':>3} {'FP/G':>5}   "
                 f"{'Receive':<22} {'Pos':>3} {'FP/G':>5}   "
                 f"{'My +/-':>6}")
    lines.append(_divider(76))

    for s in suggestions:
        lines.append(
            f"{s['send_player']:<22} {s['send_position']:>3} "
            f"{s['send_fpg']:>5.2f}   "
            f"{s['receive_player']:<22} {s['receive_position']:>3} "
            f"{s['receive_fpg']:>5.2f}   "
            f"{s['my_upgrade']:>+6.2f}"
        )

    return "\n".join(lines)


def format_trade_targets(data: list[dict]) -> str:
    """Format buy-low trade target candidates.

    Args:
        data: Output from get_trade_candidates().

    Returns:
        Formatted table string.
    """
    if not data:
        return "No buy-low trade targets found matching criteria."

    lines = []
    lines.append("=== Buy-Low Trade Targets ===")
    lines.append("")
    lines.append(f"{'Player':<22} {'Owner':<18} {'Pos':>3} {'GP':>3} "
                 f"{'Szn FP/G':>8} {'L7 FP/G':>7} {'TOI/G':>6} {'Line':>7} {'Trend':>10}")
    lines.append(_divider(90))

    for p in data:
        name = (p.get("player_name") or "")[:21]
        owner = (p.get("owner_team_name") or "")[:17]
        rank = p.get("owner_rank")
        rank_tag = f" (#{rank})" if rank else ""
        owner_display = (owner + rank_tag)[:17]
        tpg = p.get("toi_per_game", 0)
        pos = p.get("position", "")
        toi_str = f"{tpg // 60}:{tpg % 60:02d}" if tpg and pos != "G" else ("  -  " if pos == "G" else "0:00")
        lt = _line_tag(p.get("line_info"))
        signal = p.get("signal", "trending_up")
        if signal == "high_toi_underperformer":
            trend_str = "[HIGH TOI]"
        else:
            trend_pct = p.get("trend_pct", 0)
            trend_str = f"+{trend_pct:.0f}%"
        lines.append(
            f"{name:<22} {owner_display:<18} {p.get('position', ''):>3} "
            f"{p.get('games_played', 0):>3} "
            f"{p.get('season_fpg', 0.0):>8.2f} "
            f"{p.get('recent_7_fpg', 0.0):>7.2f} "
            f"{toi_str:>6} {lt:>7} {trend_str:>10}"
        )

    return "\n".join(lines)


def format_drop_candidates(data: list[dict]) -> str:
    """Format drop candidate analysis.

    Args:
        data: Output from get_drop_candidates().

    Returns:
        Formatted table string.
    """
    if not data:
        return "No drop candidates identified."

    lines = []
    lines.append("=== Drop Candidates (Weakest Roster Players) ===")
    lines.append("")
    lines.append(f"{'Player':<22} {'Pos':>3} {'GP':>3} {'Szn FP/G':>8} "
                 f"{'L14 FP/G':>8} {'Line':>7} {'Trend':<8} {'Injury':<12}")
    lines.append(_divider(80))

    for p in data:
        name = (p.get("player_name") or "")[:21]
        trend = p.get("trend", "neutral")
        if trend == "hot":
            trend_str = "^^ HOT"
        elif trend == "cold":
            trend_str = "vv COLD"
        else:
            trend_str = "-- Neut"
        inj = _injury_tag(p.get("injury"))
        lt = _line_tag(p.get("line_info"))
        lines.append(
            f"{name:<22} {p.get('position', ''):>3} "
            f"{p.get('games_played', 0):>3} "
            f"{p.get('season_fpg', 0.0):>8.2f} "
            f"{p.get('recent_14_fpg', 0.0):>8.2f} "
            f"{lt:>7} {trend_str:<8} {inj:<12}"
        )
        if p.get("recent_news"):
            news_text = p["recent_news"]
            # Strip "Player Name: " prefix from headline to avoid redundancy
            pname = p.get("player_name", "")
            if news_text.startswith(f"{pname}: "):
                news_text = news_text[len(pname) + 2:]
            # Truncate at word boundary
            if len(news_text) > 70:
                news_text = news_text[:67].rsplit(" ", 1)[0] + "..."
            lines.append(f"  {'':>22} News: {news_text}")

    return "\n".join(lines)


def format_roster_moves(drops: list[dict], pickups: list[dict]) -> str:
    """Format combined drop and pickup recommendations.

    Args:
        drops: Output from get_drop_candidates().
        pickups: Output from get_pickup_recommendations().

    Returns:
        Formatted two-section string.
    """
    lines = []

    # Section 1: Drop Candidates
    lines.append("=== RECOMMENDED DROPS ===")
    lines.append("")
    if drops:
        lines.append(f"{'Player':<22} {'Pos':>3} {'Szn FP/G':>8} "
                     f"{'L14 FP/G':>8} {'Trend':<8}")
        lines.append(_divider(54))
        for p in drops:
            name = (p.get("player_name") or "")[:21]
            trend = p.get("trend", "neutral")
            trend_str = ("^^ HOT" if trend == "hot"
                         else "vv COLD" if trend == "cold"
                         else "-- Neut")
            inj = _injury_tag(p.get("injury"))
            display_name = f"{name} {inj}".strip()[:21]
            lines.append(
                f"{display_name:<22} {p.get('position', ''):>3} "
                f"{p.get('season_fpg', 0.0):>8.2f} "
                f"{p.get('recent_14_fpg', 0.0):>8.2f} "
                f"{trend_str:<8}"
            )
    else:
        lines.append("  No clear drop candidates.")

    lines.append("")

    # Section 2: Pickup Recommendations
    lines.append("=== RECOMMENDED PICKUPS ===")
    lines.append("")
    if pickups:
        lines.append(f"{'Pickup':<20} {'Pos':>3} {'FP/G':>5}  "
                     f"{'Drop':<20} {'FP/G':>5} {'Upg':>7}  {'Reason':<40}")
        lines.append(_divider(106))
        for r in pickups:
            pickup = (r.get("pickup_name") or "")[:19]
            drop = (r.get("drop_name") or "")[:19]
            lines.append(
                f"{pickup:<20} {r.get('pickup_position', ''):>3} "
                f"{r.get('pickup_fpg', 0.0):>5.2f}  "
                f"{drop:<20} {r.get('drop_fpg', 0.0):>5.2f} "
                f"{r.get('fpg_upgrade', 0.0):>+7.2f}  "
                f"{(r.get('reason') or '')[:40]:<40}"
            )
    else:
        lines.append("  No clear pickup recommendations.")

    return "\n".join(lines)
