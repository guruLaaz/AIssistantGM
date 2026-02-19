"""Claude tool-use definitions and dispatch for the fantasy hockey assistant."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from assistant import queries, formatters


@dataclass
class SessionContext:
    """Runtime context passed to every tool invocation."""

    conn: sqlite3.Connection
    team_id: str
    season: str


# ---------------------------------------------------------------------------
# Tool definitions (Anthropic SDK format)
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "get_my_roster",
        "description": (
            "Get the user's fantasy roster with player stats and calculated "
            "fantasy points. Returns every rostered player with GP, key stats, "
            "FP, FP/G, and injury status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sort_by": {
                    "type": "string",
                    "enum": ["fantasy_points", "fpts_per_game", "goals", "assists", "points"],
                    "description": "Sort the roster by this stat. Default: fpts_per_game.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_roster_analysis",
        "description": (
            "Analyze the user's roster: position breakdown, average FP/G by "
            "position group (F/D/G), bottom 3 performers, and injured players."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "search_free_agents",
        "description": (
            "Search for the best available free agents not on any fantasy roster. "
            "Can filter by position and minimum games played."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "position": {
                    "type": "string",
                    "enum": ["any", "F", "C", "L", "R", "D", "G"],
                    "description": "Position filter. 'any' for all positions. Default: any.",
                },
                "sort_by": {
                    "type": "string",
                    "enum": ["fpts_per_game", "fantasy_points"],
                    "description": "Sort key. Default: fpts_per_game.",
                },
                "min_games": {
                    "type": "integer",
                    "description": "Minimum games played to include. Default: 10.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results. Default: 20.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_player_stats",
        "description": (
            "Get detailed stats for a single player: season totals, recent "
            "game log, injury status, and recent news."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "Player name to look up (fuzzy matching supported).",
                },
                "num_recent_games": {
                    "type": "integer",
                    "description": "Number of recent games to show in the game log. Default: 5.",
                },
            },
            "required": ["player_name"],
        },
    },
    {
        "name": "compare_players",
        "description": (
            "Side-by-side comparison of 2-5 players showing season stats "
            "and fantasy points."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_names": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 2,
                    "maxItems": 5,
                    "description": "List of 2-5 player names to compare.",
                },
            },
            "required": ["player_names"],
        },
    },
    {
        "name": "get_player_trends",
        "description": (
            "Analyze a player's recent performance trends. Shows FP/G averages "
            "over last 7 games, last 14 games, and full season, plus a "
            "hot/cold/neutral trend indicator."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": "Player name to analyze.",
                },
            },
            "required": ["player_name"],
        },
    },
    {
        "name": "get_news_briefing",
        "description": (
            "Get recent player news. Can show news for a specific player, "
            "all players on the user's roster, or league-wide news."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "player_name": {
                    "type": "string",
                    "description": (
                        "Player name for player-specific news. "
                        "Omit to get news for the user's roster."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of news items. Default: 15.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_schedule_analysis",
        "description": (
            "Analyze upcoming schedule for a team or player. Shows game count, "
            "opponents, home/away, and back-to-back situations."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "team_or_player": {
                    "type": "string",
                    "description": (
                        "3-letter NHL team abbreviation (e.g. 'TOR', 'MTL') "
                        "or a player name."
                    ),
                },
                "days_ahead": {
                    "type": "integer",
                    "description": "Number of days to look ahead. Default: 14.",
                },
            },
            "required": ["team_or_player"],
        },
    },
    {
        "name": "get_league_standings",
        "description": (
            "Get the current fantasy league standings with W/L record, "
            "points, points for/against, and streak."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "get_injuries",
        "description": (
            "Get injury report. Can show injuries for the user's roster, "
            "a specific NHL team, or all injuries league-wide."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["my_roster", "all", "team"],
                    "description": (
                        "Scope of the injury report. "
                        "'my_roster' = user's fantasy roster, "
                        "'all' = all injuries, "
                        "'team' = specific NHL team. Default: my_roster."
                    ),
                },
                "team": {
                    "type": "string",
                    "description": (
                        "3-letter NHL team abbreviation. Required when scope='team'."
                    ),
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_team_roster",
        "description": (
            "View any fantasy team's full roster with player stats and "
            "fantasy points. Use this to scout an opponent's team before "
            "proposing a trade. Accepts team name or short name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "team_name": {
                    "type": "string",
                    "description": (
                        "Fantasy team name or short name to look up "
                        "(fuzzy matching supported)."
                    ),
                },
            },
            "required": ["team_name"],
        },
    },
    {
        "name": "suggest_trades",
        "description": (
            "Suggest player-for-player trades with a specific opponent. "
            "Analyzes both rosters to find swaps where both teams improve — "
            "trading away surplus to fill a need. Shows FP/G upgrade for "
            "each side."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "opponent_team_name": {
                    "type": "string",
                    "description": (
                        "Opponent's team name or short name."
                    ),
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of trade suggestions. Default: 5.",
                },
            },
            "required": ["opponent_team_name"],
        },
    },
    {
        "name": "get_trade_targets",
        "description": (
            "Find buy-low trade targets on other fantasy teams — players "
            "trending up (last 7 games FP/G > season FP/G by 20%+). "
            "Includes the owner team name and standings rank to help "
            "identify GMs who might be willing to sell."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of trade targets to return. Default: 15.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_roster_moves",
        "description": (
            "Get combined drop candidates and pickup recommendations. "
            "Shows the weakest players on the user's roster (based on "
            "last 14 games) and the best available free agent replacements "
            "at each position, with the FP/G upgrade for each swap."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


def dispatch_tool(tool_name: str, tool_input: dict, context: SessionContext) -> str:
    """Execute a tool call and return the formatted result string.

    Args:
        tool_name: Name of the tool to execute.
        tool_input: Input parameters from Claude.
        context: Session context with conn, team_id, season.

    Returns:
        Formatted result string to send back as tool_result.
    """
    conn = context.conn
    team_id = context.team_id
    season = context.season

    try:
        if tool_name == "get_my_roster":
            data = queries.get_my_roster(conn, team_id, season)
            sort_by = tool_input.get("sort_by", "fpts_per_game")
            data.sort(key=lambda p: p.get(sort_by, 0), reverse=True)
            return formatters.format_roster(data)

        if tool_name == "get_roster_analysis":
            data = queries.get_roster_analysis(conn, team_id, season)
            lines = []
            lines.append("=== Roster Analysis ===")
            lines.append("")
            lines.append("Position Breakdown:")
            for pos, count in data["position_counts"].items():
                avg = data["avg_fpts_by_position"].get(pos, 0.0)
                lines.append(f"  {pos}: {count} players, {avg:.2f} avg FP/G")
            lines.append("")
            lines.append("Bottom Performers (by FP/G):")
            for p in data["bottom_performers"]:
                lines.append(
                    f"  {p['player_name']:<22} {p['position']:>3} "
                    f"{p['fpts_per_game']:.2f} FP/G  ({p['games_played']} GP)"
                )
            if data["injured_players"]:
                lines.append("")
                lines.append("Injured Players:")
                for p in data["injured_players"]:
                    inj = p.get("injury", {})
                    lines.append(
                        f"  {p['player_name']:<22} "
                        f"{inj.get('injury_type', 'Unknown')} - "
                        f"{inj.get('status', 'Unknown')}"
                    )
            else:
                lines.append("")
                lines.append("No injured players on roster.")
            return "\n".join(lines)

        if tool_name == "search_free_agents":
            data = queries.search_free_agents(
                conn,
                season,
                position=tool_input.get("position", "any"),
                sort_by=tool_input.get("sort_by", "fpts_per_game"),
                min_games=tool_input.get("min_games", 10),
                limit=tool_input.get("limit", 20),
            )
            return formatters.format_free_agents(data)

        if tool_name == "get_player_stats":
            player_name = tool_input["player_name"]
            recent_games = tool_input.get("num_recent_games", 5)
            data = queries.get_player_stats(conn, player_name, season, recent_games)
            if data is None:
                return f"Player not found: '{player_name}'. Try a different spelling or name."
            return formatters.format_player_card(data)

        if tool_name == "compare_players":
            player_names = tool_input["player_names"]
            data = queries.compare_players(conn, player_names, season)
            if not data:
                return (
                    "Could not find any of the requested players: "
                    + ", ".join(player_names)
                )
            found = [p["player"]["full_name"] for p in data]
            missing = [n for n in player_names if not any(
                n.lower() in f.lower() for f in found
            )]
            result = formatters.format_comparison(data)
            if missing:
                result += f"\n\nNote: Could not find: {', '.join(missing)}"
            return result

        if tool_name == "get_player_trends":
            player_name = tool_input["player_name"]
            data = queries.get_player_trends(conn, player_name, season)
            if data is None:
                return f"Player not found: '{player_name}'. Try a different spelling or name."
            return formatters.format_trends(data)

        if tool_name == "get_news_briefing":
            player_name = tool_input.get("player_name")
            limit = tool_input.get("limit", 15)
            if player_name:
                data = queries.get_recent_news(conn, player_name=player_name, limit=limit)
            else:
                data = queries.get_recent_news(conn, team_id=team_id, limit=limit)
            return formatters.format_news(data)

        if tool_name == "get_schedule_analysis":
            team_or_player = tool_input["team_or_player"]
            days_ahead = tool_input.get("days_ahead", 14)
            data = queries.get_schedule_analysis(conn, team_or_player, season, days_ahead)
            if data is None:
                return f"Could not find team or player: '{team_or_player}'."
            return formatters.format_schedule(data)

        if tool_name == "get_league_standings":
            data = queries.get_league_standings(conn)
            return formatters.format_standings(data)

        if tool_name == "get_injuries":
            scope = tool_input.get("scope", "my_roster")
            team = tool_input.get("team")
            if scope == "my_roster":
                data = queries.get_injuries(conn, scope="my_roster", team_id=team_id)
            elif scope == "team":
                if not team:
                    return "Please specify a team abbreviation (e.g. 'TOR', 'MTL')."
                data = queries.get_injuries(conn, scope="team", team_id=team)
            else:
                data = queries.get_injuries(conn, scope="all")
            return formatters.format_injuries(data)

        if tool_name == "get_team_roster":
            team_name = tool_input["team_name"]
            data = queries.get_team_roster(conn, team_name, season)
            if data is None:
                return f"Team not found: '{team_name}'. Try a different name."
            return formatters.format_team_roster(data)

        if tool_name == "suggest_trades":
            opponent = tool_input["opponent_team_name"]
            limit = tool_input.get("limit", 5)
            data = queries.suggest_trades(conn, team_id, opponent, season, limit)
            if data is None:
                return f"Team not found: '{opponent}'. Try a different name."
            return formatters.format_trade_suggestions(data)

        if tool_name == "get_trade_targets":
            limit = tool_input.get("limit", 15)
            data = queries.get_trade_candidates(conn, team_id, season, limit=limit)
            return formatters.format_trade_targets(data)

        if tool_name == "get_roster_moves":
            drops = queries.get_drop_candidates(conn, team_id, season)
            pickups = queries.get_pickup_recommendations(conn, team_id, season)
            return formatters.format_roster_moves(drops, pickups)

        return f"Unknown tool: {tool_name}"

    except Exception as e:
        return f"Error executing {tool_name}: {e}"
