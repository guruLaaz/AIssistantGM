"""Output formatting utilities for Fantrax CLI."""

import json
from typing import List

from rich.console import Console
from rich.table import Table


def _get_team_attr(team, attr: str, default=''):
    """Helper to get team attribute from object or dict."""
    if isinstance(team, dict):
        return team.get(attr, default)
    return getattr(team, attr, default)


def format_teams_table(teams: List, league_name: str = None, standings: List = None) -> None:
    """
    Display teams in a formatted table using Rich.

    Args:
        teams: List of Team objects from FantraxAPI or dicts with team data.
        league_name: Optional league name to display in title.
        standings: Optional list of standings dicts with rank, wins, losses, etc.
    """
    console = Console()

    # Build standings lookup by team_id if provided
    standings_by_team = {}
    if standings:
        for s in standings:
            standings_by_team[s.get('team_id') or s.get('id')] = s

    # Create table
    title = f"Standings - {league_name}" if standings and league_name else (f"Teams - {league_name}" if league_name else "Teams")
    table = Table(title=title, show_header=True, header_style="bold magenta")

    # Add columns
    if standings:
        table.add_column("Rank", style="bold cyan", width=4, justify="right")
        table.add_column("Team Name", style="green", min_width=20)
        table.add_column("Short", style="yellow", width=8)
        table.add_column("FPts", style="blue", width=10, justify="right")
        table.add_column("FP/G", style="bold yellow", width=8, justify="right")
        table.add_column("GP", style="cyan", width=4, justify="right")
    else:
        table.add_column("#", style="dim", width=4, justify="right")
        table.add_column("Team ID", style="cyan", min_width=12)
        table.add_column("Team Name", style="green", min_width=20)
        table.add_column("Short", style="yellow", width=10)

    # Sort by rank if we have standings
    if standings:
        sorted_teams = sorted(teams, key=lambda t: standings_by_team.get(_get_team_attr(t, 'id'), {}).get('rank', 999))
    else:
        sorted_teams = teams

    # Add rows
    for idx, team in enumerate(sorted_teams, start=1):
        team_id = _get_team_attr(team, 'id')
        team_name = _get_team_attr(team, 'name')
        team_short = _get_team_attr(team, 'short') or _get_team_attr(team, 'short_name', '')

        if standings and team_id in standings_by_team:
            s = standings_by_team[team_id]
            fpts = s.get('points_for', 0)
            # Use stored games_played and fpg if available, otherwise calculate
            gp = s.get('games_played', 0)
            fpg = s.get('fpg', 0)
            # Fallback calculation if not stored
            if gp == 0 and fpts > 0:
                gp = s.get('wins', 0) + s.get('losses', 0) + s.get('ties', 0)
                fpg = fpts / gp if gp > 0 else 0.0

            table.add_row(
                str(s.get('rank', idx)),
                team_name,
                team_short,
                f"{fpts:,.1f}" if fpts else "0.0",
                f"{fpg:.2f}",
                str(gp)
            )
        else:
            table.add_row(
                str(idx),
                team_id,
                team_name,
                team_short
            )

    # Print table
    console.print(table)
    console.print(f"\n[bold]Total teams:[/bold] {len(teams)}")


def format_teams_json(teams: List, league_id: str, league_name: str = None, year: int = None, standings: List = None) -> None:
    """
    Display teams in JSON format.

    Args:
        teams: List of Team objects from FantraxAPI or dicts with team data.
        league_id: League ID.
        league_name: Optional league name.
        year: Optional league year.
        standings: Optional list of standings dicts with rank, wins, losses, etc.
    """
    console = Console()

    # Build standings lookup by team_id if provided
    standings_by_team = {}
    if standings:
        for s in standings:
            standings_by_team[s.get('team_id') or s.get('id')] = s

    # Build JSON structure
    output = {
        "league_id": league_id,
    }

    if league_name:
        output["league_name"] = league_name
    if year:
        output["year"] = year

    teams_list = []
    for team in teams:
        team_id = _get_team_attr(team, 'id')
        team_name = _get_team_attr(team, 'name')
        team_short = _get_team_attr(team, 'short') or _get_team_attr(team, 'short_name', '')

        team_data = {
            "id": team_id,
            "name": team_name,
            "short": team_short
        }

        # Add standings data if available
        if team_id in standings_by_team:
            s = standings_by_team[team_id]
            fpts = s.get('points_for', 0)
            # Use stored games_played and fpg if available
            gp = s.get('games_played', 0)
            fpg = s.get('fpg', 0)
            # Fallback calculation if not stored
            if gp == 0 and fpts > 0:
                gp = s.get('wins', 0) + s.get('losses', 0) + s.get('ties', 0)
                fpg = round(fpts / gp, 2) if gp > 0 else 0.0
            team_data["standings"] = {
                "rank": s.get('rank'),
                "fpts": fpts,
                "fpg": fpg,
                "games_played": gp,
                "wins": s.get('wins', 0),
                "losses": s.get('losses', 0),
                "ties": s.get('ties', 0),
                "win_percentage": s.get('win_percentage', 0),
                "games_back": s.get('games_back', 0),
                "points_against": s.get('points_against', 0),
                "streak": s.get('streak'),
                "waiver_order": s.get('waiver_order')
            }

        teams_list.append(team_data)

    # Sort by rank if standings available
    if standings:
        teams_list.sort(key=lambda t: t.get('standings', {}).get('rank', 999))

    output["teams"] = teams_list

    # Print formatted JSON
    console.print_json(json.dumps(output, indent=2))


def format_teams_simple(teams: List, standings: List = None) -> None:
    """
    Display teams in simple text format.

    Args:
        teams: List of Team objects from FantraxAPI or dicts with team data.
        standings: Optional list of standings dicts with rank, wins, losses, etc.
    """
    # Build standings lookup by team_id if provided
    standings_by_team = {}
    if standings:
        for s in standings:
            standings_by_team[s.get('team_id') or s.get('id')] = s

    # Sort by rank if we have standings
    if standings:
        sorted_teams = sorted(teams, key=lambda t: standings_by_team.get(_get_team_attr(t, 'id'), {}).get('rank', 999))
    else:
        sorted_teams = teams

    for team in sorted_teams:
        team_id = _get_team_attr(team, 'id')
        team_name = _get_team_attr(team, 'name')
        team_short = _get_team_attr(team, 'short') or _get_team_attr(team, 'short_name', '')

        if team_id in standings_by_team:
            s = standings_by_team[team_id]
            fpts = s.get('points_for', 0)
            # Use stored games_played and fpg if available
            gp = s.get('games_played', 0)
            fpg = s.get('fpg', 0)
            # Fallback calculation if not stored
            if gp == 0 and fpts > 0:
                gp = s.get('wins', 0) + s.get('losses', 0) + s.get('ties', 0)
                fpg = fpts / gp if gp > 0 else 0.0
            print(f"{s.get('rank', '?')}. {team_name} ({team_short}) - {fpts:,.1f} FPts, {fpg:.2f} FP/G, {gp} GP")
        else:
            print(f"{team_name} ({team_short})")


def format_roster_table(roster, team_name: str = None, recent_stats: dict = None, last_n_days: int = None, recent_trends: dict = None) -> None:
    """
    Display roster in a formatted table using Rich.

    Args:
        roster: Roster object from FantraxAPI.
        team_name: Optional team name to display in title.
        recent_stats: Optional dictionary of recent player stats.
        last_n_days: Optional number of days for recent stats.
        recent_trends: Optional dictionary of 7/14/30 day trends.
    """
    console = Console()

    # Create table
    if recent_trends:
        title = f"Roster - {team_name} (with Recent Trends)" if team_name else "Roster (with Recent Trends)"
    elif recent_stats and last_n_days:
        title = f"Roster - {team_name} (Last {last_n_days} Days)" if team_name else f"Roster (Last {last_n_days} Days)"
    else:
        title = f"Roster - {team_name}" if team_name else "Roster"
    table = Table(title=title, show_header=True, header_style="bold magenta")

    # Add columns
    table.add_column("#", style="dim", width=3, justify="right")
    table.add_column("Pos", style="cyan", width=4)
    table.add_column("Status", style="magenta", width=7)
    table.add_column("Inj", style="red", width=4)
    table.add_column("Player Name", style="green", min_width=18)
    table.add_column("Salary", style="blue", width=10, justify="right")

    if recent_trends:
        # Add trend columns: 3 weeks (Sat-Fri), 14-day, 30-day
        table.add_column("W1 GP", style="yellow", width=5, justify="right")
        table.add_column("W1 FP/G", style="bold yellow", width=7, justify="right")
        table.add_column("W2 GP", style="yellow", width=5, justify="right")
        table.add_column("W2 FP/G", style="bold yellow", width=7, justify="right")
        table.add_column("W3 GP", style="yellow", width=5, justify="right")
        table.add_column("W3 FP/G", style="bold yellow", width=7, justify="right")
        table.add_column("14d", style="cyan", width=4, justify="right")
        table.add_column("30d", style="cyan", width=4, justify="right")
    elif recent_stats:
        table.add_column("Games", style="yellow", width=8, justify="right")
        table.add_column("FP Total", style="yellow", width=10, justify="right")
        table.add_column(f"FP/G (L{last_n_days}d)", style="bold yellow", width=12, justify="right")
    else:
        table.add_column("FP Total", style="yellow", width=10, justify="right")
        table.add_column("FP/G", style="yellow", width=10, justify="right")

    # Add rows
    for idx, row in enumerate(roster.rows, start=1):
        player_name = row.player.name if row.player else "(Empty)"
        position_short = row.position.short_name

        # Determine roster status based on status_id
        status_id = getattr(row, 'status_id', None)
        if status_id == "1":
            roster_status = "Active"
        elif status_id == "2":
            roster_status = "Res"
        elif status_id == "3":
            roster_status = "IR"
        else:
            roster_status = "?"

        # Determine injury report from player flags
        injury_report = "-"
        if row.player:
            if row.player.suspended:
                injury_report = "Susp"
            elif row.player.injured_reserve:
                injury_report = "IR"
            elif row.player.out:
                injury_report = "Out"
            elif row.player.day_to_day:
                injury_report = "DTD"

        # Handle salary with backward compatibility
        salary_value = getattr(row, 'salary', None)
        salary = f"${salary_value:,.0f}" if salary_value else "-"

        if recent_trends and row.player:
            # Show recent trends (3 weeks + 14d + 30d)
            player_id = row.player.id
            if player_id in recent_trends:
                trends = recent_trends[player_id]
                # Week 1 (most recent)
                gp_w1 = str(trends['week1']['games_played'])
                fpg_w1 = f"{trends['week1']['fpg']:.2f}"
                # Week 2
                gp_w2 = str(trends['week2']['games_played'])
                fpg_w2 = f"{trends['week2']['fpg']:.2f}"
                # Week 3
                gp_w3 = str(trends['week3']['games_played'])
                fpg_w3 = f"{trends['week3']['fpg']:.2f}"
                # 14-day and 30-day FP/G only (to save space)
                fpg_14 = f"{trends['14']['fpg']:.2f}"
                fpg_30 = f"{trends['30']['fpg']:.2f}"
            else:
                gp_w1 = gp_w2 = gp_w3 = "0"
                fpg_w1 = fpg_w2 = fpg_w3 = "0.00"
                fpg_14 = fpg_30 = "0.00"

            table.add_row(
                str(idx),
                position_short,
                roster_status,
                injury_report,
                player_name,
                salary,
                gp_w1,
                fpg_w1,
                gp_w2,
                fpg_w2,
                gp_w3,
                fpg_w3,
                fpg_14,
                fpg_30
            )
        elif recent_stats and row.player:
            # Show recent stats
            player_id = row.player.id
            if player_id in recent_stats:
                stats = recent_stats[player_id]
                games = str(stats['games_played'])
                fp_total = f"{stats['total_points']:.1f}"
                fp_per_game = f"{stats['fpg']:.2f}"
            else:
                games = "0"
                fp_total = "0.0"
                fp_per_game = "0.00"

            table.add_row(
                str(idx),
                position_short,
                roster_status,
                injury_report,
                player_name,
                salary,
                games,
                fp_total,
                fp_per_game
            )
        else:
            # Show season stats
            fp_total = f"{row.total_fantasy_points:.1f}" if row.total_fantasy_points else "-"
            fp_per_game = f"{row.fantasy_points_per_game:.2f}" if row.fantasy_points_per_game else "-"

            table.add_row(
                str(idx),
                position_short,
                roster_status,
                injury_report,
                player_name,
                salary,
                fp_total,
                fp_per_game
            )

    # Print table
    console.print(table)
    console.print(f"\n[bold]Roster slots:[/bold] {len(roster.rows)}")
    console.print(f"[bold]Active:[/bold] {roster.active}/{roster.active_max}")
    console.print(f"[bold]Reserve:[/bold] {roster.reserve}/{roster.reserve_max}")
    console.print(f"[bold]Injured:[/bold] {roster.injured}/{roster.injured_max}")


def format_roster_json(roster, team_id: str, team_name: str = None, recent_stats: dict = None, last_n_days: int = None, recent_trends: dict = None) -> None:
    """
    Display roster in JSON format.

    Args:
        roster: Roster object from FantraxAPI.
        team_id: Team ID.
        team_name: Optional team name.
        recent_stats: Optional dictionary of recent player stats.
        last_n_days: Optional number of days for recent stats.
        recent_trends: Optional dictionary of 7/14/30 day trends.
    """
    console = Console()

    # Build JSON structure
    output = {
        "team_id": team_id,
    }

    if team_name:
        output["team_name"] = team_name

    if last_n_days:
        output["stats_period"] = f"last_{last_n_days}_days"

    if recent_trends:
        output["stats_period"] = "recent_trends"

    output["roster_stats"] = {
        "active": f"{roster.active}/{roster.active_max}",
        "reserve": f"{roster.reserve}/{roster.reserve_max}",
        "injured": f"{roster.injured}/{roster.injured_max}",
    }

    def get_roster_status(row):
        """Helper to get roster status from status_id."""
        status_id = getattr(row, 'status_id', None)
        if status_id == "1":
            return "Active"
        elif status_id == "2":
            return "Reserve"
        elif status_id == "3":
            return "IR"
        else:
            return "Unknown"

    def get_injury_report(row):
        """Helper to get injury report from player flags."""
        if not row.player:
            return None
        if row.player.suspended:
            return "Suspended"
        elif row.player.injured_reserve:
            return "IR"
        elif row.player.out:
            return "Out"
        elif row.player.day_to_day:
            return "DTD"
        else:
            return None

    def get_trends_for_player(player_id, trends_dict):
        """Helper to get trends data for a player."""
        empty_week = {"games_played": 0, "total_points": 0.0, "fpg": 0.0, "start": "", "end": ""}
        empty_period = {"games_played": 0, "total_points": 0.0, "fpg": 0.0}
        if not trends_dict or player_id not in trends_dict:
            return {
                "week1": empty_week,
                "week2": empty_week,
                "week3": empty_week,
                "14_day": empty_period,
                "30_day": empty_period
            }
        t = trends_dict[player_id]
        return {
            "week1": t.get('week1', empty_week),
            "week2": t.get('week2', empty_week),
            "week3": t.get('week3', empty_week),
            "14_day": {"games_played": t['14']['games_played'], "total_points": t['14']['total_points'], "fpg": t['14']['fpg']},
            "30_day": {"games_played": t['30']['games_played'], "total_points": t['30']['total_points'], "fpg": t['30']['fpg']}
        }

    if recent_trends:
        output["players"] = [
            {
                "position": row.position.short_name,
                "roster_status": get_roster_status(row),
                "injury_report": get_injury_report(row),
                "player_name": row.player.name if row.player else None,
                "salary": getattr(row, 'salary', None),
                "trends": get_trends_for_player(row.player.id if row.player else None, recent_trends),
            }
            for row in roster.rows
        ]
    elif last_n_days:
        # Ensure recent_stats is a dict (could be None or empty)
        stats_dict = recent_stats if recent_stats else {}
        output["players"] = [
            {
                "position": row.position.short_name,
                "roster_status": get_roster_status(row),
                "injury_report": get_injury_report(row),
                "player_name": row.player.name if row.player else None,
                "salary": getattr(row, 'salary', None),
                "games_played": stats_dict[row.player.id]['games_played'] if row.player and row.player.id in stats_dict else 0,
                "total_fantasy_points": stats_dict[row.player.id]['total_points'] if row.player and row.player.id in stats_dict else 0.0,
                "fantasy_points_per_game": stats_dict[row.player.id]['fpg'] if row.player and row.player.id in stats_dict else 0.0,
            }
            for row in roster.rows
        ]
    else:
        output["players"] = [
            {
                "position": row.position.short_name,
                "roster_status": get_roster_status(row),
                "injury_report": get_injury_report(row),
                "player_name": row.player.name if row.player else None,
                "salary": getattr(row, 'salary', None),
                "total_fantasy_points": row.total_fantasy_points,
                "fantasy_points_per_game": row.fantasy_points_per_game,
            }
            for row in roster.rows
        ]

    # Print formatted JSON
    console.print_json(json.dumps(output, indent=2))


def format_roster_simple(roster, recent_stats: dict = None, recent_trends: dict = None) -> None:
    """
    Display roster in simple text format.

    Args:
        roster: Roster object from FantraxAPI.
        recent_stats: Optional dictionary of recent player stats.
        recent_trends: Optional dictionary of 7/14/30 day trends.
    """
    for row in roster.rows:
        player_name = row.player.name if row.player else "(Empty)"
        salary_value = getattr(row, 'salary', None)
        salary_str = f"${salary_value:,.0f}" if salary_value else "N/A"

        # Determine roster status from status_id
        status_id = getattr(row, 'status_id', None)
        if status_id == "1":
            roster_status = "Active"
        elif status_id == "2":
            roster_status = "Reserve"
        elif status_id == "3":
            roster_status = "IR"
        else:
            roster_status = "Unknown"

        # Determine injury report from player flags
        injury_report = ""
        if row.player:
            if row.player.suspended:
                injury_report = " [Suspended]"
            elif row.player.injured_reserve:
                injury_report = " [IR]"
            elif row.player.out:
                injury_report = " [Out]"
            elif row.player.day_to_day:
                injury_report = " [DTD]"

        if recent_trends and row.player and row.player.id in recent_trends:
            trends = recent_trends[row.player.id]
            w1 = trends.get('week1', {})
            w2 = trends.get('week2', {})
            w3 = trends.get('week3', {})
            print(f"{row.position.short_name} ({roster_status}): {player_name}{injury_report} - {salary_str} | W1:{w1.get('games_played', 0)}G/{w1.get('fpg', 0):.2f} W2:{w2.get('games_played', 0)}G/{w2.get('fpg', 0):.2f} W3:{w3.get('games_played', 0)}G/{w3.get('fpg', 0):.2f} 14d:{trends['14']['fpg']:.2f} 30d:{trends['30']['fpg']:.2f}")
        elif recent_stats and row.player and row.player.id in recent_stats:
            stats = recent_stats[row.player.id]
            print(f"{row.position.short_name} ({roster_status}): {player_name}{injury_report} - {salary_str} ({stats['games_played']}G, {stats['fpg']:.2f} FP/G)")
        else:
            print(f"{row.position.short_name} ({roster_status}): {player_name}{injury_report} - {salary_str}")
