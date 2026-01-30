"""Output formatting utilities for Fantrax CLI."""

import json
from typing import List

from rich.console import Console
from rich.table import Table


def format_teams_table(teams: List, league_name: str = None) -> None:
    """
    Display teams in a formatted table using Rich.

    Args:
        teams: List of Team objects from FantraxAPI.
        league_name: Optional league name to display in title.
    """
    console = Console()

    # Create table
    table = Table(title=f"Teams - {league_name}" if league_name else "Teams", show_header=True, header_style="bold magenta")

    # Add columns
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Team ID", style="cyan", min_width=12)
    table.add_column("Team Name", style="green", min_width=20)
    table.add_column("Short", style="yellow", width=10)

    # Add rows
    for idx, team in enumerate(teams, start=1):
        table.add_row(
            str(idx),
            team.id,
            team.name,
            team.short
        )

    # Print table
    console.print(table)
    console.print(f"\n[bold]Total teams:[/bold] {len(teams)}")


def format_teams_json(teams: List, league_id: str, league_name: str = None, year: int = None) -> None:
    """
    Display teams in JSON format.

    Args:
        teams: List of Team objects from FantraxAPI.
        league_id: League ID.
        league_name: Optional league name.
        year: Optional league year.
    """
    console = Console()

    # Build JSON structure
    output = {
        "league_id": league_id,
    }

    if league_name:
        output["league_name"] = league_name
    if year:
        output["year"] = year

    output["teams"] = [
        {
            "id": team.id,
            "name": team.name,
            "short": team.short
        }
        for team in teams
    ]

    # Print formatted JSON
    console.print_json(json.dumps(output, indent=2))


def format_teams_simple(teams: List) -> None:
    """
    Display teams in simple text format.

    Args:
        teams: List of Team objects from FantraxAPI.
    """
    for team in teams:
        print(f"{team.name} ({team.short})")


def format_roster_table(roster, team_name: str = None, recent_stats: dict = None, last_n_days: int = None) -> None:
    """
    Display roster in a formatted table using Rich.

    Args:
        roster: Roster object from FantraxAPI.
        team_name: Optional team name to display in title.
        recent_stats: Optional dictionary of recent player stats.
        last_n_days: Optional number of days for recent stats.
    """
    console = Console()

    # Create table
    if recent_stats and last_n_days:
        title = f"Roster - {team_name} (Last {last_n_days} Days)" if team_name else f"Roster (Last {last_n_days} Days)"
    else:
        title = f"Roster - {team_name}" if team_name else "Roster"
    table = Table(title=title, show_header=True, header_style="bold magenta")

    # Add columns
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Pos", style="cyan", width=6)
    table.add_column("Roster Status", style="magenta", width=13)
    table.add_column("Inj Report", style="red", width=10)
    table.add_column("Player Name", style="green", min_width=20)
    table.add_column("Salary", style="blue", width=10, justify="right")

    if recent_stats:
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
            roster_status = "Reserve"
        elif status_id == "3":
            roster_status = "IR"
        else:
            roster_status = "Unknown"

        # Determine injury report from player flags
        injury_report = "-"
        if row.player:
            if row.player.suspended:
                injury_report = "Suspended"
            elif row.player.injured_reserve:
                injury_report = "IR"
            elif row.player.out:
                injury_report = "Out"
            elif row.player.day_to_day:
                injury_report = "DTD"

        # Handle salary with backward compatibility
        salary_value = getattr(row, 'salary', None)
        salary = f"${salary_value:,.0f}" if salary_value else "-"

        if recent_stats and row.player:
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


def format_roster_json(roster, team_id: str, team_name: str = None, recent_stats: dict = None, last_n_days: int = None) -> None:
    """
    Display roster in JSON format.

    Args:
        roster: Roster object from FantraxAPI.
        team_id: Team ID.
        team_name: Optional team name.
        recent_stats: Optional dictionary of recent player stats.
        last_n_days: Optional number of days for recent stats.
    """
    console = Console()

    # Build JSON structure
    output = {
        "team_id": team_id,
    }

    if team_name:
        output["team_name"] = team_name

    if recent_stats and last_n_days:
        output["stats_period"] = f"last_{last_n_days}_days"

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

    if recent_stats:
        output["players"] = [
            {
                "position": row.position.short_name,
                "roster_status": get_roster_status(row),
                "injury_report": get_injury_report(row),
                "player_name": row.player.name if row.player else None,
                "salary": getattr(row, 'salary', None),
                "games_played": recent_stats[row.player.id]['games_played'] if row.player and row.player.id in recent_stats else 0,
                "total_fantasy_points": recent_stats[row.player.id]['total_points'] if row.player and row.player.id in recent_stats else 0.0,
                "fantasy_points_per_game": recent_stats[row.player.id]['fpg'] if row.player and row.player.id in recent_stats else 0.0,
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


def format_roster_simple(roster, recent_stats: dict = None) -> None:
    """
    Display roster in simple text format.

    Args:
        roster: Roster object from FantraxAPI.
        recent_stats: Optional dictionary of recent player stats.
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

        if recent_stats and row.player and row.player.id in recent_stats:
            stats = recent_stats[row.player.id]
            print(f"{row.position.short_name} ({roster_status}): {player_name}{injury_report} - {salary_str} ({stats['games_played']}G, {stats['fpg']:.2f} FP/G)")
        else:
            print(f"{row.position.short_name} ({roster_status}): {player_name}{injury_report} - {salary_str}")
