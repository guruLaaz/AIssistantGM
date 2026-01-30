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


def format_roster_table(roster, team_name: str = None) -> None:
    """
    Display roster in a formatted table using Rich.

    Args:
        roster: Roster object from FantraxAPI.
        team_name: Optional team name to display in title.
    """
    console = Console()

    # Create table
    title = f"Roster - {team_name}" if team_name else "Roster"
    table = Table(title=title, show_header=True, header_style="bold magenta")

    # Add columns
    table.add_column("#", style="dim", width=4, justify="right")
    table.add_column("Position", style="cyan", width=10)
    table.add_column("Player Name", style="green", min_width=20)
    table.add_column("FP Total", style="yellow", width=10, justify="right")
    table.add_column("FP/G", style="yellow", width=10, justify="right")

    # Add rows
    for idx, row in enumerate(roster.rows, start=1):
        player_name = row.player.name if row.player else "(Empty)"
        fp_total = f"{row.total_fantasy_points:.1f}" if row.total_fantasy_points else "-"
        fp_per_game = f"{row.fantasy_points_per_game:.2f}" if row.fantasy_points_per_game else "-"

        table.add_row(
            str(idx),
            row.position.short_name,
            player_name,
            fp_total,
            fp_per_game
        )

    # Print table
    console.print(table)
    console.print(f"\n[bold]Roster slots:[/bold] {len(roster.rows)}")
    console.print(f"[bold]Active:[/bold] {roster.active}/{roster.active_max}")
    console.print(f"[bold]Reserve:[/bold] {roster.reserve}/{roster.reserve_max}")
    console.print(f"[bold]Injured:[/bold] {roster.injured}/{roster.injured_max}")


def format_roster_json(roster, team_id: str, team_name: str = None) -> None:
    """
    Display roster in JSON format.

    Args:
        roster: Roster object from FantraxAPI.
        team_id: Team ID.
        team_name: Optional team name.
    """
    console = Console()

    # Build JSON structure
    output = {
        "team_id": team_id,
    }

    if team_name:
        output["team_name"] = team_name

    output["roster_stats"] = {
        "active": f"{roster.active}/{roster.active_max}",
        "reserve": f"{roster.reserve}/{roster.reserve_max}",
        "injured": f"{roster.injured}/{roster.injured_max}",
    }

    output["players"] = [
        {
            "position": row.position.short_name,
            "player_name": row.player.name if row.player else None,
            "total_fantasy_points": row.total_fantasy_points,
            "fantasy_points_per_game": row.fantasy_points_per_game,
        }
        for row in roster.rows
    ]

    # Print formatted JSON
    console.print_json(json.dumps(output, indent=2))


def format_roster_simple(roster) -> None:
    """
    Display roster in simple text format.

    Args:
        roster: Roster object from FantraxAPI.
    """
    for row in roster.rows:
        player_name = row.player.name if row.player else "(Empty)"
        print(f"{row.position.short_name}: {player_name}")
