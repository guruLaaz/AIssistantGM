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
