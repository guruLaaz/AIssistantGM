"""Teams command implementation."""

import traceback
import typer
from typing_extensions import Annotated
from rich.console import Console

from fantrax_cli.cli import OutputFormat
from fantrax_cli.config import load_config
from fantrax_cli.auth import get_authenticated_league
from fantrax_cli.display import format_teams_table, format_teams_json, format_teams_simple


def teams_command(
    ctx: typer.Context,
    format: Annotated[OutputFormat, typer.Option(
        "--format", "-f",
        help="Output format"
    )] = OutputFormat.table,
):
    """Display list of teams in the league."""
    console = Console()

    try:
        # Load configuration
        league_id_override = ctx.obj.get("league_id")
        config = load_config(league_id=league_id_override)

        # Authenticate and get League instance
        with console.status("[bold green]Authenticating with Fantrax..."):
            # Monkey-patch the League class to handle missing scoringPeriodList
            from fantraxapi.objs.league import League
            from fantraxapi.objs.scoring_period import ScoringPeriod
            from fantraxapi import api
            from datetime import datetime

            original_reset_info = League.reset_info

            def patched_reset_info(self):
                responses = api.get_init_info(self)
                self.name = responses[0]["fantasySettings"]["leagueName"]
                self.year = responses[0]["fantasySettings"]["subtitle"]
                self.start_date = datetime.fromtimestamp(responses[0]["fantasySettings"]["season"]["startDate"] / 1e3)
                self.end_date = datetime.fromtimestamp(responses[0]["fantasySettings"]["season"]["endDate"] / 1e3)
                from fantraxapi.objs.position import Position
                from fantraxapi.objs.status import Status
                self.positions = {k: Position(self, v) for k, v in responses[0]["positionMap"].items()}
                self.status = {k: Status(self, v) for k, v in responses[1]["allObjs"].items() if "name" in v}
                period_to_day_list = {}
                for s in responses[4]["displayedLists"]["periodList"]:
                    period, s = s.split(" ", maxsplit=1)
                    period_to_day_list[s[5:-1]] = int(period)
                self.scoring_dates = {}
                for day in responses[2]["dates"]:
                    from datetime import date
                    scoring_date = datetime.strptime(day["object1"], "%Y-%m-%d").date()
                    key = scoring_date.strftime("%b %d")
                    if "0" in key and not key.endswith("0"):
                        key = key.replace("0", "")
                    self.scoring_dates[period_to_day_list[key]] = scoring_date
                # Handle leagues that don't have scoringPeriodList
                if "displayedLists" in responses[3] and "scoringPeriodList" in responses[3]["displayedLists"]:
                    self.scoring_periods = {p["value"]: ScoringPeriod(self, p) for p in responses[3]["displayedLists"]["scoringPeriodList"] if p["name"] != "Full Season"}
                else:
                    self.scoring_periods = {}
                self._scoring_periods_lookup = None
                self._update_teams(responses[3]["fantasyTeams"])

            League.reset_info = patched_reset_info

            league = get_authenticated_league(
                config.league_id,
                config.username,
                config.password,
                config.cookie_path,
                config.min_request_interval
            )

        # Retrieve teams
        with console.status("[bold green]Fetching teams..."):
            teams = league.teams

        if not teams:
            console.print("[yellow]No teams found in this league.[/yellow]")
            return

        # Format and display based on selected format
        if format == OutputFormat.table:
            format_teams_table(teams, league_name=league.name)
        elif format == OutputFormat.json:
            format_teams_json(
                teams,
                league_id=config.league_id,
                league_name=league.name,
                year=league.year
            )
        else:  # simple format
            format_teams_simple(teams)

    except ValueError as e:
        # Configuration error
        console.print(f"[bold red]Configuration Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)
    except Exception as e:
        # General error
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        console.print("\n[dim]Full traceback:[/dim]")
        console.print(traceback.format_exc())
        raise typer.Exit(code=1)
