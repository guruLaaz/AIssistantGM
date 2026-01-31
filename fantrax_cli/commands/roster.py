"""Roster command implementation."""

import traceback
import typer
from typing import Optional
from typing_extensions import Annotated
from rich.console import Console

from fantrax_cli.cli import OutputFormat
from fantrax_cli.config import load_config
from fantrax_cli.auth import get_authenticated_league
from fantrax_cli.display import format_roster_table, format_roster_json, format_roster_simple
from fantrax_cli.stats import calculate_recent_fpg, calculate_recent_trends


def roster_command(
    ctx: typer.Context,
    team_identifier: Annotated[str, typer.Argument(
        help="Team name or ID to display roster for"
    )],
    format: Annotated[OutputFormat, typer.Option(
        "--format", "-f",
        help="Output format"
    )] = OutputFormat.table,
    last_n_days: Annotated[Optional[int], typer.Option(
        "--last-n-days",
        help="Calculate FP/G over the last N days (Warning: requires N API calls, takes ~N seconds)"
    )] = None,
    trends: Annotated[bool, typer.Option(
        "--trends", "-t",
        help="Show recent trends (7-day, 14-day, 30-day FP/G). Takes ~30 seconds."
    )] = False,
):
    """Display roster for a specific team."""
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

        # Find the team
        with console.status("[bold green]Finding team..."):
            try:
                team = league.team(team_identifier)
            except Exception as e:
                console.print(f"[bold red]Error:[/bold red] Could not find team '{team_identifier}'")
                console.print("[yellow]Available teams:[/yellow]")
                for t in league.teams:
                    console.print(f"  - {t.name} ({t.short}) [ID: {t.id}]")
                raise typer.Exit(code=1)

        # Retrieve roster
        with console.status(f"[bold green]Fetching roster for {team.name}..."):
            roster = team.roster()

        if not roster or not roster.rows:
            console.print(f"[yellow]No roster found for team {team.name}.[/yellow]")
            return

        # Calculate recent FP/G if requested
        recent_stats = None
        if last_n_days:
            if last_n_days < 1 or last_n_days > 365:
                console.print("[bold red]Error:[/bold red] --last-n-days must be between 1 and 365")
                raise typer.Exit(code=1)
            recent_stats = calculate_recent_fpg(league, team.id, last_n_days)

        # Calculate recent trends if requested
        recent_trends = None
        if trends:
            recent_trends = calculate_recent_trends(league, team.id)

        # Format and display based on selected format
        if format == OutputFormat.table:
            format_roster_table(roster, team_name=team.name, recent_stats=recent_stats, last_n_days=last_n_days, recent_trends=recent_trends)
        elif format == OutputFormat.json:
            format_roster_json(
                roster,
                team_id=team.id,
                team_name=team.name,
                recent_stats=recent_stats,
                last_n_days=last_n_days,
                recent_trends=recent_trends
            )
        else:  # simple format
            format_roster_simple(roster, recent_stats=recent_stats, recent_trends=recent_trends)

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
