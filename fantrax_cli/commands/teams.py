"""Teams command implementation."""

import traceback
import typer
from typing_extensions import Annotated
from rich.console import Console

from fantrax_cli.cli import OutputFormat
from fantrax_cli.config import load_config
from fantrax_cli.auth import get_authenticated_league
from fantrax_cli.display import format_teams_table, format_teams_json, format_teams_simple
from fantrax_cli.database import DatabaseManager
from fantrax_cli.cache import CacheManager, format_cache_age


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
        league_id_override = ctx.obj.get("league_id") if ctx.obj else None
        no_cache = ctx.obj.get("no_cache", False) if ctx.obj else False
        refresh = ctx.obj.get("refresh", False) if ctx.obj else False
        config = load_config(league_id=league_id_override)

        # Initialize database and cache manager
        db = DatabaseManager(db_path=config.database_path)
        cache = CacheManager(db, config)

        # Check cache first (unless --no-cache or --refresh)
        if config.cache_enabled and not no_cache and not refresh:
            # Try to get teams with standings
            cache_result = cache.get_teams_with_standings()
            standings_data = None

            if cache_result.from_cache and not cache_result.stale:
                league_name = cache.get_league_name() or "Unknown League"
                teams_data = cache_result.data

                # Check if we have standings (rank column exists and has values)
                has_standings = any(t.get('rank') is not None for t in teams_data)
                if has_standings:
                    standings_data = teams_data  # Already includes standings

                # Show cache status
                age_str = format_cache_age(cache_result.cache_age_hours)
                console.print(f"[dim]Using cached data (synced {age_str})[/dim]\n")

                # Format and display with standings
                _display_teams(teams_data, league_name, config.league_id, format, console, standings_data)
                return

        # Cache miss or refresh requested - fetch from API
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
                config.min_request_interval,
                config.selenium_timeout,
                config.login_wait_time,
                config.browser_window_size,
                config.user_agent
            )

        # Retrieve teams
        with console.status("[bold green]Fetching teams..."):
            teams = league.teams

        if not teams:
            console.print("[yellow]No teams found in this league.[/yellow]")
            return

        # Fetch standings using raw API to get correct field names
        standings_data = None
        with console.status("[bold green]Fetching standings..."):
            try:
                from fantraxapi import api as fantrax_api

                response = fantrax_api.get_standings(league)

                if response and 'tableList' in response and response['tableList']:
                    table_data = response['tableList'][0]
                    fields = {c["key"]: i for i, c in enumerate(table_data["header"]["cells"])}

                    standings_data = []
                    for obj in table_data["rows"]:
                        team_id = obj["fixedCells"][1]["teamId"]
                        rank = int(obj["fixedCells"][0]["content"])
                        cells = obj["cells"]

                        def get_cell(field_name, default=0):
                            if field_name not in fields:
                                return default
                            idx = fields[field_name]
                            content = cells[idx].get("content", "")
                            if content == "" or content == "-":
                                return default
                            try:
                                return float(content.replace(",", ""))
                            except (ValueError, TypeError):
                                return default

                        # Try different field names for fantasy points (varies by league type)
                        fpts = get_cell("fantasyPoints", 0) or get_cell("fPts", 0) or get_cell("pointsFor", 0)
                        gp = get_cell("sc", 0) or get_cell("gp", 0)
                        fpg = get_cell("FPtsPerGame", 0) or get_cell("fpGp", 0) or get_cell("fPtsPerGp", 0)

                        standings_data.append({
                            'team_id': team_id,
                            'rank': rank,
                            'wins': int(get_cell("win", 0)),
                            'losses': int(get_cell("loss", 0)),
                            'ties': int(get_cell("tie", 0)),
                            'points': int(get_cell("points", 0)),
                            'win_percentage': get_cell("winpc", 0),
                            'games_back': get_cell("gamesback", 0),
                            'waiver_order': int(get_cell("wwOrder", 0)) if get_cell("wwOrder", 0) else None,
                            'points_for': fpts,
                            'points_against': get_cell("pointsAgainst", 0),
                            'streak': cells[fields["streak"]].get("content", "") if "streak" in fields else "",
                            'games_played': int(gp),
                            'fpg': fpg
                        })
            except Exception as e:
                console.print(f"[yellow]Warning: Could not fetch standings: {e}[/yellow]")

        # Update cache with fresh data (unless --no-cache)
        if config.cache_enabled and not no_cache:
            try:
                # Save league metadata
                db.save_league_metadata(
                    league_id=config.league_id,
                    name=league.name,
                    year=getattr(league, 'year', None),
                    start_date=getattr(league, 'start_date', None),
                    end_date=getattr(league, 'end_date', None)
                )
                # Save teams
                teams_cache_data = [
                    {'id': t.id, 'name': t.name, 'short_name': t.short}
                    for t in teams
                ]
                db.save_teams(config.league_id, teams_cache_data)
                # Log the sync
                sync_id = db.log_sync_start('teams', config.league_id)
                db.log_sync_complete(sync_id, 6)  # ~6 API calls for auth

                # Save standings if we got them
                if standings_data:
                    db.save_standings(config.league_id, standings_data)
                    sync_id = db.log_sync_start('standings', config.league_id)
                    db.log_sync_complete(sync_id, 1)
            except Exception:
                pass  # Don't fail if caching fails

        # Format and display based on selected format
        if format == OutputFormat.table:
            format_teams_table(teams, league_name=league.name, standings=standings_data)
        elif format == OutputFormat.json:
            format_teams_json(
                teams,
                league_id=config.league_id,
                league_name=league.name,
                year=league.year,
                standings=standings_data
            )
        else:  # simple format
            format_teams_simple(teams, standings=standings_data)

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


def _display_teams(teams_data: list[dict], league_name: str, league_id: str, format: OutputFormat, console: Console, standings_data: list[dict] = None):
    """Display teams from cached data."""
    if not teams_data:
        console.print("[yellow]No teams found in cache.[/yellow]")
        return

    # Convert cached dicts to mock Team objects for display functions
    class MockTeam:
        def __init__(self, data):
            self.id = data['id']
            self.name = data['name']
            self.short = data.get('short_name', data.get('short', ''))

    teams = [MockTeam(t) for t in teams_data]

    # Use standings_data if provided, otherwise try to extract from teams_data (for get_teams_with_standings result)
    standings = standings_data
    if standings is None and teams_data:
        # Check if teams_data has embedded standings info
        if any(t.get('rank') is not None for t in teams_data):
            standings = [
                {
                    'team_id': t['id'],
                    'rank': t.get('rank'),
                    'wins': t.get('wins', 0),
                    'losses': t.get('losses', 0),
                    'ties': t.get('ties', 0),
                    'points': t.get('points', 0),
                    'win_percentage': t.get('win_percentage', 0),
                    'games_back': t.get('games_back', 0),
                    'waiver_order': t.get('waiver_order'),
                    'points_for': t.get('points_for', 0),
                    'points_against': t.get('points_against', 0),
                    'streak': t.get('streak'),
                    'games_played': t.get('games_played', 0),
                    'fpg': t.get('fpg', 0)
                }
                for t in teams_data if t.get('rank') is not None
            ]

    if format == OutputFormat.table:
        format_teams_table(teams, league_name=league_name, standings=standings)
    elif format == OutputFormat.json:
        format_teams_json(
            teams,
            league_id=league_id,
            league_name=league_name,
            standings=standings
        )
    else:
        format_teams_simple(teams, standings=standings)
