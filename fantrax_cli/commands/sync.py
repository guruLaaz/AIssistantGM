"""Sync command implementation."""

import typer
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table
from datetime import datetime

from fantrax_cli.config import load_config
from fantrax_cli.auth import get_authenticated_league
from fantrax_cli.database import DatabaseManager, get_cache_age_hours
from fantrax_cli.sync import SyncManager, get_sync_status


def sync_command(
    ctx: typer.Context,
    full: Annotated[bool, typer.Option(
        "--full",
        help="Full sync: teams, standings, all rosters, daily scores (per config), and trends"
    )] = False,
    teams: Annotated[bool, typer.Option(
        "--teams",
        help="Sync teams only"
    )] = False,
    standings: Annotated[bool, typer.Option(
        "--standings",
        help="Sync league standings"
    )] = False,
    rosters: Annotated[bool, typer.Option(
        "--rosters",
        help="Sync all team rosters"
    )] = False,
    scores: Annotated[int, typer.Option(
        "--scores",
        help="Sync N days of daily scores"
    )] = 0,
    trends: Annotated[bool, typer.Option(
        "--trends",
        help="Calculate trends from cached daily scores"
    )] = False,
    free_agents: Annotated[bool, typer.Option(
        "--free-agents", "--fa",
        help="Sync free agent listings"
    )] = False,
    news: Annotated[bool, typer.Option(
        "--news",
        help="Sync player news for all rostered players"
    )] = False,
    no_news: Annotated[bool, typer.Option(
        "--no-news",
        help="Skip player news sync during --full sync"
    )] = False,
    status: Annotated[bool, typer.Option(
        "--status", "-s",
        help="Show cache status and last sync times"
    )] = False,
    clear: Annotated[bool, typer.Option(
        "--clear",
        help="Clear all cached data"
    )] = False,
    yes: Annotated[bool, typer.Option(
        "--yes", "-y",
        help="Skip confirmation prompts"
    )] = False,
):
    """
    Synchronize data from Fantrax API to local database.

    Run with --full for a complete sync, or use specific flags for partial syncs.
    Use --status to check cache freshness.

    Examples:
        fantrax sync --full          # Full sync (~55 seconds)
        fantrax sync --status        # Check cache status
        fantrax sync --rosters       # Update just rosters
        fantrax sync --clear         # Clear local cache
    """
    console = Console()

    try:
        # Load configuration
        league_id_override = ctx.obj.get("league_id") if ctx.obj else None
        config = load_config(league_id=league_id_override)

        # Initialize database
        db = DatabaseManager(db_path=config.database_path)

        # Handle --clear
        if clear:
            if yes or typer.confirm("Are you sure you want to clear all cached data?"):
                db.clear_all()
                console.print("[green]✓[/green] Cache cleared successfully")
            return

        # Handle --status
        if status:
            _show_status(console, db, config.league_id)
            return

        # If no specific flags, show help
        if not any([full, teams, standings, rosters, scores > 0, trends, free_agents, news]):
            console.print("[yellow]No sync option specified. Use --help for options.[/yellow]")
            console.print("\nQuick options:")
            console.print("  [bold]fantrax sync --full[/bold]       Full sync (includes news)")
            console.print("  [bold]fantrax sync --status[/bold]     Check cache status")
            console.print("  [bold]fantrax sync --standings[/bold]  Update standings")
            console.print("  [bold]fantrax sync --rosters[/bold]    Update just rosters")
            console.print("  [bold]fantrax sync --news[/bold]       Update player news")
            return

        # Authenticate with Fantrax
        console.print("\n[bold]Fantrax Data Sync[/bold]")
        console.print("=" * 40)

        with console.status("[bold green]Authenticating with Fantrax..."):
            # Apply the same monkey-patch as roster command
            from fantraxapi.objs.league import League
            from fantraxapi.objs.scoring_period import ScoringPeriod
            from fantraxapi import api
            from datetime import datetime as dt

            original_reset_info = League.reset_info

            def patched_reset_info(self):
                responses = api.get_init_info(self)
                self.name = responses[0]["fantasySettings"]["leagueName"]
                self.year = responses[0]["fantasySettings"]["subtitle"]
                self.start_date = dt.fromtimestamp(responses[0]["fantasySettings"]["season"]["startDate"] / 1e3)
                self.end_date = dt.fromtimestamp(responses[0]["fantasySettings"]["season"]["endDate"] / 1e3)
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
                    scoring_date = dt.strptime(day["object1"], "%Y-%m-%d").date()
                    key = scoring_date.strftime("%b %d")
                    if "0" in key and not key.endswith("0"):
                        key = key.replace("0", "")
                    self.scoring_dates[period_to_day_list[key]] = scoring_date
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

        console.print(f"[green]✓[/green] Authenticated to league: {league.name}")

        # Create sync manager
        sync_manager = SyncManager(league, db, console, config)

        # Handle --full
        if full:
            console.print("\n[bold]Starting full sync...[/bold]")
            result = sync_manager.sync_all(
                include_trends=True,
                include_free_agents=True,
                include_news=not no_news
            )
            _show_sync_result(console, result)
            return

        # Handle individual sync options
        api_calls = 0

        if teams:
            console.print("\n[bold blue]→[/bold blue] Syncing teams...")
            sync_manager.sync_league_metadata()
            count = sync_manager.sync_teams()
            console.print(f"[green]✓[/green] Synced {count} teams")
            api_calls += sync_manager.api_calls

        if standings:
            console.print("\n[bold blue]→[/bold blue] Syncing standings...")
            if not teams:
                sync_manager.sync_league_metadata()
                sync_manager.sync_teams()
            count = sync_manager.sync_standings()
            console.print(f"[green]✓[/green] Synced standings for {count} teams")
            api_calls += sync_manager.api_calls

        if rosters:
            console.print("\n[bold blue]→[/bold blue] Syncing rosters...")
            if not teams:
                sync_manager.sync_league_metadata()
                sync_manager.sync_teams()
            result = sync_manager.sync_all_rosters()
            console.print(f"[green]✓[/green] Synced {result['players']} players, {result['roster_slots']} roster slots")
            api_calls += sync_manager.api_calls

        if scores > 0:
            console.print(f"\n[bold blue]→[/bold blue] Syncing {scores} days of daily scores...")
            if not teams and not rosters:
                sync_manager.sync_league_metadata()
                sync_manager.sync_teams()
            count = sync_manager.sync_daily_scores(days=scores)
            console.print(f"[green]✓[/green] Synced {count} score records")
            api_calls += sync_manager.api_calls

        if trends:
            console.print("\n[bold blue]→[/bold blue] Calculating trends from cached scores...")
            count = sync_manager.sync_trends()
            console.print(f"[green]✓[/green] Calculated trends for {count} players")

        if free_agents:
            console.print("\n[bold blue]→[/bold blue] Syncing free agents...")
            if not teams:
                sync_manager.sync_league_metadata()
                sync_manager.sync_teams()
            count = sync_manager.sync_free_agents()
            console.print(f"[green]✓[/green] Synced {count} free agents")
            api_calls += sync_manager.api_calls

        if news:
            console.print("\n[bold blue]→[/bold blue] Syncing player news...")
            if not teams and not rosters:
                sync_manager.sync_league_metadata()
                sync_manager.sync_teams()
                sync_manager.sync_all_rosters()
            count = sync_manager.sync_player_news()
            console.print(f"[green]✓[/green] Synced news for {count} players")
            api_calls += sync_manager.api_calls

        console.print(f"\n[bold]Sync complete![/bold] Total API calls: {api_calls}")
        console.print(f"Cache location: {config.database_path}")

    except ValueError as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        import traceback
        console.print("\n[dim]Full traceback:[/dim]")
        console.print(traceback.format_exc())
        raise typer.Exit(code=1)


def _show_status(console: Console, db: DatabaseManager, league_id: str) -> None:
    """Display cache status information."""
    status = get_sync_status(db, league_id)

    console.print("\n[bold]Cache Status[/bold]")
    console.print("=" * 40)

    if not status['has_data']:
        console.print("[yellow]No cached data found.[/yellow]")
        console.print("Run [bold]fantrax sync --full[/bold] to populate the cache.")
        return

    console.print(f"League: {status.get('league_name', league_id)}")
    console.print(f"Database: {db.db_path}")

    # Data counts
    console.print("\n[bold]Cached Data:[/bold]")
    counts = status['data_counts']
    console.print(f"  Teams: {counts.get('teams', 0)}")
    console.print(f"  Rostered Players: {counts.get('rostered_players', 0)}")
    if 'daily_scores_range' in counts:
        dr = counts['daily_scores_range']
        console.print(f"  Daily Scores: {dr['start']} to {dr['end']}")

    # Sync history
    console.print("\n[bold]Last Syncs:[/bold]")

    table = Table(show_header=True, header_style="bold")
    table.add_column("Type")
    table.add_column("Last Sync")
    table.add_column("Age")
    table.add_column("API Calls")

    for sync_type, info in status['sync_types'].items():
        if info:
            age = info.get('age_hours')
            if age is not None:
                if age < 1:
                    age_str = f"{int(age * 60)} min ago"
                elif age < 24:
                    age_str = f"{age:.1f} hours ago"
                else:
                    age_str = f"{age / 24:.1f} days ago"
            else:
                age_str = "Unknown"

            # Format timestamp
            try:
                ts = datetime.fromisoformat(info['last_sync'])
                ts_str = ts.strftime("%Y-%m-%d %H:%M")
            except:
                ts_str = info['last_sync']

            table.add_row(
                sync_type,
                ts_str,
                age_str,
                str(info.get('api_calls', 0))
            )

    if status['sync_types']:
        console.print(table)
    else:
        console.print("  [dim]No sync history found[/dim]")


def _show_sync_result(console: Console, result: dict) -> None:
    """Display sync result summary."""
    console.print("\n" + "=" * 40)
    console.print("[bold green]✓ Sync completed successfully![/bold green]")
    console.print()
    console.print(f"  Teams synced: {result.get('teams', 0)}")
    if result.get('standings', 0) > 0:
        console.print(f"  Standings synced: {result.get('standings', 0)}")
    console.print(f"  Players synced: {result.get('players', 0)}")
    console.print(f"  Roster slots: {result.get('roster_slots', 0)}")
    console.print(f"  Daily scores: {result.get('daily_scores', 0)}")
    console.print(f"  Player trends: {result.get('trends', 0)}")
    if result.get('free_agents', 0) > 0:
        console.print(f"  Free agents: {result.get('free_agents', 0)}")
    if result.get('player_news', 0) > 0:
        console.print(f"  Player news: {result.get('player_news', 0)}")
    console.print()
    console.print(f"  Total API calls: {result.get('api_calls', 0)}")
