"""Roster command implementation."""

import traceback
import typer
from typing import Optional
from typing_extensions import Annotated
from rich.console import Console

from aissistant_gm.fantrax.cli import OutputFormat
from aissistant_gm.fantrax.config import load_config
from aissistant_gm.fantrax.auth import get_authenticated_league
from aissistant_gm.fantrax.display import format_roster_table, format_roster_json, format_roster_simple
from aissistant_gm.fantrax.stats import calculate_recent_fpg, calculate_recent_trends
from aissistant_gm.fantrax.database import DatabaseManager
from aissistant_gm.fantrax.cache import CacheManager, format_cache_age


def roster_command(
    ctx: typer.Context,
    team_identifier: Annotated[Optional[str], typer.Argument(
        help="Team name or ID to display roster for (defaults to your team)"
    )] = None,
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
        league_id_override = ctx.obj.get("league_id") if ctx.obj else None
        no_cache = ctx.obj.get("no_cache", False) if ctx.obj else False
        refresh = ctx.obj.get("refresh", False) if ctx.obj else False
        config = load_config(league_id=league_id_override)

        # Initialize database and cache manager
        db = DatabaseManager(db_path=config.database_path)
        cache = CacheManager(db, config)

        # Try to get team from cache first (for team ID lookup) - only if team_identifier provided
        team_from_cache = cache.get_team_by_identifier(team_identifier) if team_identifier else None
        team_id = team_from_cache['id'] if team_from_cache else None

        # Check if we can serve from cache (basic roster without last_n_days)
        # Only use cache if team_identifier was explicitly provided
        if team_identifier and config.cache_enabled and not no_cache and not refresh and not last_n_days:
            if team_id:
                # Check roster cache
                roster_result = cache.get_roster(team_id)

                if roster_result.from_cache and not roster_result.stale:
                    # If trends requested, check trends cache
                    if trends:
                        trends_result = cache.get_player_trends(team_id)
                        if trends_result.from_cache and not trends_result.stale:
                            # Both roster and trends are cached
                            age_str = format_cache_age(roster_result.cache_age_hours)
                            console.print(f"[dim]Using cached data (synced {age_str})[/dim]\n")
                            _display_cached_roster(
                                roster_result.data,
                                team_from_cache,
                                trends_result.data,
                                format,
                                console
                            )
                            return
                    else:
                        # No trends requested, serve from cache
                        age_str = format_cache_age(roster_result.cache_age_hours)
                        console.print(f"[dim]Using cached data (synced {age_str})[/dim]\n")
                        _display_cached_roster(
                            roster_result.data,
                            team_from_cache,
                            None,
                            format,
                            console
                        )
                        return

        # Cache miss or features not supported by cache - fetch from API
        with console.status("[bold green]Authenticating with Fantrax..."):
            # Monkey-patch the League class to handle missing scoringPeriodList
            from aissistant_gm.fantrax.fantraxapi.objs.league import League
            from aissistant_gm.fantrax.fantraxapi.objs.scoring_period import ScoringPeriod
            from aissistant_gm.fantrax.fantraxapi import api
            from datetime import datetime

            original_reset_info = League.reset_info

            def patched_reset_info(self):
                responses = api.get_init_info(self)
                self.name = responses[0]["fantasySettings"]["leagueName"]
                self.year = responses[0]["fantasySettings"]["subtitle"]
                self.start_date = datetime.fromtimestamp(responses[0]["fantasySettings"]["season"]["startDate"] / 1e3)
                self.end_date = datetime.fromtimestamp(responses[0]["fantasySettings"]["season"]["endDate"] / 1e3)
                from aissistant_gm.fantrax.fantraxapi.objs.position import Position
                from aissistant_gm.fantrax.fantraxapi.objs.status import Status
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
                # Extract the user's team ID from the roster response (called without teamId, returns user's team)
                fantasy_teams = responses[3]["fantasyTeams"]
                if isinstance(fantasy_teams, dict) and fantasy_teams:
                    self.my_team_id = next(iter(fantasy_teams.keys()))
                elif isinstance(fantasy_teams, list) and fantasy_teams:
                    self.my_team_id = fantasy_teams[0].get("id")
                self._update_teams(fantasy_teams)

            League.reset_info = patched_reset_info

            # Add my_team property if not present
            if not hasattr(League, 'my_team') or not isinstance(getattr(League, 'my_team', None), property):
                def _get_my_team(self):
                    if hasattr(self, 'my_team_id') and self.my_team_id and self.my_team_id in self.team_lookup:
                        return self.team_lookup[self.my_team_id]
                    return None
                League.my_team = property(_get_my_team)

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

        # Find the team
        with console.status("[bold green]Finding team..."):
            if team_identifier:
                # Team specified - look it up
                try:
                    team = league.team(team_identifier)
                except Exception as e:
                    console.print(f"[bold red]Error:[/bold red] Could not find team '{team_identifier}'")
                    console.print("[yellow]Available teams:[/yellow]")
                    for t in league.teams:
                        console.print(f"  - {t.name} ({t.short}) [ID: {t.id}]")
                    raise typer.Exit(code=1)
            else:
                # No team specified - use logged-in user's team
                team = league.my_team
                if not team:
                    console.print("[bold red]Error:[/bold red] Could not determine your team. Please specify a team name or ID.")
                    console.print("[yellow]Available teams:[/yellow]")
                    for t in league.teams:
                        console.print(f"  - {t.name} ({t.short}) [ID: {t.id}]")
                    raise typer.Exit(code=1)
                # Only show message for non-JSON formats (JSON already includes team info)
                if format != OutputFormat.json:
                    console.print(f"[dim]Using your team: {team.name}[/dim]")

        # Retrieve roster
        with console.status(f"[bold green]Fetching roster for {team.name}..."):
            roster = team.roster()

        if not roster or not roster.rows:
            console.print(f"[yellow]No roster found for team {team.name}.[/yellow]")
            return

        # Update cache with roster data
        if config.cache_enabled and not no_cache:
            try:
                _cache_roster(db, config.league_id, team, roster)
            except Exception:
                pass  # Don't fail if caching fails

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
            recent_trends = calculate_recent_trends(league, team.id, days=config.sync_days_scores)
            # Cache the trends
            if config.cache_enabled and not no_cache and recent_trends:
                try:
                    for player_id, trend_data in recent_trends.items():
                        # Convert to cache format
                        cache_trends = {}
                        for period, data in trend_data.items():
                            cache_trends[period] = {
                                'total': data.get('total_points', 0),
                                'games': data.get('games', 0),
                                'fpg': data.get('fpg', 0),
                                'start': data.get('start', ''),
                                'end': data.get('end', '')
                            }
                        db.save_player_trends(player_id, cache_trends)
                    # Log trends sync
                    sync_id = db.log_sync_start('trends', config.league_id)
                    db.log_sync_complete(sync_id, 35)
                except Exception:
                    pass

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


def _cache_roster(db: DatabaseManager, league_id: str, team, roster) -> None:
    """Cache roster data to database."""
    players = []
    roster_rows = []

    for row in roster.rows:
        if row.player:
            player = row.player
            player_team = getattr(player, 'team', None)
            players.append({
                'id': player.id,
                'name': player.name,
                'short_name': getattr(player, 'short_name', None),
                'team_name': player_team.name if player_team else None,
                'team_short_name': player_team.short if player_team else None,
                'position_short_names': ','.join(p.short for p in player.positions) if player.positions else None,
                'day_to_day': 1 if getattr(player, 'day_to_day', False) else 0,
                'out': 1 if getattr(player, 'out', False) else 0,
                'injured_reserve': 1 if getattr(player, 'injured_reserve', False) else 0,
                'suspended': 1 if getattr(player, 'suspended', False) else 0
            })

        roster_rows.append({
            'player_id': row.player.id if row.player else None,
            'position_id': row.position.id if row.position else '',
            'position_short': row.position.short if row.position else '',
            'status_id': row.status_id,
            'salary': float(row.salary) if row.salary else None,
            'total_fantasy_points': row.fantasy_points,
            'fantasy_points_per_game': row.fantasy_points_per_game
        })

    if players:
        db.save_players(players)
    db.save_roster(team.id, roster_rows)

    # Log the sync
    sync_id = db.log_sync_start('rosters', league_id)
    db.log_sync_complete(sync_id, 1)


def _display_cached_roster(
    roster_data: list[dict],
    team_data: dict,
    trends_data: Optional[dict],
    format: OutputFormat,
    console: Console
) -> None:
    """Display roster from cached data."""
    if not roster_data:
        console.print("[yellow]No roster data in cache.[/yellow]")
        return

    # Create mock objects for display functions
    class MockPosition:
        def __init__(self, short):
            self.short = short

    class MockTeam:
        def __init__(self, name, short):
            self.name = name
            self.short = short

    class MockPlayer:
        def __init__(self, data):
            self.id = data.get('player_id') or data.get('id')
            self.name = data.get('player_name') or data.get('name', '')
            self.team = MockTeam(
                data.get('team_name', ''),
                data.get('team_short_name', '')
            ) if data.get('team_name') else None
            pos_str = data.get('position_short_names', '')
            self.positions = [MockPosition(p) for p in pos_str.split(',')] if pos_str else []
            self.day_to_day = bool(data.get('day_to_day', 0))
            self.out = bool(data.get('out', 0))
            self.injured_reserve = bool(data.get('injured_reserve', 0))
            self.suspended = bool(data.get('suspended', 0))

    class MockRosterRow:
        def __init__(self, data):
            self.position = MockPosition(data.get('position_short', ''))
            self.player = MockPlayer(data) if data.get('player_id') else None
            self.status_id = data.get('status_id')
            self.salary = data.get('salary')
            self.fantasy_points = data.get('total_fantasy_points')
            self.fantasy_points_per_game = data.get('fantasy_points_per_game')

    class MockRoster:
        def __init__(self, rows_data):
            self.rows = [MockRosterRow(r) for r in rows_data]

    roster = MockRoster(roster_data)
    team_name = team_data.get('name', 'Unknown Team')

    # Convert trends data format if present
    recent_trends = None
    if trends_data:
        recent_trends = {}
        for player_id, periods in trends_data.items():
            recent_trends[player_id] = {}
            for period, data in periods.items():
                recent_trends[player_id][period] = {
                    'total_points': data.get('total', 0),
                    'games_played': data.get('games', 0),
                    'fpg': data.get('fpg', 0),
                    'start': data.get('start', ''),
                    'end': data.get('end', '')
                }

    if format == OutputFormat.table:
        format_roster_table(roster, team_name=team_name, recent_trends=recent_trends)
    elif format == OutputFormat.json:
        format_roster_json(
            roster,
            team_id=team_data.get('id', ''),
            team_name=team_name,
            recent_trends=recent_trends
        )
    else:
        format_roster_simple(roster, recent_trends=recent_trends)
