"""News command implementation."""

import typer
from typing import Optional
from typing_extensions import Annotated
from rich.console import Console

from aissistant_gm.fantrax.types import OutputFormat
from aissistant_gm.fantrax.config import load_config
from aissistant_gm.fantrax.database import DatabaseManager
from aissistant_gm.fantrax.cache import CacheManager, format_cache_age
from aissistant_gm.fantrax.display import (
    format_news_table,
    format_news_detail,
    format_news_json,
    format_news_simple
)


def news_command(
    ctx: typer.Context,
    player: Annotated[Optional[str], typer.Argument(
        help="Player name to show news for (partial match supported)"
    )] = None,
    team: Annotated[Optional[str], typer.Option(
        "--team", "-t",
        help="Show news for all players on a team"
    )] = None,
    limit: Annotated[int, typer.Option(
        "--limit", "-n",
        help="Maximum news items per player"
    )] = 5,
    all_news: Annotated[bool, typer.Option(
        "--all", "-a",
        help="Show all recent news (ignores player/team filters)"
    )] = False,
    format: Annotated[OutputFormat, typer.Option(
        "--format", "-f",
        help="Output format"
    )] = OutputFormat.table,
):
    """
    Show player news and injury updates.

    By default, shows news for players on your roster. Use filters to narrow down.

    Examples:
        fantrax news                     # News for your roster
        fantrax news "Andrew Copp"       # News for specific player
        fantrax news --team "Team Name"  # News for all players on team
        fantrax news --all               # All recent news
    """
    console = Console()

    try:
        # Load configuration
        league_id_override = ctx.obj.get("league_id") if ctx.obj else None
        config = load_config(league_id=league_id_override)

        # Initialize database and cache manager
        db = DatabaseManager(db_path=config.database_path)
        cache = CacheManager(db, config)

        # Check if we have any news synced
        news_result = cache.get_all_player_news(limit=1)
        if not news_result.from_cache or not news_result.data:
            console.print("[yellow]No player news found in cache.[/yellow]")
            console.print("Run [bold]fantrax sync --news[/bold] or [bold]fantrax sync --full[/bold] to sync player news.")
            return

        # Show cache status
        if news_result.cache_age_hours is not None:
            age_str = format_cache_age(news_result.cache_age_hours)
            console.print(f"[dim]Using cached news (synced {age_str})[/dim]\n")

        # Determine what news to show
        if all_news:
            # Show all recent news (up to max_news_per_player total)
            news_items = db.get_all_player_news(limit=config.max_news_per_player)
            _display_news(news_items, format, console, "All Recent News")

        elif player:
            # Find player by name (partial match)
            news_items = _get_news_for_player_name(db, player, limit)
            if news_items:
                player_name = news_items[0].get('player_name', player)
                _display_news(news_items, format, console, f"News for {player_name}")
            else:
                console.print(f"[yellow]No news found for player matching '{player}'[/yellow]")

        elif team:
            # Get news for all players on a team
            team_data = cache.get_team_by_identifier(team)
            if not team_data:
                console.print(f"[red]Team not found: {team}[/red]")
                return

            news_result = cache.get_news_for_roster(team_data['id'], limit_per_player=limit)
            if news_result.data:
                # Flatten the dict into a list
                all_items = []
                for player_id, items in news_result.data.items():
                    all_items.extend(items)
                # Sort by date
                all_items.sort(key=lambda x: x.get('news_date', ''), reverse=True)
                _display_news(all_items, format, console, f"News for {team_data['name']}")
            else:
                console.print(f"[yellow]No news found for team {team_data['name']}[/yellow]")

        else:
            # Default: show news for user's roster (first team, or specified default)
            teams = db.get_teams(config.league_id)
            if not teams:
                console.print("[yellow]No teams found. Run 'fantrax sync --full' first.[/yellow]")
                return

            # Get first team's roster news
            team_data = teams[0]
            news_result = cache.get_news_for_roster(team_data['id'], limit_per_player=limit)
            if news_result.data:
                all_items = []
                for player_id, items in news_result.data.items():
                    all_items.extend(items)
                all_items.sort(key=lambda x: x.get('news_date', ''), reverse=True)
                _display_news(all_items, format, console, f"News for {team_data['name']}")
            else:
                console.print(f"[yellow]No news found for your roster.[/yellow]")
                console.print("Run [bold]fantrax sync --news[/bold] to sync player news.")

    except ValueError as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        import traceback
        console.print("\n[dim]Full traceback:[/dim]")
        console.print(traceback.format_exc())
        raise typer.Exit(code=1)


def _get_news_for_player_name(db: DatabaseManager, name: str, limit: int) -> list:
    """Find player by name and return their news."""
    # Search for player in database (case-insensitive partial match)
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name FROM players
            WHERE LOWER(name) LIKE LOWER(?)
            LIMIT 1
        """, (f"%{name}%",))
        row = cursor.fetchone()

    if not row:
        return []

    player_id = row['id']
    return db.get_player_news(player_id, limit=limit)


def _display_news(news_items: list, format: OutputFormat, console: Console, title: str = None):
    """Display news items in the specified format."""
    if format == OutputFormat.json:
        format_news_json(news_items)
    elif format == OutputFormat.simple:
        if title:
            print(title)
            print("=" * len(title))
        format_news_simple(news_items)
    else:
        # Table format - use detail view for single player, table for multiple
        if len(set(item.get('player_id') for item in news_items)) == 1:
            # Single player - show detailed view
            player_name = news_items[0].get('player_name') if news_items else None
            format_news_detail(news_items, player_name)
        else:
            # Multiple players - show table view
            format_news_table(news_items, title)
