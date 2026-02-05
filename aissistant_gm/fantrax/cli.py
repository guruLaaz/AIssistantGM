"""Main CLI interface for Fantrax CLI."""

import sys
import io
import typer
from typing import Optional
from typing_extensions import Annotated

from aissistant_gm.fantrax import __version__
from aissistant_gm.fantrax.types import OutputFormat  # Re-export for backward compatibility

# Configure UTF-8 encoding for stdout/stderr to handle Unicode characters on Windows
# This fixes issues with team names containing special characters like "Udûn"
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


app = typer.Typer(
    name="fantrax",
    help="CLI wrapper for Fantrax Fantasy Sports API",
    add_completion=False
)


def version_callback(value: bool):
    """Display version information."""
    if value:
        typer.echo(f"Fantrax CLI version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    ctx: typer.Context,
    league_id: Annotated[Optional[str], typer.Option(
        "--league-id", "-l",
        help="Fantrax League ID (overrides FANTRAX_LEAGUE_ID env var)"
    )] = None,
    version: Annotated[Optional[bool], typer.Option(
        "--version", "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit"
    )] = None,
    no_cache: Annotated[bool, typer.Option(
        "--no-cache",
        help="Bypass local cache and always fetch fresh data from API"
    )] = False,
    refresh: Annotated[bool, typer.Option(
        "--refresh", "-r",
        help="Force refresh cache before executing command"
    )] = False,
):
    """
    Fantrax CLI - Interact with Fantrax Fantasy Sports API.

    Use environment variables or .env file for configuration.
    See .env.example for required variables.
    """
    # Store options in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj["league_id"] = league_id
    ctx.obj["no_cache"] = no_cache
    ctx.obj["refresh"] = refresh


# Import and register commands
from aissistant_gm.fantrax.commands.teams import teams_command
from aissistant_gm.fantrax.commands.roster import roster_command
from aissistant_gm.fantrax.commands.players import players_command
from aissistant_gm.fantrax.commands.sync import sync_command
from aissistant_gm.fantrax.commands.news import news_command
app.command("teams")(teams_command)
app.command("roster")(roster_command)
app.command("players")(players_command)
app.command("sync")(sync_command)
app.command("news")(news_command)


if __name__ == "__main__":
    app()
