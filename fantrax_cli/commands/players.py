"""Players command implementation."""

import typer
from typing import Optional
from typing_extensions import Annotated
from rich.console import Console
from rich.table import Table

from fantrax_cli.cli import OutputFormat
from fantrax_cli.config import load_config
from fantrax_cli.auth import get_authenticated_league
from fantrax_cli.stats import fetch_fa_player_trends
from fantraxapi.api import Method, _request
import json


def players_command(
    ctx: typer.Context,
    position: Annotated[Optional[str], typer.Option(
        "--position", "-p",
        help="Filter by position (F, D, G, or ALL)"
    )] = None,
    limit: Annotated[int, typer.Option(
        "--limit", "-n",
        help="Number of players to display"
    )] = 25,
    sort: Annotated[str, typer.Option(
        "--sort", "-s",
        help="Sort by: fpts, fpg, rank, salary"
    )] = "fpts",
    format: Annotated[OutputFormat, typer.Option(
        "--format", "-f",
        help="Output format"
    )] = OutputFormat.table,
    trends: Annotated[bool, typer.Option(
        "--trends", "-t",
        help="Show recent trends (7-day, 14-day, 30-day FP/G). Requires 5 extra API calls."
    )] = False,
):
    """List available free agent players."""
    console = Console()

    try:
        # Load configuration
        league_id_override = ctx.obj.get("league_id")
        config = load_config(league_id=league_id_override)

        # Authenticate and get League instance
        with console.status("[bold green]Authenticating with Fantrax..."):
            # Apply the same monkey-patch as roster command
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

        # Map sort options to API sort keys
        sort_mapping = {
            'fpts': 'SCORE',
            'fpg': 'FPTS_PER_GAME',
            'rank': 'RANK',
            'salary': 'SALARY'
        }
        sort_key = sort_mapping.get(sort.lower(), 'SCORE')

        # Map position filter
        pos_mapping = {
            'f': '2010',   # Skaters/Forwards
            'd': '2010',   # Skaters/Defensemen (same group in NHL)
            'g': '2020',   # Goalies
            'all': None
        }
        pos_filter = pos_mapping.get(position.lower(), None) if position else None

        # Fetch available players
        with console.status("[bold green]Fetching available players..."):
            response = _request(
                league.league_id,
                Method(
                    'getPlayerStats',
                    statusOrTeam='ALL_AVAILABLE',
                    maxResultsPerPage=str(limit),
                    sortType=sort_key,
                    scoringCategoryType=pos_filter
                ),
                session=league.session
            )

        if not response or 'statsTable' not in response:
            console.print("[yellow]No available players found.[/yellow]")
            return

        players = response['statsTable']
        total_available = response.get('paginatedResultSet', {}).get('totalNumResults', len(players))

        # Fetch trends if requested
        player_trends = {}
        if trends:
            # Get player IDs from the results
            player_ids = [p.get('scorer', {}).get('scorerId') for p in players]
            player_trends = fetch_fa_player_trends(
                league,
                player_ids,
                limit=limit,
                sort_key=sort_key,
                pos_filter=pos_filter
            )

        if format == OutputFormat.json:
            output = {
                'total_available': total_available,
                'showing': len(players),
                'players': []
            }
            if trends:
                output['stats_period'] = 'recent_trends'

            for player in players:
                scorer = player.get('scorer', {})
                cells = player.get('cells', [])
                player_id = scorer.get('scorerId')

                player_data = {
                    'id': player_id,
                    'name': scorer.get('name'),
                    'team': scorer.get('teamShortName'),
                    'position': scorer.get('posShortNames'),
                    'rank': cells[0].get('content') if len(cells) > 0 else None,
                    'status': cells[1].get('content') if len(cells) > 1 else None,
                    'age': cells[2].get('content') if len(cells) > 2 else None,
                    'salary': cells[4].get('content') if len(cells) > 4 else None,
                    'fpts': cells[5].get('content') if len(cells) > 5 else None,
                    'fpg': cells[6].get('content') if len(cells) > 6 else None,
                }

                # Add trends if available
                if trends and player_id in player_trends:
                    t = player_trends[player_id]
                    player_data['trends'] = {
                        'week1': t.get('week1', {}),
                        'week2': t.get('week2', {}),
                        'week3': t.get('week3', {}),
                        '14_day': t.get('14', {}),
                        '30_day': t.get('30', {})
                    }

                output['players'].append(player_data)
            print(json.dumps(output, indent=2))
        else:
            # Table format
            title = f"Available Players ({len(players)} of {total_available:,})"
            if trends:
                title += " - with Recent Trends"
            table = Table(title=title)

            table.add_column("#", style="dim", width=3)
            table.add_column("Rk", justify="right", width=3)

            if trends:
                # Narrower player column when showing trends
                table.add_column("Player", min_width=14)
                table.add_column("Pos", width=3)
                table.add_column("Team", width=4)
                table.add_column("Salary", justify="right", width=9)
                # Add trend columns
                table.add_column("W1", justify="right", width=4, style="yellow")
                table.add_column("W2", justify="right", width=4, style="yellow")
                table.add_column("W3", justify="right", width=4, style="yellow")
                table.add_column("14d", justify="right", width=4, style="cyan")
                table.add_column("30d", justify="right", width=4, style="cyan")
            else:
                table.add_column("Player", min_width=18)
                table.add_column("Pos", width=4)
                table.add_column("Team", width=5)
                table.add_column("Salary", justify="right", width=10)
                table.add_column("FPts", justify="right", width=6)
                table.add_column("FP/G", justify="right", width=6)

            for idx, player in enumerate(players, 1):
                scorer = player.get('scorer', {})
                cells = player.get('cells', [])
                player_id = scorer.get('scorerId')

                name = scorer.get('name', 'Unknown')
                team = scorer.get('teamShortName', '')
                position_str = scorer.get('posShortNames', '')

                rank = cells[0].get('content', '') if len(cells) > 0 else ''
                salary = cells[4].get('content', '') if len(cells) > 4 else ''

                # Format salary
                if salary and salary.isdigit():
                    salary = f"${int(salary):,}"

                if trends:
                    # Get trends data for this player
                    t = player_trends.get(player_id, {})
                    w1_fpg = f"{t.get('week1', {}).get('fpg', 0):.2f}"
                    w2_fpg = f"{t.get('week2', {}).get('fpg', 0):.2f}"
                    w3_fpg = f"{t.get('week3', {}).get('fpg', 0):.2f}"
                    d14_fpg = f"{t.get('14', {}).get('fpg', 0):.2f}"
                    d30_fpg = f"{t.get('30', {}).get('fpg', 0):.2f}"

                    table.add_row(
                        str(idx),
                        rank,
                        name,
                        position_str,
                        team,
                        salary,
                        w1_fpg,
                        w2_fpg,
                        w3_fpg,
                        d14_fpg,
                        d30_fpg
                    )
                else:
                    fpts = cells[5].get('content', '') if len(cells) > 5 else ''
                    fpg = cells[6].get('content', '') if len(cells) > 6 else ''

                    table.add_row(
                        str(idx),
                        rank,
                        name,
                        position_str,
                        team,
                        salary,
                        fpts,
                        fpg
                    )

            console.print(table)
            console.print(f"\n[dim]Showing top {len(players)} of {total_available:,} available players[/dim]")

    except ValueError as e:
        console.print(f"[bold red]Configuration Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        import traceback
        console.print("\n[dim]Full traceback:[/dim]")
        console.print(traceback.format_exc())
        raise typer.Exit(code=1)
