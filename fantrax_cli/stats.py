"""Statistics calculation utilities for Fantrax CLI."""

from datetime import date, timedelta
from collections import defaultdict
from rich.console import Console


def calculate_recent_fpg(league, team_id: str, last_n_days: int) -> dict:
    """
    Calculate fantasy points per game for players over the last N days.

    Args:
        league: League instance
        team_id: Team ID to calculate stats for
        last_n_days: Number of days to look back

    Returns:
        Dictionary mapping player IDs to their recent FP/G stats:
        {
            player_id: {
                'total_points': float,
                'games_played': int,
                'fpg': float
            }
        }
    """
    console = Console(stderr=True)
    today = date.today()
    start_date = today - timedelta(days=last_n_days)

    # Track points for each player
    player_stats = defaultdict(lambda: {'total_points': 0.0, 'games_played': 0})

    console.print(f"\n[yellow]Fetching scores from {start_date} to {today} ({last_n_days} days)...[/yellow]")
    console.print("[yellow]This will take approximately {:.0f} seconds due to rate limiting...[/yellow]\n".format(last_n_days))

    # Fetch live scores for each day
    with console.status("[bold green]Fetching daily scores...") as status:
        current_date = start_date
        days_processed = 0

        while current_date <= today:
            try:
                # Update status
                days_processed += 1
                status.update(f"[bold green]Fetching scores for {current_date} ({days_processed}/{last_n_days + 1})...")

                # Get live scores for this date
                live_scores = league.live_scores(current_date)

                # Process scores for the specified team
                if team_id in live_scores:
                    for live_player in live_scores[team_id]:
                        player_id = live_player.id
                        points = live_player.points

                        # Only count if player actually played (scored points)
                        if points and points > 0:
                            player_stats[player_id]['total_points'] += points
                            player_stats[player_id]['games_played'] += 1

            except Exception as e:
                # Skip dates that don't have data (e.g., off-season, future dates)
                pass

            current_date += timedelta(days=1)

    # Calculate FP/G for each player
    result = {}
    for player_id, stats in player_stats.items():
        games = stats['games_played']
        if games > 0:
            result[player_id] = {
                'total_points': stats['total_points'],
                'games_played': games,
                'fpg': stats['total_points'] / games
            }
        else:
            result[player_id] = {
                'total_points': 0.0,
                'games_played': 0,
                'fpg': 0.0
            }

    console.print(f"\n[green]✓[/green] Fetched scores for {last_n_days} days\n")
    return result
