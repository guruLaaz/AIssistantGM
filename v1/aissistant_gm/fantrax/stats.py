"""Statistics calculation utilities for Fantrax CLI."""

from datetime import date, timedelta
from collections import defaultdict
from rich.console import Console
from aissistant_gm.fantrax.fantraxapi import api


def _get_daily_scores_for_team(league, team_id: str, scoring_date: date) -> dict:
    """
    Get daily scores for a team from the raw API.

    Works for both H2H and roto/points leagues by extracting data
    directly from allTeamsStats without requiring matchups.

    Args:
        league: League instance
        team_id: Team ID
        scoring_date: Date to get scores for

    Returns:
        Dictionary mapping player IDs to their points for that day
    """
    if scoring_date not in league.scoring_dates.values():
        return {}

    try:
        response = api.get_live_scoring_stats(league, scoring_date=scoring_date)

        # Get stats from allTeamsStats (works for all league types)
        all_teams_stats = response.get('statsPerTeam', {}).get('allTeamsStats', {})

        if team_id not in all_teams_stats:
            return {}

        team_data = all_teams_stats[team_id]
        player_scores = {}

        # Get active roster stats
        # Include ALL appearances in ACTIVE roster, including DNP (0-point) games
        # This matches how Fantrax calculates trends - they count all games where
        # the player was in the active lineup, not just games where they played
        if 'ACTIVE' in team_data:
            stats_map = team_data['ACTIVE'].get('statsMap', {})
            for scorer_id, pts_data in stats_map.items():
                if not scorer_id.startswith("_"):
                    # object1 contains the total fantasy points
                    points = pts_data.get('object1', 0.0)
                    if points is not None:
                        player_scores[scorer_id] = points if points else 0.0

        return player_scores

    except Exception:
        return {}


def _get_fantrax_week_boundaries(reference_date: date) -> list:
    """
    Get Fantrax week boundaries (Saturday-Friday periods).

    Returns list of (start_date, end_date, label) tuples for the last 3 weeks,
    plus 14-day and 30-day periods.
    """
    # Find the most recent Friday (end of current Fantrax week)
    days_since_friday = (reference_date.weekday() - 4) % 7
    current_week_end = reference_date - timedelta(days=days_since_friday)

    # If we're on Saturday, use the previous Friday as end
    if reference_date.weekday() == 5:  # Saturday
        current_week_end = reference_date - timedelta(days=1)

    weeks = []
    for i in range(3):
        week_end = current_week_end - timedelta(days=7 * i)
        week_start = week_end - timedelta(days=6)  # Saturday is 6 days before Friday
        label = f"week{i+1}"  # week1 = most recent, week2 = previous, week3 = oldest
        weeks.append((week_start, week_end, label))

    return weeks


def calculate_recent_trends(league, team_id: str, days: int = 35) -> dict:
    """
    Calculate fantasy points per game for players using Fantrax week boundaries.

    Fantrax uses Saturday-Friday weeks. This returns:
    - 3 separate 7-day windows (the last 3 weeks)
    - 14-day total (last 2 weeks combined)
    - 30-day total

    Args:
        league: League instance
        team_id: Team ID to calculate stats for
        days: Number of days to fetch (default: 35, provides buffer for week boundaries)

    Returns:
        Dictionary mapping player IDs to their recent trends:
        {
            player_id: {
                'week1': {'total_points': float, 'games_played': int, 'fpg': float, 'start': str, 'end': str},
                'week2': {'total_points': float, 'games_played': int, 'fpg': float, 'start': str, 'end': str},
                'week3': {'total_points': float, 'games_played': int, 'fpg': float, 'start': str, 'end': str},
                '14': {'total_points': float, 'games_played': int, 'fpg': float},
                '30': {'total_points': float, 'games_played': int, 'fpg': float}
            }
        }
    """
    console = Console(stderr=True)
    today = date.today()
    start_date = today - timedelta(days=days)  # Extra buffer for week boundaries

    # Track daily points for each player
    # player_daily[player_id] = [(date, points), ...]
    player_daily = defaultdict(list)

    console.print(f"\n[yellow]Fetching scores from {start_date} to {today}...[/yellow]")
    console.print(f"[yellow]This will take approximately {days} seconds due to rate limiting...[/yellow]\n")

    # Fetch live scores for each day
    with console.status("[bold green]Fetching daily scores...") as status:
        current_date = start_date
        days_processed = 0
        total_days = (today - start_date).days + 1

        while current_date <= today:
            try:
                days_processed += 1
                status.update(f"[bold green]Fetching scores for {current_date} ({days_processed}/{total_days})...")

                # Use custom function that works for all league types
                daily_scores = _get_daily_scores_for_team(league, team_id, current_date)

                for player_id, points in daily_scores.items():
                    player_daily[player_id].append((current_date, points))

            except Exception:
                pass

            current_date += timedelta(days=1)

    # Get Fantrax week boundaries
    weeks = _get_fantrax_week_boundaries(today)

    # Get week boundaries for 14-day and 30-day calculations
    # Fantrax 14-day = week1 + week2 combined (last two complete weeks)
    # Fantrax 30-day = from 30 days ago up to the most recent Friday (week1 end)
    week1_start, week1_end, _ = weeks[0]
    week2_start, week2_end, _ = weeks[1]
    cutoff_30 = week1_end - timedelta(days=29)  # 30 days ending at week1 Friday

    # Calculate trends for each player
    result = {}

    for player_id, daily_scores in player_daily.items():
        trends = {}

        # Calculate for each week
        for week_start, week_end, label in weeks:
            filtered = [(d, p) for d, p in daily_scores if week_start <= d <= week_end]
            total = sum(p for _, p in filtered)
            games = len(filtered)
            trends[label] = {
                'total_points': round(total, 1),
                'games_played': games,
                'fpg': round(total / games, 2) if games > 0 else 0.0,
                'start': week_start.strftime('%b %d'),
                'end': week_end.strftime('%b %d')
            }

        # Calculate 14-day (week1 + week2 combined - last two complete Fantrax weeks)
        filtered_14 = [(d, p) for d, p in daily_scores if week2_start <= d <= week1_end]
        total_14 = sum(p for _, p in filtered_14)
        games_14 = len(filtered_14)
        trends['14'] = {
            'total_points': round(total_14, 1),
            'games_played': games_14,
            'fpg': round(total_14 / games_14, 2) if games_14 > 0 else 0.0
        }

        # Calculate 30-day (from cutoff to week1 Friday)
        filtered_30 = [(d, p) for d, p in daily_scores if cutoff_30 <= d <= week1_end]
        total_30 = sum(p for _, p in filtered_30)
        games_30 = len(filtered_30)
        trends['30'] = {
            'total_points': round(total_30, 1),
            'games_played': games_30,
            'fpg': round(total_30 / games_30, 2) if games_30 > 0 else 0.0
        }

        result[player_id] = trends

    console.print(f"\n[green]✓[/green] Calculated trends for {len(result)} players\n")
    return result


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


def fetch_fa_player_trends(league, player_ids: list, limit: int = 25, sort_key: str = 'SCORE', pos_filter: str = None) -> dict:
    """
    Fetch trends for free agent players using the getPlayerStats API with date ranges.

    This is more efficient than daily fetching - only requires 5 API calls total
    (one for each time period: W1, W2, W3, 14-day, 30-day).

    Args:
        league: League instance
        player_ids: List of player IDs to get trends for (used to filter results)
        limit: Number of players to fetch per API call
        sort_key: Sort key for API call
        pos_filter: Position filter for API call

    Returns:
        Dictionary mapping player IDs to their trends data
    """
    from aissistant_gm.fantrax.fantraxapi.api import Method, _request
    console = Console(stderr=True)
    today = date.today()

    # Get Fantrax week boundaries
    weeks = _get_fantrax_week_boundaries(today)
    week1_start, week1_end, _ = weeks[0]
    week2_start, week2_end, _ = weeks[1]
    week3_start, week3_end, _ = weeks[2]

    # Calculate 14-day and 30-day boundaries
    cutoff_30 = week1_end - timedelta(days=29)

    # Define the periods we need to fetch
    periods = [
        ('week1', week1_start, week1_end),
        ('week2', week2_start, week2_end),
        ('week3', week3_start, week3_end),
        ('14', week2_start, week1_end),  # 14-day = week1 + week2
        ('30', cutoff_30, week1_end),    # 30-day
    ]

    # Store results: player_id -> period -> stats
    player_trends = defaultdict(dict)

    console.print(f"\n[yellow]Fetching trends data (5 API calls)...[/yellow]")

    with console.status("[bold green]Fetching period stats...") as status:
        for period_name, start_date, end_date in periods:
            status.update(f"[bold green]Fetching {period_name} stats ({start_date} to {end_date})...")

            try:
                method_kwargs = {
                    'statusOrTeam': 'ALL_AVAILABLE',
                    'maxResultsPerPage': str(limit),
                    'sortType': sort_key,
                    'timeframeTypeCode': 'BY_DATE',
                    'startDate': start_date.strftime('%Y-%m-%d'),
                    'endDate': end_date.strftime('%Y-%m-%d')
                }
                if pos_filter:
                    method_kwargs['scoringCategoryType'] = pos_filter

                response = _request(
                    league.league_id,
                    Method('getPlayerStats', **method_kwargs),
                    session=league.session
                )

                if response and 'statsTable' in response:
                    for player in response['statsTable']:
                        scorer = player.get('scorer', {})
                        player_id = scorer.get('scorerId')
                        cells = player.get('cells', [])

                        # Cell 5 = FPts, Cell 6 = FP/G for the period
                        fpts_str = cells[5].get('content', '0') if len(cells) > 5 else '0'
                        fpg_str = cells[6].get('content', '0') if len(cells) > 6 else '0'

                        # Parse values (handle empty strings)
                        try:
                            fpts = float(fpts_str) if fpts_str else 0.0
                        except (ValueError, TypeError):
                            fpts = 0.0
                        try:
                            fpg = float(fpg_str) if fpg_str else 0.0
                        except (ValueError, TypeError):
                            fpg = 0.0

                        # Calculate games played (FPts / FP/G if FP/G > 0)
                        if fpg > 0:
                            games = round(fpts / fpg)
                        else:
                            games = 0

                        # Store the data (use 'total'/'games' to match save_player_trends format)
                        if period_name.startswith('week'):
                            player_trends[player_id][period_name] = {
                                'total': round(fpts, 1),
                                'games': games,
                                'fpg': round(fpg, 2),
                                'start': start_date.isoformat(),
                                'end': end_date.isoformat()
                            }
                        else:
                            player_trends[player_id][period_name] = {
                                'total': round(fpts, 1),
                                'games': games,
                                'fpg': round(fpg, 2),
                                'start': start_date.isoformat(),
                                'end': end_date.isoformat()
                            }

            except Exception as e:
                console.print(f"[red]Error fetching {period_name}: {e}[/red]")

    # Fill in missing periods with zeros for players that didn't appear in some periods
    empty_week = {'total': 0.0, 'games': 0, 'fpg': 0.0, 'start': '', 'end': ''}
    empty_period = {'total': 0.0, 'games': 0, 'fpg': 0.0, 'start': '', 'end': ''}

    result = {}
    for player_id, trends in player_trends.items():
        result[player_id] = {
            'week1': trends.get('week1', {**empty_week, 'start': week1_start.isoformat(), 'end': week1_end.isoformat()}),
            'week2': trends.get('week2', {**empty_week, 'start': week2_start.isoformat(), 'end': week2_end.isoformat()}),
            'week3': trends.get('week3', {**empty_week, 'start': week3_start.isoformat(), 'end': week3_end.isoformat()}),
            '14': trends.get('14', {**empty_period, 'start': week2_start.isoformat(), 'end': week1_end.isoformat()}),
            '30': trends.get('30', {**empty_period, 'start': cutoff_30.isoformat(), 'end': week1_end.isoformat()})
        }

    console.print(f"\n[green]✓[/green] Fetched trends for {len(result)} players\n")
    return result
