"""Sync manager for fetching and storing Fantrax data."""

from datetime import date, timedelta
from typing import Optional, Callable
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from fantrax_cli.database import DatabaseManager
from fantrax_cli.stats import _get_daily_scores_for_team, _get_fantrax_week_boundaries


class SyncManager:
    """Manages synchronization between Fantrax API and local database."""

    def __init__(
        self,
        league,
        db: DatabaseManager,
        console: Optional[Console] = None
    ):
        """
        Initialize sync manager.

        Args:
            league: Authenticated fantraxapi League instance
            db: DatabaseManager instance
            console: Optional Rich console for output
        """
        self.league = league
        self.db = db
        self.console = console or Console()
        self.api_calls = 0

    def sync_all(
        self,
        include_trends: bool = True,
        days_of_scores: int = 35,
        include_free_agents: bool = False,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> dict:
        """
        Perform a full sync of all data.

        Args:
            include_trends: Whether to sync daily scores and calculate trends
            days_of_scores: Number of days of daily scores to fetch
            include_free_agents: Whether to sync free agent listings
            progress_callback: Optional callback for progress updates (message, current, total)

        Returns:
            Dictionary with sync statistics
        """
        sync_id = self.db.log_sync_start('full', self.league.league_id)
        self.api_calls = 0

        try:
            stats = {
                'teams': 0,
                'standings': 0,
                'players': 0,
                'roster_slots': 0,
                'daily_scores': 0,
                'trends': 0,
                'free_agents': 0
            }

            # Step 1: Sync league metadata
            self._log_step("Syncing league metadata...")
            self.sync_league_metadata()

            # Step 2: Sync teams
            self._log_step("Syncing teams...")
            stats['teams'] = self.sync_teams()

            # Step 2.5: Sync standings
            self._log_step("Syncing standings...")
            stats['standings'] = self.sync_standings()

            # Step 3: Sync all rosters
            self._log_step("Syncing rosters...")
            roster_stats = self.sync_all_rosters()
            stats['players'] = roster_stats['players']
            stats['roster_slots'] = roster_stats['roster_slots']

            # Step 4: Sync daily scores (if trends requested)
            if include_trends:
                self._log_step(f"Syncing daily scores ({days_of_scores} days)...")
                stats['daily_scores'] = self.sync_daily_scores(days=days_of_scores)

                # Step 5: Calculate and store trends
                self._log_step("Calculating trends...")
                stats['trends'] = self.sync_trends()

            # Step 6: Sync free agents (if requested)
            if include_free_agents:
                self._log_step("Syncing free agents...")
                stats['free_agents'] = self.sync_free_agents()

            self.db.log_sync_complete(sync_id, self.api_calls)
            return {
                'status': 'completed',
                'api_calls': self.api_calls,
                **stats
            }

        except Exception as e:
            self.db.log_sync_failed(sync_id, str(e))
            raise

    def sync_league_metadata(self) -> None:
        """Sync league metadata to database."""
        # League data is already fetched during authentication (6 API calls)
        self.db.save_league_metadata(
            league_id=self.league.league_id,
            name=self.league.name,
            year=getattr(self.league, 'year', None),
            start_date=getattr(self.league, 'start_date', None),
            end_date=getattr(self.league, 'end_date', None)
        )

    def sync_teams(self) -> int:
        """
        Sync all teams to database.

        Returns:
            Number of teams synced
        """
        teams_data = []
        for team in self.league.teams:
            teams_data.append({
                'id': team.id,
                'name': team.name,
                'short_name': team.short,
                'logo_url': getattr(team, 'logo', None)
            })

        self.db.save_teams(self.league.league_id, teams_data)
        return len(teams_data)

    def sync_standings(self, debug: bool = False) -> int:
        """
        Sync standings to database.

        Args:
            debug: If True, print raw API field names for debugging

        Returns:
            Number of team standings synced
        """
        from fantraxapi import api as fantrax_api

        try:
            # Get raw API response to see available fields
            response = fantrax_api.get_standings(self.league)
            self.api_calls += 1

            if not response or 'tableList' not in response or not response['tableList']:
                self.console.print("[yellow]Warning: No standings data in API response[/yellow]")
                return 0

            table_data = response['tableList'][0]

            # Extract field names from header
            fields = {c["key"]: i for i, c in enumerate(table_data["header"]["cells"])}

            if debug:
                self.console.print(f"\n[bold cyan]Available standings fields:[/bold cyan] {list(fields.keys())}")

            # Parse standings data directly from raw response
            standings_data = []
            for obj in table_data["rows"]:
                team_id = obj["fixedCells"][1]["teamId"]
                rank = int(obj["fixedCells"][0]["content"])
                cells = obj["cells"]

                # Helper to get cell value
                def get_cell(field_name, default=0):
                    if field_name not in fields:
                        return default
                    idx = fields[field_name]
                    content = cells[idx].get("content", "")
                    if content == "" or content == "-":
                        return default
                    try:
                        # Handle comma-separated numbers
                        return float(content.replace(",", ""))
                    except (ValueError, TypeError):
                        return default

                # Try different field names for fantasy points (varies by league type)
                # fantasyPoints, fPts, pointsFor are all possible field names
                fpts = get_cell("fantasyPoints", 0) or get_cell("fPts", 0) or get_cell("pointsFor", 0)
                # sc (scorer count), gp are possible field names for games played
                gp = get_cell("sc", 0) or get_cell("gp", 0)
                # FPtsPerGame, fpGp, fPtsPerGp are possible field names for FP/G
                fpg = get_cell("FPtsPerGame", 0) or get_cell("fpGp", 0) or get_cell("fPtsPerGp", 0)

                if debug and rank == 1:
                    self.console.print(f"[dim]First team raw values: fantasyPoints={get_cell('fantasyPoints')}, sc={get_cell('sc')}, FPtsPerGame={get_cell('FPtsPerGame')}[/dim]")

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
                    # Store GP separately for display
                    'games_played': int(gp),
                    'fpg': fpg
                })

        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not fetch standings: {e}[/yellow]")
            return 0

        self.db.save_standings(self.league.league_id, standings_data)

        # Log the sync
        sync_id = self.db.log_sync_start('standings', self.league.league_id)
        self.db.log_sync_complete(sync_id, 1)

        return len(standings_data)

    def sync_roster(self, team_id: str) -> dict:
        """
        Sync roster for a single team.

        Args:
            team_id: The team ID

        Returns:
            Dictionary with player and slot counts
        """
        # Find the team
        team = None
        for t in self.league.teams:
            if t.id == team_id:
                team = t
                break

        if not team:
            raise ValueError(f"Team not found: {team_id}")

        # Fetch roster (1 API call)
        roster = team.roster()
        self.api_calls += 1

        # Extract players and roster slots
        players = []
        roster_rows = []

        for row in roster.rows:
            # Extract player info
            if row.player:
                player = row.player
                player_team = getattr(player, 'team', None)
                players.append({
                    'id': player.id,
                    'name': player.name,
                    'short_name': getattr(player, 'short_name', None),
                    'team_name': player_team.name if player_team else None,
                    'team_short_name': player_team.short if player_team else None,
                    'position_short_names': ','.join(getattr(p, 'short', getattr(p, 'short_name', '')) for p in player.positions) if player.positions else None,
                    'day_to_day': 1 if getattr(player, 'day_to_day', False) else 0,
                    'out': 1 if getattr(player, 'out', False) else 0,
                    'injured_reserve': 1 if getattr(player, 'injured_reserve', False) else 0,
                    'suspended': 1 if getattr(player, 'suspended', False) else 0
                })

            # Extract roster slot info
            roster_rows.append({
                'player_id': row.player.id if row.player else None,
                'position_id': getattr(row.position, 'id', '') if row.position else '',
                'position_short': getattr(row.position, 'short', getattr(row.position, 'short_name', '')) if row.position else '',
                'status_id': getattr(row, 'status_id', None),
                'salary': float(row.salary) if getattr(row, 'salary', None) else None,
                'total_fantasy_points': getattr(row, 'total_fantasy_points', getattr(row, 'fantasy_points', None)),
                'fantasy_points_per_game': getattr(row, 'fantasy_points_per_game', None)
            })

        # Save to database
        if players:
            self.db.save_players(players)
        self.db.save_roster(team_id, roster_rows)

        return {
            'players': len(players),
            'roster_slots': len(roster_rows)
        }

    def sync_all_rosters(self) -> dict:
        """
        Sync rosters for all teams.

        Returns:
            Dictionary with total player and slot counts
        """
        total_players = 0
        total_slots = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Syncing rosters...", total=len(self.league.teams))

            for team in self.league.teams:
                progress.update(task, description=f"Syncing {team.short}...")
                stats = self.sync_roster(team.id)
                total_players += stats['players']
                total_slots += stats['roster_slots']
                progress.advance(task)

        return {
            'players': total_players,
            'roster_slots': total_slots
        }

    def sync_daily_scores(self, days: int = 35) -> int:
        """
        Sync daily scores for all teams over the past N days.

        Args:
            days: Number of days to fetch

        Returns:
            Total number of score records saved
        """
        today = date.today()
        start_date = today - timedelta(days=days)
        total_scores = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=self.console
        ) as progress:
            task = progress.add_task("Fetching daily scores...", total=days + 1)

            current_date = start_date
            while current_date <= today:
                progress.update(task, description=f"Fetching {current_date}...")

                # Fetch scores for all teams for this date
                for team in self.league.teams:
                    scores = _get_daily_scores_for_team(self.league, team.id, current_date)
                    if scores:
                        self.db.save_daily_scores(team.id, current_date, scores)
                        total_scores += len(scores)

                self.api_calls += 1  # One API call per date (covers all teams)
                current_date += timedelta(days=1)
                progress.advance(task)

        return total_scores

    def sync_trends(self) -> int:
        """
        Calculate and store trends from cached daily scores.

        Returns:
            Number of players with trends calculated
        """
        today = date.today()

        # Get Fantrax week boundaries
        weeks = _get_fantrax_week_boundaries(today)
        week1_start, week1_end, _ = weeks[0]
        week2_start, week2_end, _ = weeks[1]
        week3_start, week3_end, _ = weeks[2]
        cutoff_30 = week1_end - timedelta(days=29)

        # Get all teams
        teams = self.db.get_teams(self.league.league_id)

        # Collect all daily scores for the 30-day period
        # Group by player
        player_daily = {}  # player_id -> [(date, points), ...]

        for team in teams:
            scores = self.db.get_daily_scores_for_team(
                team['id'],
                cutoff_30,
                week1_end
            )
            for score in scores:
                player_id = score['player_id']
                scoring_date = date.fromisoformat(score['scoring_date'])
                points = score['fantasy_points']

                if player_id not in player_daily:
                    player_daily[player_id] = []
                player_daily[player_id].append((scoring_date, points))

        # Calculate trends for each player
        players_processed = 0

        for player_id, daily_scores in player_daily.items():
            trends = {}

            # Calculate for each week
            for week_start, week_end, label in weeks:
                filtered = [(d, p) for d, p in daily_scores if week_start <= d <= week_end]
                total = sum(p for _, p in filtered)
                games = len(filtered)
                trends[label] = {
                    'total': round(total, 1),
                    'games': games,
                    'fpg': round(total / games, 2) if games > 0 else 0.0,
                    'start': week_start.isoformat(),
                    'end': week_end.isoformat()
                }

            # Calculate 14-day (week1 + week2)
            filtered_14 = [(d, p) for d, p in daily_scores if week2_start <= d <= week1_end]
            total_14 = sum(p for _, p in filtered_14)
            games_14 = len(filtered_14)
            trends['14'] = {
                'total': round(total_14, 1),
                'games': games_14,
                'fpg': round(total_14 / games_14, 2) if games_14 > 0 else 0.0,
                'start': week2_start.isoformat(),
                'end': week1_end.isoformat()
            }

            # Calculate 30-day
            filtered_30 = [(d, p) for d, p in daily_scores if cutoff_30 <= d <= week1_end]
            total_30 = sum(p for _, p in filtered_30)
            games_30 = len(filtered_30)
            trends['30'] = {
                'total': round(total_30, 1),
                'games': games_30,
                'fpg': round(total_30 / games_30, 2) if games_30 > 0 else 0.0,
                'start': cutoff_30.isoformat(),
                'end': week1_end.isoformat()
            }

            self.db.save_player_trends(player_id, trends)
            players_processed += 1

        return players_processed

    def sync_free_agents(
        self,
        sort_keys: Optional[list] = None,
        limit: int = 100
    ) -> int:
        """
        Sync free agent listings.

        Args:
            sort_keys: List of sort keys to fetch (default: ['SCORE'])
            limit: Maximum number of players to fetch per sort/position combo

        Returns:
            Number of free agents synced
        """
        from fantrax_cli.stats import fetch_fa_player_trends

        if sort_keys is None:
            sort_keys = ['SCORE']

        total_fa = 0

        for sort_key in sort_keys:
            # Fetch free agents (this makes API calls internally)
            fa_data = self._fetch_free_agents(sort_key, limit)
            if fa_data:
                # Save players
                self.db.save_players(fa_data['players'])
                # Save free agent listings
                self.db.save_free_agents(fa_data['listings'], sort_key, None)
                total_fa += len(fa_data['listings'])

        return total_fa

    def _fetch_free_agents(self, sort_key: str, limit: int) -> Optional[dict]:
        """
        Fetch free agents from API.

        Args:
            sort_key: Sort key (e.g., 'SCORE', 'FPTS_PER_GAME')
            limit: Maximum number to fetch

        Returns:
            Dictionary with 'players' and 'listings' lists, or None on error
        """
        from fantraxapi.api import Method, _request

        try:
            method_kwargs = {
                'statusOrTeam': 'ALL_AVAILABLE',
                'maxResultsPerPage': str(limit),
                'sortType': sort_key
            }

            response = _request(
                self.league.league_id,
                Method('getPlayerStats', **method_kwargs),
                session=self.league.session
            )
            self.api_calls += 1

            if not response or 'statsTable' not in response:
                return None

            players = []
            listings = []

            for player_data in response['statsTable']:
                scorer = player_data.get('scorer', {})
                player_id = scorer.get('scorerId')
                cells = player_data.get('cells', [])

                # Extract player info
                players.append({
                    'id': player_id,
                    'name': scorer.get('name', ''),
                    'short_name': scorer.get('shortName'),
                    'team_name': scorer.get('teamName'),
                    'team_short_name': scorer.get('teamShortName'),
                    'position_short_names': scorer.get('posShortNames'),
                    'day_to_day': 1 if scorer.get('isDayToDay') else 0,
                    'out': 1 if scorer.get('isOut') else 0,
                    'injured_reserve': 1 if scorer.get('isInjuredReserve') else 0,
                    'suspended': 1 if scorer.get('isSuspended') else 0
                })

                # Extract listing info
                # Standard cells: 0=checkbox, 1=player, 2=status, 3=pos, 4=team, 5=FPts, 6=FP/G
                fpts_str = cells[5].get('content', '0') if len(cells) > 5 else '0'
                fpg_str = cells[6].get('content', '0') if len(cells) > 6 else '0'

                try:
                    fpts = float(fpts_str) if fpts_str else 0.0
                except (ValueError, TypeError):
                    fpts = 0.0
                try:
                    fpg = float(fpg_str) if fpg_str else 0.0
                except (ValueError, TypeError):
                    fpg = 0.0

                listings.append({
                    'id': player_id,
                    'total_fpts': fpts,
                    'fpg': fpg
                })

            return {'players': players, 'listings': listings}

        except Exception as e:
            self.console.print(f"[red]Error fetching free agents: {e}[/red]")
            return None

    def _log_step(self, message: str) -> None:
        """Log a sync step."""
        self.console.print(f"[bold blue]→[/bold blue] {message}")


def get_sync_status(db: DatabaseManager, league_id: str) -> dict:
    """
    Get comprehensive sync status for a league.

    Args:
        db: DatabaseManager instance
        league_id: League ID

    Returns:
        Dictionary with sync status information
    """
    from fantrax_cli.database import get_cache_age_hours

    status = {
        'league_id': league_id,
        'has_data': False,
        'last_full_sync': None,
        'sync_types': {},
        'data_counts': {}
    }

    # Get league metadata
    league_meta = db.get_league_metadata(league_id)
    if league_meta:
        status['has_data'] = True
        status['league_name'] = league_meta['name']

    # Get sync status for all types
    all_syncs = db.get_all_sync_status(league_id)
    for sync_type, sync_info in all_syncs.items():
        if sync_info:
            age_hours = get_cache_age_hours(sync_info['completed_at'])
            status['sync_types'][sync_type] = {
                'last_sync': sync_info['completed_at'],
                'age_hours': round(age_hours, 1) if age_hours else None,
                'api_calls': sync_info['api_calls_made']
            }
            if sync_type == 'full':
                status['last_full_sync'] = sync_info['completed_at']

    # Get data counts
    teams = db.get_teams(league_id)
    status['data_counts']['teams'] = len(teams)

    # Count players (approximate via roster slots)
    total_players = 0
    for team in teams:
        roster = db.get_roster(team['id'])
        total_players += len([r for r in roster if r.get('player_id')])
    status['data_counts']['rostered_players'] = total_players

    # Get daily scores date range
    date_range = db.get_daily_scores_date_range()
    if date_range:
        status['data_counts']['daily_scores_range'] = {
            'start': date_range[0],
            'end': date_range[1]
        }

    return status
