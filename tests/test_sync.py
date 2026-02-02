"""Unit tests for sync module."""

import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import tempfile

from fantrax_cli.database import DatabaseManager
from fantrax_cli.sync import SyncManager, get_sync_status


@pytest.fixture
def temp_db_path():
    """Create a temporary database file path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir) / "test_cache.db"


@pytest.fixture
def db_manager(temp_db_path):
    """Create a DatabaseManager with a temporary database."""
    return DatabaseManager(db_path=temp_db_path)


@pytest.fixture
def mock_league():
    """Create a mock League object."""
    league = Mock()
    league.league_id = "test_league_123"
    league.name = "Test Fantasy League"
    league.year = "2024-25"
    league.start_date = datetime(2024, 10, 1)
    league.end_date = datetime(2025, 4, 30)
    league.scoring_dates = {
        i: date(2025, 1, 1) + timedelta(days=i)
        for i in range(35)
    }

    # Create mock teams
    team1 = Mock()
    team1.id = "team1"
    team1.name = "Team Alpha"
    team1.short = "ALP"
    team1.logo = None

    team2 = Mock()
    team2.id = "team2"
    team2.name = "Team Beta"
    team2.short = "BET"
    team2.logo = None

    league.teams = [team1, team2]

    return league


@pytest.fixture
def mock_roster():
    """Create a mock Roster object."""
    # Create mock player
    player_team = Mock()
    player_team.name = "Boston Bruins"
    player_team.short = "BOS"

    position = Mock()
    position.id = "2010"
    position.short = "C"

    player = Mock()
    player.id = "player1"
    player.name = "Test Player"
    player.short_name = "T. Player"
    player.team = player_team
    player.positions = [position]
    player.day_to_day = False
    player.out = False
    player.injured_reserve = False
    player.suspended = False

    # Create mock roster row
    row = Mock()
    row.player = player
    row.position = position
    row.status_id = "1"
    row.salary = 5.0
    row.total_fantasy_points = 100.5
    row.fantasy_points_per_game = 2.5

    # Create empty slot
    empty_row = Mock()
    empty_row.player = None
    empty_row.position = position
    empty_row.status_id = "1"
    empty_row.salary = None
    empty_row.total_fantasy_points = None
    empty_row.fantasy_points_per_game = None

    roster = Mock()
    roster.rows = [row, empty_row]

    return roster


class TestSyncManager:
    """Tests for SyncManager class."""

    def test_init(self, db_manager, mock_league):
        """Test SyncManager initialization."""
        manager = SyncManager(mock_league, db_manager)
        assert manager.league == mock_league
        assert manager.db == db_manager
        assert manager.api_calls == 0

    def test_sync_league_metadata(self, db_manager, mock_league):
        """Test syncing league metadata."""
        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()

        # Verify data was saved
        meta = db_manager.get_league_metadata(mock_league.league_id)
        assert meta is not None
        assert meta['name'] == "Test Fantasy League"
        assert meta['year'] == "2024-25"

    def test_sync_teams(self, db_manager, mock_league):
        """Test syncing teams."""
        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_teams()

        assert count == 2

        # Verify teams were saved
        teams = db_manager.get_teams(mock_league.league_id)
        assert len(teams) == 2
        team_names = {t['name'] for t in teams}
        assert "Team Alpha" in team_names
        assert "Team Beta" in team_names

    def test_sync_roster(self, db_manager, mock_league, mock_roster):
        """Test syncing a single team roster."""
        # Configure mock team to return mock roster
        mock_league.teams[0].roster.return_value = mock_roster

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_roster("team1")

        assert result['players'] == 1
        assert result['roster_slots'] == 2
        assert manager.api_calls == 1

        # Verify player was saved
        player = db_manager.get_player("player1")
        assert player is not None
        assert player['name'] == "Test Player"

        # Verify roster was saved
        roster = db_manager.get_roster("team1")
        assert len(roster) == 2

    def test_sync_all_rosters(self, db_manager, mock_league, mock_roster):
        """Test syncing all team rosters."""
        # Configure both teams to return mock rosters
        for team in mock_league.teams:
            team.roster.return_value = mock_roster

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_all_rosters()

        assert result['players'] == 2  # 1 player per team
        assert result['roster_slots'] == 4  # 2 slots per team
        assert manager.api_calls == 2  # 1 call per team

    @patch('fantrax_cli.sync._get_daily_scores_for_team')
    def test_sync_daily_scores(self, mock_get_scores, db_manager, mock_league):
        """Test syncing daily scores."""
        # Mock daily scores
        mock_get_scores.return_value = {
            'player1': 5.5,
            'player2': 3.0
        }

        manager = SyncManager(mock_league, db_manager)
        manager.sync_teams()  # Need teams first

        count = manager.sync_daily_scores(days=3)

        # Should have scores for 4 days (today + 3 days back)
        # 2 players per day * 2 teams * 4 days = potentially 16, but we mock same return
        assert count > 0

    def test_sync_trends_from_cached_scores(self, db_manager, mock_league):
        """Test calculating trends from cached daily scores."""
        manager = SyncManager(mock_league, db_manager)
        manager.sync_teams()

        # Manually add some daily scores
        today = date.today()
        for i in range(14):
            scoring_date = today - timedelta(days=i)
            db_manager.save_daily_scores(
                "team1",
                scoring_date,
                {"player1": 2.5 + (i % 3)}  # Vary points slightly
            )

        # Calculate trends
        count = manager.sync_trends()
        assert count >= 1

        # Verify trends were saved
        trends = db_manager.get_player_trends("player1")
        assert 'week1' in trends or 'week2' in trends or '14' in trends


class TestSyncManagerFullSync:
    """Tests for full sync operation."""

    @patch('fantrax_cli.sync._get_daily_scores_for_team')
    def test_sync_all(self, mock_get_scores, db_manager, mock_league, mock_roster):
        """Test full sync operation."""
        # Configure mocks
        for team in mock_league.teams:
            team.roster.return_value = mock_roster

        mock_get_scores.return_value = {'player1': 2.5}

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_all(
            include_trends=True,
            days_of_scores=3,
            include_free_agents=False
        )

        assert result['status'] == 'completed'
        assert result['teams'] == 2
        assert result['players'] >= 0
        assert result['api_calls'] > 0

        # Verify sync was logged
        last_sync = db_manager.get_last_sync(mock_league.league_id, 'full')
        assert last_sync is not None
        assert last_sync['status'] == 'completed'

    @patch('fantrax_cli.sync._get_daily_scores_for_team')
    def test_sync_all_logs_failure(self, mock_get_scores, db_manager, mock_league):
        """Test that failed syncs are logged."""
        # Make the roster call fail
        mock_league.teams[0].roster.side_effect = Exception("API Error")

        manager = SyncManager(mock_league, db_manager)

        with pytest.raises(Exception):
            manager.sync_all(include_trends=False)

        # Check that failure was NOT logged as completed
        last_sync = db_manager.get_last_sync(mock_league.league_id, 'full')
        assert last_sync is None  # get_last_sync only returns completed syncs


class TestGetSyncStatus:
    """Tests for get_sync_status function."""

    def test_empty_database(self, db_manager):
        """Test status with empty database."""
        status = get_sync_status(db_manager, "test_league")

        assert status['league_id'] == "test_league"
        assert status['has_data'] is False
        assert status['last_full_sync'] is None

    def test_with_data(self, db_manager, mock_league, mock_roster):
        """Test status after syncing data."""
        # Configure mock
        for team in mock_league.teams:
            team.roster.return_value = mock_roster

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()
        manager.sync_all_rosters()

        # Log a completed sync
        sync_id = db_manager.log_sync_start('full', mock_league.league_id)
        db_manager.log_sync_complete(sync_id, 10)

        status = get_sync_status(db_manager, mock_league.league_id)

        assert status['has_data'] is True
        assert status['league_name'] == "Test Fantasy League"
        assert status['data_counts']['teams'] == 2
        assert 'full' in status['sync_types']

    def test_with_daily_scores(self, db_manager, mock_league):
        """Test status includes daily scores range."""
        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        # Add daily scores
        db_manager.save_daily_scores("team1", date(2025, 1, 1), {"p1": 1.0})
        db_manager.save_daily_scores("team1", date(2025, 1, 15), {"p1": 2.0})

        status = get_sync_status(db_manager, mock_league.league_id)

        assert 'daily_scores_range' in status['data_counts']
        assert status['data_counts']['daily_scores_range']['start'] == '2025-01-01'
        assert status['data_counts']['daily_scores_range']['end'] == '2025-01-15'


class TestSyncRosterWithVariousPlayers:
    """Tests for roster syncing with different player states."""

    def test_sync_roster_with_injured_player(self, db_manager, mock_league):
        """Test syncing roster with injured player."""
        # Create injured player
        player_team = Mock()
        player_team.name = "Boston Bruins"
        player_team.short = "BOS"

        position = Mock()
        position.id = "2010"
        position.short = "C"

        player = Mock()
        player.id = "injured_player"
        player.name = "Injured Guy"
        player.short_name = "I. Guy"
        player.team = player_team
        player.positions = [position]
        player.day_to_day = False
        player.out = False
        player.injured_reserve = True
        player.suspended = False

        row = Mock()
        row.player = player
        row.position = position
        row.status_id = "3"  # IR
        row.salary = 3.0
        row.total_fantasy_points = 0.0
        row.fantasy_points_per_game = 0.0

        roster = Mock()
        roster.rows = [row]

        mock_league.teams[0].roster.return_value = roster

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_roster("team1")

        assert result['players'] == 1

        # Verify injured status was saved
        player_data = db_manager.get_player("injured_player")
        assert player_data['injured_reserve'] == 1

    def test_sync_roster_empty_slot(self, db_manager, mock_league):
        """Test syncing roster with empty slots only."""
        position = Mock()
        position.id = "2010"
        position.short = "C"

        row = Mock()
        row.player = None
        row.position = position
        row.status_id = "1"
        row.salary = None
        row.total_fantasy_points = None
        row.fantasy_points_per_game = None

        roster = Mock()
        roster.rows = [row]

        mock_league.teams[0].roster.return_value = roster

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_roster("team1")

        assert result['players'] == 0
        assert result['roster_slots'] == 1


class TestSyncFreeAgents:
    """Tests for free agent syncing."""

    @patch('fantrax_cli.sync.SyncManager._fetch_free_agents')
    def test_sync_free_agents(self, mock_fetch, db_manager, mock_league):
        """Test syncing free agents."""
        mock_fetch.return_value = {
            'players': [
                {'id': 'fa1', 'name': 'Free Agent 1', 'position_short_names': 'C'},
                {'id': 'fa2', 'name': 'Free Agent 2', 'position_short_names': 'LW'},
            ],
            'listings': [
                {'id': 'fa1', 'total_fpts': 50.0, 'fpg': 2.5},
                {'id': 'fa2', 'total_fpts': 45.0, 'fpg': 2.25},
            ]
        }

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_free_agents(sort_keys=['SCORE'], limit=10)

        assert count == 2

        # Verify players were saved
        player = db_manager.get_player('fa1')
        assert player is not None
        assert player['name'] == 'Free Agent 1'

    @patch('fantrax_cli.sync.SyncManager._fetch_free_agents')
    def test_sync_free_agents_error(self, mock_fetch, db_manager, mock_league):
        """Test handling of free agent fetch error."""
        mock_fetch.return_value = None

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_free_agents()

        assert count == 0
