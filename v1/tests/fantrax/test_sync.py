"""Unit tests for sync module."""

import pytest
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import tempfile

from aissistant_gm.fantrax.database import DatabaseManager
from aissistant_gm.fantrax.sync import SyncManager, get_sync_status


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

    # Create mock standings
    mock_record1 = Mock()
    mock_record1.team = team1
    mock_record1.win = 10
    mock_record1.loss = 5
    mock_record1.tie = 2
    mock_record1.points = 22
    mock_record1.win_percentage = 0.647
    mock_record1.games_back = 0
    mock_record1.wavier_wire_order = 2
    mock_record1.points_for = 1250.5
    mock_record1.points_against = 1100.0
    mock_record1.streak = "W3"

    mock_record2 = Mock()
    mock_record2.team = team2
    mock_record2.win = 8
    mock_record2.loss = 7
    mock_record2.tie = 2
    mock_record2.points = 18
    mock_record2.win_percentage = 0.529
    mock_record2.games_back = 2
    mock_record2.wavier_wire_order = 1
    mock_record2.points_for = 1100.0
    mock_record2.points_against = 1150.0
    mock_record2.streak = "L1"

    mock_standings = Mock()
    mock_standings.ranks = {1: mock_record1, 2: mock_record2}
    league.standings.return_value = mock_standings

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

    @patch('aissistant_gm.fantrax.sync._get_daily_scores_for_team')
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

    @patch('aissistant_gm.fantrax.sync._get_daily_scores_for_team')
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

    @patch('aissistant_gm.fantrax.sync._get_daily_scores_for_team')
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


class TestSyncStandings:
    """Tests for sync_standings method."""

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_success(self, mock_get_standings, db_manager, mock_league):
        """Test syncing standings from raw API response."""
        # Mock API response (raw format used by sync_standings)
        mock_get_standings.return_value = {
            'tableList': [{
                'header': {
                    'cells': [
                        {'key': 'rank'},
                        {'key': 'team'},
                        {'key': 'win'},
                        {'key': 'loss'},
                        {'key': 'tie'},
                        {'key': 'points'},
                        {'key': 'winpc'},
                        {'key': 'gamesback'},
                        {'key': 'wwOrder'},
                        {'key': 'fantasyPoints'},
                        {'key': 'pointsAgainst'},
                        {'key': 'streak'},
                        {'key': 'sc'},
                        {'key': 'FPtsPerGame'}
                    ]
                },
                'rows': [
                    {
                        'fixedCells': [
                            {'content': '1'},
                            {'teamId': 'team1', 'content': 'Team Alpha'}
                        ],
                        'cells': [
                            {'content': '1'},  # rank
                            {'content': 'Team Alpha'},  # team
                            {'content': '10'},  # win
                            {'content': '5'},  # loss
                            {'content': '2'},  # tie
                            {'content': '22'},  # points
                            {'content': '0.647'},  # winpc
                            {'content': '0'},  # gamesback
                            {'content': '2'},  # wwOrder
                            {'content': '1,250.5'},  # fantasyPoints
                            {'content': '1,100.0'},  # pointsAgainst
                            {'content': 'W3'},  # streak
                            {'content': '17'},  # sc (games played)
                            {'content': '73.5'}  # FPtsPerGame
                        ]
                    },
                    {
                        'fixedCells': [
                            {'content': '2'},
                            {'teamId': 'team2', 'content': 'Team Beta'}
                        ],
                        'cells': [
                            {'content': '2'},
                            {'content': 'Team Beta'},
                            {'content': '8'},
                            {'content': '7'},
                            {'content': '2'},
                            {'content': '18'},
                            {'content': '0.529'},
                            {'content': '2'},
                            {'content': '1'},
                            {'content': '1,100.0'},
                            {'content': '1,150.0'},
                            {'content': 'L1'},
                            {'content': '17'},
                            {'content': '64.7'}
                        ]
                    }
                ]
            }]
        }

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        count = manager.sync_standings()

        assert count == 2
        assert manager.api_calls == 1

        # Verify standings were saved
        standings = db_manager.get_standings(mock_league.league_id)
        assert len(standings) == 2
        # Verify first team standings
        team1_standing = next((s for s in standings if s['team_id'] == 'team1'), None)
        assert team1_standing is not None
        assert team1_standing['wins'] == 10
        assert team1_standing['losses'] == 5
        assert team1_standing['ties'] == 2

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_empty_response(self, mock_get_standings, db_manager, mock_league):
        """Test handling of empty API response."""
        mock_get_standings.return_value = {}

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_standings()

        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_no_tablelist(self, mock_get_standings, db_manager, mock_league):
        """Test handling when tableList is missing."""
        mock_get_standings.return_value = {'tableList': []}

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_standings()

        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_api_exception(self, mock_get_standings, db_manager, mock_league):
        """Test handling of API exception."""
        mock_get_standings.side_effect = Exception("API Error")

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_standings()

        # Should handle gracefully and return 0
        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_missing_fields(self, mock_get_standings, db_manager, mock_league):
        """Test handling of missing optional fields in API response."""
        # Response with minimal fields
        mock_get_standings.return_value = {
            'tableList': [{
                'header': {
                    'cells': [
                        {'key': 'win'},
                        {'key': 'loss'}
                    ]
                },
                'rows': [
                    {
                        'fixedCells': [
                            {'content': '1'},
                            {'teamId': 'team1', 'content': 'Team Alpha'}
                        ],
                        'cells': [
                            {'content': '10'},
                            {'content': '5'}
                        ]
                    }
                ]
            }]
        }

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        count = manager.sync_standings()

        # Should still work with missing fields (using defaults)
        assert count == 1

    @patch('aissistant_gm.fantrax.fantraxapi.api.get_standings')
    def test_sync_standings_dash_values(self, mock_get_standings, db_manager, mock_league):
        """Test handling of dash '-' values (common for missing data)."""
        mock_get_standings.return_value = {
            'tableList': [{
                'header': {
                    'cells': [
                        {'key': 'win'},
                        {'key': 'loss'},
                        {'key': 'fantasyPoints'}
                    ]
                },
                'rows': [
                    {
                        'fixedCells': [
                            {'content': '1'},
                            {'teamId': 'team1', 'content': 'Team Alpha'}
                        ],
                        'cells': [
                            {'content': '10'},
                            {'content': '-'},  # Dash value
                            {'content': '-'}   # Dash value
                        ]
                    }
                ]
            }]
        }

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        count = manager.sync_standings()

        # Should handle dash values as defaults (0)
        assert count == 1


class TestSyncTransactions:
    """Tests for sync_transactions method."""

    def test_sync_transactions_success(self, db_manager, mock_league):
        """Test syncing transactions."""
        # Create mock transaction
        mock_player = Mock()
        mock_player.id = "player1"
        mock_player.name = "Test Player"
        mock_player.short_name = "T. Player"
        mock_player.team = Mock()
        mock_player.team.name = "Boston Bruins"
        mock_player.team.short = "BOS"
        mock_player.positions = []
        mock_player.day_to_day = False
        mock_player.out = False
        mock_player.injured_reserve = False
        mock_player.suspended = False
        mock_player.type = "CLAIM"

        mock_tx = Mock()
        mock_tx.id = "tx1"
        mock_tx.team = Mock()
        mock_tx.team.id = "team1"
        mock_tx.date = date(2025, 1, 15)
        mock_tx.players = [mock_player]

        mock_league.transactions.return_value = [mock_tx]

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        count = manager.sync_transactions(count=10)

        assert count == 1
        assert manager.api_calls == 1

        # Verify transaction was saved
        transactions = db_manager.get_transactions(mock_league.league_id)
        assert len(transactions) == 1
        assert transactions[0]['id'] == 'tx1'

    def test_sync_transactions_empty(self, db_manager, mock_league):
        """Test handling of empty transactions list."""
        mock_league.transactions.return_value = []

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_transactions()

        assert count == 0

    def test_sync_transactions_none(self, db_manager, mock_league):
        """Test handling of None transactions response."""
        mock_league.transactions.return_value = None

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_transactions()

        assert count == 0

    def test_sync_transactions_api_error(self, db_manager, mock_league):
        """Test handling of API error."""
        mock_league.transactions.side_effect = Exception("API Error")

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_transactions()

        # Should handle gracefully and return 0
        assert count == 0

    def test_sync_transactions_multiple_players(self, db_manager, mock_league):
        """Test transaction with multiple players (e.g., trade)."""
        # Create mock players
        mock_player1 = Mock()
        mock_player1.id = "player1"
        mock_player1.name = "Player One"
        mock_player1.short_name = "P. One"
        mock_player1.team = None  # Free agent
        mock_player1.positions = []
        mock_player1.day_to_day = False
        mock_player1.out = False
        mock_player1.injured_reserve = False
        mock_player1.suspended = False
        mock_player1.type = "DROP"

        mock_player2 = Mock()
        mock_player2.id = "player2"
        mock_player2.name = "Player Two"
        mock_player2.short_name = "P. Two"
        mock_player2.team = Mock()
        mock_player2.team.name = "Toronto Maple Leafs"
        mock_player2.team.short = "TOR"
        mock_player2.positions = [Mock(short="C")]
        mock_player2.day_to_day = True
        mock_player2.out = False
        mock_player2.injured_reserve = False
        mock_player2.suspended = False
        mock_player2.type = "ADD"

        mock_tx = Mock()
        mock_tx.id = "tx2"
        mock_tx.team = Mock()
        mock_tx.team.id = "team1"
        mock_tx.date = date(2025, 1, 16)
        mock_tx.players = [mock_player1, mock_player2]

        mock_league.transactions.return_value = [mock_tx]

        manager = SyncManager(mock_league, db_manager)
        manager.sync_league_metadata()
        manager.sync_teams()

        count = manager.sync_transactions()

        assert count == 1

        # Verify both players were saved
        player1 = db_manager.get_player("player1")
        player2 = db_manager.get_player("player2")
        assert player1 is not None
        assert player2 is not None
        assert player2['day_to_day'] == 1


class TestSyncFreeAgents:
    """Tests for free agent syncing."""

    @patch('aissistant_gm.fantrax.sync.SyncManager._fetch_free_agents')
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
        # Use include_trends=False to avoid needing to mock the trends API
        result = manager.sync_free_agents(sort_keys=['SCORE'], limit=10, include_trends=False)

        assert result['players'] == 2
        assert result['trends'] == 0

        # Verify players were saved
        player = db_manager.get_player('fa1')
        assert player is not None
        assert player['name'] == 'Free Agent 1'

    @patch('aissistant_gm.fantrax.sync.SyncManager._fetch_free_agents')
    def test_sync_free_agents_error(self, mock_fetch, db_manager, mock_league):
        """Test handling of free agent fetch error."""
        mock_fetch.return_value = None

        manager = SyncManager(mock_league, db_manager)
        result = manager.sync_free_agents(include_trends=False)

        assert result['players'] == 0
        assert result['trends'] == 0


class TestSyncPlayerNews:
    """Tests for player news syncing."""

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news(self, mock_request, db_manager, mock_league):
        """Test syncing player news."""
        # Set up roster with players
        db_manager.save_league_metadata('test_league', 'Test League')
        db_manager.save_teams('test_league', [{'id': 'team1', 'name': 'Team 1', 'short': 'T1'}])
        db_manager.save_players([{'id': 'player1', 'name': 'Test Player'}])
        db_manager.save_roster('team1', [
            {'player_id': 'player1', 'position_id': '1', 'position_short': 'C'}
        ])

        # Mock the API response (new getPlayerNews format)
        mock_request.return_value = {
            'stories': [
                {
                    'scorerFantasy': {
                        'scorerId': 'player1',
                        'name': 'Test Player'
                    },
                    'playerNews': {
                        'newsDate': 1737795600000,  # Timestamp in ms
                        'headlineNoBrief': 'Player scores hat trick',
                        'analysis': 'Amazing performance.'
                    }
                }
            ]
        }

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_player_news(player_ids=['player1'])

        assert count == 1

        # Verify news was saved
        news = db_manager.get_player_news('player1')
        assert len(news) == 1
        assert news[0]['headline'] == 'Player scores hat trick'

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news_api_error(self, mock_request, db_manager, mock_league):
        """Test handling of API error during news sync."""
        db_manager.save_players([{'id': 'player1', 'name': 'Test Player'}])
        db_manager.save_roster('team1', [
            {'player_id': 'player1', 'position_id': '1', 'position_short': 'C'}
        ])

        mock_request.side_effect = Exception("API Error")

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_player_news(player_ids=['player1'])

        # Should handle error gracefully and return 0
        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news_no_news(self, mock_request, db_manager, mock_league):
        """Test syncing player with no news."""
        db_manager.save_players([{'id': 'player1', 'name': 'Test Player'}])

        # API response with no stories
        mock_request.return_value = {'stories': []}

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_player_news(player_ids=['player1'])

        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news_empty_player_list(self, mock_request, db_manager, mock_league):
        """Test syncing with empty player list still calls API but filters results."""
        # Even with empty filter, API is called but nothing is stored
        mock_request.return_value = {'stories': []}

        manager = SyncManager(mock_league, db_manager)
        count = manager.sync_player_news(player_ids=[])

        # With empty player list, nothing should be stored
        assert count == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news_filters_by_player_ids(self, mock_request, db_manager, mock_league):
        """Test that sync_player_news filters news to specified player IDs."""
        from aissistant_gm.fantrax.config import Config

        # Set up roster with one player
        db_manager.save_league_metadata('test_league', 'Test League')
        db_manager.save_teams('test_league', [{'id': 'team1', 'name': 'Team 1', 'short': 'T1'}])
        db_manager.save_players([
            {'id': 'rostered1', 'name': 'Rostered Player'},
            {'id': 'fa1', 'name': 'Free Agent 1'},
            {'id': 'other', 'name': 'Other Player'},
        ])
        db_manager.save_roster('team1', [
            {'player_id': 'rostered1', 'position_id': '1', 'position_short': 'C'}
        ])

        # Set up free agents
        db_manager.save_free_agents([
            {'id': 'fa1', 'fpg': 3.0},
        ], 'SCORE', None)

        # Mock the API response with news for multiple players
        mock_request.return_value = {
            'stories': [
                {
                    'scorerFantasy': {'scorerId': 'rostered1', 'name': 'Rostered Player'},
                    'playerNews': {
                        'newsDate': 1737795600000,
                        'headlineNoBrief': 'Rostered player news',
                        'analysis': 'Analysis'
                    }
                },
                {
                    'scorerFantasy': {'scorerId': 'fa1', 'name': 'Free Agent 1'},
                    'playerNews': {
                        'newsDate': 1737795600000,
                        'headlineNoBrief': 'FA news',
                        'analysis': 'FA Analysis'
                    }
                },
                {
                    'scorerFantasy': {'scorerId': 'other', 'name': 'Other Player'},
                    'playerNews': {
                        'newsDate': 1737795600000,
                        'headlineNoBrief': 'Other player news',
                        'analysis': 'Other'
                    }
                },
            ]
        }

        # Create config with fa_news_limit=1 (only top 1 FA)
        config = Config(
            username='test', password='test', league_id='test_league',
            fa_news_limit=1
        )

        manager = SyncManager(mock_league, db_manager, config=config)
        count = manager.sync_player_news()  # No explicit player_ids - should auto-detect

        # Should sync for rostered player (1) + top FA (1) = 2 players (not 'other')
        assert count == 2
        # Verify only rostered1 and fa1 have news
        assert len(db_manager.get_player_news('rostered1')) == 1
        assert len(db_manager.get_player_news('fa1')) == 1
        assert len(db_manager.get_player_news('other')) == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api.request')
    def test_sync_player_news_fa_disabled(self, mock_request, db_manager, mock_league):
        """Test that free agents are not included in filter when fa_news_limit=0."""
        from aissistant_gm.fantrax.config import Config

        # Set up roster and free agents
        db_manager.save_league_metadata('test_league', 'Test League')
        db_manager.save_teams('test_league', [{'id': 'team1', 'name': 'Team 1', 'short': 'T1'}])
        db_manager.save_players([
            {'id': 'rostered1', 'name': 'Rostered Player'},
            {'id': 'fa1', 'name': 'Free Agent 1'},
        ])
        db_manager.save_roster('team1', [
            {'player_id': 'rostered1', 'position_id': '1', 'position_short': 'C'}
        ])
        db_manager.save_free_agents([{'id': 'fa1', 'fpg': 3.0}], 'SCORE', None)

        mock_request.return_value = {
            'stories': [
                {
                    'scorerFantasy': {'scorerId': 'rostered1', 'name': 'Rostered Player'},
                    'playerNews': {
                        'newsDate': 1737795600000,
                        'headlineNoBrief': 'Rostered news',
                        'analysis': 'Analysis'
                    }
                },
                {
                    'scorerFantasy': {'scorerId': 'fa1', 'name': 'Free Agent 1'},
                    'playerNews': {
                        'newsDate': 1737795600000,
                        'headlineNoBrief': 'FA news',
                        'analysis': 'FA Analysis'
                    }
                },
            ]
        }

        # Config with fa_news_limit=0 (disabled)
        config = Config(
            username='test', password='test', league_id='test_league',
            fa_news_limit=0
        )

        manager = SyncManager(mock_league, db_manager, config=config)
        count = manager.sync_player_news()

        # Should only sync rostered player, not FA (fa1 filtered out)
        assert count == 1
        assert len(db_manager.get_player_news('rostered1')) == 1
        assert len(db_manager.get_player_news('fa1')) == 0


# ============================================================================
# Tests for sync_command function (CLI command)
# ============================================================================

import aissistant_gm.fantrax.commands.sync as sync_command_module
import typer


class TestSyncCommandFunction:
    """Tests for the sync_command CLI function."""

    def _create_mock_context(self, league_id=None):
        """Create a mock typer context."""
        ctx = Mock(spec=typer.Context)
        ctx.obj = {"league_id": league_id}
        return ctx

    def _create_mock_config(self):
        """Create a mock config object."""
        config = Mock()
        config.league_id = "test-league-123"
        config.database_path = ":memory:"
        config.username = "test@test.com"
        config.password = "testpass"
        config.cookie_path = "/tmp/cookies.json"
        config.cookie_file = "/tmp/cookies.json"
        config.min_request_interval = 1
        config.selenium_timeout = 10
        config.login_wait_time = 5
        config.browser_window_size = "1920,1080"
        config.user_agent = "TestAgent"
        config.scraper_max_retries = 3
        config.scraper_retry_delay = 2.0
        config.scraper_retry_backoff = 2.0
        config.max_news_per_player = 30
        return config

    def test_no_option_shows_help(self):
        """Test that no option shows help message."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            # Execute without any flags
            sync_command_module.sync_command(ctx)

            # Should show "No sync option specified" message
            mock_console.print.assert_any_call("[yellow]No sync option specified. Use --help for options.[/yellow]")

    def test_status_flag_shows_status(self):
        """Test that --status flag shows cache status."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class, \
             patch.object(sync_command_module, '_show_status') as mock_show_status:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            sync_command_module.sync_command(ctx, status=True)

            mock_show_status.assert_called_once_with(mock_console, mock_db, mock_config.league_id)

    def test_clear_flag_with_confirmation(self):
        """Test that --clear flag clears cache with confirmation."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class, \
             patch.object(sync_command_module, 'typer') as mock_typer:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            mock_typer.confirm.return_value = True

            sync_command_module.sync_command(ctx, clear=True)

            mock_db.clear_all.assert_called_once()
            mock_console.print.assert_any_call("[green]✓[/green] Cache cleared successfully")

    def test_clear_flag_cancelled(self):
        """Test that --clear flag respects cancelled confirmation."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class, \
             patch.object(sync_command_module, 'typer') as mock_typer:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            mock_typer.confirm.return_value = False

            sync_command_module.sync_command(ctx, clear=True)

            mock_db.clear_all.assert_not_called()

    def test_clear_flag_with_yes_skips_confirmation(self):
        """Test that --clear --yes skips confirmation."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            sync_command_module.sync_command(ctx, clear=True, yes=True)

            mock_db.clear_all.assert_called_once()

    def test_teams_flag_syncs_teams(self):
        """Test that --teams flag syncs teams."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class, \
             patch.object(sync_command_module, 'get_authenticated_league') as mock_auth, \
             patch.object(sync_command_module, 'SyncManager') as mock_sync_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_auth.return_value = mock_league

            mock_sync_manager = Mock()
            mock_sync_manager.api_calls = 1
            mock_sync_manager.sync_teams.return_value = 6
            mock_sync_class.return_value = mock_sync_manager

            sync_command_module.sync_command(ctx, teams=True)

            mock_sync_manager.sync_league_metadata.assert_called_once()
            mock_sync_manager.sync_teams.assert_called_once()

    def test_config_error_exits(self):
        """Test that configuration error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'Console') as mock_console_class:

            mock_load_config.side_effect = ValueError("Missing required config")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                sync_command_module.sync_command(ctx, teams=True)

            assert exc_info.value.exit_code == 1

    def test_general_error_exits(self):
        """Test that general error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(sync_command_module, 'load_config') as mock_load_config, \
             patch.object(sync_command_module, 'DatabaseManager') as mock_db_class, \
             patch.object(sync_command_module, 'Console') as mock_console_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db_class.side_effect = Exception("Database error")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                sync_command_module.sync_command(ctx, teams=True)

            assert exc_info.value.exit_code == 1


class TestShowStatus:
    """Tests for the _show_status helper function."""

    def test_empty_database(self):
        """Test status display with empty database."""
        mock_console = Mock()
        mock_db = Mock()

        with patch.object(sync_command_module, 'get_sync_status') as mock_get_status:
            mock_get_status.return_value = {
                'has_data': False,
                'league_id': 'test-league',
                'data_counts': {},
                'sync_types': {}
            }

            sync_command_module._show_status(mock_console, mock_db, 'test-league')

            mock_console.print.assert_any_call("[yellow]No cached data found.[/yellow]")

    def test_with_data(self):
        """Test status display with existing data."""
        mock_console = Mock()
        mock_db = Mock()
        mock_db.db_path = "/path/to/db.sqlite"

        with patch.object(sync_command_module, 'get_sync_status') as mock_get_status:
            mock_get_status.return_value = {
                'has_data': True,
                'league_name': 'Test League',
                'league_id': 'test-league',
                'data_counts': {
                    'teams': 6,
                    'rostered_players': 120
                },
                'sync_types': {
                    'full': {
                        'last_sync': '2025-01-15T10:30:00',
                        'age_hours': 2.5,
                        'api_calls': 50
                    }
                }
            }

            sync_command_module._show_status(mock_console, mock_db, 'test-league')

            # Verify league name and db path are printed
            mock_console.print.assert_any_call("League: Test League")
            mock_console.print.assert_any_call("Database: /path/to/db.sqlite")

    def test_with_daily_scores_range(self):
        """Test status display shows daily scores range."""
        mock_console = Mock()
        mock_db = Mock()
        mock_db.db_path = "/path/to/db.sqlite"

        with patch.object(sync_command_module, 'get_sync_status') as mock_get_status:
            mock_get_status.return_value = {
                'has_data': True,
                'league_name': 'Test League',
                'league_id': 'test-league',
                'data_counts': {
                    'teams': 6,
                    'rostered_players': 120,
                    'daily_scores_range': {
                        'start': '2025-01-01',
                        'end': '2025-01-15'
                    }
                },
                'sync_types': {}
            }

            sync_command_module._show_status(mock_console, mock_db, 'test-league')

            mock_console.print.assert_any_call("  Daily Scores: 2025-01-01 to 2025-01-15")


class TestShowSyncResult:
    """Tests for the _show_sync_result helper function."""

    def test_basic_result(self):
        """Test sync result display with basic data."""
        mock_console = Mock()

        result = {
            'status': 'completed',
            'teams': 6,
            'players': 120,
            'roster_slots': 180,
            'daily_scores': 1000,
            'trends': 100,
            'api_calls': 50
        }

        sync_command_module._show_sync_result(mock_console, result)

        # Verify success message
        mock_console.print.assert_any_call("[bold green]✓ Sync completed successfully![/bold green]")
        mock_console.print.assert_any_call("  Teams synced: 6")
        mock_console.print.assert_any_call("  Players synced: 120")

    def test_result_with_all_fields(self):
        """Test sync result display with all optional fields."""
        mock_console = Mock()

        result = {
            'status': 'completed',
            'teams': 6,
            'standings': 6,
            'players': 120,
            'roster_slots': 180,
            'daily_scores': 1000,
            'trends': 100,
            'transactions': 50,
            'free_agents': 500,
            'player_news': 200,
            'api_calls': 75
        }

        sync_command_module._show_sync_result(mock_console, result)

        # Verify all optional fields are displayed
        mock_console.print.assert_any_call("  Standings synced: 6")
        mock_console.print.assert_any_call("  Transactions: 50")
        mock_console.print.assert_any_call("  Free agents: 500")
        mock_console.print.assert_any_call("  Player news: 200")
        mock_console.print.assert_any_call("  Total API calls: 75")
