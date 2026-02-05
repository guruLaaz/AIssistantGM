"""Tests for display module."""

import json
from io import StringIO
from unittest.mock import Mock, patch
import pytest
from aissistant_gm.fantrax.display import (
    _get_team_attr,
    format_teams_table,
    format_teams_json,
    format_teams_simple,
    format_roster_table,
    format_roster_json,
    format_roster_simple,
)


class TestGetTeamAttr:
    """Test _get_team_attr helper function."""

    def test_get_attr_from_dict(self):
        """Test getting attribute from dict."""
        team = {'id': 'team1', 'name': 'Test Team'}
        assert _get_team_attr(team, 'id') == 'team1'
        assert _get_team_attr(team, 'name') == 'Test Team'

    def test_get_attr_from_object(self):
        """Test getting attribute from object."""
        class Team:
            id = 'team1'
            name = 'Test Team'
        team = Team()
        assert _get_team_attr(team, 'id') == 'team1'
        assert _get_team_attr(team, 'name') == 'Test Team'

    def test_get_attr_with_default(self):
        """Test default value when attribute missing."""
        team = {'id': 'team1'}
        assert _get_team_attr(team, 'name', 'Unknown') == 'Unknown'

    def test_get_attr_missing_returns_empty_string(self):
        """Test empty string default when attribute missing."""
        team = {'id': 'team1'}
        assert _get_team_attr(team, 'name') == ''


class MockTeam:
    """Mock Team object for testing."""

    def __init__(self, team_id, name, short):
        self.id = team_id
        self.name = name
        self.short = short


@pytest.fixture
def sample_teams():
    """Create sample teams for testing."""
    return [
        MockTeam("team1", "The Warriors", "WAR"),
        MockTeam("team2", "Team Phoenix", "PHX"),
        MockTeam("team3", "Ice Dragons", "ICE"),
    ]


class TestFormatTeamsTable:
    """Test format_teams_table function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_without_league_name(self, mock_console, sample_teams):
        """Test table formatting without league name."""
        format_teams_table(sample_teams)

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_with_league_name(self, mock_console, sample_teams):
        """Test table formatting with league name."""
        format_teams_table(sample_teams, league_name="Test League")

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_empty_list(self, mock_console):
        """Test table formatting with empty teams list."""
        format_teams_table([])

        # Should still create console
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_with_standings(self, mock_console, sample_teams):
        """Test table formatting with standings data."""
        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 150.5, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 15, 'fpg': 10.03},
            {'team_id': 'team2', 'rank': 2, 'points_for': 140.0, 'wins': 8, 'losses': 7, 'ties': 0, 'games_played': 15, 'fpg': 9.33},
            {'team_id': 'team3', 'rank': 3, 'points_for': 130.0, 'wins': 5, 'losses': 10, 'ties': 0, 'games_played': 15, 'fpg': 8.67},
        ]
        format_teams_table(sample_teams, league_name="Test League", standings=standings)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_standings_calculates_fpg(self, mock_console, sample_teams):
        """Test that FP/G is calculated when not provided."""
        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 150.0, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 0},
        ]
        format_teams_table(sample_teams[:1], standings=standings)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_table_standings_with_dict_teams(self, mock_console):
        """Test table formatting with dict teams and standings."""
        teams = [
            {'id': 'team1', 'name': 'Warriors', 'short_name': 'WAR'},
            {'id': 'team2', 'name': 'Phoenix', 'short_name': 'PHX'},
        ]
        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 100.0, 'games_played': 10, 'fpg': 10.0},
            {'team_id': 'team2', 'rank': 2, 'points_for': 80.0, 'games_played': 10, 'fpg': 8.0},
        ]
        format_teams_table(teams, standings=standings)
        mock_console.assert_called_once()


class TestFormatTeamsJson:
    """Test format_teams_json function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_basic(self, mock_console, sample_teams):
        """Test JSON formatting with just teams and league_id."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_teams_json(sample_teams, league_id="test123")

        # Verify print_json was called
        mock_console_instance.print_json.assert_called_once()

        # Get the JSON string that was passed
        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["league_id"] == "test123"
        assert len(output_data["teams"]) == 3
        assert output_data["teams"][0]["id"] == "team1"
        assert output_data["teams"][0]["name"] == "The Warriors"
        assert output_data["teams"][0]["short"] == "WAR"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_with_league_name(self, mock_console, sample_teams):
        """Test JSON formatting with league name."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_teams_json(
            sample_teams,
            league_id="test123",
            league_name="Test League"
        )

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["league_name"] == "Test League"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_with_year(self, mock_console, sample_teams):
        """Test JSON formatting with year."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_teams_json(
            sample_teams,
            league_id="test123",
            year="2025-2026"
        )

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["year"] == "2025-2026"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_empty_list(self, mock_console):
        """Test JSON formatting with empty teams list."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_teams_json([], league_id="test123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["teams"] == []

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_with_standings(self, mock_console, sample_teams):
        """Test JSON formatting with standings data."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 150.5, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 15, 'fpg': 10.03},
            {'team_id': 'team2', 'rank': 2, 'points_for': 140.0, 'wins': 8, 'losses': 7, 'ties': 0, 'games_played': 15, 'fpg': 9.33},
        ]

        format_teams_json(sample_teams[:2], league_id="test123", standings=standings)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert 'standings' in output_data['teams'][0]
        assert output_data['teams'][0]['standings']['rank'] == 1
        assert output_data['teams'][0]['standings']['fpts'] == 150.5

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_teams_json_standings_calculates_fpg(self, mock_console, sample_teams):
        """Test JSON standings calculates FP/G when not provided."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 150.0, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 0},
        ]

        format_teams_json(sample_teams[:1], league_id="test123", standings=standings)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        # FP/G should be calculated: 150 / 15 = 10.0
        assert output_data['teams'][0]['standings']['fpg'] == 10.0


class TestFormatTeamsSimple:
    """Test format_teams_simple function."""

    def test_format_teams_simple(self, sample_teams, capsys):
        """Test simple text formatting."""
        format_teams_simple(sample_teams)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert len(output_lines) == 3
        assert "The Warriors (WAR)" in output_lines[0]
        assert "Team Phoenix (PHX)" in output_lines[1]
        assert "Ice Dragons (ICE)" in output_lines[2]

    def test_format_teams_simple_empty_list(self, capsys):
        """Test simple formatting with empty teams list."""
        format_teams_simple([])

        captured = capsys.readouterr()
        assert captured.out == ""

    def test_format_teams_simple_single_team(self, capsys):
        """Test simple formatting with single team."""
        teams = [MockTeam("team1", "Solo Team", "SOL")]
        format_teams_simple(teams)

        captured = capsys.readouterr()
        assert "Solo Team (SOL)" in captured.out

    def test_format_teams_simple_with_standings(self, sample_teams, capsys):
        """Test simple formatting with standings data."""
        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 1500.5, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 15, 'fpg': 100.03},
            {'team_id': 'team2', 'rank': 2, 'points_for': 1400.0, 'wins': 8, 'losses': 7, 'ties': 0, 'games_played': 15, 'fpg': 93.33},
            {'team_id': 'team3', 'rank': 3, 'points_for': 1300.0, 'wins': 5, 'losses': 10, 'ties': 0, 'games_played': 15, 'fpg': 86.67},
        ]
        format_teams_simple(sample_teams, standings=standings)

        captured = capsys.readouterr()
        output = captured.out

        # Verify standings info is included
        assert "1." in output
        assert "1,500.5 FPts" in output
        assert "FP/G" in output
        assert "GP" in output

    def test_format_teams_simple_with_standings_calculates_fpg(self, sample_teams, capsys):
        """Test simple formatting calculates FP/G when not provided."""
        standings = [
            {'team_id': 'team1', 'rank': 1, 'points_for': 150.0, 'wins': 10, 'losses': 5, 'ties': 0, 'games_played': 0},
        ]
        format_teams_simple(sample_teams[:1], standings=standings)

        captured = capsys.readouterr()
        output = captured.out

        # FP/G should be calculated: 150 / 15 = 10.0
        assert "10.00 FP/G" in output
        assert "15 GP" in output

    def test_format_teams_simple_with_standings_id_key(self, capsys):
        """Test simple formatting with standings using 'id' key instead of 'team_id'."""
        teams = [{'id': 'team1', 'name': 'Test Team', 'short_name': 'TT'}]
        standings = [
            {'id': 'team1', 'rank': 1, 'points_for': 100.0, 'games_played': 10, 'fpg': 10.0},
        ]
        format_teams_simple(teams, standings=standings)

        captured = capsys.readouterr()
        assert "1." in captured.out
        assert "100.0 FPts" in captured.out


class MockPosition:
    """Mock Position object for testing."""

    def __init__(self, short_name):
        self.short_name = short_name


class MockPlayer:
    """Mock Player object for testing."""

    def __init__(self, player_id=None, name=None, suspended=False, injured_reserve=False, out=False, day_to_day=False):
        self.id = player_id
        self.name = name
        self.suspended = suspended
        self.injured_reserve = injured_reserve
        self.out = out
        self.day_to_day = day_to_day


class MockRosterRow:
    """Mock RosterRow object for testing."""

    def __init__(self, position_short, player_name=None, fp_total=None, fp_per_game=None, status_id=None, salary=None, player_id=None, suspended=False, injured_reserve=False, out=False, day_to_day=False):
        self.position = MockPosition(position_short)
        if player_name:
            self.player = MockPlayer(
                player_id=player_id or f"player_{player_name.replace(' ', '_').lower()}",
                name=player_name,
                suspended=suspended,
                injured_reserve=injured_reserve,
                out=out,
                day_to_day=day_to_day
            )
        else:
            self.player = None
        self.total_fantasy_points = fp_total
        self.fantasy_points_per_game = fp_per_game
        self.status_id = status_id
        self.salary = salary


class MockRoster:
    """Mock Roster object for testing."""

    def __init__(self, rows, active=10, active_max=10, reserve=3, reserve_max=5, injured=0, injured_max=3):
        self.rows = rows
        self.active = active
        self.active_max = active_max
        self.reserve = reserve
        self.reserve_max = reserve_max
        self.injured = injured
        self.injured_max = injured_max


@pytest.fixture
def sample_roster():
    """Create sample roster for testing."""
    rows = [
        MockRosterRow("C", "Connor McDavid", 125.5, 8.37),
        MockRosterRow("LW", "Artemi Panarin", 110.2, 7.35),
        MockRosterRow("RW", "Nikita Kucherov", 118.7, 7.91),
        MockRosterRow("D", "Cale Makar", 95.3, 6.35),
        MockRosterRow("G", "Igor Shesterkin", 88.4, 5.89),
        MockRosterRow("BN", None),  # Empty bench slot
    ]
    return MockRoster(rows, active=5, active_max=5, reserve=1, reserve_max=3, injured=0, injured_max=2)


class TestFormatRosterTable:
    """Test format_roster_table function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_without_team_name(self, mock_console, sample_roster):
        """Test roster table formatting without team name."""
        format_roster_table(sample_roster)

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_team_name(self, mock_console, sample_roster):
        """Test roster table formatting with team name."""
        format_roster_table(sample_roster, team_name="Test Team")

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_empty_roster(self, mock_console):
        """Test roster table formatting with empty roster."""
        empty_roster = MockRoster([])
        format_roster_table(empty_roster)

        # Should still create console
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_recent_trends(self, mock_console):
        """Test roster table formatting with recent trends."""
        rows = [
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
            MockRosterRow("LW", "Artemi Panarin", 110.2, 7.35, status_id="1", player_id="p2"),
        ]
        roster = MockRoster(rows)

        recent_trends = {
            "p1": {
                "week1": {"games_played": 3, "fpg": 9.5},
                "week2": {"games_played": 4, "fpg": 8.2},
                "week3": {"games_played": 3, "fpg": 7.8},
                "14": {"games_played": 7, "fpg": 8.7},
                "30": {"games_played": 14, "fpg": 8.3}
            },
            "p2": {
                "week1": {"games_played": 2, "fpg": 7.5},
                "week2": {"games_played": 3, "fpg": 6.8},
                "week3": {"games_played": 4, "fpg": 7.2},
                "14": {"games_played": 5, "fpg": 7.1},
                "30": {"games_played": 12, "fpg": 7.0}
            }
        }

        format_roster_table(roster, team_name="Test Team", recent_trends=recent_trends)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_recent_trends_missing_player(self, mock_console):
        """Test roster table with trends but player not in trends dict."""
        rows = [MockRosterRow("C", "Unknown Player", 50.0, 5.0, status_id="1", player_id="unknown")]
        roster = MockRoster(rows)

        recent_trends = {}  # Empty trends dict

        format_roster_table(roster, recent_trends=recent_trends)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_recent_stats(self, mock_console):
        """Test roster table formatting with recent stats."""
        rows = [
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
            MockRosterRow("LW", "Artemi Panarin", 110.2, 7.35, status_id="2", player_id="p2"),
        ]
        roster = MockRoster(rows)

        recent_stats = {
            "p1": {"games_played": 5, "total_points": 45.5, "fpg": 9.1},
            "p2": {"games_played": 5, "total_points": 35.0, "fpg": 7.0},
        }

        format_roster_table(roster, recent_stats=recent_stats, last_n_days=7)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_recent_stats_missing_player(self, mock_console):
        """Test roster table with stats but player not in stats dict."""
        rows = [MockRosterRow("C", "Unknown Player", 50.0, 5.0, status_id="1", player_id="unknown")]
        roster = MockRoster(rows)

        recent_stats = {}  # Empty stats dict

        format_roster_table(roster, recent_stats=recent_stats, last_n_days=7)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_various_status_ids(self, mock_console):
        """Test roster table with different status IDs."""
        rows = [
            MockRosterRow("C", "Active Player", 100.0, 5.0, status_id="1"),  # Active
            MockRosterRow("BN", "Reserve Player", 80.0, 4.0, status_id="2"),  # Reserve
            MockRosterRow("IR", "Injured Player", 60.0, 3.0, status_id="3"),  # IR
            MockRosterRow("X", "Unknown Status", 40.0, 2.0, status_id="99"),  # Unknown
        ]
        roster = MockRoster(rows)

        format_roster_table(roster)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_injury_reports(self, mock_console):
        """Test roster table with various injury report flags."""
        rows = [
            MockRosterRow("C", "Suspended Player", 100.0, 5.0, status_id="1", suspended=True),
            MockRosterRow("LW", "IR Player", 80.0, 4.0, status_id="1", injured_reserve=True),
            MockRosterRow("RW", "Out Player", 60.0, 3.0, status_id="1", out=True),
            MockRosterRow("D", "DTD Player", 40.0, 2.0, status_id="1", day_to_day=True),
        ]
        roster = MockRoster(rows)

        format_roster_table(roster)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_table_with_salary(self, mock_console):
        """Test roster table with salary data."""
        rows = [
            MockRosterRow("C", "Rich Player", 100.0, 5.0, status_id="1", salary=12500000.0),
            MockRosterRow("LW", "Budget Player", 80.0, 4.0, status_id="1", salary=925000.0),
            MockRosterRow("RW", "No Salary", 60.0, 3.0, status_id="1"),  # No salary
        ]
        roster = MockRoster(rows)

        format_roster_table(roster)
        mock_console.assert_called_once()


class TestFormatRosterJson:
    """Test format_roster_json function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_basic(self, mock_console, sample_roster):
        """Test JSON formatting with just roster and team_id."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_roster_json(sample_roster, team_id="team123")

        # Verify print_json was called
        mock_console_instance.print_json.assert_called_once()

        # Get the JSON string that was passed
        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["team_id"] == "team123"
        assert "roster_stats" in output_data
        assert output_data["roster_stats"]["active"] == "5/5"
        assert output_data["roster_stats"]["reserve"] == "1/3"
        assert output_data["roster_stats"]["injured"] == "0/2"
        assert len(output_data["players"]) == 6

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_team_name(self, mock_console, sample_roster):
        """Test JSON formatting with team name."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_roster_json(sample_roster, team_id="team123", team_name="Test Team")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["team_name"] == "Test Team"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_empty_slots(self, mock_console):
        """Test JSON formatting with empty roster slots."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        roster_with_empty = MockRoster([MockRosterRow("BN", None)])
        format_roster_json(roster_with_empty, team_id="team123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["players"][0]["player_name"] is None

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_recent_trends(self, mock_console):
        """Test JSON formatting with recent trends."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
        ]
        roster = MockRoster(rows)

        recent_trends = {
            "p1": {
                "week1": {"games_played": 3, "total_points": 28.5, "fpg": 9.5, "start": "2025-01-20", "end": "2025-01-26"},
                "week2": {"games_played": 4, "total_points": 32.8, "fpg": 8.2, "start": "2025-01-13", "end": "2025-01-19"},
                "week3": {"games_played": 3, "total_points": 23.4, "fpg": 7.8, "start": "2025-01-06", "end": "2025-01-12"},
                "14": {"games_played": 7, "total_points": 60.9, "fpg": 8.7},
                "30": {"games_played": 14, "total_points": 116.2, "fpg": 8.3}
            }
        }

        format_roster_json(roster, team_id="team123", recent_trends=recent_trends)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["stats_period"] == "recent_trends"
        assert "trends" in output_data["players"][0]
        assert output_data["players"][0]["trends"]["week1"]["fpg"] == 9.5

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_recent_trends_missing_player(self, mock_console):
        """Test JSON formatting with trends but player not in trends dict."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [
            MockRosterRow("C", "Known Player", 100.0, 5.0, status_id="1", player_id="known"),
            MockRosterRow("LW", "Unknown Player", 50.0, 5.0, status_id="1", player_id="unknown"),
        ]
        roster = MockRoster(rows)

        # Provide trends only for one player
        recent_trends = {
            "known": {
                "week1": {"games_played": 3, "total_points": 28.5, "fpg": 9.5, "start": "", "end": ""},
                "week2": {"games_played": 4, "total_points": 32.8, "fpg": 8.2, "start": "", "end": ""},
                "week3": {"games_played": 3, "total_points": 23.4, "fpg": 7.8, "start": "", "end": ""},
                "14": {"games_played": 7, "total_points": 60.9, "fpg": 8.7},
                "30": {"games_played": 14, "total_points": 116.2, "fpg": 8.3}
            }
        }

        format_roster_json(roster, team_id="team123", recent_trends=recent_trends)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        # Known player should have actual trends
        assert output_data["players"][0]["trends"]["week1"]["games_played"] == 3
        # Unknown player should have empty default trends
        assert output_data["players"][1]["trends"]["week1"]["games_played"] == 0

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_last_n_days(self, mock_console):
        """Test JSON formatting with last N days stats."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
            MockRosterRow("LW", "Artemi Panarin", 110.2, 7.35, status_id="2", player_id="p2"),
        ]
        roster = MockRoster(rows)

        recent_stats = {
            "p1": {"games_played": 5, "total_points": 45.5, "fpg": 9.1},
            "p2": {"games_played": 5, "total_points": 35.0, "fpg": 7.0},
        }

        format_roster_json(roster, team_id="team123", recent_stats=recent_stats, last_n_days=7)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["stats_period"] == "last_7_days"
        assert output_data["players"][0]["games_played"] == 5
        assert output_data["players"][0]["fantasy_points_per_game"] == 9.1

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_last_n_days_missing_player(self, mock_console):
        """Test JSON formatting with last N days but player not in stats dict."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [MockRosterRow("C", "Unknown Player", 50.0, 5.0, status_id="1", player_id="unknown")]
        roster = MockRoster(rows)

        recent_stats = {}  # Empty stats dict

        format_roster_json(roster, team_id="team123", recent_stats=recent_stats, last_n_days=7)

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        # Should have default zero values
        assert output_data["players"][0]["games_played"] == 0
        assert output_data["players"][0]["fantasy_points_per_game"] == 0.0

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_various_status_ids(self, mock_console):
        """Test JSON formatting with different status IDs."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [
            MockRosterRow("C", "Active Player", 100.0, 5.0, status_id="1"),  # Active
            MockRosterRow("BN", "Reserve Player", 80.0, 4.0, status_id="2"),  # Reserve
            MockRosterRow("IR", "IR Player", 60.0, 3.0, status_id="3"),  # IR
            MockRosterRow("X", "Unknown Status", 40.0, 2.0, status_id="99"),  # Unknown
        ]
        roster = MockRoster(rows)

        format_roster_json(roster, team_id="team123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["players"][0]["roster_status"] == "Active"
        assert output_data["players"][1]["roster_status"] == "Reserve"
        assert output_data["players"][2]["roster_status"] == "IR"
        assert output_data["players"][3]["roster_status"] == "Unknown"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_roster_json_with_injury_reports(self, mock_console):
        """Test JSON formatting with various injury report flags."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        rows = [
            MockRosterRow("C", "Suspended Player", 100.0, 5.0, status_id="1", suspended=True),
            MockRosterRow("LW", "IR Player", 80.0, 4.0, status_id="1", injured_reserve=True),
            MockRosterRow("RW", "Out Player", 60.0, 3.0, status_id="1", out=True),
            MockRosterRow("D", "DTD Player", 40.0, 2.0, status_id="1", day_to_day=True),
            MockRosterRow("G", "Healthy Player", 30.0, 1.5, status_id="1"),
        ]
        roster = MockRoster(rows)

        format_roster_json(roster, team_id="team123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["players"][0]["injury_report"] == "Suspended"
        assert output_data["players"][1]["injury_report"] == "IR"
        assert output_data["players"][2]["injury_report"] == "Out"
        assert output_data["players"][3]["injury_report"] == "DTD"
        assert output_data["players"][4]["injury_report"] is None


class TestFormatRosterSimple:
    """Test format_roster_simple function."""

    def test_format_roster_simple(self, sample_roster, capsys):
        """Test simple text formatting."""
        format_roster_simple(sample_roster)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert len(output_lines) == 6
        assert "C (Unknown): Connor McDavid" in output_lines[0]
        assert "LW (Unknown): Artemi Panarin" in output_lines[1]
        assert "BN (Unknown): (Empty)" in output_lines[5]

    def test_format_roster_simple_empty_roster(self, capsys):
        """Test simple formatting with empty roster."""
        empty_roster = MockRoster([])
        format_roster_simple(empty_roster)

        captured = capsys.readouterr()
        assert captured.out == ""

    def test_format_roster_simple_all_empty_slots(self, capsys):
        """Test simple formatting with all empty slots."""
        roster = MockRoster([
            MockRosterRow("C", None),
            MockRosterRow("LW", None),
        ])
        format_roster_simple(roster)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert len(output_lines) == 2
        assert "C (Unknown): (Empty)" in output_lines[0]
        assert "LW (Unknown): (Empty)" in output_lines[1]

    def test_format_roster_simple_with_status_ids(self, capsys):
        """Test simple formatting shows roster status correctly."""
        roster = MockRoster([
            MockRosterRow("C", "Active Player", 100.0, 5.0, status_id="1"),  # Active
            MockRosterRow("BN", "Reserve Player", 80.0, 4.0, status_id="2"),  # Reserve
            MockRosterRow("IR", "IR Player", 60.0, 3.0, status_id="3"),  # IR
            MockRosterRow("X", "Unknown Status", 40.0, 2.0, status_id="99"),  # Unknown
        ])
        format_roster_simple(roster)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert "(Active)" in output_lines[0]
        assert "(Reserve)" in output_lines[1]
        assert "(IR)" in output_lines[2]
        assert "(Unknown)" in output_lines[3]

    def test_format_roster_simple_with_injury_reports(self, capsys):
        """Test simple formatting shows injury flags correctly."""
        roster = MockRoster([
            MockRosterRow("C", "Suspended Player", 100.0, 5.0, status_id="1", suspended=True),
            MockRosterRow("LW", "IR Player", 80.0, 4.0, status_id="1", injured_reserve=True),
            MockRosterRow("RW", "Out Player", 60.0, 3.0, status_id="1", out=True),
            MockRosterRow("D", "DTD Player", 40.0, 2.0, status_id="1", day_to_day=True),
        ])
        format_roster_simple(roster)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert "[Suspended]" in output_lines[0]
        assert "[IR]" in output_lines[1]
        assert "[Out]" in output_lines[2]
        assert "[DTD]" in output_lines[3]

    def test_format_roster_simple_with_recent_trends(self, capsys):
        """Test simple formatting with recent trends."""
        roster = MockRoster([
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
        ])

        recent_trends = {
            "p1": {
                "week1": {"games_played": 3, "fpg": 9.5},
                "week2": {"games_played": 4, "fpg": 8.2},
                "week3": {"games_played": 3, "fpg": 7.8},
                "14": {"games_played": 7, "fpg": 8.7},
                "30": {"games_played": 14, "fpg": 8.3}
            }
        }

        format_roster_simple(roster, recent_trends=recent_trends)

        captured = capsys.readouterr()
        output = captured.out

        assert "W1:3G/9.50" in output
        assert "W2:4G/8.20" in output
        assert "W3:3G/7.80" in output
        assert "14d:8.70" in output
        assert "30d:8.30" in output

    def test_format_roster_simple_with_recent_stats(self, capsys):
        """Test simple formatting with recent stats."""
        roster = MockRoster([
            MockRosterRow("C", "Connor McDavid", 125.5, 8.37, status_id="1", player_id="p1"),
        ])

        recent_stats = {
            "p1": {"games_played": 5, "total_points": 45.5, "fpg": 9.1},
        }

        format_roster_simple(roster, recent_stats=recent_stats)

        captured = capsys.readouterr()
        output = captured.out

        assert "5G" in output
        assert "9.10 FP/G" in output

    def test_format_roster_simple_with_salary(self, capsys):
        """Test simple formatting shows salary correctly."""
        roster = MockRoster([
            MockRosterRow("C", "Rich Player", 100.0, 5.0, status_id="1", salary=12500000.0),
            MockRosterRow("LW", "No Salary", 80.0, 4.0, status_id="1"),
        ])
        format_roster_simple(roster)

        captured = capsys.readouterr()
        output_lines = captured.out.strip().split('\n')

        assert "$12,500,000" in output_lines[0]
        assert "N/A" in output_lines[1]


# ==================== Player News Display Tests ====================

from aissistant_gm.fantrax.display import (
    format_news_table,
    format_news_detail,
    format_news_json,
    format_news_simple
)


@pytest.fixture
def sample_news_items():
    """Create sample news items for testing."""
    return [
        {
            'player_id': 'p1',
            'player_name': 'Connor McDavid',
            'news_date': '2025-01-25T14:13:00',
            'headline': 'McDavid scores two goals in victory',
            'analysis': 'McDavid continues his dominant play with two goals.'
        },
        {
            'player_id': 'p2',
            'player_name': 'Leon Draisaitl',
            'news_date': '2025-01-24T10:00:00',
            'headline': 'Draisaitl adds assist in overtime win',
            'analysis': None
        },
        {
            'player_id': 'p1',
            'player_name': 'Connor McDavid',
            'news_date': '2025-01-20T09:00:00',
            'headline': 'McDavid listed day-to-day with minor injury',
            'analysis': 'Expected to return within a week.'
        }
    ]


class TestFormatNewsTable:
    """Test format_news_table function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_table_basic(self, mock_console, sample_news_items):
        """Test news table formatting."""
        format_news_table(sample_news_items)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_table_with_title(self, mock_console, sample_news_items):
        """Test news table formatting with custom title."""
        format_news_table(sample_news_items, title="Team News")
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_table_empty(self, mock_console):
        """Test news table formatting with empty list."""
        mock_instance = Mock()
        mock_console.return_value = mock_instance

        format_news_table([])

        # Should print "No news items found" message
        mock_instance.print.assert_called()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_table_date_formats(self, mock_console):
        """Test news table handles various date formats."""
        news_items = [
            {'player_name': 'Player 1', 'news_date': '2025-01-25T14:13:00Z', 'headline': 'ISO date with Z'},
            {'player_name': 'Player 2', 'news_date': '2025-01-24', 'headline': 'Simple YYYY-MM-DD date'},
            {'player_name': 'Player 3', 'news_date': 'Invalid Date', 'headline': 'Invalid date format'},
            {'player_name': 'Player 4', 'news_date': '', 'headline': 'Empty date'},
            {'player_name': 'Player 5', 'headline': 'Missing date field'},
        ]
        format_news_table(news_items)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_table_truncates_long_headlines(self, mock_console):
        """Test news table truncates headlines over 80 characters."""
        news_items = [{
            'player_name': 'Test Player',
            'news_date': '2025-01-25',
            'headline': 'A' * 100  # 100 character headline
        }]
        format_news_table(news_items)
        mock_console.assert_called_once()


class TestFormatNewsDetail:
    """Test format_news_detail function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_detail_basic(self, mock_console, sample_news_items):
        """Test detailed news formatting."""
        format_news_detail(sample_news_items)
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_detail_with_player_name(self, mock_console, sample_news_items):
        """Test detailed news with player name header."""
        format_news_detail(sample_news_items, player_name="Connor McDavid")
        mock_console.assert_called_once()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_detail_empty(self, mock_console):
        """Test detailed news formatting with empty list."""
        mock_instance = Mock()
        mock_console.return_value = mock_instance

        format_news_detail([])

        # Should print "No news items found" message
        mock_instance.print.assert_called()

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_detail_date_formats(self, mock_console):
        """Test detailed news handles various date formats."""
        news_items = [
            {'news_date': '2025-01-25T14:13:00Z', 'headline': 'ISO date with Z', 'analysis': None},
            {'news_date': '2025-01-24', 'headline': 'Simple YYYY-MM-DD date', 'analysis': None},
            {'news_date': 'Invalid Date', 'headline': 'Invalid date format', 'analysis': None},
            {'news_date': '', 'headline': 'Empty date', 'analysis': 'Some analysis'},
        ]
        format_news_detail(news_items)
        mock_console.assert_called_once()


class TestFormatNewsJson:
    """Test format_news_json function."""

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_json_basic(self, mock_console, sample_news_items):
        """Test JSON news formatting."""
        mock_instance = Mock()
        mock_console.return_value = mock_instance

        format_news_json(sample_news_items)

        call_args = mock_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert 'news_items' in output_data
        assert 'count' in output_data
        assert output_data['count'] == 3
        assert len(output_data['news_items']) == 3

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_json_with_player_name(self, mock_console, sample_news_items):
        """Test JSON news formatting with player name."""
        mock_instance = Mock()
        mock_console.return_value = mock_instance

        format_news_json(sample_news_items, player_name="Connor McDavid")

        call_args = mock_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data['player_name'] == "Connor McDavid"

    @patch('aissistant_gm.fantrax.display.Console')
    def test_format_news_json_empty(self, mock_console):
        """Test JSON news formatting with empty list."""
        mock_instance = Mock()
        mock_console.return_value = mock_instance

        format_news_json([])

        call_args = mock_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data['count'] == 0
        assert output_data['news_items'] == []


class TestFormatNewsSimple:
    """Test format_news_simple function."""

    def test_format_news_simple(self, sample_news_items, capsys):
        """Test simple news formatting."""
        format_news_simple(sample_news_items)

        captured = capsys.readouterr()
        output = captured.out

        assert 'Connor McDavid' in output
        assert 'Leon Draisaitl' in output
        assert 'McDavid scores two goals' in output
        assert 'Draisaitl adds assist' in output

    def test_format_news_simple_with_analysis(self, capsys):
        """Test simple news formatting includes analysis."""
        news_items = [{
            'player_name': 'Test Player',
            'news_date': '2025-01-25',
            'headline': 'Test headline',
            'analysis': 'Test analysis text'
        }]
        format_news_simple(news_items)

        captured = capsys.readouterr()
        assert 'Analysis: Test analysis text' in captured.out

    def test_format_news_simple_empty(self, capsys):
        """Test simple news formatting with empty list."""
        format_news_simple([])

        captured = capsys.readouterr()
        assert 'No news items found' in captured.out
