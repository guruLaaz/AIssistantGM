"""Tests for display module."""

import json
from io import StringIO
from unittest.mock import Mock, patch
import pytest
from fantrax_cli.display import (
    format_teams_table,
    format_teams_json,
    format_teams_simple,
    format_roster_table,
    format_roster_json,
    format_roster_simple,
)


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

    @patch('fantrax_cli.display.Console')
    def test_format_teams_table_without_league_name(self, mock_console, sample_teams):
        """Test table formatting without league name."""
        format_teams_table(sample_teams)

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('fantrax_cli.display.Console')
    def test_format_teams_table_with_league_name(self, mock_console, sample_teams):
        """Test table formatting with league name."""
        format_teams_table(sample_teams, league_name="Test League")

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('fantrax_cli.display.Console')
    def test_format_teams_table_empty_list(self, mock_console):
        """Test table formatting with empty teams list."""
        format_teams_table([])

        # Should still create console
        mock_console.assert_called_once()


class TestFormatTeamsJson:
    """Test format_teams_json function."""

    @patch('fantrax_cli.display.Console')
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

    @patch('fantrax_cli.display.Console')
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

    @patch('fantrax_cli.display.Console')
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

    @patch('fantrax_cli.display.Console')
    def test_format_teams_json_empty_list(self, mock_console):
        """Test JSON formatting with empty teams list."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_teams_json([], league_id="test123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["teams"] == []


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


class MockPosition:
    """Mock Position object for testing."""

    def __init__(self, short_name):
        self.short_name = short_name


class MockPlayer:
    """Mock Player object for testing."""

    def __init__(self, name, suspended=False, injured_reserve=False, out=False, day_to_day=False):
        self.name = name
        self.suspended = suspended
        self.injured_reserve = injured_reserve
        self.out = out
        self.day_to_day = day_to_day


class MockRosterRow:
    """Mock RosterRow object for testing."""

    def __init__(self, position_short, player_name=None, fp_total=None, fp_per_game=None, status_id=None, salary=None):
        self.position = MockPosition(position_short)
        self.player = MockPlayer(player_name) if player_name else None
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

    @patch('fantrax_cli.display.Console')
    def test_format_roster_table_without_team_name(self, mock_console, sample_roster):
        """Test roster table formatting without team name."""
        format_roster_table(sample_roster)

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('fantrax_cli.display.Console')
    def test_format_roster_table_with_team_name(self, mock_console, sample_roster):
        """Test roster table formatting with team name."""
        format_roster_table(sample_roster, team_name="Test Team")

        # Verify Console was instantiated
        mock_console.assert_called_once()

    @patch('fantrax_cli.display.Console')
    def test_format_roster_table_empty_roster(self, mock_console):
        """Test roster table formatting with empty roster."""
        empty_roster = MockRoster([])
        format_roster_table(empty_roster)

        # Should still create console
        mock_console.assert_called_once()


class TestFormatRosterJson:
    """Test format_roster_json function."""

    @patch('fantrax_cli.display.Console')
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

    @patch('fantrax_cli.display.Console')
    def test_format_roster_json_with_team_name(self, mock_console, sample_roster):
        """Test JSON formatting with team name."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        format_roster_json(sample_roster, team_id="team123", team_name="Test Team")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["team_name"] == "Test Team"

    @patch('fantrax_cli.display.Console')
    def test_format_roster_json_empty_slots(self, mock_console):
        """Test JSON formatting with empty roster slots."""
        mock_console_instance = Mock()
        mock_console.return_value = mock_console_instance

        roster_with_empty = MockRoster([MockRosterRow("BN", None)])
        format_roster_json(roster_with_empty, team_id="team123")

        call_args = mock_console_instance.print_json.call_args[0][0]
        output_data = json.loads(call_args)

        assert output_data["players"][0]["player_name"] is None


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
