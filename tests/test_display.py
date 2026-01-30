"""Tests for display module."""

import json
from io import StringIO
from unittest.mock import Mock, patch
import pytest
from fantrax_cli.display import format_teams_table, format_teams_json, format_teams_simple


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
