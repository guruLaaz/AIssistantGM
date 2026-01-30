"""Integration tests for Fantrax CLI commands.

These tests verify that the CLI commands work correctly end-to-end by running
actual commands and validating their output.

Run with: pytest integration_tests/
Or skip: pytest -m "not integration"
"""

import subprocess
import json
import pytest


@pytest.fixture
def cli_runner():
    """Fixture to run CLI commands."""
    def run_command(*args):
        """Run a fantrax CLI command and return the result."""
        result = subprocess.run(
            ["fantrax"] + list(args),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return result
    return run_command


# Teams command tests
@pytest.mark.integration
class TestTeamsCommand:
    """Integration tests for the teams command."""

    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
        (["--format", "simple"], "simple"),
    ])
    def test_teams_output_formats(self, cli_runner, format_arg, format_name):
        """Test teams command with different output formats."""
        result = cli_runner("teams", *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        if format_name == "table":
            assert "Teams" in result.stdout
            assert "Total teams:" in result.stdout
        elif format_name == "json":
            data = json.loads(result.stdout)
            assert "league_id" in data
            assert "teams" in data
            assert len(data["teams"]) > 0
        elif format_name == "simple":
            lines = result.stdout.strip().split('\n')
            assert len(lines) > 0
            assert '(' in lines[0] and ')' in lines[0]


# Roster command tests
@pytest.mark.integration
class TestRosterCommand:
    """Integration tests for the roster command."""

    TEAM_NAME = "Bois ton (dro)let"

    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
        (["--format", "simple"], "simple"),
    ])
    def test_roster_output_formats(self, cli_runner, format_arg, format_name):
        """Test roster command with different output formats."""
        result = cli_runner("roster", self.TEAM_NAME, *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        if format_name == "table":
            # Check for expected columns (note: Rich may wrap headers across lines)
            expected_columns = ["Pos", "Roster", "Status", "Inj", "Report", "Salary", "FP Total", "FP/G"]
            for col in expected_columns:
                assert col in result.stdout, f"Missing column: {col}"
        elif format_name == "json":
            data = json.loads(result.stdout)
            assert "team_id" in data
            assert "roster_stats" in data
            assert "players" in data
        elif format_name == "simple":
            lines = result.stdout.strip().split('\n')
            assert len(lines) > 0
            # Format: "Pos (Status): Name - Salary"
            assert '(' in lines[0] and ')' in lines[0] and ':' in lines[0]

    def test_roster_status_column(self, cli_runner):
        """Test that roster status (Active/Reserve/IR) is displayed."""
        result = cli_runner("roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Check that roster_status field exists
        if data["players"]:
            player = data["players"][0]
            assert "roster_status" in player
            assert "injury_report" in player

    def test_roster_status_values(self, cli_runner):
        """Test that roster contains expected status values."""
        result = cli_runner("roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Collect all roster statuses
        statuses = {p["roster_status"] for p in data["players"] if p["roster_status"]}

        # Should have at least one of: Active, Reserve, IR
        assert statuses, "No roster statuses found"
        assert statuses <= {"Active", "Reserve", "IR", "Unknown"}, f"Unexpected statuses: {statuses}"

    def test_roster_injury_report(self, cli_runner):
        """Test that injury report can be present."""
        result = cli_runner("roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Check that injury_report field exists (can be null)
        if data["players"]:
            player = data["players"][0]
            assert "injury_report" in player

    def test_roster_with_recent_stats(self, cli_runner):
        """Test roster command with --last-n-days option."""
        # This test is slow (makes multiple API calls)
        result = cli_runner("roster", self.TEAM_NAME, "--last-n-days", "7", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Should have stats_period in output
        assert "stats_period" in data
        assert data["stats_period"] == "last_7_days"

        # Players should have games_played field
        if data["players"]:
            player = data["players"][0]
            assert "games_played" in player


# Smoke tests for other potential commands
@pytest.mark.integration
class TestCliBasics:
    """Basic smoke tests for CLI."""

    def test_cli_help(self, cli_runner):
        """Test that help command works."""
        result = cli_runner("--help")
        assert result.returncode == 0
        assert "fantrax" in result.stdout.lower() or "usage" in result.stdout.lower()

    def test_invalid_command(self, cli_runner):
        """Test that invalid command returns non-zero exit code."""
        result = cli_runner("nonexistent-command")
        assert result.returncode != 0
