"""Shared fixtures for integration tests."""

import subprocess

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


def populate_database():
    """Run sync commands to populate the database.

    This is called by module-level fixtures to ensure database has data
    before data validation tests run.
    """
    def run_sync(*args):
        result = subprocess.run(
            ["fantrax", "sync"] + list(args),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return result.returncode == 0

    # Run syncs in order of dependency
    # Rosters first (needed for many other operations)
    run_sync("--rosters")
    # Then standings, free agents
    run_sync("--standings")
    run_sync("--free-agents")
    # Scores and trends (trends depends on scores)
    run_sync("--scores", "14")
    run_sync("--trends")
    # News sync (for player news tests)
    run_sync("--news")
