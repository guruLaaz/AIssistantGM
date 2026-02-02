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
