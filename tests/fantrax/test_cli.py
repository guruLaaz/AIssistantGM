"""Tests for CLI module."""

import pytest
from unittest.mock import Mock, patch, MagicMock
import typer
from typer.testing import CliRunner

from aissistant_gm.fantrax.cli import (
    OutputFormat,
    version_callback,
    main,
    app
)
from aissistant_gm.fantrax import __version__


runner = CliRunner()


class TestOutputFormat:
    """Test OutputFormat enum."""

    def test_output_format_table_value(self):
        """Test table format value."""
        assert OutputFormat.table.value == "table"
        assert OutputFormat.table == "table"

    def test_output_format_json_value(self):
        """Test json format value."""
        assert OutputFormat.json.value == "json"
        assert OutputFormat.json == "json"

    def test_output_format_simple_value(self):
        """Test simple format value."""
        assert OutputFormat.simple.value == "simple"
        assert OutputFormat.simple == "simple"

    def test_output_format_is_string_enum(self):
        """Test OutputFormat is a string enum."""
        assert isinstance(OutputFormat.table, str)
        assert isinstance(OutputFormat.json, str)
        assert isinstance(OutputFormat.simple, str)

    def test_output_format_all_values(self):
        """Test all OutputFormat values exist."""
        values = [f.value for f in OutputFormat]
        assert set(values) == {"table", "json", "simple"}


class TestVersionCallback:
    """Test version_callback function."""

    def test_version_callback_false_does_nothing(self):
        """Test that False value does nothing."""
        # Should return None and not raise
        result = version_callback(False)
        assert result is None

    def test_version_callback_none_does_nothing(self):
        """Test that None value does nothing."""
        result = version_callback(None)
        assert result is None

    def test_version_callback_true_raises_exit(self, capsys):
        """Test that True value displays version and raises Exit."""
        with pytest.raises(typer.Exit):
            version_callback(True)

        captured = capsys.readouterr()
        assert __version__ in captured.out
        assert "Fantrax CLI version" in captured.out


class TestMainCallback:
    """Test main callback function."""

    def test_main_stores_league_id_in_context(self):
        """Test that main stores league_id in context."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id="test123", version=None, no_cache=False, refresh=False)

        ctx.ensure_object.assert_called_once_with(dict)
        assert ctx.obj["league_id"] == "test123"

    def test_main_stores_no_cache_flag(self):
        """Test that main stores no_cache flag in context."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id=None, version=None, no_cache=True, refresh=False)

        assert ctx.obj["no_cache"] is True

    def test_main_stores_refresh_flag(self):
        """Test that main stores refresh flag in context."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id=None, version=None, no_cache=False, refresh=True)

        assert ctx.obj["refresh"] is True

    def test_main_ensures_object_dict(self):
        """Test that main calls ensure_object with dict."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id=None, version=None, no_cache=False, refresh=False)

        ctx.ensure_object.assert_called_once_with(dict)

    def test_main_stores_all_options(self):
        """Test that main stores all options in context."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id="league456", version=None, no_cache=True, refresh=True)

        assert ctx.obj["league_id"] == "league456"
        assert ctx.obj["no_cache"] is True
        assert ctx.obj["refresh"] is True

    def test_main_with_none_league_id(self):
        """Test that main handles None league_id."""
        ctx = Mock(spec=typer.Context)
        ctx.ensure_object = Mock(return_value={})
        ctx.obj = {}

        main(ctx, league_id=None, version=None, no_cache=False, refresh=False)

        assert ctx.obj["league_id"] is None


class TestCliApp:
    """Test CLI app integration."""

    def test_app_has_teams_command(self):
        """Test that app has teams command registered."""
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "teams" in command_names

    def test_app_has_roster_command(self):
        """Test that app has roster command registered."""
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "roster" in command_names

    def test_app_has_players_command(self):
        """Test that app has players command registered."""
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "players" in command_names

    def test_app_has_sync_command(self):
        """Test that app has sync command registered."""
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "sync" in command_names

    def test_app_has_news_command(self):
        """Test that app has news command registered."""
        command_names = [cmd.name for cmd in app.registered_commands]
        assert "news" in command_names

    def test_app_help(self):
        """Test that app --help works."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "CLI wrapper for Fantrax" in result.stdout

    def test_app_version_flag(self):
        """Test that --version flag works."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert __version__ in result.stdout

    def test_app_version_short_flag(self):
        """Test that -v flag works."""
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert __version__ in result.stdout
