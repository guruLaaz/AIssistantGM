"""Tests for teams command."""

import pytest
from unittest.mock import Mock, patch, MagicMock

import aissistant_gm.fantrax.commands.teams as teams_module
from aissistant_gm.fantrax.types import OutputFormat
from aissistant_gm.fantrax.cache import CacheResult


class TestTeamsCommand:
    """Test teams_command function."""

    def test_cache_hit_displays_teams_from_cache(self):
        """Test that cached teams are displayed when cache is fresh."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class:

            # Setup mock config
            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_load_config.return_value = mock_config

            # Setup mock context
            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            # Setup mock cache
            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_league_name.return_value = "Test League"
            mock_cache.get_teams_with_standings.return_value = CacheResult(
                data=[
                    {'id': 'team1', 'name': 'Team One', 'short_name': 'T1', 'rank': 1},
                    {'id': 'team2', 'name': 'Team Two', 'short_name': 'T2', 'rank': 2},
                ],
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            # Execute
            with patch.object(teams_module, 'format_teams_table') as mock_format:
                teams_module.teams_command(ctx, format=OutputFormat.table)

                # Assert
                mock_cache.get_teams_with_standings.assert_called_once()
                mock_format.assert_called_once()

    def test_cache_hit_json_format(self):
        """Test that cached teams are displayed in JSON format."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_league_name.return_value = "Test League"
            mock_cache.get_teams_with_standings.return_value = CacheResult(
                data=[{'id': 'team1', 'name': 'Team One', 'short_name': 'T1', 'rank': 1}],
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            with patch.object(teams_module, 'format_teams_json') as mock_format:
                teams_module.teams_command(ctx, format=OutputFormat.json)
                mock_format.assert_called_once()

    def test_cache_hit_simple_format(self):
        """Test that cached teams are displayed in simple format."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_league_name.return_value = "Test League"
            mock_cache.get_teams_with_standings.return_value = CacheResult(
                data=[{'id': 'team1', 'name': 'Team One', 'short_name': 'T1', 'rank': 1}],
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            with patch.object(teams_module, 'format_teams_simple') as mock_format:
                teams_module.teams_command(ctx, format=OutputFormat.simple)
                mock_format.assert_called_once()

    def test_no_cache_flag_skips_cache(self):
        """Test that --no-cache flag skips cache lookup."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": True, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = [Mock(id='t1', name='Team One', short='T1')]
            mock_auth.return_value = mock_league
            mock_standings.return_value = None

            with patch.object(teams_module, 'format_teams_table'):
                teams_module.teams_command(ctx, format=OutputFormat.table)

            # Cache should not have been checked
            mock_cache.get_teams_with_standings.assert_not_called()

    def test_refresh_flag_skips_cache(self):
        """Test that --refresh flag skips cache lookup."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": True}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = [Mock(id='t1', name='Team One', short='T1')]
            mock_auth.return_value = mock_league
            mock_standings.return_value = None

            with patch.object(teams_module, 'format_teams_table'):
                teams_module.teams_command(ctx, format=OutputFormat.table)

            mock_cache.get_teams_with_standings.assert_not_called()

    def test_stale_cache_fetches_from_api(self):
        """Test that stale cache triggers API fetch."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            # Return stale cache
            mock_cache.get_teams_with_standings.return_value = CacheResult(
                data=[{'id': 'team1', 'name': 'Team One'}],
                from_cache=True,
                cache_age_hours=100.0,
                stale=True
            )

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = [Mock(id='t1', name='Team One', short='T1')]
            mock_auth.return_value = mock_league
            mock_standings.return_value = None

            with patch.object(teams_module, 'format_teams_table'):
                teams_module.teams_command(ctx, format=OutputFormat.table)

            # Should have called API
            mock_auth.assert_called_once()

    def test_cache_disabled_fetches_from_api(self):
        """Test that disabled cache fetches from API."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = False
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = [Mock(id='t1', name='Team One', short='T1')]
            mock_auth.return_value = mock_league
            mock_standings.return_value = None

            with patch.object(teams_module, 'format_teams_table'):
                teams_module.teams_command(ctx, format=OutputFormat.table)

            mock_auth.assert_called_once()
            mock_cache.get_teams_with_standings.assert_not_called()

    def test_no_teams_found_shows_message(self):
        """Test that message is shown when no teams are found."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth:

            mock_config = Mock()
            mock_config.cache_enabled = False
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = []  # No teams
            mock_auth.return_value = mock_league

            teams_module.teams_command(ctx, format=OutputFormat.table)

            # Should show no teams message
            mock_console.print.assert_any_call("[yellow]No teams found in this league.[/yellow]")

    def test_configuration_error_raises_exit(self):
        """Test that configuration error raises typer.Exit."""
        import typer
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class:

            mock_load_config.side_effect = ValueError("Missing username")

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                teams_module.teams_command(ctx, format=OutputFormat.table)

            assert exc_info.value.exit_code == 1
            mock_console.print.assert_any_call("[bold red]Configuration Error:[/bold red] Missing username")

    def test_general_error_raises_exit(self):
        """Test that general error raises typer.Exit."""
        import typer
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth:

            mock_config = Mock()
            mock_config.cache_enabled = False
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_auth.side_effect = Exception("Network error")

            with pytest.raises(typer.Exit) as exc_info:
                teams_module.teams_command(ctx, format=OutputFormat.table)

            assert exc_info.value.exit_code == 1

    def test_api_fetch_with_standings(self):
        """Test that standings are fetched and displayed from API."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": True, "refresh": False}

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.start_date = None
            mock_league.end_date = None
            mock_team = Mock()
            mock_team.id = 't1'
            mock_team.name = 'Team One'
            mock_team.short = 'T1'
            mock_league.teams = [mock_team]
            mock_auth.return_value = mock_league

            # Mock standings response
            mock_standings.return_value = {
                'tableList': [{
                    'header': {
                        'cells': [
                            {'key': 'win'},
                            {'key': 'loss'},
                            {'key': 'fantasyPoints'},
                            {'key': 'sc'},
                            {'key': 'FPtsPerGame'},
                            {'key': 'streak'}
                        ]
                    },
                    'rows': [{
                        'fixedCells': [
                            {'content': '1'},
                            {'teamId': 't1'}
                        ],
                        'cells': [
                            {'content': '10'},
                            {'content': '5'},
                            {'content': '150.5'},
                            {'content': '15'},
                            {'content': '10.03'},
                            {'content': 'W3'}
                        ]
                    }]
                }]
            }

            with patch.object(teams_module, 'format_teams_table') as mock_format:
                teams_module.teams_command(ctx, format=OutputFormat.table)

                # Verify standings were parsed and passed
                mock_format.assert_called_once()
                call_kwargs = mock_format.call_args[1]
                assert call_kwargs['standings'] is not None
                assert len(call_kwargs['standings']) == 1
                assert call_kwargs['standings'][0]['team_id'] == 't1'
                assert call_kwargs['standings'][0]['wins'] == 10

    def test_standings_error_continues_without_standings(self):
        """Test that standings fetch error doesn't fail the command."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class, \
             patch.object(teams_module, 'get_authenticated_league') as mock_auth, \
             patch('aissistant_gm.fantrax.fantraxapi.api.get_standings') as mock_standings:

            mock_config = Mock()
            mock_config.cache_enabled = False
            mock_config.league_id = "test_league"
            mock_config.database_path = "/tmp/test.db"
            mock_config.username = "test@example.com"
            mock_config.password = "testpass"
            mock_config.cookie_path = "/tmp/cookies"
            mock_config.min_request_interval = 0
            mock_config.selenium_timeout = 10
            mock_config.login_wait_time = 5
            mock_config.browser_window_size = "1920,1080"
            mock_config.user_agent = "Test Agent"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": None, "no_cache": False, "refresh": False}

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock(return_value=None)
            mock_console.status.return_value.__exit__ = Mock(return_value=None)
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.name = "Test League"
            mock_league.year = "2024"
            mock_league.teams = [Mock(id='t1', name='Team One', short='T1')]
            mock_auth.return_value = mock_league
            mock_standings.side_effect = Exception("Standings error")

            with patch.object(teams_module, 'format_teams_table') as mock_format:
                teams_module.teams_command(ctx, format=OutputFormat.table)

                # Should still display teams without standings
                mock_format.assert_called_once()
                call_kwargs = mock_format.call_args[1]
                assert call_kwargs['standings'] is None

    def test_league_id_override_from_context(self):
        """Test that league_id can be overridden from context."""
        with patch.object(teams_module, 'load_config') as mock_load_config, \
             patch.object(teams_module, 'DatabaseManager') as mock_db_class, \
             patch.object(teams_module, 'CacheManager') as mock_cache_class, \
             patch.object(teams_module, 'Console') as mock_console_class:

            mock_config = Mock()
            mock_config.cache_enabled = True
            mock_config.league_id = "override_league"
            mock_config.database_path = "/tmp/test.db"
            mock_load_config.return_value = mock_config

            ctx = Mock()
            ctx.obj = {"league_id": "override_league", "no_cache": False, "refresh": False}

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_league_name.return_value = "Override League"
            mock_cache.get_teams_with_standings.return_value = CacheResult(
                data=[{'id': 'team1', 'name': 'Team One', 'short_name': 'T1', 'rank': 1}],
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            with patch.object(teams_module, 'format_teams_table'):
                teams_module.teams_command(ctx, format=OutputFormat.table)

            # Verify load_config was called with the override
            mock_load_config.assert_called_once_with(league_id="override_league")


class TestDisplayTeams:
    """Test _display_teams function."""

    def test_display_empty_teams_shows_message(self):
        """Test that empty teams list shows warning message."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            console = Mock()
            teams_module._display_teams([], "Test League", "league1", OutputFormat.table, console)

            console.print.assert_called_once_with("[yellow]No teams found in cache.[/yellow]")
            mock_format.assert_not_called()

    def test_display_teams_table_format(self):
        """Test that teams are displayed in table format."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            teams_data = [
                {'id': 'team1', 'name': 'Team One', 'short_name': 'T1'},
                {'id': 'team2', 'name': 'Team Two', 'short_name': 'T2'},
            ]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.table, console)

            mock_format.assert_called_once()
            call_args = mock_format.call_args
            # First positional arg is the teams list
            assert len(call_args[0][0]) == 2

    def test_display_teams_json_format(self):
        """Test that teams are displayed in JSON format."""
        with patch.object(teams_module, 'format_teams_json') as mock_format:
            teams_data = [{'id': 'team1', 'name': 'Team One', 'short_name': 'T1'}]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.json, console)

            mock_format.assert_called_once()

    def test_display_teams_simple_format(self):
        """Test that teams are displayed in simple format."""
        with patch.object(teams_module, 'format_teams_simple') as mock_format:
            teams_data = [{'id': 'team1', 'name': 'Team One', 'short_name': 'T1'}]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.simple, console)

            mock_format.assert_called_once()

    def test_display_teams_extracts_standings_from_data(self):
        """Test that standings are extracted from teams_data when present."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            teams_data = [
                {
                    'id': 'team1',
                    'name': 'Team One',
                    'short_name': 'T1',
                    'rank': 1,
                    'wins': 10,
                    'losses': 5,
                    'points_for': 150.5,
                    'fpg': 10.0
                }
            ]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.table, console)

            mock_format.assert_called_once()
            call_kwargs = mock_format.call_args[1]
            assert call_kwargs['standings'] is not None
            assert len(call_kwargs['standings']) == 1
            assert call_kwargs['standings'][0]['rank'] == 1

    def test_display_teams_uses_provided_standings_data(self):
        """Test that provided standings_data is used over embedded data."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            teams_data = [{'id': 'team1', 'name': 'Team One', 'short_name': 'T1'}]
            standings_data = [{'team_id': 'team1', 'rank': 2, 'wins': 15}]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.table, console, standings_data)

            mock_format.assert_called_once()
            call_kwargs = mock_format.call_args[1]
            assert call_kwargs['standings'] == standings_data

    def test_display_teams_handles_short_vs_short_name(self):
        """Test that both 'short' and 'short_name' keys work."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            teams_data = [
                {'id': 'team1', 'name': 'Team One', 'short': 'T1'},  # Uses 'short'
            ]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.table, console)

            mock_format.assert_called_once()
            # Check the MockTeam object was created correctly
            teams = mock_format.call_args[0][0]
            assert teams[0].short == 'T1'

    def test_display_teams_no_standings_in_data(self):
        """Test display when teams_data has no standings info."""
        with patch.object(teams_module, 'format_teams_table') as mock_format:
            teams_data = [
                {'id': 'team1', 'name': 'Team One', 'short_name': 'T1'},  # No rank
            ]
            console = Mock()

            teams_module._display_teams(teams_data, "Test League", "league1", OutputFormat.table, console)

            mock_format.assert_called_once()
            call_kwargs = mock_format.call_args[1]
            # Should have no standings since rank is None
            assert call_kwargs['standings'] is None
