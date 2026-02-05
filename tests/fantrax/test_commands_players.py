"""Unit tests for the players command module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import typer
import json

import aissistant_gm.fantrax.commands.players as players_module
from aissistant_gm.fantrax.types import OutputFormat
from aissistant_gm.fantrax.cache import CacheResult


class TestPlayersCommand:
    """Tests for the players_command function."""

    def _create_mock_context(self, league_id=None, no_cache=False, refresh=False):
        """Create a mock typer context."""
        ctx = Mock(spec=typer.Context)
        ctx.obj = {
            "league_id": league_id,
            "no_cache": no_cache,
            "refresh": refresh
        }
        return ctx

    def _create_mock_config(self):
        """Create a mock config object."""
        config = Mock()
        config.league_id = "test-league-123"
        config.database_path = ":memory:"
        config.cache_enabled = True
        config.username = "test@test.com"
        config.password = "testpass"
        config.cookie_path = "/tmp/cookies.json"
        config.min_request_interval = 1
        config.selenium_timeout = 10
        config.login_wait_time = 5
        config.browser_window_size = "1920,1080"
        config.user_agent = "TestAgent"
        return config

    def _create_mock_player_response(self, count=3):
        """Create a mock API response for player stats."""
        players = []
        for i in range(count):
            players.append({
                'scorer': {
                    'scorerId': f'player-{i}',
                    'name': f'Player {i}',
                    'shortName': f'P. {i}',
                    'teamName': 'NHL Team',
                    'teamShortName': 'NHL',
                    'posShortNames': 'C,LW'
                },
                'cells': [
                    {'content': str(i + 1)},  # rank
                    {'content': 'A'},  # status
                    {'content': '25'},  # age
                    {'content': ''},  # something
                    {'content': '5000000'},  # salary
                    {'content': f'{100 - i * 10:.1f}'},  # fpts
                    {'content': f'{2.5 - i * 0.1:.2f}'}  # fpg
                ]
            })

        return {
            'statsTable': players,
            'paginatedResultSet': {
                'totalNumResults': count * 10
            }
        }

    def test_cache_hit_displays_players_from_cache(self):
        """Test that cache hit displays players without API call."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, '_display_cached_players') as mock_display, \
             patch.object(players_module, 'format_cache_age') as mock_format_age:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            fa_data = [
                {'player_id': 'p1', 'name': 'Player 1', 'position_short_names': 'C'}
            ]
            mock_cache.get_free_agents.return_value = CacheResult(
                data=fa_data,
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            mock_format_age.return_value = "1 hour ago"

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            # Execute
            players_module.players_command(ctx)

            # Verify cache was used
            mock_cache.get_free_agents.assert_called_once()
            mock_display.assert_called_once()

    def test_cache_hit_with_trends_both_cached(self):
        """Test that cache hit with trends uses cached trends data."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, '_display_cached_players') as mock_display, \
             patch.object(players_module, 'format_cache_age') as mock_format_age:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            fa_data = [{'player_id': 'p1', 'name': 'Player 1'}]
            mock_cache.get_free_agents.return_value = CacheResult(
                data=fa_data, from_cache=True, cache_age_hours=1.0, stale=False
            )

            trends_data = {'p1': {'week1': {'fpg': 2.5}}}
            mock_cache.get_fa_trends.return_value = CacheResult(
                data=trends_data, from_cache=True, cache_age_hours=0.5, stale=False
            )

            mock_format_age.return_value = "1 hour ago"
            mock_console = Mock()
            mock_console_class.return_value = mock_console

            # Execute with trends flag
            players_module.players_command(ctx, trends=True)

            # Verify both FA and trends cache were checked
            mock_cache.get_free_agents.assert_called_once()
            mock_cache.get_fa_trends.assert_called_once_with(['p1'])
            mock_display.assert_called_once()

    def test_cache_miss_calls_api(self):
        """Test that cache miss triggers API call."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request, \
             patch.object(players_module, '_cache_free_agents') as mock_cache_fa:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_free_agents.return_value = CacheResult(
                data=None, from_cache=False, cache_age_hours=None, stale=True
            )

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            # Execute
            players_module.players_command(ctx)

            # Verify API was called
            mock_auth.assert_called_once()
            mock_request.assert_called_once()

    def test_no_cache_flag_bypasses_cache(self):
        """Test that --no-cache flag bypasses cache lookup."""
        ctx = self._create_mock_context(no_cache=True)

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx)

            # Verify cache.get_free_agents was NOT called
            mock_cache.get_free_agents.assert_not_called()
            mock_auth.assert_called_once()

    def test_refresh_flag_bypasses_cache(self):
        """Test that --refresh flag bypasses cache lookup."""
        ctx = self._create_mock_context(refresh=True)

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx)

            mock_cache.get_free_agents.assert_not_called()
            mock_auth.assert_called_once()

    def test_cache_disabled_bypasses_cache(self):
        """Test that disabled cache skips cache lookup."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx)

            mock_cache.get_free_agents.assert_not_called()
            mock_auth.assert_called_once()

    def test_position_filter_forward(self):
        """Test that position filter 'f' maps correctly."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, position='f')

            # Verify _request was called with position filter
            call_args = mock_request.call_args
            method_arg = call_args[0][1]  # Second positional arg is the Method
            assert method_arg.kwargs.get('scoringCategoryType') == '2010'

    def test_position_filter_goalie(self):
        """Test that position filter 'g' maps correctly."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, position='g')

            call_args = mock_request.call_args
            method_arg = call_args[0][1]
            assert method_arg.kwargs.get('scoringCategoryType') == '2020'

    def test_sort_by_fpts(self):
        """Test that sort option 'fpts' maps correctly."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, sort='fpts')

            call_args = mock_request.call_args
            method_arg = call_args[0][1]
            assert method_arg.kwargs.get('sortType') == 'SCORE'

    def test_sort_by_fpg(self):
        """Test that sort option 'fpg' maps correctly."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, sort='fpg')

            call_args = mock_request.call_args
            method_arg = call_args[0][1]
            assert method_arg.kwargs.get('sortType') == 'FPTS_PER_GAME'

    def test_limit_parameter(self):
        """Test that limit parameter is passed to API."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, limit=50)

            call_args = mock_request.call_args
            method_arg = call_args[0][1]
            assert method_arg.kwargs.get('maxResultsPerPage') == '50'

    def test_json_format_output(self, capsys):
        """Test that JSON format outputs valid JSON."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response(count=2)

            players_module.players_command(ctx, format=OutputFormat.json)

            captured = capsys.readouterr()
            output = json.loads(captured.out)

            assert 'total_available' in output
            assert 'showing' in output
            assert 'players' in output
            assert len(output['players']) == 2

    def test_table_format_output(self):
        """Test that table format displays correctly."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()

            players_module.players_command(ctx, format=OutputFormat.table)

            # Verify table was printed
            assert mock_console.print.called

    def test_no_players_found_displays_message(self):
        """Test that no players found displays appropriate message."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            # Empty response
            mock_request.return_value = {}

            players_module.players_command(ctx)

            mock_console.print.assert_any_call("[yellow]No available players found.[/yellow]")

    def test_trends_fetched_when_requested(self):
        """Test that trends are fetched when --trends flag is used."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'CacheManager') as mock_cache_class, \
             patch.object(players_module, 'Console') as mock_console_class, \
             patch.object(players_module, 'get_authenticated_league') as mock_auth, \
             patch.object(players_module, '_request') as mock_request, \
             patch.object(players_module, 'fetch_fa_player_trends') as mock_fetch_trends:

            mock_config = self._create_mock_config()
            mock_config.cache_enabled = False
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_league.league_id = "test-league-123"
            mock_league.session = Mock()
            mock_auth.return_value = mock_league

            mock_request.return_value = self._create_mock_player_response()
            mock_fetch_trends.return_value = {
                'player-0': {'week1': {'fpg': 2.5}, 'week2': {'fpg': 2.3}}
            }

            players_module.players_command(ctx, trends=True)

            mock_fetch_trends.assert_called_once()

    def test_config_error_exits(self):
        """Test that configuration error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'Console') as mock_console_class:

            mock_load_config.side_effect = ValueError("Missing required config")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                players_module.players_command(ctx)

            assert exc_info.value.exit_code == 1

    def test_general_error_exits(self):
        """Test that general error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(players_module, 'load_config') as mock_load_config, \
             patch.object(players_module, 'DatabaseManager') as mock_db_class, \
             patch.object(players_module, 'Console') as mock_console_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db_class.side_effect = Exception("Database error")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                players_module.players_command(ctx)

            assert exc_info.value.exit_code == 1


class TestCacheFreeAgents:
    """Tests for the _cache_free_agents helper function."""

    def test_cache_free_agents_saves_data(self):
        """Test that _cache_free_agents saves players and FA listings."""
        mock_db = Mock()
        league_id = "test-league"

        players = [
            {
                'scorer': {
                    'scorerId': 'player-1',
                    'name': 'Test Player',
                    'shortName': 'T. Player',
                    'teamName': 'NHL Team',
                    'teamShortName': 'NHL',
                    'posShortNames': 'C,LW',
                    'isDayToDay': False,
                    'isOut': False,
                    'isInjuredReserve': False,
                    'isSuspended': False
                },
                'cells': [
                    {'content': '1'},
                    {'content': 'A'},
                    {'content': '25'},
                    {'content': ''},
                    {'content': '5000000'},
                    {'content': '100.5'},
                    {'content': '2.50'}
                ]
            }
        ]

        players_module._cache_free_agents(mock_db, league_id, players, 'SCORE', None)

        # Verify save_players was called
        mock_db.save_players.assert_called_once()
        players_arg = mock_db.save_players.call_args[0][0]
        assert len(players_arg) == 1
        assert players_arg[0]['id'] == 'player-1'

        # Verify save_free_agents was called
        mock_db.save_free_agents.assert_called_once()

        # Verify sync was logged
        mock_db.log_sync_start.assert_called_once_with('free_agents', league_id)
        mock_db.log_sync_complete.assert_called_once()

    def test_cache_free_agents_handles_invalid_numbers(self):
        """Test that _cache_free_agents handles invalid numeric values."""
        mock_db = Mock()
        league_id = "test-league"

        players = [
            {
                'scorer': {
                    'scorerId': 'player-1',
                    'name': 'Test Player'
                },
                'cells': [
                    {'content': '1'},
                    {'content': ''},
                    {'content': ''},
                    {'content': ''},
                    {'content': 'N/A'},  # Invalid salary
                    {'content': 'invalid'},  # Invalid fpts
                    {'content': ''}  # Empty fpg
                ]
            }
        ]

        # Should not raise
        players_module._cache_free_agents(mock_db, league_id, players, 'SCORE', None)

        # Verify it was saved with 0.0 defaults
        mock_db.save_free_agents.assert_called_once()
        fa_arg = mock_db.save_free_agents.call_args[0][0]
        assert fa_arg[0]['total_fpts'] == 0.0
        assert fa_arg[0]['fpg'] == 0.0

    def test_cache_free_agents_empty_list(self):
        """Test that _cache_free_agents handles empty player list."""
        mock_db = Mock()
        league_id = "test-league"

        players_module._cache_free_agents(mock_db, league_id, [], 'SCORE', None)

        # save_players should NOT be called with empty list
        mock_db.save_players.assert_not_called()
        mock_db.save_free_agents.assert_not_called()
        # Sync should still be logged
        mock_db.log_sync_start.assert_called_once()


class TestDisplayCachedPlayers:
    """Tests for the _display_cached_players helper function."""

    def test_display_cached_players_table_format(self):
        """Test _display_cached_players with table format."""
        fa_data = [
            {
                'player_id': 'p1',
                'name': 'Player One',
                'team_short_name': 'NHL',
                'position_short_names': 'C,LW',
                'rank': 1,
                'salary': '$5,000,000',
                'total_fpts': 100.5,
                'fpg': 2.5
            }
        ]

        mock_console = Mock()

        players_module._display_cached_players(
            fa_data, None, OutputFormat.table, mock_console, limit=25
        )

        # Verify table was printed
        assert mock_console.print.called

    def test_display_cached_players_json_format(self, capsys):
        """Test _display_cached_players with JSON format."""
        fa_data = [
            {
                'player_id': 'p1',
                'name': 'Player One',
                'team_short_name': 'NHL',
                'position_short_names': 'C',
                'rank': 1,
                'salary': '5000000',
                'total_fpts': 100.5,
                'fpg': 2.5
            }
        ]

        mock_console = Mock()

        players_module._display_cached_players(
            fa_data, None, OutputFormat.json, mock_console, limit=25
        )

        captured = capsys.readouterr()
        output = json.loads(captured.out)

        assert output['total_available'] == 1
        assert len(output['players']) == 1
        assert output['players'][0]['name'] == 'Player One'

    def test_display_cached_players_with_trends(self):
        """Test _display_cached_players with trends data."""
        fa_data = [
            {
                'player_id': 'p1',
                'name': 'Player One',
                'team_short_name': 'NHL',
                'position_short_names': 'C',
                'rank': 1
            }
        ]
        trends_data = {
            'p1': {
                'week1': {'fpg': 2.5},
                'week2': {'fpg': 2.3},
                'week3': {'fpg': 2.1},
                '14': {'fpg': 2.4},
                '30': {'fpg': 2.2}
            }
        }

        mock_console = Mock()

        players_module._display_cached_players(
            fa_data, trends_data, OutputFormat.table, mock_console, limit=25
        )

        # Verify table with trends was printed
        assert mock_console.print.called

    def test_display_cached_players_empty_data(self):
        """Test _display_cached_players with empty FA data."""
        mock_console = Mock()

        players_module._display_cached_players(
            [], None, OutputFormat.table, mock_console, limit=25
        )

        mock_console.print.assert_called_once_with(
            "[yellow]No free agent data in cache.[/yellow]"
        )

    def test_display_cached_players_limit_applied(self, capsys):
        """Test that limit is applied to displayed players."""
        fa_data = [
            {'player_id': f'p{i}', 'name': f'Player {i}'} for i in range(10)
        ]

        mock_console = Mock()

        players_module._display_cached_players(
            fa_data, None, OutputFormat.json, mock_console, limit=5
        )

        captured = capsys.readouterr()
        output = json.loads(captured.out)

        assert output['showing'] == 5
        assert output['total_available'] == 10
        assert len(output['players']) == 5

    def test_display_cached_players_json_with_trends(self, capsys):
        """Test _display_cached_players JSON output includes trends."""
        fa_data = [
            {
                'player_id': 'p1',
                'name': 'Player One',
                'team_short_name': 'NHL',
                'position_short_names': 'C'
            }
        ]
        trends_data = {
            'p1': {
                'week1': {'fpg': 2.5, 'games_played': 3},
                '14': {'fpg': 2.4}
            }
        }

        mock_console = Mock()

        players_module._display_cached_players(
            fa_data, trends_data, OutputFormat.json, mock_console, limit=25
        )

        captured = capsys.readouterr()
        output = json.loads(captured.out)

        assert 'stats_period' in output
        assert output['stats_period'] == 'recent_trends'
        assert 'trends' in output['players'][0]
