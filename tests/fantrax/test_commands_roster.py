"""Unit tests for the roster command module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
import typer

import aissistant_gm.fantrax.commands.roster as roster_module
from aissistant_gm.fantrax.types import OutputFormat
from aissistant_gm.fantrax.cache import CacheResult


class TestRosterCommand:
    """Tests for the roster_command function."""

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
        config.sync_days_scores = 35
        return config

    def _create_mock_team(self, team_id="team-1", name="Test Team", short="TT"):
        """Create a mock team object."""
        team = Mock()
        team.id = team_id
        team.name = name
        team.short = short
        return team

    def _create_mock_roster(self):
        """Create a mock roster with player rows."""
        roster = Mock()

        # Create mock player
        player = Mock()
        player.id = "player-1"
        player.name = "Test Player"
        player.short_name = "T. Player"
        player.team = Mock(name="NHL Team", short="NHL")
        player.positions = [Mock(short="C"), Mock(short="LW")]
        player.day_to_day = False
        player.out = False
        player.injured_reserve = False
        player.suspended = False

        # Create mock row
        row = Mock()
        row.player = player
        row.position = Mock(id="C", short="C")
        row.status_id = "1"
        row.salary = 5.5
        row.fantasy_points = 100.5
        row.fantasy_points_per_game = 2.5

        roster.rows = [row]
        return roster

    def test_cache_hit_displays_roster_from_cache(self):
        """Test that cache hit displays roster without API call."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, '_display_cached_roster') as mock_display, \
             patch.object(roster_module, 'format_cache_age') as mock_format_age:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Setup cache hit
            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team', 'short_name': 'TT'}
            mock_cache.get_team_by_identifier.return_value = team_data

            roster_data = [
                {'player_id': 'p1', 'player_name': 'Player 1', 'position_short': 'C'}
            ]
            mock_cache.get_roster.return_value = CacheResult(
                data=roster_data,
                from_cache=True,
                cache_age_hours=1.0,
                stale=False
            )

            mock_format_age.return_value = "1 hour ago"

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            # Execute with team_identifier
            roster_module.roster_command(ctx, team_identifier="Test Team")

            # Verify cache was used
            mock_cache.get_team_by_identifier.assert_called_once_with("Test Team")
            mock_cache.get_roster.assert_called_once_with('team-1')
            mock_display.assert_called_once()

    def test_cache_hit_with_trends_both_cached(self):
        """Test that cache hit with trends flag uses cached trends."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, '_display_cached_roster') as mock_display, \
             patch.object(roster_module, 'format_cache_age') as mock_format_age:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team'}
            mock_cache.get_team_by_identifier.return_value = team_data

            roster_data = [{'player_id': 'p1', 'position_short': 'C'}]
            mock_cache.get_roster.return_value = CacheResult(
                data=roster_data, from_cache=True, cache_age_hours=1.0, stale=False
            )

            trends_data = {'p1': {'7d': {'fpg': 2.5}}}
            mock_cache.get_player_trends.return_value = CacheResult(
                data=trends_data, from_cache=True, cache_age_hours=0.5, stale=False
            )

            mock_format_age.return_value = "1 hour ago"
            mock_console = Mock()
            mock_console_class.return_value = mock_console

            # Execute with trends flag
            roster_module.roster_command(ctx, team_identifier="Test Team", trends=True)

            # Verify both roster and trends cache were checked
            mock_cache.get_roster.assert_called_once()
            mock_cache.get_player_trends.assert_called_once_with('team-1')
            mock_display.assert_called_once()

    def test_cache_miss_calls_api(self):
        """Test that cache miss triggers API call."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table, \
             patch.object(roster_module, '_cache_roster') as mock_cache_roster:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            # Setup cache miss
            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team'}
            mock_cache.get_team_by_identifier.return_value = team_data

            mock_cache.get_roster.return_value = CacheResult(
                data=None, from_cache=False, cache_age_hours=None, stale=True
            )

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            # Setup league mock
            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.team.return_value = mock_team
            mock_auth.return_value = mock_league

            # Execute
            roster_module.roster_command(ctx, team_identifier="Test Team")

            # Verify API was called
            mock_auth.assert_called_once()
            mock_league.team.assert_called_once_with("Test Team")
            mock_format_table.assert_called_once()

    def test_no_team_identifier_uses_my_team(self):
        """Test that no team identifier uses logged-in user's team."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table, \
             patch.object(roster_module, '_cache_roster') as mock_cache_roster:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            # Setup league with my_team
            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            # Execute without team_identifier
            roster_module.roster_command(ctx, team_identifier=None)

            # Verify my_team was used
            mock_team.roster.assert_called_once()

    def test_team_not_found_exits_with_error(self):
        """Test that team not found displays error and exits."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            # Setup league that raises error on team lookup
            mock_league = Mock()
            mock_league.team.side_effect = Exception("Team not found")
            mock_league.teams = [self._create_mock_team()]
            mock_auth.return_value = mock_league

            # Execute and expect exit
            with pytest.raises(typer.Exit) as exc_info:
                roster_module.roster_command(ctx, team_identifier="NonExistent")

            assert exc_info.value.exit_code == 1
            mock_console.print.assert_any_call(
                "[bold red]Error:[/bold red] Could not find team 'NonExistent'"
            )

    def test_my_team_not_found_exits_with_error(self):
        """Test that missing my_team displays error and exits."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            # Setup league without my_team
            mock_league = Mock()
            mock_league.my_team = None
            mock_league.teams = [self._create_mock_team()]
            mock_auth.return_value = mock_league

            # Execute without team_identifier
            with pytest.raises(typer.Exit) as exc_info:
                roster_module.roster_command(ctx, team_identifier=None)

            assert exc_info.value.exit_code == 1

    def test_no_cache_flag_bypasses_cache(self):
        """Test that --no-cache flag bypasses cache lookup."""
        ctx = self._create_mock_context(no_cache=True)

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team'}
            mock_cache.get_team_by_identifier.return_value = team_data

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.team.return_value = mock_team
            mock_auth.return_value = mock_league

            roster_module.roster_command(ctx, team_identifier="Test Team")

            # Verify cache.get_roster was NOT called
            mock_cache.get_roster.assert_not_called()
            # API was called instead
            mock_auth.assert_called_once()

    def test_refresh_flag_bypasses_cache(self):
        """Test that --refresh flag bypasses cache lookup."""
        ctx = self._create_mock_context(refresh=True)

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team'}
            mock_cache.get_team_by_identifier.return_value = team_data

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.team.return_value = mock_team
            mock_auth.return_value = mock_league

            roster_module.roster_command(ctx, team_identifier="Test Team")

            # Verify cache.get_roster was NOT called
            mock_cache.get_roster.assert_not_called()
            mock_auth.assert_called_once()

    def test_last_n_days_bypasses_cache(self):
        """Test that --last-n-days flag bypasses cache."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table, \
             patch.object(roster_module, 'calculate_recent_fpg') as mock_calc_fpg:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache

            team_data = {'id': 'team-1', 'name': 'Test Team'}
            mock_cache.get_team_by_identifier.return_value = team_data

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.team.return_value = mock_team
            mock_auth.return_value = mock_league

            mock_calc_fpg.return_value = {'player-1': {'total': 25.0, 'games': 10, 'fpg': 2.5}}

            roster_module.roster_command(ctx, team_identifier="Test Team", last_n_days=7)

            # Cache for roster is bypassed when last_n_days is set
            mock_cache.get_roster.assert_not_called()
            mock_calc_fpg.assert_called_once()

    def test_last_n_days_invalid_range_exits(self):
        """Test that invalid --last-n-days value exits with error."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            # Test invalid value (too large)
            with pytest.raises(typer.Exit) as exc_info:
                roster_module.roster_command(ctx, team_identifier=None, last_n_days=500)

            assert exc_info.value.exit_code == 1

    def test_json_format_output(self):
        """Test that JSON format option is passed correctly."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_json') as mock_format_json, \
             patch.object(roster_module, '_cache_roster') as mock_cache_roster:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            roster_module.roster_command(ctx, team_identifier=None, format=OutputFormat.json)

            mock_format_json.assert_called_once()

    def test_simple_format_output(self):
        """Test that simple format option is passed correctly."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_simple') as mock_format_simple, \
             patch.object(roster_module, '_cache_roster') as mock_cache_roster:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            roster_module.roster_command(ctx, team_identifier=None, format=OutputFormat.simple)

            mock_format_simple.assert_called_once()

    def test_empty_roster_displays_message(self):
        """Test that empty roster displays appropriate message."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            # Empty roster
            mock_roster = Mock()
            mock_roster.rows = []
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            roster_module.roster_command(ctx, team_identifier=None)

            mock_console.print.assert_any_call(
                f"[yellow]No roster found for team {mock_team.name}.[/yellow]"
            )

    def test_config_error_exits(self):
        """Test that configuration error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'Console') as mock_console_class:

            mock_load_config.side_effect = ValueError("Missing required config")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                roster_module.roster_command(ctx)

            assert exc_info.value.exit_code == 1

    def test_general_error_exits(self):
        """Test that general error exits with code 1."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'Console') as mock_console_class:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db_class.side_effect = Exception("Database error")

            mock_console = Mock()
            mock_console_class.return_value = mock_console

            with pytest.raises(typer.Exit) as exc_info:
                roster_module.roster_command(ctx)

            assert exc_info.value.exit_code == 1

    def test_trends_calculation_and_caching(self):
        """Test that trends are calculated and cached when requested."""
        ctx = self._create_mock_context()

        with patch.object(roster_module, 'load_config') as mock_load_config, \
             patch.object(roster_module, 'DatabaseManager') as mock_db_class, \
             patch.object(roster_module, 'CacheManager') as mock_cache_class, \
             patch.object(roster_module, 'Console') as mock_console_class, \
             patch.object(roster_module, 'get_authenticated_league') as mock_auth, \
             patch.object(roster_module, 'format_roster_table') as mock_format_table, \
             patch.object(roster_module, 'calculate_recent_trends') as mock_calc_trends, \
             patch.object(roster_module, '_cache_roster') as mock_cache_roster:

            mock_config = self._create_mock_config()
            mock_load_config.return_value = mock_config

            mock_db = Mock()
            mock_db_class.return_value = mock_db

            mock_cache = Mock()
            mock_cache_class.return_value = mock_cache
            mock_cache.get_team_by_identifier.return_value = None

            mock_console = Mock()
            mock_console.status.return_value.__enter__ = Mock()
            mock_console.status.return_value.__exit__ = Mock()
            mock_console_class.return_value = mock_console

            mock_league = Mock()
            mock_team = self._create_mock_team()
            mock_roster = self._create_mock_roster()
            mock_team.roster.return_value = mock_roster
            mock_league.my_team = mock_team
            mock_auth.return_value = mock_league

            mock_calc_trends.return_value = {
                'player-1': {
                    '7d': {'total_points': 20, 'games': 5, 'fpg': 4.0, 'start': '2024-01-01', 'end': '2024-01-07'}
                }
            }

            roster_module.roster_command(ctx, team_identifier=None, trends=True)

            mock_calc_trends.assert_called_once_with(mock_league, mock_team.id, days=35)
            # Verify trends are passed to display
            call_args = mock_format_table.call_args
            assert call_args.kwargs.get('recent_trends') is not None


class TestCacheRoster:
    """Tests for the _cache_roster helper function."""

    def test_cache_roster_saves_players_and_roster(self):
        """Test that _cache_roster saves players and roster data."""
        mock_db = Mock()
        league_id = "test-league"

        # Create mock team
        team = Mock()
        team.id = "team-1"

        # Create mock roster
        player = Mock()
        player.id = "player-1"
        player.name = "Test Player"
        player.short_name = "T. Player"
        player.team = Mock()
        player.team.name = "NHL Team"
        player.team.short = "NHL"
        player.positions = [Mock(short="C"), Mock(short="LW")]
        player.day_to_day = False
        player.out = False
        player.injured_reserve = False
        player.suspended = False

        row = Mock()
        row.player = player
        row.position = Mock(id="C", short="C")
        row.status_id = "1"
        row.salary = 5.5
        row.fantasy_points = 100.5
        row.fantasy_points_per_game = 2.5

        roster = Mock()
        roster.rows = [row]

        roster_module._cache_roster(mock_db, league_id, team, roster)

        # Verify save_players was called
        mock_db.save_players.assert_called_once()
        players_arg = mock_db.save_players.call_args[0][0]
        assert len(players_arg) == 1
        assert players_arg[0]['id'] == 'player-1'
        assert players_arg[0]['name'] == 'Test Player'

        # Verify save_roster was called
        mock_db.save_roster.assert_called_once()

        # Verify sync was logged
        mock_db.log_sync_start.assert_called_once_with('rosters', league_id)
        mock_db.log_sync_complete.assert_called_once()

    def test_cache_roster_handles_empty_player_slot(self):
        """Test that _cache_roster handles roster rows without players."""
        mock_db = Mock()
        league_id = "test-league"

        team = Mock()
        team.id = "team-1"

        # Row without player (empty slot)
        row = Mock()
        row.player = None
        row.position = Mock(id="BN", short="BN")
        row.status_id = None
        row.salary = None
        row.fantasy_points = None
        row.fantasy_points_per_game = None

        roster = Mock()
        roster.rows = [row]

        roster_module._cache_roster(mock_db, league_id, team, roster)

        # save_players should NOT be called with empty list
        mock_db.save_players.assert_not_called()
        # save_roster should still be called
        mock_db.save_roster.assert_called_once()


class TestDisplayCachedRoster:
    """Tests for the _display_cached_roster helper function."""

    def test_display_cached_roster_table_format(self):
        """Test _display_cached_roster with table format."""
        roster_data = [
            {
                'player_id': 'p1',
                'player_name': 'Player One',
                'position_short': 'C',
                'team_name': 'NHL Team',
                'team_short_name': 'NHL',
                'position_short_names': 'C,LW',
                'total_fantasy_points': 100.0,
                'fantasy_points_per_game': 2.5,
                'status_id': '1',
                'salary': 5.5,
                'day_to_day': 0,
                'out': 0,
                'injured_reserve': 0,
                'suspended': 0
            }
        ]
        team_data = {'id': 'team-1', 'name': 'Test Team'}

        mock_console = Mock()

        with patch.object(roster_module, 'format_roster_table') as mock_format:
            roster_module._display_cached_roster(
                roster_data, team_data, None, OutputFormat.table, mock_console
            )

            mock_format.assert_called_once()
            call_kwargs = mock_format.call_args.kwargs
            assert call_kwargs['team_name'] == 'Test Team'

    def test_display_cached_roster_json_format(self):
        """Test _display_cached_roster with JSON format."""
        roster_data = [
            {
                'player_id': 'p1',
                'player_name': 'Player One',
                'position_short': 'C'
            }
        ]
        team_data = {'id': 'team-1', 'name': 'Test Team'}

        mock_console = Mock()

        with patch.object(roster_module, 'format_roster_json') as mock_format:
            roster_module._display_cached_roster(
                roster_data, team_data, None, OutputFormat.json, mock_console
            )

            mock_format.assert_called_once()

    def test_display_cached_roster_simple_format(self):
        """Test _display_cached_roster with simple format."""
        roster_data = [
            {
                'player_id': 'p1',
                'player_name': 'Player One',
                'position_short': 'C'
            }
        ]
        team_data = {'id': 'team-1', 'name': 'Test Team'}

        mock_console = Mock()

        with patch.object(roster_module, 'format_roster_simple') as mock_format:
            roster_module._display_cached_roster(
                roster_data, team_data, None, OutputFormat.simple, mock_console
            )

            mock_format.assert_called_once()

    def test_display_cached_roster_with_trends(self):
        """Test _display_cached_roster with trends data."""
        roster_data = [
            {
                'player_id': 'p1',
                'player_name': 'Player One',
                'position_short': 'C'
            }
        ]
        team_data = {'id': 'team-1', 'name': 'Test Team'}
        trends_data = {
            'p1': {
                '7d': {'total': 20, 'games': 5, 'fpg': 4.0, 'start': '2024-01-01', 'end': '2024-01-07'}
            }
        }

        mock_console = Mock()

        with patch.object(roster_module, 'format_roster_table') as mock_format:
            roster_module._display_cached_roster(
                roster_data, team_data, trends_data, OutputFormat.table, mock_console
            )

            mock_format.assert_called_once()
            call_kwargs = mock_format.call_args.kwargs
            assert call_kwargs['recent_trends'] is not None

    def test_display_cached_roster_empty_data(self):
        """Test _display_cached_roster with empty roster data."""
        roster_data = []
        team_data = {'id': 'team-1', 'name': 'Test Team'}

        mock_console = Mock()

        roster_module._display_cached_roster(
            roster_data, team_data, None, OutputFormat.table, mock_console
        )

        mock_console.print.assert_called_once_with(
            "[yellow]No roster data in cache.[/yellow]"
        )
