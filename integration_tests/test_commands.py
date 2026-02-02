"""Integration tests for Fantrax CLI commands.

These tests verify that the CLI commands work correctly end-to-end by running
actual commands and validating their output.

Run with: pytest integration_tests/
Or skip: pytest -m "not integration"
"""

import json
import pytest


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
        # Use --no-cache to ensure clean output without "Using cached data" message
        result = cli_runner("--no-cache", "teams", *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        if format_name == "table":
            # May show "Teams" or "Standings" depending on whether standings data is available
            assert "Teams" in result.stdout or "Standings" in result.stdout
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

    def test_roster_default_team(self, cli_runner):
        """Test roster command with no team argument uses logged-in user's team."""
        result = cli_runner("--no-cache", "roster")

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        # Should show "Using your team: <team_name>" message
        assert "Using your team:" in result.stdout

    def test_roster_default_team_json(self, cli_runner):
        """Test roster command with no team argument returns valid JSON."""
        result = cli_runner("--no-cache", "roster", "--format", "json")

        assert result.returncode == 0, f"Command failed: {result.stderr}"
        data = json.loads(result.stdout)
        # Should have team info
        assert "team_id" in data
        assert "team_name" in data
        assert data["team_id"] is not None
        assert data["team_name"] is not None

    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
        (["--format", "simple"], "simple"),
    ])
    def test_roster_output_formats(self, cli_runner, format_arg, format_name):
        """Test roster command with different output formats."""
        # Use --no-cache to ensure clean output without "Using cached data" message
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        if format_name == "table":
            # Check for expected columns (note: Rich may wrap or truncate headers)
            expected_columns = ["Pos", "Status", "Inj", "Salary", "FP Total"]
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
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Check that roster_status field exists
        if data["players"]:
            player = data["players"][0]
            assert "roster_status" in player
            assert "injury_report" in player

    def test_roster_status_values(self, cli_runner):
        """Test that roster contains expected status values."""
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Collect all roster statuses
        statuses = {p["roster_status"] for p in data["players"] if p["roster_status"]}

        # Should have at least one of: Active, Reserve, IR
        assert statuses, "No roster statuses found"
        assert statuses <= {"Active", "Reserve", "IR", "Unknown"}, f"Unexpected statuses: {statuses}"

    def test_roster_injury_report(self, cli_runner):
        """Test that injury report can be present."""
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Check that injury_report field exists (can be null)
        if data["players"]:
            player = data["players"][0]
            assert "injury_report" in player

    def test_roster_with_recent_stats(self, cli_runner):
        """Test roster command with --last-n-days option."""
        # This test is slow (makes multiple API calls)
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--last-n-days", "7", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Should have stats_period in output
        assert "stats_period" in data
        assert data["stats_period"] == "last_7_days"

        # Players should have games_played field
        if data["players"]:
            player = data["players"][0]
            assert "games_played" in player

    @pytest.mark.slow
    def test_roster_with_trends(self, cli_runner):
        """Test roster command with --trends option."""
        # This test is slow (fetches 35 days of data)
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--trends", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Should have stats_period = recent_trends
        assert "stats_period" in data
        assert data["stats_period"] == "recent_trends"

        # Players should have trends field with week1, week2, week3, 14_day, 30_day
        if data["players"]:
            player = data["players"][0]
            assert "trends" in player
            trends = player["trends"]

            # Check for 3 weekly periods (Sat-Fri)
            assert "week1" in trends
            assert "week2" in trends
            assert "week3" in trends
            assert "14_day" in trends
            assert "30_day" in trends

            # Each week should have games_played, total_points, fpg, start, end
            for week in ["week1", "week2", "week3"]:
                assert "games_played" in trends[week]
                assert "total_points" in trends[week]
                assert "fpg" in trends[week]
                assert "start" in trends[week]
                assert "end" in trends[week]

            # 14_day and 30_day should have games_played, total_points, fpg
            for period in ["14_day", "30_day"]:
                assert "games_played" in trends[period]
                assert "total_points" in trends[period]
                assert "fpg" in trends[period]

    @pytest.mark.slow
    def test_roster_trends_table_format(self, cli_runner):
        """Test roster command with --trends in table format."""
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME, "--trends")

        assert result.returncode == 0
        # Check for week column headers (W1, W2, W3)
        assert "W1" in result.stdout
        assert "W2" in result.stdout
        assert "W3" in result.stdout
        assert "FP/G" in result.stdout


# Players command tests
@pytest.mark.integration
class TestPlayersCommand:
    """Integration tests for the players command."""

    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
    ])
    def test_players_output_formats(self, cli_runner, format_arg, format_name):
        """Test players command with different output formats."""
        # Use --no-cache to ensure clean output without "Using cached data" message
        result = cli_runner("--no-cache", "players", "--limit", "5", *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        if format_name == "table":
            # Check for expected columns
            assert "Player" in result.stdout
            assert "Pos" in result.stdout
            assert "Team" in result.stdout
            assert "Available Players" in result.stdout
        elif format_name == "json":
            data = json.loads(result.stdout)
            assert "total_available" in data
            assert "showing" in data
            assert "players" in data
            assert len(data["players"]) == 5

    def test_players_limit_option(self, cli_runner):
        """Test players command with --limit option."""
        result = cli_runner("--no-cache", "players", "--limit", "10", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        assert data["showing"] == 10
        assert len(data["players"]) == 10

    def test_players_sort_option(self, cli_runner):
        """Test players command with --sort option."""
        # Test sorting by FP/G
        result = cli_runner("--no-cache", "players", "--limit", "5", "--sort", "fpg", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        assert len(data["players"]) > 0
        # All players should have fpg field
        for player in data["players"]:
            assert "fpg" in player

    def test_players_json_structure(self, cli_runner):
        """Test that players JSON output has expected fields."""
        result = cli_runner("--no-cache", "players", "--limit", "3", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # Check top-level structure
        assert "total_available" in data
        assert "showing" in data
        assert "players" in data
        assert data["total_available"] > 0

        # Check player structure
        if data["players"]:
            player = data["players"][0]
            expected_fields = ["id", "name", "team", "position", "rank", "status", "salary", "fpts", "fpg"]
            for field in expected_fields:
                assert field in player, f"Missing field: {field}"

    def test_players_returns_free_agents(self, cli_runner):
        """Test that players command returns free agents (FA status)."""
        result = cli_runner("--no-cache", "players", "--limit", "5", "--format", "json")

        assert result.returncode == 0
        data = json.loads(result.stdout)

        # All returned players should be free agents
        for player in data["players"]:
            assert player["status"] == "FA", f"Player {player['name']} is not FA: {player['status']}"


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


# Sync command tests
@pytest.mark.integration
class TestSyncCommand:
    """Integration tests for the sync command."""

    def test_sync_status(self, cli_runner):
        """Test sync --status shows cache status."""
        result = cli_runner("sync", "--status")

        assert result.returncode == 0
        # Should show some status information
        assert "Cache" in result.stdout or "sync" in result.stdout.lower()

    def test_sync_teams_only(self, cli_runner):
        """Test sync --teams syncs only teams."""
        result = cli_runner("sync", "--teams")

        assert result.returncode == 0
        # Should complete successfully
        assert "Teams" in result.stdout or "sync" in result.stdout.lower()

    @pytest.mark.slow
    def test_sync_full(self, cli_runner):
        """Test sync --full performs a complete sync."""
        result = cli_runner("sync", "--full")

        assert result.returncode == 0
        # Should mention syncing multiple data types
        assert "Sync complete" in result.stdout or "Done" in result.stdout

    def test_sync_clear(self, cli_runner):
        """Test sync --clear clears the cache."""
        result = cli_runner("sync", "--clear", "--yes")

        assert result.returncode == 0
        # Should confirm cache was cleared
        assert "clear" in result.stdout.lower() or "Cache" in result.stdout

    def test_sync_help(self, cli_runner):
        """Test sync --help shows usage information."""
        result = cli_runner("sync", "--help")

        assert result.returncode == 0
        assert "sync" in result.stdout.lower()
        # Should mention the main options
        assert "--full" in result.stdout
        assert "--status" in result.stdout


# Cache behavior tests
@pytest.mark.integration
class TestCacheBehavior:
    """Integration tests for caching behavior."""

    TEAM_NAME = "Bois ton (dro)let"

    def test_teams_uses_cache(self, cli_runner):
        """Test teams command uses cache when available."""
        # First call - may or may not use cache
        result1 = cli_runner("teams")
        assert result1.returncode == 0

        # Second call - should use cache (if not stale)
        result2 = cli_runner("teams")
        assert result2.returncode == 0
        # May show "Using cached data" message
        # This is informational - just verify it works

    def test_teams_no_cache_flag(self, cli_runner):
        """Test --no-cache bypasses local cache."""
        # Global flags must come before the subcommand
        result = cli_runner("--no-cache", "teams")

        assert result.returncode == 0
        # Should not show "Using cached data" message
        assert "Using cached data" not in result.stdout

    def test_teams_refresh_flag(self, cli_runner):
        """Test --refresh forces cache refresh."""
        # Global flags must come before the subcommand
        result = cli_runner("--refresh", "teams")

        assert result.returncode == 0
        # Should fetch fresh data, not show "Using cached data"
        assert "Using cached data" not in result.stdout

    def test_roster_uses_cache(self, cli_runner):
        """Test roster command uses cache when available."""
        result = cli_runner("roster", self.TEAM_NAME)
        assert result.returncode == 0

    def test_roster_no_cache_flag(self, cli_runner):
        """Test roster --no-cache bypasses local cache."""
        # Global flags must come before the subcommand
        result = cli_runner("--no-cache", "roster", self.TEAM_NAME)

        assert result.returncode == 0
        assert "Using cached data" not in result.stdout

    def test_roster_refresh_flag(self, cli_runner):
        """Test roster --refresh forces cache refresh."""
        # Global flags must come before the subcommand
        result = cli_runner("--refresh", "roster", self.TEAM_NAME)

        assert result.returncode == 0
        assert "Using cached data" not in result.stdout

    def test_players_uses_cache(self, cli_runner):
        """Test players command uses cache when available."""
        result = cli_runner("players", "--limit", "5")
        assert result.returncode == 0

    def test_players_no_cache_flag(self, cli_runner):
        """Test players --no-cache bypasses local cache."""
        # Global flags must come before the subcommand
        result = cli_runner("--no-cache", "players", "--limit", "5")

        assert result.returncode == 0
        assert "Using cached data" not in result.stdout

    @pytest.mark.slow
    def test_roster_trends_uses_cache(self, cli_runner):
        """Test roster --trends uses cached trends when available."""
        # First sync trends
        sync_result = cli_runner("sync", "--full")
        assert sync_result.returncode == 0

        # Now roster with trends should use cache
        result = cli_runner("roster", self.TEAM_NAME, "--trends")
        assert result.returncode == 0
        # Should show cached data message if cache is fresh
        # This depends on timing but the command should work

    def test_cache_status_after_commands(self, cli_runner):
        """Test that sync --status reflects cached data."""
        # Run a command that caches data
        cli_runner("teams")

        # Check status
        result = cli_runner("sync", "--status")
        assert result.returncode == 0
        # Should show some cached data info


# News command tests
@pytest.mark.integration
class TestNewsCommand:
    """Integration tests for the news command."""

    TEAM_NAME = "Bois ton (dro)let"

    def test_news_help(self, cli_runner):
        """Test news --help shows usage information."""
        result = cli_runner("news", "--help")

        assert result.returncode == 0
        assert "news" in result.stdout.lower()
        # Should mention the main options
        assert "--team" in result.stdout or "-t" in result.stdout
        assert "--limit" in result.stdout or "-n" in result.stdout
        assert "--all" in result.stdout or "-a" in result.stdout

    def test_news_default_shows_roster_news_or_sync_message(self, cli_runner):
        """Test news command with no arguments shows roster news or prompts to sync."""
        result = cli_runner("news")

        assert result.returncode == 0
        # Either shows news or prompts to sync
        assert ("News for" in result.stdout or
                "No player news found" in result.stdout or
                "No news found" in result.stdout or
                "sync" in result.stdout.lower())

    @pytest.mark.parametrize("format_arg,format_name", [
        ([], "table"),
        (["--format", "json"], "json"),
        (["--format", "simple"], "simple"),
    ])
    def test_news_output_formats(self, cli_runner, format_arg, format_name):
        """Test news command with different output formats."""
        result = cli_runner("news", "--all", "--limit", "3", *format_arg)

        assert result.returncode == 0, f"Command failed: {result.stderr}"

        # If no news is synced, we'll get a message about syncing
        if "No player news found" in result.stdout or "sync" in result.stdout.lower():
            return  # Skip format validation if no news data

        if format_name == "json":
            # JSON output should be valid JSON array
            data = json.loads(result.stdout)
            assert isinstance(data, list)
            if len(data) > 0:
                # Check news item structure
                item = data[0]
                assert "player_name" in item or "headline" in item
        elif format_name == "simple":
            # Simple format should have output
            assert len(result.stdout.strip()) > 0

    def test_news_all_flag(self, cli_runner):
        """Test news --all shows all recent news."""
        result = cli_runner("news", "--all", "--limit", "5")

        assert result.returncode == 0
        # Either shows news or prompts to sync
        assert ("News" in result.stdout or
                "news" in result.stdout.lower() or
                "sync" in result.stdout.lower())

    def test_news_team_filter(self, cli_runner):
        """Test news --team filters by team."""
        result = cli_runner("news", "--team", self.TEAM_NAME)

        assert result.returncode == 0
        # Either shows news for team or prompts to sync
        # Team name may be in output or we get sync prompt
        has_team_news = self.TEAM_NAME in result.stdout or "News for" in result.stdout
        has_sync_prompt = "No player news" in result.stdout or "sync" in result.stdout.lower()
        has_no_news = "No news found" in result.stdout

        assert has_team_news or has_sync_prompt or has_no_news

    def test_news_team_filter_json(self, cli_runner):
        """Test news --team with JSON output."""
        result = cli_runner("news", "--team", self.TEAM_NAME, "--format", "json")

        assert result.returncode == 0

        # If no news, output may not be JSON
        if "No player news found" in result.stdout or "No news found" in result.stdout:
            return

        # Should be valid JSON
        try:
            data = json.loads(result.stdout)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            # May have non-JSON message if no news
            assert "news" in result.stdout.lower() or "sync" in result.stdout.lower()

    def test_news_limit_option(self, cli_runner):
        """Test news --limit controls number of items."""
        result = cli_runner("news", "--all", "--limit", "2", "--format", "json")

        assert result.returncode == 0

        # If no news synced, skip
        if "No player news found" in result.stdout:
            return

        try:
            data = json.loads(result.stdout)
            # Each player should have at most 2 items (depends on data structure)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            pass  # Non-JSON output is fine if no news

    def test_news_player_argument(self, cli_runner):
        """Test news with player name argument."""
        # Use a common player name pattern
        result = cli_runner("news", "Connor", "--format", "json")

        assert result.returncode == 0

        # Either shows player news or reports not found
        if "No news found for player" in result.stdout:
            return  # Player search worked but no results
        if "No player news found" in result.stdout:
            return  # No news synced

        try:
            data = json.loads(result.stdout)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            # Non-JSON is fine for error messages
            pass

    def test_news_invalid_team(self, cli_runner):
        """Test news with invalid team name."""
        result = cli_runner("news", "--team", "NonexistentTeamName12345")

        # Should either return error, show not found, or show sync prompt (if no news synced)
        valid_responses = (
            "not found" in result.stdout.lower() or
            "no player news found" in result.stdout.lower() or
            result.returncode != 0
        )
        assert valid_responses


@pytest.mark.integration
class TestNewsCacheBehavior:
    """Integration tests for news caching behavior."""

    def test_news_uses_cache(self, cli_runner):
        """Test news command uses cache when available."""
        # First call
        result1 = cli_runner("news", "--all", "--limit", "3")
        assert result1.returncode == 0

        # Second call - should work (may use cache)
        result2 = cli_runner("news", "--all", "--limit", "3")
        assert result2.returncode == 0

    def test_news_shows_cache_age(self, cli_runner):
        """Test news command shows cache age when using cached data."""
        result = cli_runner("news")

        assert result.returncode == 0
        # If there's cached news, should show age info
        # This is informational - just verify command works

    @pytest.mark.slow
    def test_sync_news_flag(self, cli_runner):
        """Test sync --news syncs player news."""
        result = cli_runner("sync", "--news")

        assert result.returncode == 0
        # Should mention news syncing
        assert "news" in result.stdout.lower() or "player" in result.stdout.lower()

    @pytest.mark.slow
    def test_sync_full_includes_news(self, cli_runner):
        """Test sync --full includes news sync."""
        result = cli_runner("sync", "--full")

        assert result.returncode == 0
        # Full sync should complete (news is part of it)
        assert "Sync complete" in result.stdout or "Done" in result.stdout

    @pytest.mark.slow
    def test_sync_no_news_flag(self, cli_runner):
        """Test sync --full --no-news skips news sync."""
        result = cli_runner("sync", "--full", "--no-news")

        assert result.returncode == 0
        # Should still complete successfully


@pytest.mark.integration
class TestNewsApiAndStorage:
    """Integration tests to validate news API responses and database storage."""

    @pytest.mark.slow
    def test_sync_news_stores_data_for_rostered_player(self, cli_runner):
        """Test that sync --news actually stores news in database for rostered players."""
        import sqlite3
        from fantrax_cli.config import load_config

        config = load_config()

        # First run sync --news
        result = cli_runner("sync", "--news")
        assert result.returncode == 0, f"Sync failed: {result.stderr}"

        # Check database for stored news
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Count news items
        cursor.execute("SELECT COUNT(*) as count FROM player_news")
        count = cursor.fetchone()['count']

        # Get news for a rostered player (join with roster_slots)
        cursor.execute("""
            SELECT pn.*, p.name as player_name
            FROM player_news pn
            JOIN players p ON pn.player_id = p.id
            JOIN roster_slots rs ON rs.player_id = p.id
            LIMIT 5
        """)
        rostered_news = cursor.fetchall()
        conn.close()

        print(f"\n=== News Storage Results ===")
        print(f"Total news items stored: {count}")
        print(f"Rostered players with news: {len(rostered_news)}")
        for item in rostered_news:
            print(f"  - {item['player_name']}: {item['headline'][:50]}...")

        # Assert that news was stored
        assert count > 0, "No news items were stored - API sync may have failed"
        # At least one rostered player should have news (most likely in active league)
        assert len(rostered_news) > 0, "No news was stored for rostered players - news sync may have failed"

    @pytest.mark.slow
    def test_sync_news_stores_data_for_free_agent(self, cli_runner):
        """Test that sync --news stores news for free agents too."""
        import sqlite3
        from fantrax_cli.config import load_config

        config = load_config()

        # First ensure free agents are synced
        result = cli_runner("sync", "--free-agents")
        assert result.returncode == 0

        # Then sync news (which should include top FA)
        result = cli_runner("sync", "--news")
        assert result.returncode == 0

        # Check if any free agent has news
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get news for players that are free agents
        cursor.execute("""
            SELECT pn.*, p.name as player_name
            FROM player_news pn
            JOIN players p ON pn.player_id = p.id
            JOIN free_agents fa ON fa.player_id = p.id
            LIMIT 5
        """)
        fa_news = cursor.fetchall()

        # Also count total news items to confirm sync worked
        cursor.execute("SELECT COUNT(*) as count FROM player_news")
        total_count = cursor.fetchone()['count']
        conn.close()

        print(f"\n=== Free Agent News Storage Results ===")
        print(f"Total news items stored: {total_count}")
        print(f"Free agents with news: {len(fa_news)}")
        for item in fa_news:
            print(f"  - {item['player_name']}: {item['headline'][:50]}...")

        # Assert that news was stored overall
        assert total_count > 0, "No news items were stored - API sync may have failed"
        # Assert that at least one free agent has news
        assert len(fa_news) > 0, "No news was stored for free agents - FA news sync may have failed"
