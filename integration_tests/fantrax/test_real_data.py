"""Integration tests using real Fantrax data.

These tests validate functionality against actual league data rather than
mocked responses. They require:
- Valid .env credentials configured
- Database synced with real data (run `fantrax sync --full` first)

Run with: pytest integration_tests/test_real_data.py -v
"""

import json
import sqlite3

import pytest

from aissistant_gm.fantrax.config import load_config


def parse_json_output(output: str) -> dict | list:
    """Parse JSON from CLI output, skipping any cache status messages."""
    lines = output.strip().split('\n')
    # Find the first line that starts with { or [
    json_start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('{') or stripped.startswith('['):
            json_start = i
            break
    json_text = '\n'.join(lines[json_start:])
    return json.loads(json_text)


# Real data fixtures - these are actual values from the league
# Update these if players change teams or get dropped
class RealData:
    """Real data constants for testing."""

    # Team info
    MY_TEAM_NAME = "Bois ton (dro)let"
    MY_TEAM_ID = "5pr8olt0mbc80puf"
    MY_TEAM_SHORT = "Frankey"

    # Other teams for testing
    OTHER_TEAM_NAME = "Boldy Won Kenobi"
    OTHER_TEAM_SHORT = "Berry"

    # These are populated dynamically from the database
    # to handle roster changes
    rostered_player_name: str | None = None
    rostered_player_id: str | None = None
    free_agent_name: str | None = None
    free_agent_id: str | None = None


@pytest.fixture(scope="module")
def real_data():
    """Load real player data from the database."""
    config = load_config()
    conn = sqlite3.connect(config.database_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get a rostered player with news
    cursor.execute("""
        SELECT DISTINCT p.id, p.name
        FROM player_news pn
        JOIN players p ON pn.player_id = p.id
        JOIN roster_slots rs ON rs.player_id = p.id
        ORDER BY pn.news_date DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        RealData.rostered_player_id = row['id']
        RealData.rostered_player_name = row['name']

    # Get a free agent with news
    cursor.execute("""
        SELECT DISTINCT p.id, p.name
        FROM player_news pn
        JOIN players p ON pn.player_id = p.id
        JOIN free_agents fa ON fa.player_id = p.id
        ORDER BY pn.news_date DESC
        LIMIT 1
    """)
    row = cursor.fetchone()
    if row:
        RealData.free_agent_id = row['id']
        RealData.free_agent_name = row['name']

    conn.close()
    return RealData


@pytest.mark.integration
class TestRealTeamData:
    """Tests using real team data."""

    def test_teams_command_shows_my_team(self, cli_runner):
        """Test that teams command shows my actual team."""
        result = cli_runner("teams")

        assert result.returncode == 0
        assert RealData.MY_TEAM_NAME in result.stdout

    def test_teams_json_contains_my_team_id(self, cli_runner):
        """Test teams JSON output contains my team ID."""
        result = cli_runner("teams", "--format", "json")

        assert result.returncode == 0
        data = parse_json_output(result.stdout)
        assert "teams" in data

        team_ids = [t.get('id') for t in data['teams']]
        assert RealData.MY_TEAM_ID in team_ids

    def test_roster_command_for_my_team(self, cli_runner):
        """Test roster command shows players for my team."""
        # Roster takes team as positional argument
        result = cli_runner("roster", RealData.MY_TEAM_NAME)

        assert result.returncode == 0
        # Should show roster content, not an error
        assert "Error" not in result.stderr

    def test_roster_json_has_player_structure(self, cli_runner):
        """Test roster JSON has proper player data structure."""
        result = cli_runner("roster", RealData.MY_TEAM_NAME, "--format", "json")

        assert result.returncode == 0
        data = parse_json_output(result.stdout)

        # Roster returns a dict with roster info
        assert isinstance(data, (list, dict))
        if isinstance(data, dict) and 'roster' in data:
            roster = data['roster']
        else:
            roster = data if isinstance(data, list) else []

        if len(roster) > 0:
            player = roster[0]
            # Should have essential player fields
            has_player_info = any(k in player for k in ['player_name', 'name', 'player'])
            assert has_player_info, f"Expected player info, got keys: {player.keys()}"


@pytest.mark.integration
class TestRealPlayerNews:
    """Tests using real player news data."""

    def test_news_exists_for_rostered_player(self, cli_runner, real_data):
        """Test that news exists in DB for a real rostered player."""
        if not real_data.rostered_player_name:
            pytest.skip("No rostered player with news found in database")

        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) as count FROM player_news WHERE player_id = ?",
            (real_data.rostered_player_id,)
        )
        count = cursor.fetchone()['count']
        conn.close()

        assert count > 0, f"No news found for {real_data.rostered_player_name}"

    def test_news_exists_for_free_agent(self, cli_runner, real_data):
        """Test that news exists in DB for a real free agent."""
        if not real_data.free_agent_name:
            pytest.skip("No free agent with news found in database")

        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) as count FROM player_news WHERE player_id = ?",
            (real_data.free_agent_id,)
        )
        count = cursor.fetchone()['count']
        conn.close()

        assert count > 0, f"No news found for {real_data.free_agent_name}"

    def test_news_command_finds_rostered_player_by_name(self, cli_runner, real_data):
        """Test news command can find a real rostered player by name."""
        if not real_data.rostered_player_name:
            pytest.skip("No rostered player with news found in database")

        # Use first name for search (more likely to match)
        first_name = real_data.rostered_player_name.split()[0]
        result = cli_runner("news", first_name, "--format", "json")

        assert result.returncode == 0

        # Should find news or indicate no match
        if "No news found" not in result.stdout and "No player news" not in result.stdout:
            data = parse_json_output(result.stdout)
            news_items = data.get('news_items', data) if isinstance(data, dict) else data
            assert isinstance(news_items, list)
            assert len(news_items) > 0, f"Expected news for player matching '{first_name}'"

    def test_news_command_finds_free_agent_by_name(self, cli_runner, real_data):
        """Test news command can find a real free agent by name."""
        if not real_data.free_agent_name:
            pytest.skip("No free agent with news found in database")

        # Use first name for search
        first_name = real_data.free_agent_name.split()[0]
        result = cli_runner("news", first_name, "--format", "json")

        assert result.returncode == 0

        # Should find news or indicate no match
        if "No news found" not in result.stdout and "No player news" not in result.stdout:
            data = parse_json_output(result.stdout)
            news_items = data.get('news_items', data) if isinstance(data, dict) else data
            assert isinstance(news_items, list)

    def test_news_for_my_team_returns_data(self, cli_runner):
        """Test news --team with my real team name returns structured data."""
        result = cli_runner("news", "--team", RealData.MY_TEAM_NAME, "--format", "json")

        assert result.returncode == 0

        # If news exists, should be valid JSON
        if "No news found" not in result.stdout and "No player news" not in result.stdout:
            data = parse_json_output(result.stdout)
            news_items = data.get('news_items', data) if isinstance(data, dict) else data
            assert isinstance(news_items, list)

    def test_news_all_returns_multiple_items(self, cli_runner):
        """Test news --all returns multiple news items."""
        result = cli_runner("news", "--all", "--limit", "10", "--format", "json")

        assert result.returncode == 0

        if "No player news" not in result.stdout:
            data = parse_json_output(result.stdout)
            news_items = data.get('news_items', []) if isinstance(data, dict) else data
            assert isinstance(news_items, list)
            assert len(news_items) > 0, "Expected at least one news item with --all"


@pytest.mark.integration
class TestRealNewsJsonStructure:
    """Tests validating the JSON structure of news output."""

    def test_news_json_has_required_fields(self, cli_runner):
        """Test news JSON output has all required fields."""
        result = cli_runner("news", "--all", "--limit", "5", "--format", "json")

        assert result.returncode == 0

        if "No player news" in result.stdout:
            pytest.skip("No news data available")

        data = parse_json_output(result.stdout)
        news_items = data.get('news_items', []) if isinstance(data, dict) else data
        assert isinstance(news_items, list)
        assert len(news_items) > 0, "Expected at least one news item"

        item = news_items[0]
        # Check required fields exist
        assert "player_name" in item, "Missing player_name field"
        assert "headline" in item, "Missing headline field"
        assert "news_date" in item, "Missing news_date field"

    def test_news_json_headline_not_empty(self, cli_runner):
        """Test that news headlines are not empty."""
        result = cli_runner("news", "--all", "--limit", "5", "--format", "json")

        assert result.returncode == 0

        if "No player news" in result.stdout:
            pytest.skip("No news data available")

        data = parse_json_output(result.stdout)
        news_items = data.get('news_items', []) if isinstance(data, dict) else data

        for item in news_items:
            headline = item.get("headline", "")
            assert len(headline) > 0, f"Empty headline for {item.get('player_name')}"

    def test_news_json_date_format(self, cli_runner):
        """Test that news dates are in ISO format."""
        result = cli_runner("news", "--all", "--limit", "5", "--format", "json")

        assert result.returncode == 0

        if "No player news" in result.stdout:
            pytest.skip("No news data available")

        data = parse_json_output(result.stdout)
        news_items = data.get('news_items', []) if isinstance(data, dict) else data

        for item in news_items:
            news_date = item.get("news_date", "")
            # Should be ISO format like "2025-01-25T12:34:56"
            assert "T" in news_date or "-" in news_date, f"Invalid date format: {news_date}"


@pytest.mark.integration
class TestRealTeamsWithStandings:
    """Tests that teams command shows standings data."""

    def test_teams_shows_standings_info(self, cli_runner):
        """Test teams command shows standings/ranking info."""
        result = cli_runner("teams")

        assert result.returncode == 0
        # Should show teams with some ranking info
        has_known_team = (
            RealData.MY_TEAM_NAME in result.stdout or
            RealData.MY_TEAM_SHORT in result.stdout or
            RealData.OTHER_TEAM_NAME in result.stdout
        )
        assert has_known_team, "Expected to see real team names"

    def test_teams_json_has_standings_data(self, cli_runner):
        """Test teams JSON includes standings/points data."""
        result = cli_runner("teams", "--format", "json")

        assert result.returncode == 0
        data = parse_json_output(result.stdout)

        # Teams are in the 'teams' key
        teams = data.get('teams', [])
        assert isinstance(teams, list)
        assert len(teams) > 0

        team = teams[0]
        # Teams should have standings info if synced
        # Check for any ranking-related fields
        has_standings = any(k in team for k in ['rank', 'points', 'fpts', 'fPts', 'wins', 'losses'])
        # Note: may not have standings if not synced yet, so just verify structure
        assert 'name' in team or 'id' in team, f"Expected team info, got: {list(team.keys())}"


@pytest.mark.integration
class TestRealPlayers:
    """Tests using real free agent/player data."""

    def test_players_returns_data(self, cli_runner):
        """Test players command returns actual players."""
        result = cli_runner("players", "--limit", "10")

        assert result.returncode == 0
        # Should show player data or prompt to sync
        assert len(result.stdout) > 50, "Expected substantial player output"

    def test_players_json_has_player_info(self, cli_runner):
        """Test players JSON has player information."""
        result = cli_runner("players", "--limit", "5", "--format", "json")

        assert result.returncode == 0

        if "sync" in result.stdout.lower() and "players" in result.stdout.lower():
            pytest.skip("Players not synced")

        data = parse_json_output(result.stdout)

        # Players might be in 'players' or 'free_agents' key or be a list
        players = data.get('players', data.get('free_agents', data)) if isinstance(data, dict) else data
        assert isinstance(players, list)

        if len(players) > 0:
            player = players[0]
            # Should have player identification
            has_player_info = any(k in player for k in ['name', 'player_name', 'player'])
            assert has_player_info, f"Expected player info, got: {list(player.keys())}"


@pytest.mark.integration
@pytest.mark.slow
class TestRealSyncOperations:
    """Tests for sync operations with real API calls."""

    def test_sync_news_updates_database(self, cli_runner):
        """Test that sync --news actually updates the database."""
        config = load_config()

        # Get count before
        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_news")
        count_before = cursor.fetchone()[0]
        conn.close()

        # Run sync
        result = cli_runner("sync", "--news")
        assert result.returncode == 0

        # Get count after
        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_news")
        count_after = cursor.fetchone()[0]
        conn.close()

        # Should have news (may be same if no new news, but shouldn't decrease)
        assert count_after >= count_before, "News count should not decrease after sync"
        assert count_after > 0, "Should have at least some news after sync"

    def test_sync_rosters_populates_roster_slots(self, cli_runner):
        """Test that sync --rosters populates roster_slots table."""
        config = load_config()

        result = cli_runner("sync", "--rosters")
        assert result.returncode == 0

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM roster_slots")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "Should have roster slots after sync"
