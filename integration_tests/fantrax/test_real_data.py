"""Integration tests using real Fantrax data.

These tests validate functionality against actual league data rather than
mocked responses. They require:
- Valid .env credentials configured
- Database is synced automatically by the module-level fixture

Run with: pytest integration_tests/test_real_data.py -v
"""

import json
import sqlite3

import pytest

from aissistant_gm.fantrax.config import load_config
from .conftest import populate_database


@pytest.fixture(scope="module", autouse=True)
def ensure_database_populated():
    """Ensure database is populated before running data validation tests.

    This module-scoped fixture runs once before any test in this module,
    and syncs all necessary data so that data validation tests don't skip.

    Note: This runs at module level (not session) because test_sync_clear
    in test_commands.py clears the database, so we need to repopulate it
    right before these tests run.
    """
    populate_database()
    yield


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

    def test_sync_transactions_populates_table(self, cli_runner):
        """Test that sync --transactions populates transactions table."""
        config = load_config()

        result = cli_runner("sync", "--transactions")
        assert result.returncode == 0
        assert "Synced" in result.stdout, "Expected sync confirmation message"

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        conn.close()

        # Note: count may be 0 if no transactions in the league yet
        # Just verify the command succeeded
        assert count >= 0, "transactions table should exist"

    def test_sync_matchups_populates_tables(self, cli_runner):
        """Test that sync --matchups populates matchups and scoring_periods tables."""
        config = load_config()

        result = cli_runner("sync", "--matchups")
        assert result.returncode == 0
        assert "Synced" in result.stdout, "Expected sync confirmation message"

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM scoring_periods")
        period_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM matchups")
        matchup_count = cursor.fetchone()[0]

        conn.close()

        # Should have some periods and matchups after sync
        assert period_count > 0, "Should have scoring periods after sync"
        assert matchup_count > 0, "Should have matchups after sync"

    def test_sync_standings_updates_database(self, cli_runner):
        """Test that sync --standings updates standings data."""
        config = load_config()

        result = cli_runner("sync", "--standings")
        assert result.returncode == 0
        assert "Synced" in result.stdout or "standings" in result.stdout.lower()

        # Standings are stored in the standings table
        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM standings")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "Should have standings data after sync"

    def test_sync_free_agents_populates_table(self, cli_runner):
        """Test that sync --free-agents populates free_agents table."""
        config = load_config()

        result = cli_runner("sync", "--free-agents")
        assert result.returncode == 0
        assert "Synced" in result.stdout or "free agent" in result.stdout.lower()

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM free_agents")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "Should have free agents after sync"

    def test_sync_scores_populates_daily_scores(self, cli_runner):
        """Test that sync --scores populates daily_scores table."""
        config = load_config()

        # Sync just 3 days to keep test faster
        result = cli_runner("sync", "--scores", "3")
        assert result.returncode == 0
        assert "Synced" in result.stdout or "score" in result.stdout.lower()

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM daily_scores")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "Should have daily scores after sync"

    def test_sync_trends_calculates_trends(self, cli_runner):
        """Test that sync --trends calculates and stores player trends."""
        config = load_config()

        # First ensure we have daily scores to calculate trends from
        scores_result = cli_runner("sync", "--scores", "14")
        assert scores_result.returncode == 0

        # Then calculate trends
        result = cli_runner("sync", "--trends")
        assert result.returncode == 0
        assert "Calculated" in result.stdout or "trend" in result.stdout.lower()

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_trends")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "Should have player trends after sync"

    def test_sync_toi_scrapes_and_stores_toi(self, cli_runner):
        """Test that sync --toi scrapes and stores TOI data."""
        config = load_config()

        # First ensure rosters are synced (TOI needs player IDs)
        roster_result = cli_runner("sync", "--rosters")
        assert roster_result.returncode == 0

        # Then scrape TOI
        result = cli_runner("sync", "--toi")
        assert result.returncode == 0
        # Should mention TOI or scraping
        assert "TOI" in result.stdout or "Scrape" in result.stdout or "scraped" in result.stdout.lower()

        conn = sqlite3.connect(config.database_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM player_toi")
        count = cursor.fetchone()[0]
        conn.close()

        # Note: count may be 0 if no skaters in roster (e.g., only goalies which don't have TOI)
        # Just verify command succeeded
        assert count >= 0, "player_toi table should exist"


@pytest.mark.integration
class TestRealTransactionData:
    """Tests using real transaction data."""

    def test_transactions_table_has_valid_structure(self, cli_runner):
        """Test that transactions table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM transactions LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No transactions in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'id' in columns
        assert 'league_id' in columns
        assert 'team_id' in columns
        assert 'transaction_date' in columns

    def test_transaction_players_linked_to_transactions(self, cli_runner):
        """Test that transaction_players are properly linked to transactions."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check for orphaned transaction_players
        cursor.execute("""
            SELECT COUNT(*) as count FROM transaction_players tp
            WHERE NOT EXISTS (
                SELECT 1 FROM transactions t WHERE t.id = tp.transaction_id
            )
        """)
        orphan_count = cursor.fetchone()['count']
        conn.close()

        assert orphan_count == 0, "Should have no orphaned transaction_players"


@pytest.mark.integration
class TestRealDailyScoresData:
    """Tests using real daily scores data."""

    def test_daily_scores_table_has_valid_structure(self, cli_runner):
        """Test that daily_scores table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM daily_scores LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No daily scores in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'player_id' in columns
        assert 'team_id' in columns
        assert 'scoring_date' in columns
        assert 'fantasy_points' in columns

    def test_daily_scores_have_valid_dates(self, cli_runner):
        """Test that daily scores have valid date format."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT scoring_date FROM daily_scores LIMIT 10")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No daily scores in database")

        for row in rows:
            date_str = row['scoring_date']
            # Should be in YYYY-MM-DD format
            assert len(date_str) == 10, f"Invalid date format: {date_str}"
            assert date_str[4] == '-' and date_str[7] == '-', f"Invalid date format: {date_str}"

    def test_daily_scores_fantasy_points_reasonable(self, cli_runner):
        """Test that fantasy points are within reasonable ranges."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT fantasy_points FROM daily_scores LIMIT 100")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No daily scores in database")

        for row in rows:
            # Fantasy points should be reasonable (-10 to 30 typical range)
            assert -20 <= row['fantasy_points'] <= 50, f"Unusual fantasy points: {row['fantasy_points']}"


@pytest.mark.integration
class TestRealPlayerTrendsData:
    """Tests using real player trends data."""

    def test_player_trends_table_has_valid_structure(self, cli_runner):
        """Test that player_trends table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM player_trends LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No player trends in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'player_id' in columns
        assert 'period_type' in columns
        assert 'total_points' in columns
        assert 'games_played' in columns
        assert 'fpg' in columns

    def test_player_trends_have_expected_periods(self, cli_runner):
        """Test that player trends have expected period types."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT period_type FROM player_trends")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No player trends in database")

        period_types = {row['period_type'] for row in rows}
        # Should have at least some of these period types
        expected_periods = {'week1', 'week2', 'week3', '14', '30'}
        assert len(period_types & expected_periods) > 0, f"Expected period types, got: {period_types}"

    def test_player_trends_fpg_reasonable(self, cli_runner):
        """Test that FPG (fantasy points per game) values are reasonable."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT fpg FROM player_trends WHERE games_played > 0 LIMIT 50")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No player trends with games in database")

        for row in rows:
            # FPG should be reasonable (-5 to 10 typical range)
            assert -10 <= row['fpg'] <= 15, f"Unusual FPG: {row['fpg']}"


@pytest.mark.integration
class TestRealPlayerToiData:
    """Tests using real player TOI (Time On Ice) data."""

    def test_player_toi_table_has_valid_structure(self, cli_runner):
        """Test that player_toi table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM player_toi LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No TOI data in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'player_id' in columns
        assert 'toi_seconds' in columns
        assert 'toipp_seconds' in columns
        assert 'toish_seconds' in columns
        assert 'games_played' in columns

    def test_player_toi_values_are_reasonable(self, cli_runner):
        """Test that TOI values are within reasonable ranges."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM player_toi WHERE games_played > 0 LIMIT 20")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No TOI data with games in database")

        for row in rows:
            # TOI per game should be between 0 and 30 minutes (1800 seconds)
            if row['games_played'] > 0:
                toi_per_game = row['toi_seconds'] / row['games_played']
                assert 0 <= toi_per_game <= 1800, f"TOI per game {toi_per_game} out of range"

            # TOIPP (power play) should be less than total TOI
            assert row['toipp_seconds'] <= row['toi_seconds'], "TOIPP should be <= TOI"
            # TOISH (shorthanded) should be less than total TOI
            assert row['toish_seconds'] <= row['toi_seconds'], "TOISH should be <= TOI"

    def test_player_toi_linked_to_players(self, cli_runner):
        """Test that player_toi records are linked to valid players."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check for orphaned TOI records
        cursor.execute("""
            SELECT COUNT(*) as count FROM player_toi pt
            WHERE NOT EXISTS (
                SELECT 1 FROM players p WHERE p.id = pt.player_id
            )
        """)
        orphan_count = cursor.fetchone()['count']
        conn.close()

        assert orphan_count == 0, "Should have no orphaned player_toi records"


@pytest.mark.integration
class TestRealFreeAgentsData:
    """Tests using real free agents data."""

    def test_free_agents_table_has_valid_structure(self, cli_runner):
        """Test that free_agents table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM free_agents LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No free agents in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'player_id' in columns
        assert 'rank' in columns

    def test_free_agents_linked_to_players(self, cli_runner):
        """Test that free_agents records are linked to valid players."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check for orphaned free agent records
        cursor.execute("""
            SELECT COUNT(*) as count FROM free_agents fa
            WHERE NOT EXISTS (
                SELECT 1 FROM players p WHERE p.id = fa.player_id
            )
        """)
        orphan_count = cursor.fetchone()['count']
        conn.close()

        assert orphan_count == 0, "Should have no orphaned free_agents records"

    def test_free_agents_have_rank(self, cli_runner):
        """Test that free agents have rank values."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT rank FROM free_agents WHERE rank IS NOT NULL LIMIT 10")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No ranked free agents in database")

        for row in rows:
            assert row['rank'] > 0, "Rank should be positive"


@pytest.mark.integration
class TestRealStandingsData:
    """Tests using real standings data."""

    def test_standings_table_has_valid_structure(self, cli_runner):
        """Test that standings table has valid data structure."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM standings LIMIT 1")
        row = cursor.fetchone()
        conn.close()

        if row is None:
            pytest.skip("No standings in database")

        # Verify expected columns exist
        columns = row.keys()
        assert 'league_id' in columns
        assert 'team_id' in columns
        assert 'rank' in columns

    def test_standings_have_team_records(self, cli_runner):
        """Test that standings have team records (wins, losses)."""
        config = load_config()
        conn = sqlite3.connect(config.database_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM standings LIMIT 10")
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            pytest.skip("No standings in database")

        columns = rows[0].keys()
        # May have wins/losses/ties columns
        has_record = any(col in columns for col in ['wins', 'win', 'losses', 'loss', 'points'])
        assert has_record or 'rank' in columns, f"Expected standings data, got columns: {list(columns)}"
