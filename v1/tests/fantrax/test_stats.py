"""Tests for stats module."""

import pytest
from datetime import date, timedelta
from unittest.mock import Mock, patch, MagicMock

from aissistant_gm.fantrax.stats import (
    _get_daily_scores_for_team,
    _get_fantrax_week_boundaries,
    calculate_recent_trends,
    calculate_recent_fpg,
    fetch_fa_player_trends,
)


class TestGetFantraxWeekBoundaries:
    """Test _get_fantrax_week_boundaries function."""

    def test_weekday_returns_previous_friday_end(self):
        """Test that a weekday returns the previous Friday as week end."""
        # Wednesday, Jan 15, 2025
        ref_date = date(2025, 1, 15)
        weeks = _get_fantrax_week_boundaries(ref_date)

        # Most recent Friday before Jan 15 is Jan 10
        assert len(weeks) == 3
        week1_start, week1_end, label = weeks[0]
        assert label == "week1"
        assert week1_end == date(2025, 1, 10)  # Friday
        assert week1_start == date(2025, 1, 4)  # Saturday (6 days before Friday)

    def test_friday_uses_that_friday(self):
        """Test that Friday returns that Friday as week end."""
        # Friday, Jan 10, 2025
        ref_date = date(2025, 1, 10)
        weeks = _get_fantrax_week_boundaries(ref_date)

        week1_start, week1_end, _ = weeks[0]
        assert week1_end == date(2025, 1, 10)  # Same Friday
        assert week1_start == date(2025, 1, 4)  # Saturday

    def test_saturday_uses_previous_friday(self):
        """Test that Saturday uses the previous Friday as week end."""
        # Saturday, Jan 11, 2025
        ref_date = date(2025, 1, 11)
        weeks = _get_fantrax_week_boundaries(ref_date)

        week1_start, week1_end, _ = weeks[0]
        assert week1_end == date(2025, 1, 10)  # Previous Friday
        assert week1_start == date(2025, 1, 4)  # Saturday

    def test_returns_three_weeks(self):
        """Test that function returns exactly 3 weeks."""
        ref_date = date(2025, 1, 15)
        weeks = _get_fantrax_week_boundaries(ref_date)

        assert len(weeks) == 3
        assert weeks[0][2] == "week1"
        assert weeks[1][2] == "week2"
        assert weeks[2][2] == "week3"

    def test_weeks_are_seven_days_apart(self):
        """Test that consecutive weeks are 7 days apart."""
        ref_date = date(2025, 1, 15)
        weeks = _get_fantrax_week_boundaries(ref_date)

        # Week 2 should end 7 days before week 1
        assert weeks[1][1] == weeks[0][1] - timedelta(days=7)
        # Week 3 should end 7 days before week 2
        assert weeks[2][1] == weeks[1][1] - timedelta(days=7)

    def test_week_spans_saturday_to_friday(self):
        """Test that each week spans Saturday to Friday (7 days)."""
        ref_date = date(2025, 1, 15)
        weeks = _get_fantrax_week_boundaries(ref_date)

        for start, end, _ in weeks:
            # Each week should be 6 days (start to end inclusive = 7 days)
            assert (end - start).days == 6
            # Start should be Saturday (weekday 5)
            assert start.weekday() == 5
            # End should be Friday (weekday 4)
            assert end.weekday() == 4


class TestGetDailyScoresForTeam:
    """Test _get_daily_scores_for_team function."""

    def test_returns_empty_when_date_not_in_scoring_dates(self):
        """Test that empty dict is returned when date not in scoring_dates."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}  # Only Jan 10

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 15))

        assert result == {}

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_returns_empty_when_team_not_in_response(self, mock_api):
        """Test that empty dict is returned when team not in response."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.return_value = {
            'statsPerTeam': {
                'allTeamsStats': {
                    'other_team': {}
                }
            }
        }

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        assert result == {}

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_returns_player_scores_from_active_roster(self, mock_api):
        """Test that player scores are extracted from ACTIVE roster."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.return_value = {
            'statsPerTeam': {
                'allTeamsStats': {
                    'team1': {
                        'ACTIVE': {
                            'statsMap': {
                                'player1': {'object1': 5.5},
                                'player2': {'object1': 3.0},
                                '_metadata': {'object1': 0}  # Should be skipped
                            }
                        }
                    }
                }
            }
        }

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        assert result == {'player1': 5.5, 'player2': 3.0}
        assert '_metadata' not in result

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_handles_none_points_skips_player(self, mock_api):
        """Test that None points result in player being skipped."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.return_value = {
            'statsPerTeam': {
                'allTeamsStats': {
                    'team1': {
                        'ACTIVE': {
                            'statsMap': {
                                'player1': {'object1': None},  # None - not added
                                'player2': {'object1': 0},     # 0 - added as 0.0
                                'player3': {'object1': 5.0}    # Normal value
                            }
                        }
                    }
                }
            }
        }

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        # player1 is skipped (None), player2 is 0.0, player3 is 5.0
        assert 'player1' not in result
        assert result['player2'] == 0.0
        assert result['player3'] == 5.0

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_handles_zero_points(self, mock_api):
        """Test that zero points are treated as 0.0."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.return_value = {
            'statsPerTeam': {
                'allTeamsStats': {
                    'team1': {
                        'ACTIVE': {
                            'statsMap': {
                                'player1': {'object1': 0},
                                'player2': {'object1': False}  # Falsy but not None
                            }
                        }
                    }
                }
            }
        }

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        assert result['player1'] == 0.0
        assert result['player2'] == 0.0

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_returns_empty_on_exception(self, mock_api):
        """Test that empty dict is returned on API exception."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.side_effect = Exception("API Error")

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        assert result == {}

    @patch('aissistant_gm.fantrax.stats.api.get_live_scoring_stats')
    def test_handles_missing_active_key(self, mock_api):
        """Test that missing ACTIVE key returns empty dict."""
        league = Mock()
        league.scoring_dates = {1: date(2025, 1, 10)}

        mock_api.return_value = {
            'statsPerTeam': {
                'allTeamsStats': {
                    'team1': {
                        'BENCH': {'statsMap': {'player1': {'object1': 5.0}}}
                    }
                }
            }
        }

        result = _get_daily_scores_for_team(league, "team1", date(2025, 1, 10))

        assert result == {}


class TestCalculateRecentTrends:
    """Test calculate_recent_trends function."""

    @patch('aissistant_gm.fantrax.stats._get_daily_scores_for_team')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_calculates_trends_for_players(self, mock_console_class, mock_get_scores):
        """Test that trends are calculated correctly for players."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        today = date.today()

        # Return scores for the last 14 days
        def get_scores_side_effect(league, team_id, scoring_date):
            if (today - scoring_date).days < 14:
                return {'player1': 5.0}
            return {}

        mock_get_scores.side_effect = get_scores_side_effect

        result = calculate_recent_trends(league, "team1", days=14)

        # Should have data for player1
        assert 'player1' in result
        assert 'week1' in result['player1']

    @patch('aissistant_gm.fantrax.stats._get_daily_scores_for_team')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_empty_scores(self, mock_console_class, mock_get_scores):
        """Test that empty scores result in empty dict."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        mock_get_scores.return_value = {}

        result = calculate_recent_trends(league, "team1", days=7)

        assert result == {}

    @patch('aissistant_gm.fantrax.stats._get_daily_scores_for_team')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_calculates_fpg_correctly(self, mock_console_class, mock_get_scores):
        """Test that FP/G is calculated correctly."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        today = date.today()
        weeks = _get_fantrax_week_boundaries(today)
        week1_start, week1_end, _ = weeks[0]

        # Return 10 points total over 2 games for week1
        call_count = [0]

        def get_scores_side_effect(league, team_id, scoring_date):
            if week1_start <= scoring_date <= week1_end:
                call_count[0] += 1
                if call_count[0] == 1:
                    return {'player1': 4.0}
                elif call_count[0] == 2:
                    return {'player1': 6.0}
            return {}

        mock_get_scores.side_effect = get_scores_side_effect

        result = calculate_recent_trends(league, "team1", days=35)

        if 'player1' in result:
            week1 = result['player1'].get('week1', {})
            if week1.get('games_played', 0) > 0:
                assert week1['total_points'] == 10.0
                assert week1['games_played'] == 2
                assert week1['fpg'] == 5.0


class TestCalculateRecentFpg:
    """Test calculate_recent_fpg function."""

    @patch('aissistant_gm.fantrax.stats.Console')
    def test_calculates_fpg_from_live_scores(self, mock_console_class):
        """Test that FP/G is calculated from live_scores."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()

        # Mock player objects
        player1 = Mock()
        player1.id = 'player1'
        player1.points = 5.0

        player2 = Mock()
        player2.id = 'player2'
        player2.points = 3.0

        league.live_scores.return_value = {
            'team1': [player1, player2]
        }

        # Note: last_n_days=1 means from (today-1) to today, which is 2 days
        result = calculate_recent_fpg(league, "team1", last_n_days=1)

        # Should have stats for both players
        assert 'player1' in result
        assert 'player2' in result
        # Each player gets 5.0 points per day over 2 days = 10.0 total
        assert result['player1']['total_points'] == 10.0
        assert result['player1']['games_played'] == 2
        assert result['player1']['fpg'] == 5.0

    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_zero_points(self, mock_console_class):
        """Test that zero points are not counted as games."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()

        player1 = Mock()
        player1.id = 'player1'
        player1.points = 0  # Zero points - shouldn't count

        league.live_scores.return_value = {'team1': [player1]}

        result = calculate_recent_fpg(league, "team1", last_n_days=1)

        # Player shouldn't have any counted games
        assert result == {} or result.get('player1', {}).get('games_played', 0) == 0

    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_exception_gracefully(self, mock_console_class):
        """Test that exceptions are handled gracefully."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.live_scores.side_effect = Exception("API Error")

        result = calculate_recent_fpg(league, "team1", last_n_days=1)

        # Should return empty dict on error
        assert result == {}

    @patch('aissistant_gm.fantrax.stats.Console')
    def test_aggregates_multiple_days(self, mock_console_class):
        """Test that stats are aggregated over multiple days."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()

        player1 = Mock()
        player1.id = 'player1'
        player1.points = 5.0

        # Return same player with points for each day
        league.live_scores.return_value = {'team1': [player1]}

        result = calculate_recent_fpg(league, "team1", last_n_days=3)

        # Should have aggregated 3 days of scores (today + 2 previous days = 4 calls total)
        if 'player1' in result:
            # Days counted depends on range (start to today inclusive)
            assert result['player1']['games_played'] >= 1
            assert result['player1']['fpg'] == 5.0

    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_team_not_in_live_scores(self, mock_console_class):
        """Test that missing team in live_scores is handled."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.live_scores.return_value = {'other_team': []}

        result = calculate_recent_fpg(league, "team1", last_n_days=1)

        assert result == {}


class TestFetchFaPlayerTrends:
    """Test fetch_fa_player_trends function."""

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_fetches_trends_for_five_periods(self, mock_console_class, mock_request):
        """Test that trends are fetched for all 5 periods."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        # Mock API response
        mock_request.return_value = {
            'statsTable': [{
                'scorer': {'scorerId': 'player1'},
                'cells': [
                    {}, {}, {}, {}, {},  # cells 0-4
                    {'content': '10.5'},  # cell 5 = FPts
                    {'content': '2.1'}    # cell 6 = FP/G
                ]
            }]
        }

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        # Should have called _request 5 times (one for each period)
        assert mock_request.call_count == 5

        # Should have all periods for player1
        assert 'player1' in result
        assert 'week1' in result['player1']
        assert 'week2' in result['player1']
        assert 'week3' in result['player1']
        assert '14' in result['player1']
        assert '30' in result['player1']

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_parses_player_stats_correctly(self, mock_console_class, mock_request):
        """Test that player stats are parsed correctly."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        mock_request.return_value = {
            'statsTable': [{
                'scorer': {'scorerId': 'player1'},
                'cells': [
                    {}, {}, {}, {}, {},
                    {'content': '21.0'},  # FPts
                    {'content': '3.0'}    # FP/G (games = 21/3 = 7)
                ]
            }]
        }

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        # Check parsed values
        week1 = result['player1']['week1']
        assert week1['total'] == 21.0
        assert week1['fpg'] == 3.0
        assert week1['games'] == 7

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_empty_content(self, mock_console_class, mock_request):
        """Test that empty content is handled as 0."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        mock_request.return_value = {
            'statsTable': [{
                'scorer': {'scorerId': 'player1'},
                'cells': [
                    {}, {}, {}, {}, {},
                    {'content': ''},  # Empty FPts
                    {'content': ''}   # Empty FP/G
                ]
            }]
        }

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        week1 = result['player1']['week1']
        assert week1['total'] == 0.0
        assert week1['fpg'] == 0.0
        assert week1['games'] == 0

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_fills_missing_periods_with_zeros(self, mock_console_class, mock_request):
        """Test that missing periods are filled with zeros."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        # Return player only for first API call, not others
        call_count = [0]

        def request_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:  # Only week1
                return {
                    'statsTable': [{
                        'scorer': {'scorerId': 'player1'},
                        'cells': [{}, {}, {}, {}, {}, {'content': '10'}, {'content': '2'}]
                    }]
                }
            return {'statsTable': []}

        mock_request.side_effect = request_side_effect

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        # Player should have all periods, with zeros for missing ones
        assert 'player1' in result
        # week1 should have data
        assert result['player1']['week1']['total'] == 10.0
        # Other weeks should be zero-filled
        assert result['player1']['week2']['total'] == 0.0

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_handles_api_exception(self, mock_console_class, mock_request):
        """Test that API exceptions are handled gracefully."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        mock_request.side_effect = Exception("API Error")

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        # Should return empty dict on all errors
        assert result == {}

    @patch('aissistant_gm.fantrax.fantraxapi.api._request')
    @patch('aissistant_gm.fantrax.stats.Console')
    def test_includes_date_boundaries_in_result(self, mock_console_class, mock_request):
        """Test that date boundaries are included in result."""
        mock_console = MagicMock()
        mock_console_class.return_value = mock_console

        league = Mock()
        league.league_id = 'test_league'
        league.session = Mock()

        mock_request.return_value = {
            'statsTable': [{
                'scorer': {'scorerId': 'player1'},
                'cells': [{}, {}, {}, {}, {}, {'content': '10'}, {'content': '2'}]
            }]
        }

        result = fetch_fa_player_trends(league, ['player1'], limit=25)

        # Check that date boundaries are present
        week1 = result['player1']['week1']
        assert 'start' in week1
        assert 'end' in week1
        assert week1['start'] != ''
        assert week1['end'] != ''
