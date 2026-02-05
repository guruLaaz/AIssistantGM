"""Unit tests for scoring_period.py edge cases.

These tests cover edge cases that were discovered during runtime:
- Missing 'subCaption' in API response
- Period names without trailing numbers (like "Playoffs")
- Missing 'teamId' in matchup cell data
- Non-numeric score values (like "-", "", "N/A")
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from aissistant_gm.fantrax.fantraxapi.objs.scoring_period import (
    ScoringPeriod,
    ScoringPeriodResult,
    Matchup,
    _parse_score,
)
from aissistant_gm.fantrax.fantraxapi import NotTeamInLeague


@pytest.fixture
def mock_league():
    """Create a mock League object with required attributes."""
    league = MagicMock()
    league.league_id = "test_league_123"

    # Mock scoring_periods as a dict-like object
    mock_period = MagicMock()
    mock_period.number = 18
    mock_period.start = date(2024, 1, 15)
    mock_period.end = date(2024, 1, 21)
    league.scoring_periods = {18: mock_period}
    league.scoring_periods_lookup = {
        "2024-01-15 - 2024-01-21": mock_period
    }

    # Mock team lookup
    mock_team = MagicMock()
    mock_team.team_id = "team_123"
    mock_team.name = "Test Team"
    league.team = MagicMock(return_value=mock_team)

    return league


class TestScoringPeriodResult:
    """Tests for ScoringPeriodResult class edge cases."""

    def test_missing_subcaption_uses_fallback_dates(self, mock_league):
        """Test that missing subCaption doesn't cause KeyError."""
        # API response without subCaption field
        data = {
            "caption": "Period 18",
            # "subCaption" is intentionally missing
            "rows": []
        }

        result = ScoringPeriodResult(mock_league, data)

        # Should use today's date as fallback
        assert result.start == datetime.today().date()
        assert result.end == datetime.today().date()
        assert result.name == "Period 18"

    def test_period_name_without_trailing_number_uses_range_lookup(self, mock_league):
        """Test that period names like 'Playoffs Round 1' don't crash."""
        # Period name doesn't end with a number
        data = {
            "caption": "Playoffs Round One",
            "subCaption": "(Mon Jan 15, 2024 - Sun Jan 21, 2024)",
            "rows": []
        }

        result = ScoringPeriodResult(mock_league, data)

        # Should fall back to range lookup
        assert result.name == "Playoffs Round One"
        # Period should be looked up by range
        assert result.period is not None

    def test_period_name_with_trailing_number_extracts_correctly(self, mock_league):
        """Test that 'Period 18' extracts period number 18."""
        data = {
            "caption": "Period 18",
            "subCaption": "(Mon Jan 15, 2024 - Sun Jan 21, 2024)",
            "rows": []
        }

        result = ScoringPeriodResult(mock_league, data)

        assert result.name == "Period 18"
        # Should extract 18 from caption and use it to look up in scoring_periods
        assert result.period == mock_league.scoring_periods[18]

    def test_playoffs_period_uses_range_lookup(self, mock_league):
        """Test that playoff periods use range lookup instead of number extraction."""
        data = {
            "caption": "Playoffs - Week 1",
            "subCaption": "(Mon Jan 15, 2024 - Sun Jan 21, 2024)",
            "rows": []
        }

        result = ScoringPeriodResult(mock_league, data)

        assert result.playoffs is True
        # Should use range lookup for playoffs
        assert result.period is not None

    def test_missing_subcaption_and_non_numeric_caption(self, mock_league):
        """Test handling when both subCaption is missing and caption has no number."""
        # Worst case: no subCaption and no number in caption
        data = {
            "caption": "Special Period",
            "rows": []
        }

        # Use MagicMock for scoring_periods_lookup to allow .get() to return None
        mock_league.scoring_periods_lookup = MagicMock()
        mock_league.scoring_periods_lookup.get.return_value = None

        result = ScoringPeriodResult(mock_league, data)

        # Should not crash, period may be None
        assert result.name == "Special Period"
        assert result.start == datetime.today().date()
        # Period is expected to be None when both lookup methods fail
        assert result.period is None

    def test_period_none_does_not_crash_title_property(self, mock_league):
        """Test that title property handles None period gracefully."""
        data = {
            "caption": "Special Period",
            "rows": []
        }

        # Make all lookups return None
        mock_league.scoring_periods_lookup = MagicMock()
        mock_league.scoring_periods_lookup.get.return_value = None

        result = ScoringPeriodResult(mock_league, data)

        # Period is None
        assert result.period is None
        # Accessing title should not crash - it will raise AttributeError
        # This is expected behavior - consumers should check period is not None
        with pytest.raises(AttributeError):
            _ = result.title


class TestMatchup:
    """Tests for Matchup class edge cases."""

    def test_missing_teamid_in_away_team(self, mock_league):
        """Test that missing teamId in away team data doesn't cause KeyError."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Matchup data without teamId for away team
        cell_data = [
            {"content": "Away Team Name"},  # No teamId
            {"content": "100.5"},
            {"teamId": "home_team_123", "content": "Home Team"},
            {"content": "95.2"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        # Away team should fall back to content
        assert matchup.away == "Away Team Name"
        # Home team should be looked up normally
        mock_league.team.assert_called_with("home_team_123")

    def test_missing_teamid_in_home_team(self, mock_league):
        """Test that missing teamId in home team data doesn't cause KeyError."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Matchup data without teamId for home team
        cell_data = [
            {"teamId": "away_team_123", "content": "Away Team"},
            {"content": "100.5"},
            {"content": "Home Team Name"},  # No teamId
            {"content": "95.2"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        # Away team should be looked up normally
        mock_league.team.assert_called_with("away_team_123")
        # Home team should fall back to content
        assert matchup.home == "Home Team Name"

    def test_missing_teamid_in_both_teams(self, mock_league):
        """Test that missing teamId in both teams doesn't cause KeyError."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Matchup data without teamId for either team
        cell_data = [
            {"content": "Away Team Name"},  # No teamId
            {"content": "100.5"},
            {"content": "Home Team Name"},  # No teamId
            {"content": "95.2"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        # Both teams should fall back to content
        assert matchup.away == "Away Team Name"
        assert matchup.home == "Home Team Name"

    def test_missing_content_field_falls_back_to_unknown(self, mock_league):
        """Test that missing content field uses 'Unknown' fallback."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Matchup data without teamId AND without content
        cell_data = [
            {},  # No teamId, no content
            {"content": "100.5"},
            {"teamId": "home_team_123", "content": "Home Team"},
            {"content": "95.2"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        # Away team should be "Unknown"
        assert matchup.away == "Unknown"

    def test_not_team_in_league_exception_handled(self, mock_league):
        """Test that NotTeamInLeague exception is handled correctly."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Make team() raise NotTeamInLeague
        mock_league.team.side_effect = NotTeamInLeague("Team not found")

        cell_data = [
            {"teamId": "invalid_team", "content": "Fallback Away"},
            {"content": "100.5"},
            {"teamId": "another_invalid", "content": "Fallback Home"},
            {"content": "95.2"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        # Should fall back to content when NotTeamInLeague is raised
        assert matchup.away == "Fallback Away"
        assert matchup.home == "Fallback Home"

    def test_scores_parsed_correctly(self, mock_league):
        """Test that scores are parsed correctly from matchup data."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        cell_data = [
            {"content": "Away Team"},
            {"content": "1,234.56"},  # With comma
            {"content": "Home Team"},
            {"content": "987.65"}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        assert matchup.away_score == 1234.56
        assert matchup.home_score == 987.65


class TestScoringPeriod:
    """Tests for ScoringPeriod class."""

    def test_scoring_period_equality_with_int(self, mock_league):
        """Test ScoringPeriod equality comparison with int."""
        data = {
            "name": "(Jan 15/24 - Jan 21/24)",
            "value": 18
        }

        period = ScoringPeriod(mock_league, data)

        assert period == 18
        assert not period == 17

    def test_scoring_period_equality_with_string(self, mock_league):
        """Test ScoringPeriod equality comparison with numeric string."""
        data = {
            "name": "(Jan 15/24 - Jan 21/24)",
            "value": 18
        }

        period = ScoringPeriod(mock_league, data)

        assert period == "18"
        assert not period == "17"
        assert not period == "not_a_number"

    def test_scoring_period_range_format(self, mock_league):
        """Test ScoringPeriod range property format."""
        data = {
            "name": "(Jan 15/24 - Jan 21/24)",
            "value": 18
        }

        period = ScoringPeriod(mock_league, data)

        assert period.range == "2024-01-15 - 2024-01-21"


class TestParseScore:
    """Tests for the _parse_score helper function."""

    def test_parse_valid_integer(self):
        """Test parsing valid integer."""
        assert _parse_score(100) == Decimal("100")

    def test_parse_valid_float(self):
        """Test parsing valid float."""
        assert _parse_score(100.5) == Decimal("100.5")

    def test_parse_valid_string(self):
        """Test parsing valid numeric string."""
        assert _parse_score("100.5") == Decimal("100.5")

    def test_parse_string_with_comma(self):
        """Test parsing string with comma separator."""
        assert _parse_score("1,234.56") == Decimal("1234.56")

    def test_parse_dash_returns_zero(self):
        """Test that dash returns zero (common for no score)."""
        assert _parse_score("-") == Decimal(0)

    def test_parse_empty_string_returns_zero(self):
        """Test that empty string returns zero."""
        assert _parse_score("") == Decimal(0)

    def test_parse_none_returns_zero(self):
        """Test that None returns zero."""
        assert _parse_score(None) == Decimal(0)

    def test_parse_na_returns_zero(self):
        """Test that 'N/A' returns zero."""
        assert _parse_score("N/A") == Decimal(0)

    def test_parse_text_returns_zero(self):
        """Test that arbitrary text returns zero."""
        assert _parse_score("no score") == Decimal(0)

    def test_parse_negative_number(self):
        """Test parsing negative number."""
        assert _parse_score("-5.5") == Decimal("-5.5")

    def test_parse_zero(self):
        """Test parsing zero."""
        assert _parse_score("0") == Decimal(0)
        assert _parse_score(0) == Decimal(0)


class TestMatchupNonNumericScores:
    """Tests for Matchup class handling non-numeric scores."""

    def test_dash_score_returns_zero(self, mock_league):
        """Test that dash score values return 0."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        # Future matchup with dash scores
        cell_data = [
            {"content": "Away Team"},
            {"content": "-"},  # Dash for no score
            {"content": "Home Team"},
            {"content": "-"}  # Dash for no score
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        assert matchup.away_score == 0.0
        assert matchup.home_score == 0.0

    def test_empty_score_returns_zero(self, mock_league):
        """Test that empty score values return 0."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        cell_data = [
            {"content": "Away Team"},
            {"content": ""},  # Empty string
            {"content": "Home Team"},
            {"content": ""}
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        assert matchup.away_score == 0.0
        assert matchup.home_score == 0.0

    def test_missing_content_score_returns_zero(self, mock_league):
        """Test that missing content field for score returns 0."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        cell_data = [
            {"content": "Away Team"},
            {},  # No content field
            {"content": "Home Team"},
            {}  # No content field
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        assert matchup.away_score == 0.0
        assert matchup.home_score == 0.0

    def test_mixed_valid_and_invalid_scores(self, mock_league):
        """Test matchup with one valid score and one invalid."""
        mock_period = MagicMock()
        mock_period.league = mock_league
        mock_period.title = "Period 18"

        cell_data = [
            {"content": "Away Team"},
            {"content": "100.5"},  # Valid score
            {"content": "Home Team"},
            {"content": "-"}  # Invalid score
        ]

        matchup = Matchup(mock_period, 1, cell_data)

        assert matchup.away_score == 100.5
        assert matchup.home_score == 0.0
