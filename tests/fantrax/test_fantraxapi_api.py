"""Unit tests for the fantraxapi/api.py module."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from datetime import date
from json.decoder import JSONDecodeError

from aissistant_gm.fantrax.fantraxapi.api import (
    Method,
    request,
    _request,
    get_init_info,
    get_pending_transactions,
    get_standings,
    get_trade_blocks,
    get_team_roster_position_counts,
    get_team_roster_info,
    get_transaction_history,
    get_live_scoring_stats,
    get_player_profile,
    get_player_news,
)
from aissistant_gm.fantrax.fantraxapi import FantraxException
from aissistant_gm.fantrax.fantraxapi.exceptions import NotLoggedIn, NotMemberOfLeague


class TestMethod:
    """Tests for the Method class."""

    def test_init_basic(self):
        """Test Method initialization with just a name."""
        method = Method("getTeams")

        assert method.name == "getTeams"
        assert method.kwargs == {}
        assert method.response is None

    def test_init_with_kwargs(self):
        """Test Method initialization with kwargs."""
        method = Method("getTeamRosterInfo", teamId="team123", view="STATS")

        assert method.name == "getTeamRosterInfo"
        assert method.kwargs == {"teamId": "team123", "view": "STATS"}

    def test_msg_block_basic(self):
        """Test msg_block generation with basic data."""
        method = Method("getStandings")
        block = method.msg_block("league-123")

        assert block == {
            "method": "getStandings",
            "data": {"leagueId": "league-123"}
        }

    def test_msg_block_with_string_kwargs(self):
        """Test msg_block generation with string kwargs."""
        method = Method("getTeamRosterInfo", teamId="team-456", view="STATS")
        block = method.msg_block("league-123")

        assert block == {
            "method": "getTeamRosterInfo",
            "data": {
                "leagueId": "league-123",
                "teamId": "team-456",
                "view": "STATS"
            }
        }

    def test_msg_block_with_date_kwarg(self):
        """Test msg_block generation with date kwarg."""
        test_date = date(2025, 1, 15)
        method = Method("getLiveScoringStats", date=test_date)
        block = method.msg_block("league-123")

        assert block == {
            "method": "getLiveScoringStats",
            "data": {
                "leagueId": "league-123",
                "date": "2025-01-15"
            }
        }

    def test_msg_block_with_none_kwarg(self):
        """Test msg_block generation skips None kwargs."""
        method = Method("getTeamRosterInfo", teamId="team-123", period=None)
        block = method.msg_block("league-123")

        # None value should be excluded
        assert "period" not in block["data"]
        assert block == {
            "method": "getTeamRosterInfo",
            "data": {
                "leagueId": "league-123",
                "teamId": "team-123"
            }
        }

    def test_msg_block_with_integer_kwarg(self):
        """Test msg_block generation converts int to string."""
        method = Method("getTeamRosterInfo", scoringPeriod=18)
        block = method.msg_block("league-123")

        assert block["data"]["scoringPeriod"] == "18"


class TestRequest:
    """Tests for the request() wrapper function."""

    def test_request_delegates_to_internal(self):
        """Test that request() delegates to _request()."""
        mock_league = Mock()
        mock_league.league_id = "test-league-123"
        mock_league.session = Mock()

        with patch('aissistant_gm.fantrax.fantraxapi.api._request') as mock_internal:
            mock_internal.return_value = {"data": "test"}

            result = request(mock_league, Method("getTeams"))

            mock_internal.assert_called_once()
            call_args = mock_internal.call_args
            assert call_args[0][0] == "test-league-123"
            assert call_args.kwargs['session'] == mock_league.session


class TestInternalRequest:
    """Tests for the _request() function."""

    def _create_mock_response(self, data, status_code=200, reason="OK"):
        """Create a mock requests Response."""
        response = Mock()
        response.status_code = status_code
        response.reason = reason
        response.json.return_value = {"responses": [{"data": data}]}
        return response

    def test_single_method_returns_dict(self):
        """Test that single method returns dict, not list."""
        mock_session = Mock()
        mock_session.post.return_value = self._create_mock_response({"teams": []})

        result = _request("league-123", Method("getTeams"), session=mock_session)

        assert isinstance(result, dict)
        assert result == {"teams": []}

    def test_multiple_methods_returns_list(self):
        """Test that multiple methods returns list."""
        mock_session = Mock()
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "responses": [
                {"data": {"teams": []}},
                {"data": {"standings": []}}
            ]
        }
        mock_session.post.return_value = response

        methods = [Method("getTeams"), Method("getStandings")]
        result = _request("league-123", methods, session=mock_session)

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0] == {"teams": []}
        assert result[1] == {"standings": []}

    def test_uses_default_session_if_none_provided(self):
        """Test that default session is used if none provided."""
        with patch('aissistant_gm.fantrax.fantraxapi.api.default_session') as mock_default:
            mock_default.post.return_value = self._create_mock_response({})

            _request("league-123", Method("getTeams"))

            mock_default.post.assert_called_once()

    def test_sends_correct_json_payload(self):
        """Test that correct JSON payload is sent."""
        mock_session = Mock()
        mock_session.post.return_value = self._create_mock_response({})

        _request("league-123", Method("getTeams"), session=mock_session)

        call_args = mock_session.post.call_args
        assert call_args[1]["json"] == {
            "msgs": [{"method": "getTeams", "data": {"leagueId": "league-123"}}]
        }
        assert call_args[1]["params"] == {"leagueId": "league-123"}

    def test_json_decode_error_raises_fantrax_exception(self):
        """Test that JSON decode error raises FantraxException."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.json.side_effect = JSONDecodeError("test", "doc", 0)
        mock_session.post.return_value = mock_response

        with pytest.raises(FantraxException) as exc_info:
            _request("league-123", Method("getTeams"), session=mock_session)

        assert "Invalid JSON Response" in str(exc_info.value)

    def test_http_error_raises_fantrax_exception(self):
        """Test that HTTP error status raises FantraxException."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.reason = "Internal Server Error"
        mock_response.json.return_value = {"error": "Something went wrong"}
        mock_session.post.return_value = mock_response

        with pytest.raises(FantraxException) as exc_info:
            _request("league-123", Method("getTeams"), session=mock_session)

        assert "500" in str(exc_info.value)

    def test_not_logged_in_error(self):
        """Test that NOT_LOGGED_IN error raises NotLoggedIn."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pageError": {"code": "WARNING_NOT_LOGGED_IN"}
        }
        mock_session.post.return_value = mock_response

        with pytest.raises(NotLoggedIn):
            _request("league-123", Method("getTeams"), session=mock_session)

    def test_not_member_of_league_error(self):
        """Test that NOT_MEMBER_OF_LEAGUE error raises NotMemberOfLeague."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pageError": {"code": "NOT_MEMBER_OF_LEAGUE"}
        }
        mock_session.post.return_value = mock_response

        with pytest.raises(NotMemberOfLeague):
            _request("league-123", Method("getTeams"), session=mock_session)

    def test_unexpected_error(self):
        """Test that UNEXPECTED_ERROR raises FantraxException with title."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pageError": {
                "code": "UNEXPECTED_ERROR",
                "title": "An unexpected error occurred"
            }
        }
        mock_session.post.return_value = mock_response

        with pytest.raises(FantraxException) as exc_info:
            _request("league-123", Method("getTeams"), session=mock_session)

        assert "An unexpected error occurred" in str(exc_info.value)

    def test_unknown_error_code(self):
        """Test that unknown error code raises FantraxException."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pageError": {"code": "UNKNOWN_ERROR_CODE"}
        }
        mock_session.post.return_value = mock_response

        with pytest.raises(FantraxException):
            _request("league-123", Method("getTeams"), session=mock_session)


class TestAPIHelperFunctions:
    """Tests for API helper functions."""

    def _create_mock_league(self):
        """Create a mock league object."""
        league = Mock()
        league.league_id = "test-league-123"
        league.session = Mock()
        league._update_teams = Mock()
        return league

    def test_get_init_info(self):
        """Test get_init_info sends correct methods."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = [
                {}, {}, {}, {}, {}  # 5 responses
            ]

            get_init_info(mock_league)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            methods = call_args[0][1]
            assert len(methods) == 5
            method_names = [m.name for m in methods]
            assert "getFantasyLeagueInfo" in method_names
            assert "getRefObject" in method_names
            assert "getLiveScoringStats" in method_names

    def test_get_pending_transactions(self):
        """Test get_pending_transactions sends correct method."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"transactions": []}

            get_pending_transactions(mock_league)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getPendingTransactions"

    def test_get_standings_single_view(self):
        """Test get_standings with single view."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"standings": []}

            get_standings(mock_league, views="STANDINGS")

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            methods = call_args[0][1]
            assert len(methods) == 1
            assert methods[0].name == "getStandings"

    def test_get_standings_multiple_views(self):
        """Test get_standings with multiple views."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = [{"standings": []}, {"stats": []}]

            get_standings(mock_league, views=["STANDINGS", "STATS"])

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            methods = call_args[0][1]
            assert len(methods) == 2

    def test_get_standings_updates_teams(self):
        """Test get_standings calls _update_teams when fantasyTeamInfo present."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"fantasyTeamInfo": [{"id": "team1"}]}

            get_standings(mock_league, views="STANDINGS")

            mock_league._update_teams.assert_called_once_with([{"id": "team1"}])

    def test_get_trade_blocks(self):
        """Test get_trade_blocks extracts tradeBlocks."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"tradeBlocks": [{"id": "block1"}]}

            result = get_trade_blocks(mock_league)

            assert result == [{"id": "block1"}]

    def test_get_team_roster_position_counts(self):
        """Test get_team_roster_position_counts sends correct method."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"fantasyTeams": []}

            get_team_roster_position_counts(mock_league, "team-123", 18)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getTeamRosterInfo"
            assert method.kwargs["teamId"] == "team-123"
            assert method.kwargs["scoringPeriod"] == 18
            assert method.kwargs["view"] == "GAMES_PER_POS"

    def test_get_team_roster_info(self):
        """Test get_team_roster_info sends two methods."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = [
                {"fantasyTeams": []},
                {"schedule": []}
            ]

            get_team_roster_info(mock_league, "team-123", 18)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            methods = call_args[0][1]
            assert len(methods) == 2
            views = [m.kwargs["view"] for m in methods]
            assert "STATS" in views
            assert "SCHEDULE_FULL" in views

    def test_get_transaction_history(self):
        """Test get_transaction_history sends correct method."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"transactions": []}

            get_transaction_history(mock_league, per_page_results=50)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getTransactionDetailsHistory"
            assert method.kwargs["maxResultsPerPage"] == "50"

    def test_get_live_scoring_stats(self):
        """Test get_live_scoring_stats sends correct method."""
        mock_league = self._create_mock_league()
        test_date = date(2025, 1, 15)

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"stats": []}

            get_live_scoring_stats(mock_league, scoring_date=test_date)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getLiveScoringStats"
            assert method.kwargs["date"] == test_date
            assert method.kwargs["newView"] is True

    def test_get_live_scoring_stats_no_date(self):
        """Test get_live_scoring_stats without date."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"stats": []}

            get_live_scoring_stats(mock_league, scoring_date=None)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.kwargs["date"] is None

    def test_get_player_profile(self):
        """Test get_player_profile sends correct method."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"profile": {}}

            get_player_profile(mock_league, "player-123", "team-456")

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getPlayerProfile"
            assert method.kwargs["playerId"] == "player-123"
            assert method.kwargs["teamId"] == "team-456"

    def test_get_player_news(self):
        """Test get_player_news sends correct method."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"stories": []}

            get_player_news(mock_league, pool_type="ALL")

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.name == "getPlayerNews"
            assert method.kwargs["poolType"] == "ALL"

    def test_get_player_news_default_pool_type(self):
        """Test get_player_news uses default pool_type."""
        mock_league = self._create_mock_league()

        with patch('aissistant_gm.fantrax.fantraxapi.api.request') as mock_request:
            mock_request.return_value = {"stories": []}

            get_player_news(mock_league)

            mock_request.assert_called_once()
            call_args = mock_request.call_args
            method = call_args[0][1]
            assert method.kwargs["poolType"] == "ALL"
