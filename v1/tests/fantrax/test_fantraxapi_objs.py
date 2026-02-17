"""Unit tests for the fantraxapi/objs modules."""

import pytest
from unittest.mock import Mock, MagicMock, PropertyMock
from datetime import date, datetime, time

from aissistant_gm.fantrax.fantraxapi.objs.base import FantraxBaseObject
from aissistant_gm.fantrax.fantraxapi.objs.team import Team
from aissistant_gm.fantrax.fantraxapi.objs.player import Player, LivePlayer
from aissistant_gm.fantrax.fantraxapi.objs.position import Position, PositionCount
from aissistant_gm.fantrax.fantraxapi.objs.status import Status
from aissistant_gm.fantrax.fantraxapi.objs.standings import Standings, Record
from aissistant_gm.fantrax.fantraxapi.objs.game import Game
from aissistant_gm.fantrax.fantraxapi.objs.roster import Roster, RosterRow
from aissistant_gm.fantrax.fantraxapi.objs.trade import Trade, TradeDraftPick, TradePlayer
from aissistant_gm.fantrax.fantraxapi.objs.transaction import Transaction, TransactionPlayer
from aissistant_gm.fantrax.fantraxapi.exceptions import DateNotInSeason


class TestFantraxBaseObject:
    """Tests for the FantraxBaseObject base class."""

    def test_init_stores_league_and_data(self):
        """Test that base object stores league and data."""
        mock_league = Mock()
        data = {"key": "value"}

        obj = FantraxBaseObject(mock_league, data)

        assert obj.league == mock_league
        assert obj._data == data


class TestTeam:
    """Tests for the Team class."""

    def _create_mock_league(self):
        """Create a mock league."""
        league = Mock()
        league.position_counts = Mock(return_value={})
        league.live_scores = Mock(return_value={})
        league.team_roster = Mock()
        return league

    def test_init_basic(self):
        """Test Team initialization with basic data."""
        mock_league = self._create_mock_league()
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo.png"
        }

        team = Team(mock_league, "team-123", data)

        assert team.id == "team-123"
        assert team.name == "Test Team"
        assert team.short == "TT"
        assert team.logo == "http://example.com/logo.png"

    def test_init_with_logo512(self):
        """Test Team initialization prefers 512 logo."""
        mock_league = self._create_mock_league()
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo128.png",
            "logoUrl256": "http://example.com/logo256.png",
            "logoUrl512": "http://example.com/logo512.png"
        }

        team = Team(mock_league, "team-123", data)

        assert team.logo == "http://example.com/logo512.png"

    def test_init_with_logo256(self):
        """Test Team initialization uses 256 logo when 512 not available."""
        mock_league = self._create_mock_league()
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo128.png",
            "logoUrl256": "http://example.com/logo256.png"
        }

        team = Team(mock_league, "team-123", data)

        assert team.logo == "http://example.com/logo256.png"

    def test_str_returns_name(self):
        """Test that __str__ returns team name."""
        mock_league = self._create_mock_league()
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo.png"
        }

        team = Team(mock_league, "team-123", data)

        assert str(team) == "Test Team"

    def test_position_counts_delegates_to_league(self):
        """Test that position_counts delegates to league."""
        mock_league = self._create_mock_league()
        mock_league.position_counts.return_value = {"C": Mock()}
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo.png"
        }

        team = Team(mock_league, "team-123", data)
        team.position_counts(18)

        mock_league.position_counts.assert_called_once_with("team-123", scoring_period_number=18)

    def test_roster_delegates_to_league(self):
        """Test that roster delegates to league."""
        mock_league = self._create_mock_league()
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo.png"
        }

        team = Team(mock_league, "team-123", data)
        team.roster(18)

        mock_league.team_roster.assert_called_once_with(team_id="team-123", period_number=18)

    def test_live_scores_delegates_to_league(self):
        """Test that live_scores delegates to league."""
        mock_league = self._create_mock_league()
        mock_league.live_scores.return_value = {"team-123": []}
        data = {
            "name": "Test Team",
            "shortName": "TT",
            "logoUrl128": "http://example.com/logo.png"
        }

        team = Team(mock_league, "team-123", data)
        test_date = date(2025, 1, 15)
        team.live_scores(test_date)

        mock_league.live_scores.assert_called_once_with(test_date)


class TestPlayer:
    """Tests for the Player class."""

    def _create_mock_league(self):
        """Create a mock league with positions."""
        league = Mock()
        league.positions = {
            "2010": Mock(short="C"),
            "2020": Mock(short="LW"),
            "2030": Mock(short="RW")
        }
        return league

    def test_init_basic(self):
        """Test Player initialization with basic data."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C,LW",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010", "2020"]
        }

        player = Player(mock_league, data)

        assert player.id == "player-123"
        assert player.name == "Test Player"
        assert player.short_name == "T. Player"
        assert player.team_name == "Boston Bruins"
        assert player.team_short_name == "BOS"
        assert player.pos_short_name == "C,LW"
        assert len(player.positions) == 1
        assert len(player.all_positions) == 2

    def test_init_without_team_short_name(self):
        """Test Player uses team_name when teamShortName missing."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }

        player = Player(mock_league, data)

        assert player.team_short_name == "Boston Bruins"

    def test_init_with_day_to_day_icon(self):
        """Test Player initialization with day-to-day injury."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"],
            "icons": [{"typeId": "1"}]
        }

        player = Player(mock_league, data)

        assert player.day_to_day is True
        assert player.injured_reserve is False
        assert player.out is False
        assert player.suspended is False

    def test_init_with_injured_reserve_icon(self):
        """Test Player initialization with IR injury."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"],
            "icons": [{"typeId": "2"}]
        }

        player = Player(mock_league, data)

        assert player.day_to_day is False
        assert player.injured_reserve is True

    def test_init_with_out_icon(self):
        """Test Player initialization with out status."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"],
            "icons": [{"typeId": "30"}]
        }

        player = Player(mock_league, data)

        assert player.out is True

    def test_init_with_suspended_icon(self):
        """Test Player initialization with suspended status."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"],
            "icons": [{"typeId": "6"}]
        }

        player = Player(mock_league, data)

        assert player.suspended is True

    def test_injured_property(self):
        """Test the injured property returns True when any injury flag is set."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"],
            "icons": [{"typeId": "1"}]  # Day-to-day
        }

        player = Player(mock_league, data)

        assert player.injured is True

    def test_injured_property_false(self):
        """Test the injured property returns False when no injury."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }

        player = Player(mock_league, data)

        assert player.injured is False

    def test_str_returns_name(self):
        """Test that __str__ returns player name."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }

        player = Player(mock_league, data)

        assert str(player) == "Test Player"


class TestLivePlayer:
    """Tests for the LivePlayer class."""

    def _create_mock_league(self):
        """Create a mock league with positions."""
        league = Mock()
        league.positions = {
            "2010": Mock(short="C")
        }
        league.team = Mock(return_value=Mock())
        return league

    def test_init(self):
        """Test LivePlayer initialization."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-123",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }
        test_date = date(2025, 1, 15)

        player = LivePlayer(mock_league, data, "team-123", 5.5, test_date)

        assert player.points == 5.5
        assert player.points_date == test_date
        mock_league.team.assert_called_once_with("team-123")


class TestPosition:
    """Tests for the Position class."""

    def test_init(self):
        """Test Position initialization."""
        mock_league = Mock()
        data = {
            "id": "2010",
            "name": "Center",
            "shortName": "C"
        }

        position = Position(mock_league, data)

        assert position.id == "2010"
        assert position.name == "Center"
        assert position.short_name == "C"

    def test_str(self):
        """Test that __str__ returns formatted string."""
        mock_league = Mock()
        data = {
            "id": "2010",
            "name": "Center",
            "shortName": "C"
        }

        position = Position(mock_league, data)

        # __str__ returns "[id:name:short_name]"
        assert "[2010:Center:C]" == str(position)

    def test_eq(self):
        """Test position equality."""
        mock_league = Mock()
        data1 = {"id": "2010", "name": "Center", "shortName": "C"}
        data2 = {"id": "2010", "name": "Center", "shortName": "C"}
        data3 = {"id": "2020", "name": "Left Wing", "shortName": "LW"}

        pos1 = Position(mock_league, data1)
        pos2 = Position(mock_league, data2)
        pos3 = Position(mock_league, data3)

        assert pos1 == pos2
        assert pos1 != pos3


class TestPositionCount:
    """Tests for the PositionCount class."""

    def test_init(self):
        """Test PositionCount initialization."""
        mock_league = Mock()
        data = {
            "pos": "Center",
            "posShort": "C",
            "gp": 10,
            "max": 15,
            "min": 5
        }

        pc = PositionCount(mock_league, data)

        assert pc.gp == 10
        assert pc.max == 15
        assert pc.min == 5
        assert pc.name == "Center"
        assert pc.short_name == "C"

    def test_init_with_none_min_max(self):
        """Test PositionCount with non-integer min/max."""
        mock_league = Mock()
        data = {
            "pos": "Center",
            "posShort": "C",
            "gp": 10,
            "max": "-",  # Non-integer
            "min": "-"   # Non-integer
        }

        pc = PositionCount(mock_league, data)

        assert pc.min is None
        assert pc.max is None

    def test_str(self):
        """Test PositionCount __str__ method."""
        mock_league = Mock()
        data = {
            "pos": "Center",
            "posShort": "C",
            "gp": 10,
            "max": 15,
            "min": 5
        }

        pc = PositionCount(mock_league, data)

        result = str(pc)
        assert "Center" in result
        assert "10" in result


class TestStatus:
    """Tests for the Status class."""

    def test_init(self):
        """Test Status initialization."""
        mock_league = Mock()
        data = {
            "id": "1",
            "code": "ACT",
            "name": "Active",
            "shortName": "A",
            "description": "Player is active"
        }

        status = Status(mock_league, data)

        assert status.id == "1"
        assert status.code == "ACT"
        assert status.name == "Active"
        assert status.short_name == "A"
        assert status.description == "Player is active"

    def test_str(self):
        """Test that __str__ returns formatted string."""
        mock_league = Mock()
        data = {
            "id": "1",
            "code": "ACT",
            "name": "Active",
            "shortName": "A",
            "description": "Player is active"
        }

        status = Status(mock_league, data)

        # __str__ returns "[id:name]"
        assert str(status) == "[1:Active]"

    def test_eq(self):
        """Test status equality."""
        mock_league = Mock()
        data1 = {"id": "1", "code": "ACT", "name": "Active", "shortName": "A", "description": "Active"}
        data2 = {"id": "1", "code": "ACT", "name": "Active", "shortName": "A", "description": "Active"}
        data3 = {"id": "2", "code": "IR", "name": "Injured", "shortName": "IR", "description": "Injured"}

        status1 = Status(mock_league, data1)
        status2 = Status(mock_league, data2)
        status3 = Status(mock_league, data3)

        assert status1 == status2
        assert status1 != status3


class TestStandings:
    """Tests for the Standings class."""

    def _create_mock_league(self):
        """Create a mock league with teams."""
        league = Mock()
        league.team = Mock(return_value=Mock(name="Test Team"))
        return league

    def test_init_basic(self):
        """Test Standings initialization with basic data."""
        mock_league = self._create_mock_league()
        data = {
            "header": {
                "cells": [
                    {"key": "win"},
                    {"key": "loss"},
                    {"key": "tie"},
                    {"key": "points"},
                    {"key": "winpc"},
                    {"key": "gamesback"},
                    {"key": "wwOrder"},
                    {"key": "pointsFor"},
                    {"key": "pointsAgainst"},
                    {"key": "streak"}
                ]
            },
            "rows": [
                {
                    "fixedCells": [{"content": "1"}, {"teamId": "team-1"}],
                    "cells": [
                        {"content": "10"},  # win
                        {"content": "5"},   # loss
                        {"content": "2"},   # tie
                        {"content": "22"},  # points
                        {"content": "0.625"},  # winpc
                        {"content": "0"},   # gamesback
                        {"content": "3"},   # wwOrder
                        {"content": "1,234.5"},  # pointsFor
                        {"content": "1,100.0"},  # pointsAgainst
                        {"content": "W3"}   # streak
                    ]
                }
            ]
        }

        standings = Standings(mock_league, data, scoring_period_number=18)

        assert standings.scoring_period_number == 18
        assert 1 in standings.ranks
        mock_league.team.assert_called_with("team-1")

    def test_init_without_period(self):
        """Test Standings initialization without period number."""
        mock_league = self._create_mock_league()
        data = {
            "header": {"cells": []},
            "rows": []
        }

        standings = Standings(mock_league, data)

        assert standings.scoring_period_number is None

    def test_str_with_period(self):
        """Test __str__ returns formatted string with period."""
        mock_league = self._create_mock_league()
        data = {"header": {"cells": []}, "rows": []}

        standings = Standings(mock_league, data, scoring_period_number=5)

        assert "Standings" in str(standings)
        assert "Period 5" in str(standings)

    def test_str_without_period(self):
        """Test __str__ returns formatted string without period."""
        mock_league = self._create_mock_league()
        data = {"header": {"cells": []}, "rows": []}

        standings = Standings(mock_league, data)

        result = str(standings)
        assert "Standings" in result
        assert "Period" not in result


class TestRecord:
    """Tests for the Record class."""

    def _create_mock_standings(self):
        """Create a mock standings with league."""
        league = Mock()
        league.team = Mock(return_value=Mock(name="Test Team"))
        standings = Mock()
        standings.league = league
        return standings, league

    def test_init_with_all_fields(self):
        """Test Record initialization with all fields."""
        standings, league = self._create_mock_standings()
        fields = {
            "win": 0, "loss": 1, "tie": 2, "points": 3, "winpc": 4,
            "gamesback": 5, "wwOrder": 6, "pointsFor": 7, "pointsAgainst": 8, "streak": 9
        }
        data = [
            {"content": "10"},   # win
            {"content": "5"},    # loss
            {"content": "2"},    # tie
            {"content": "22"},   # points
            {"content": "0.625"},  # winpc
            {"content": "0"},    # gamesback
            {"content": "3"},    # wwOrder
            {"content": "1,234.5"},  # pointsFor
            {"content": "1,100.0"},  # pointsAgainst
            {"content": "W3"}    # streak
        ]

        record = Record(standings, "team-1", 1, fields, data)

        assert record.rank == 1
        assert record.win == 10
        assert record.loss == 5
        assert record.tie == 2
        assert record.points == 22
        assert record.win_percentage == 0.625
        assert record.games_back == 0
        assert record.wavier_wire_order == 3
        assert record.points_for == 1234.5
        assert record.points_against == 1100.0
        assert record.streak == "W3"

    def test_init_with_missing_fields(self):
        """Test Record initialization with missing fields defaults to zeros."""
        standings, league = self._create_mock_standings()
        fields = {}  # No fields
        data = []

        record = Record(standings, "team-1", 1, fields, data)

        assert record.win == 0
        assert record.loss == 0
        assert record.tie == 0
        assert record.points == 0
        assert record.win_percentage == 0.0
        assert record.games_back == 0
        assert record.wavier_wire_order == 0
        assert record.points_for == 0.0
        assert record.points_against == 0.0
        assert record.streak == ""

    def test_winpc_dash_becomes_zero(self):
        """Test that win percentage with dash value becomes 0.0."""
        standings, league = self._create_mock_standings()
        fields = {"winpc": 0}
        data = [{"content": "-"}]

        record = Record(standings, "team-1", 1, fields, data)

        assert record.win_percentage == 0.0

    def test_str(self):
        """Test __str__ returns formatted string."""
        standings, league = self._create_mock_standings()
        mock_team = Mock()
        mock_team.__str__ = Mock(return_value="Test Team")
        league.team.return_value = mock_team
        fields = {"win": 0, "loss": 1, "tie": 2}
        data = [{"content": "10"}, {"content": "5"}, {"content": "2"}]

        record = Record(standings, "team-1", 1, fields, data)

        result = str(record)
        assert "1:" in result
        assert "(10-5-2)" in result


class TestGame:
    """Tests for the Game class."""

    def _create_mock_league(self):
        """Create a mock league with dates."""
        league = Mock()
        league.start_date = datetime(2024, 10, 1)
        league.end_date = datetime(2025, 6, 30)
        return league

    def _create_mock_player(self):
        """Create a mock player."""
        player = Mock()
        player.team_short_name = "BOS"
        return player

    def test_init_home_game_with_time(self):
        """Test Game initialization for home game with start time."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {
            "eventId": "game-123",
            "content": "@TOR\u003cbr/\u003eET 7:00PM"
        }

        game = Game(mock_league, mock_player, "Wed 01/15", data)

        assert game.id == "game-123"
        assert game.opponent == "TOR"
        assert game.home is True
        assert game.away is False
        assert game.time == time(19, 0)
        assert game.date == date(2025, 1, 15)

    def test_init_away_game_with_time(self):
        """Test Game initialization for away game with start time."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {
            "eventId": "game-456",
            "content": "MTL\u003cbr/\u003eET 7:30PM"
        }

        game = Game(mock_league, mock_player, "Thu 01/16", data)

        assert game.id == "game-456"
        assert game.opponent == "MTL"
        assert game.home is False
        assert game.away is True
        assert game.time == time(19, 30)

    def test_init_completed_game_home(self):
        """Test Game initialization for completed home game."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {
            "eventId": "game-789",
            "content": "BOS 4\u003cbr/\u003eTOR 2 F"
        }

        game = Game(mock_league, mock_player, "Fri 01/17", data)

        assert game.opponent == "TOR"
        assert game.home is True
        assert game.away is False
        assert game.time is None

    def test_init_completed_game_away(self):
        """Test Game initialization for completed away game."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {
            "eventId": "game-abc",
            "content": "MTL 3\u003cbr/\u003eBOS 2 F"
        }

        game = Game(mock_league, mock_player, "Sat 01/18", data)

        assert game.opponent == "MTL"
        assert game.home is False
        assert game.away is True

    def test_date_uses_start_year(self):
        """Test that game date uses start year when in range."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {"eventId": "game-1", "content": "@TOR\u003cbr/\u003eET 7:00PM"}

        game = Game(mock_league, mock_player, "Mon 11/15", data)

        assert game.date == date(2024, 11, 15)

    def test_date_uses_end_year(self):
        """Test that game date uses end year when start year is out of range."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {"eventId": "game-2", "content": "@TOR\u003cbr/\u003eET 7:00PM"}

        game = Game(mock_league, mock_player, "Tue 03/15", data)

        assert game.date == date(2025, 3, 15)

    def test_date_not_in_season_raises(self):
        """Test that DateNotInSeason is raised for invalid date."""
        mock_league = Mock()
        mock_league.start_date = datetime(2024, 10, 1)
        mock_league.end_date = datetime(2025, 4, 30)
        mock_player = self._create_mock_player()
        data = {"eventId": "game-3", "content": "@TOR\u003cbr/\u003eET 7:00PM"}

        with pytest.raises(DateNotInSeason):
            Game(mock_league, mock_player, "Mon 08/15", data)

    def test_eq(self):
        """Test game equality based on ID."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data1 = {"eventId": "game-123", "content": "@TOR\u003cbr/\u003eET 7:00PM"}
        data2 = {"eventId": "game-123", "content": "@MTL\u003cbr/\u003eET 7:30PM"}
        data3 = {"eventId": "game-456", "content": "@TOR\u003cbr/\u003eET 7:00PM"}

        game1 = Game(mock_league, mock_player, "Wed 01/15", data1)
        game2 = Game(mock_league, mock_player, "Wed 01/15", data2)
        game3 = Game(mock_league, mock_player, "Wed 01/15", data3)

        assert game1 == game2
        assert game1 != game3

    def test_str_home(self):
        """Test __str__ for home game."""
        mock_league = self._create_mock_league()
        mock_player = self._create_mock_player()
        data = {"eventId": "game-123", "content": "@TOR\u003cbr/\u003eET 7:00PM"}

        game = Game(mock_league, mock_player, "Wed 01/15", data)

        result = str(game)
        assert "game-123" in result
        assert "TOR" in result
        assert "BOS" in result


class TestTransaction:
    """Tests for the Transaction class."""

    def _create_mock_league(self):
        """Create a mock league with positions and teams."""
        league = Mock()
        league.positions = {"2010": Mock(short="C")}
        league.team = Mock(return_value=Mock(name="Test Team"))
        return league

    def test_init(self):
        """Test Transaction initialization."""
        mock_league = self._create_mock_league()
        data = [
            {
                "txSetId": "tx-123",
                "cells": [
                    {"teamId": "team-1"},
                    {"content": "Mon Jan 15, 2025, 2:30PM"}
                ],
                "scorer": {
                    "scorerId": "player-1",
                    "name": "Test Player",
                    "shortName": "T. Player",
                    "teamName": "Boston Bruins",
                    "teamShortName": "BOS",
                    "posShortNames": "C",
                    "posIdsNoFlex": ["2010"],
                    "posIds": ["2010"]
                },
                "transactionCode": "ADD",
                "claimType": None
            }
        ]

        transaction = Transaction(mock_league, data)

        assert transaction.id == "tx-123"
        assert transaction.date == datetime(2025, 1, 15, 14, 30)
        assert len(transaction.players) == 1
        mock_league.team.assert_called_with("team-1")

    def test_init_with_claim(self):
        """Test Transaction initialization with CLAIM type."""
        mock_league = self._create_mock_league()
        data = [
            {
                "txSetId": "tx-456",
                "cells": [
                    {"teamId": "team-2"},
                    {"content": "Tue Feb 20, 2025, 10:00AM"}
                ],
                "scorer": {
                    "scorerId": "player-2",
                    "name": "Another Player",
                    "shortName": "A. Player",
                    "teamName": "Toronto Maple Leafs",
                    "teamShortName": "TOR",
                    "posShortNames": "LW",
                    "posIdsNoFlex": ["2010"],
                    "posIds": ["2010"]
                },
                "transactionCode": "CLAIM",
                "claimType": "WAIVER_CLAIM"
            }
        ]

        transaction = Transaction(mock_league, data)

        assert transaction.players[0].type == "WAIVER_CLAIM"

    def test_str(self):
        """Test Transaction __str__ method."""
        mock_league = self._create_mock_league()
        data = [
            {
                "txSetId": "tx-789",
                "cells": [
                    {"teamId": "team-1"},
                    {"content": "Wed Mar 05, 2025, 3:45PM"}
                ],
                "scorer": {
                    "scorerId": "player-3",
                    "name": "Test Player",
                    "shortName": "T. Player",
                    "teamName": "Boston Bruins",
                    "teamShortName": "BOS",
                    "posShortNames": "C",
                    "posIdsNoFlex": ["2010"],
                    "posIds": ["2010"]
                },
                "transactionCode": "DROP",
                "claimType": None
            }
        ]

        transaction = Transaction(mock_league, data)

        # __str__ returns string representation of players list
        assert str(transaction) is not None


class TestTransactionPlayer:
    """Tests for the TransactionPlayer class."""

    def _create_mock_league(self):
        """Create a mock league with positions."""
        league = Mock()
        league.positions = {"2010": Mock(short="C")}
        return league

    def test_init(self):
        """Test TransactionPlayer initialization."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-1",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }

        player = TransactionPlayer(mock_league, data, "ADD")

        assert player.name == "Test Player"
        assert player.type == "ADD"

    def test_str(self):
        """Test TransactionPlayer __str__ method."""
        mock_league = self._create_mock_league()
        data = {
            "scorerId": "player-1",
            "name": "Test Player",
            "shortName": "T. Player",
            "teamName": "Boston Bruins",
            "teamShortName": "BOS",
            "posShortNames": "C",
            "posIdsNoFlex": ["2010"],
            "posIds": ["2010"]
        }

        player = TransactionPlayer(mock_league, data, "DROP")

        result = str(player)
        assert "DROP" in result
        assert "Test Player" in result


class TestTrade:
    """Tests for the Trade class."""

    def _create_mock_league(self):
        """Create a mock league with dates and teams."""
        league = Mock()
        league.start_date = datetime(2024, 10, 1)
        league.end_date = datetime(2025, 6, 30)
        league.team = Mock(return_value=Mock(name="Test Team"))
        league.positions = {"2010": Mock(short="C")}
        return league

    def test_init_with_player_move(self):
        """Test Trade initialization with player move."""
        mock_league = self._create_mock_league()
        data = {
            "txSetId": "trade-123",
            "creatorTeamId": "team-1",
            "usefulInfo": [
                {"name": "Proposed", "value": "Jan 15, 2:30 PM 2025"},
                {"name": "Accepted", "value": "Jan 16, 10:00 AM 2025"},
                {"name": "To be executed", "value": "Jan 17, 12:00 PM 2025"}
            ],
            "moves": [
                {
                    "from": {"teamId": "team-1"},
                    "to": {"teamId": "team-2"},
                    "scorer": {
                        "scorerId": "player-1",
                        "name": "Test Player",
                        "shortName": "T. Player",
                        "teamName": "Boston Bruins",
                        "teamShortName": "BOS",
                        "posShortNames": "C",
                        "posIdsNoFlex": ["2010"],
                        "posIds": ["2010"]
                    },
                    "scorePerGame": 3.5,
                    "score": 105.0
                }
            ]
        }

        trade = Trade(mock_league, data)

        assert trade.trade_id == "trade-123"
        assert len(trade.moves) == 1
        assert isinstance(trade.moves[0], TradePlayer)

    def test_init_with_draft_pick_move(self):
        """Test Trade initialization with draft pick move."""
        mock_league = self._create_mock_league()
        data = {
            "txSetId": "trade-456",
            "creatorTeamId": "team-1",
            "usefulInfo": [
                {"name": "Proposed", "value": "Feb 10, 3:00 PM 2025"},
                {"name": "Accepted", "value": "Feb 11, 9:00 AM 2025"},
                {"name": "To be executed", "value": "Feb 12, 12:00 PM 2025"}
            ],
            "moves": [
                {
                    "from": {"teamId": "team-1"},
                    "to": {"teamId": "team-2"},
                    "draftPick": {
                        "round": 2,
                        "year": 2025,
                        "origOwnerTeam": {"id": "team-3"}
                    }
                }
            ]
        }

        trade = Trade(mock_league, data)

        assert len(trade.moves) == 1
        assert isinstance(trade.moves[0], TradeDraftPick)

    def test_str(self):
        """Test Trade __str__ method."""
        mock_league = self._create_mock_league()
        data = {
            "txSetId": "trade-789",
            "creatorTeamId": "team-1",
            "usefulInfo": [
                {"name": "Proposed", "value": "Mar 01, 1:00 PM 2025"},
                {"name": "Accepted", "value": "Mar 02, 2:00 PM 2025"},
                {"name": "To be executed", "value": "Mar 03, 12:00 PM 2025"}
            ],
            "moves": []
        }

        trade = Trade(mock_league, data)

        # __str__ joins move strings
        assert str(trade) is not None


class TestTradeDraftPick:
    """Tests for the TradeDraftPick class."""

    def _create_mock_trade(self):
        """Create a mock trade with league."""
        league = Mock()
        league.team = Mock(return_value=Mock(name="Test Team"))
        trade = Mock()
        trade.league = league
        return trade, league

    def test_init(self):
        """Test TradeDraftPick initialization."""
        trade, league = self._create_mock_trade()
        data = {
            "from": {"teamId": "team-1"},
            "to": {"teamId": "team-2"},
            "draftPick": {
                "round": 1,
                "year": 2025,
                "origOwnerTeam": {"id": "team-3"}
            }
        }

        pick = TradeDraftPick(trade, data)

        assert pick.round == 1
        assert pick.year == 2025
        league.team.assert_any_call("team-1")
        league.team.assert_any_call("team-2")
        league.team.assert_any_call("team-3")

    def test_item_description(self):
        """Test TradeDraftPick _item_description method."""
        trade, league = self._create_mock_trade()
        mock_owner = Mock()
        mock_owner.name = "Owner Team"
        league.team.return_value = mock_owner
        data = {
            "from": {"teamId": "team-1"},
            "to": {"teamId": "team-2"},
            "draftPick": {
                "round": 3,
                "year": 2026,
                "origOwnerTeam": {"id": "team-3"}
            }
        }

        pick = TradeDraftPick(trade, data)
        description = pick._item_description()

        assert "2026" in description
        assert "Round 3" in description
        assert "Owner Team" in description


class TestTradePlayer:
    """Tests for the TradePlayer class."""

    def _create_mock_trade(self):
        """Create a mock trade with league."""
        league = Mock()
        league.team = Mock(return_value=Mock(name="Test Team"))
        league.positions = {"2010": Mock(short="C")}
        trade = Mock()
        trade.league = league
        return trade, league

    def test_init(self):
        """Test TradePlayer initialization."""
        trade, league = self._create_mock_trade()
        data = {
            "from": {"teamId": "team-1"},
            "to": {"teamId": "team-2"},
            "scorer": {
                "scorerId": "player-1",
                "name": "Test Player",
                "shortName": "T. Player",
                "teamName": "Boston Bruins",
                "teamShortName": "BOS",
                "posShortNames": "C",
                "posIdsNoFlex": ["2010"],
                "posIds": ["2010"]
            },
            "scorePerGame": 4.5,
            "score": 135.0
        }

        player = TradePlayer(trade, data)

        assert player.player.name == "Test Player"
        assert player.fantasy_points_per_game == 4.5
        assert player.total_fantasy_points == 135.0

    def test_item_description(self):
        """Test TradePlayer _item_description method."""
        trade, league = self._create_mock_trade()
        data = {
            "from": {"teamId": "team-1"},
            "to": {"teamId": "team-2"},
            "scorer": {
                "scorerId": "player-1",
                "name": "Test Player",
                "shortName": "T. Player",
                "teamName": "Boston Bruins",
                "teamShortName": "BOS",
                "posShortNames": "C",
                "posIdsNoFlex": ["2010"],
                "posIds": ["2010"]
            },
            "scorePerGame": 4.5,
            "score": 135.0
        }

        player = TradePlayer(trade, data)
        description = player._item_description()

        assert "Test Player" in description
        assert "C" in description
        assert "4.5" in description


class TestRoster:
    """Tests for the Roster class."""

    def _create_mock_league(self):
        """Create a mock league with required attributes."""
        league = Mock()
        league.team = Mock(return_value=Mock(name="Test Team"))
        league.scoring_dates = {1: date(2025, 1, 15)}
        league.positions = {"2010": Mock(short_name="C")}
        return league

    def test_init_basic(self):
        """Test Roster initialization with basic data."""
        mock_league = self._create_mock_league()
        data = [
            {
                "displayedSelections": {"displayedPeriod": "1"},
                "miscData": {
                    "statusTotals": [
                        {"name": "Active", "total": "10", "max": "12"},
                        {"name": "Reserve", "total": "3", "max": "5"},
                        {"name": "Inj Res", "total": "1", "max": "2"}
                    ]
                },
                "tables": []
            },
            {"tables": []}
        ]

        roster = Roster(mock_league, "team-1", data)

        assert roster.period_number == 1
        assert roster.period_date == date(2025, 1, 15)
        assert roster.active == 10
        assert roster.active_max == 12
        assert roster.reserve == 3
        assert roster.reserve_max == 5
        assert roster.injured == 1
        assert roster.injured_max == 2
        assert roster.rows == []

    def test_init_missing_status_totals(self):
        """Test Roster initialization with missing status types."""
        mock_league = self._create_mock_league()
        data = [
            {
                "displayedSelections": {"displayedPeriod": "1"},
                "miscData": {"statusTotals": []},
                "tables": []
            },
            {"tables": []}
        ]

        roster = Roster(mock_league, "team-1", data)

        assert roster.active == 0
        assert roster.active_max == 0
        assert roster.reserve == 0
        assert roster.reserve_max == 0
        assert roster.injured == 0
        assert roster.injured_max == 0

    def test_init_with_rows(self):
        """Test Roster initialization with roster rows."""
        mock_league = self._create_mock_league()
        data = [
            {
                "displayedSelections": {"displayedPeriod": "1"},
                "miscData": {"statusTotals": []},
                "tables": [
                    {
                        "header": {"cells": []},
                        "rows": [
                            {
                                "posId": "2010",
                                "statusId": "1",
                                "cells": [],
                                "scorer": {
                                    "scorerId": "player-1",
                                    "name": "Test Player",
                                    "shortName": "T. Player",
                                    "teamName": "Boston Bruins",
                                    "teamShortName": "BOS",
                                    "posShortNames": "C",
                                    "posIdsNoFlex": ["2010"],
                                    "posIds": ["2010"]
                                }
                            }
                        ]
                    }
                ]
            },
            {
                "tables": [
                    {
                        "header": {"cells": []},
                        "rows": [{"cells": []}]
                    }
                ]
            }
        ]

        roster = Roster(mock_league, "team-1", data)

        assert len(roster.rows) == 1

    def test_str(self):
        """Test Roster __str__ method."""
        mock_league = self._create_mock_league()
        mock_team = Mock()
        mock_team.__str__ = Mock(return_value="Test Team")
        mock_league.team.return_value = mock_team
        data = [
            {
                "displayedSelections": {"displayedPeriod": "1"},
                "miscData": {"statusTotals": []},
                "tables": []
            },
            {"tables": []}
        ]

        roster = Roster(mock_league, "team-1", data)

        result = str(roster)
        assert "Roster" in result


class TestRosterRow:
    """Tests for the RosterRow class."""

    def _create_mock_roster(self):
        """Create a mock roster with league."""
        league = Mock()
        league.positions = {"2010": Mock(short_name="C")}
        roster = Mock()
        roster.league = league
        roster.period_date = date(2025, 1, 15)
        return roster, league

    def test_init_with_player(self):
        """Test RosterRow initialization with player."""
        roster, league = self._create_mock_roster()
        data = {
            "posId": "2010",
            "statusId": "1",
            "total_fantasy_points": 100.5,
            "fantasy_points_per_game": 3.5,
            "salary": 5000000.0,
            "scorer": {
                "scorerId": "player-1",
                "name": "Test Player",
                "shortName": "T. Player",
                "teamName": "Boston Bruins",
                "teamShortName": "BOS",
                "posShortNames": "C",
                "posIdsNoFlex": ["2010"],
                "posIds": ["2010"]
            },
            "future_games": {}
        }

        row = RosterRow(roster, data)

        assert row.player is not None
        assert row.player.name == "Test Player"
        assert row.status_id == "1"
        assert row.total_fantasy_points == 100.5
        assert row.fantasy_points_per_game == 3.5
        assert row.salary == 5000000.0

    def test_init_without_player(self):
        """Test RosterRow initialization without player (empty slot)."""
        roster, league = self._create_mock_roster()
        data = {
            "posId": "2010",
            "statusId": "1",
            "total_fantasy_points": None,
            "fantasy_points_per_game": None,
            "future_games": {}
        }

        row = RosterRow(roster, data)

        assert row.player is None

    def test_str_with_player(self):
        """Test RosterRow __str__ with player."""
        roster, league = self._create_mock_roster()
        mock_position = Mock()
        mock_position.short_name = "C"
        league.positions = {"2010": mock_position}
        data = {
            "posId": "2010",
            "statusId": "1",
            "total_fantasy_points": None,
            "fantasy_points_per_game": None,
            "scorer": {
                "scorerId": "player-1",
                "name": "Test Player",
                "shortName": "T. Player",
                "teamName": "Boston Bruins",
                "teamShortName": "BOS",
                "posShortNames": "C",
                "posIdsNoFlex": ["2010"],
                "posIds": ["2010"]
            },
            "future_games": {}
        }

        row = RosterRow(roster, data)

        result = str(row)
        assert "C:" in result
        assert "Test Player" in result

    def test_str_without_player(self):
        """Test RosterRow __str__ without player shows Empty."""
        roster, league = self._create_mock_roster()
        mock_position = Mock()
        mock_position.short_name = "C"
        league.positions = {"2010": mock_position}
        data = {
            "posId": "2010",
            "statusId": "1",
            "total_fantasy_points": None,
            "fantasy_points_per_game": None,
            "future_games": {}
        }

        row = RosterRow(roster, data)

        result = str(row)
        assert "Empty" in result
