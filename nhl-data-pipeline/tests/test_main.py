"""Tests for assistant/main.py — CLI entry point."""

import os
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from db.schema import init_db, get_db, upsert_player
from assistant.tools import SessionContext
from assistant.client import AssistantClient


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db_with_teams(db_path: Path) -> sqlite3.Connection:
    """DB with fantasy teams for selection."""
    init_db(db_path)
    conn = get_db(db_path)
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team1', 'lg1', 'Alpha Team', 'AT')"
    )
    conn.execute(
        "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
        "VALUES ('team2', 'lg1', 'Beta Team', 'BT')"
    )
    conn.commit()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# select_team
# ---------------------------------------------------------------------------


class TestSelectTeam:
    """Tests for the select_team function."""

    def test_displays_teams(self, db_with_teams: sqlite3.Connection) -> None:
        from assistant.main import select_team
        with patch("builtins.input", return_value="1"):
            team_id, team_name = select_team(db_with_teams)
            assert team_id == "team1"
            assert team_name == "Alpha Team"

    def test_second_team(self, db_with_teams: sqlite3.Connection) -> None:
        from assistant.main import select_team
        with patch("builtins.input", return_value="2"):
            team_id, team_name = select_team(db_with_teams)
            assert team_id == "team2"
            assert team_name == "Beta Team"

    def test_invalid_then_valid(self, db_with_teams: sqlite3.Connection) -> None:
        from assistant.main import select_team
        with patch("builtins.input", side_effect=["0", "abc", "3", "1"]):
            team_id, team_name = select_team(db_with_teams)
            assert team_id == "team1"

    def test_no_teams_exits(self, db_path: Path) -> None:
        """Empty team list causes sys.exit."""
        init_db(db_path)
        conn = get_db(db_path)
        from assistant.main import select_team
        with pytest.raises(SystemExit):
            select_team(conn)

    def test_eof_exits(self, db_with_teams: sqlite3.Connection) -> None:
        from assistant.main import select_team
        with patch("builtins.input", side_effect=EOFError):
            with pytest.raises(SystemExit):
                select_team(db_with_teams)

    def test_keyboard_interrupt_exits(self, db_with_teams: sqlite3.Connection) -> None:
        from assistant.main import select_team
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            with pytest.raises(SystemExit):
                select_team(db_with_teams)


# ---------------------------------------------------------------------------
# main — argument parsing
# ---------------------------------------------------------------------------


class TestMainArgParsing:
    """Tests for main() argument parsing and pipeline flag."""

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_default_no_pipeline(self, mock_dotenv, mock_client_cls,
                                  mock_select, mock_get_db) -> None:
        """Default invocation doesn't run the pipeline."""
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("team1", "My Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        # Simulate user typing "quit" immediately
        with patch("builtins.input", return_value="quit"):
            with patch("sys.argv", ["main.py"]):
                main()

        mock_conn.close.assert_called_once()

    @patch("assistant.main.subprocess.run")
    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_run_pipeline_first_flag(self, mock_dotenv, mock_client_cls,
                                      mock_select, mock_get_db, mock_subproc) -> None:
        """--run-pipeline-first triggers subprocess.run."""
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("team1", "My Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_subproc.return_value = MagicMock(returncode=0)

        with patch("builtins.input", return_value="quit"):
            with patch("sys.argv", ["main.py", "--run-pipeline-first"]):
                main()

        mock_subproc.assert_called_once()

    @patch("assistant.main.subprocess.run")
    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_pipeline_failure_continues(self, mock_dotenv, mock_client_cls,
                                         mock_select, mock_get_db, mock_subproc) -> None:
        """Pipeline failure prints warning but continues."""
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("team1", "My Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_subproc.return_value = MagicMock(returncode=1)

        with patch("builtins.input", return_value="quit"):
            with patch("sys.argv", ["main.py", "--run-pipeline-first"]):
                main()

        # Should still reach conn.close even after pipeline failure
        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# main — chat loop
# ---------------------------------------------------------------------------


class TestChatLoop:
    """Tests for the interactive chat loop."""

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_exit_command(self, mock_dotenv, mock_client_cls,
                          mock_select, mock_get_db) -> None:
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        with patch("builtins.input", return_value="exit"):
            with patch("sys.argv", ["main.py"]):
                main()

        mock_conn.close.assert_called_once()

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_empty_input_continues(self, mock_dotenv, mock_client_cls,
                                    mock_select, mock_get_db) -> None:
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        with patch("builtins.input", side_effect=["", "quit"]):
            with patch("sys.argv", ["main.py"]):
                main()

        # chat should not be called for empty input
        mock_client.chat.assert_not_called()

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_chat_called_for_input(self, mock_dotenv, mock_client_cls,
                                    mock_select, mock_get_db) -> None:
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client = MagicMock()
        mock_client.chat.return_value = "Response"
        mock_client_cls.return_value = mock_client

        with patch("builtins.input", side_effect=["Show my roster", "quit"]):
            with patch("sys.argv", ["main.py"]):
                main()

        mock_client.chat.assert_called_once_with("Show my roster")

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_keyboard_interrupt_in_chat(self, mock_dotenv, mock_client_cls,
                                         mock_select, mock_get_db) -> None:
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        with patch("builtins.input", side_effect=KeyboardInterrupt):
            with patch("sys.argv", ["main.py"]):
                main()

        mock_conn.close.assert_called_once()

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_chat_error_continues(self, mock_dotenv, mock_client_cls,
                                   mock_select, mock_get_db) -> None:
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client = MagicMock()
        mock_client.chat.side_effect = [Exception("API error"), "ok"]
        mock_client_cls.return_value = mock_client

        with patch("builtins.input", side_effect=["q1", "q2", "quit"]):
            with patch("sys.argv", ["main.py"]):
                main()

        # Should have called chat twice (error then success), then quit
        assert mock_client.chat.call_count == 2

    @patch("assistant.main.get_db")
    @patch("assistant.main.select_team")
    @patch("assistant.main.AssistantClient")
    @patch("assistant.main.load_dotenv")
    def test_client_init_error_exits(self, mock_dotenv, mock_client_cls,
                                      mock_select, mock_get_db) -> None:
        """RuntimeError during client init exits gracefully."""
        from assistant.main import main

        mock_conn = MagicMock()
        mock_get_db.return_value = mock_conn
        mock_select.return_value = ("t1", "Team")
        mock_client_cls.side_effect = RuntimeError("No API key")

        with patch("sys.argv", ["main.py"]):
            with pytest.raises(SystemExit):
                main()


# ---------------------------------------------------------------------------
# E2E integration — real DB → queries → formatters → tool dispatch
# ---------------------------------------------------------------------------


class _MockContentBlock:
    """Simulates an Anthropic SDK content block.

    Only sets attributes that the real SDK block type would have:
    - TextBlock has: type, text
    - ToolUseBlock has: type, id, name, input
    """

    def __init__(self, type, text=None, name=None, input=None, id=None):
        self.type = type
        if type == "text":
            self.text = text
        elif type == "tool_use":
            self.id = id
            self.name = name
            self.input = input or {}


class _MockResponse:
    """Simulates an Anthropic SDK messages.create() response."""

    def __init__(self, content, stop_reason="end_turn"):
        self.content = content
        self.stop_reason = stop_reason


def _make_stream_cm(response):
    """Create a mock context manager that mimics messages.stream()."""
    stream = MagicMock()
    stream.get_final_message.return_value = response
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=stream)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


class TestE2EIntegration:
    """End-to-end tests: real DB → real queries → real formatters → tool dispatch.

    Only the Claude API client is mocked.  Everything else runs against a real
    SQLite database with test data.
    """

    @pytest.fixture
    def e2e_db(self, tmp_path):
        """Real DB with enough data for full integration testing."""
        db_path = tmp_path / "e2e.db"
        init_db(db_path)
        conn = get_db(db_path)

        # Players
        upsert_player(conn, {
            "id": 8478402, "full_name": "Connor McDavid",
            "first_name": "Connor", "last_name": "McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        upsert_player(conn, {
            "id": 8471675, "full_name": "Sidney Crosby",
            "first_name": "Sidney", "last_name": "Crosby",
            "team_abbrev": "PIT", "position": "C",
        })
        upsert_player(conn, {
            "id": 8477424, "full_name": "Juuse Saros",
            "first_name": "Juuse", "last_name": "Saros",
            "team_abbrev": "NSH", "position": "G",
        })

        # Season totals
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8478402, NULL, '20252026', 1, 30, 40, 70, 150, 75, 200, 15, 10, 72000)"
        )
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8471675, NULL, '20252026', 1, 20, 30, 50, 75, 45, 150, 10, 8, 60000)"
        )

        # Per-game rows
        for i in range(15):
            gd = f"2025-10-{(10 + i):02d}"
            conn.execute(
                f"INSERT INTO skater_stats "
                f"(player_id, game_date, season, is_season_total, goals, assists, points, "
                f"hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8478402, '{gd}', '20252026', 0, 2, 3, 5, 10, 5, 14, 1, 0, 1200)"
            )
            conn.execute(
                f"INSERT INTO skater_stats "
                f"(player_id, game_date, season, is_season_total, goals, assists, points, "
                f"hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8471675, '{gd}', '20252026', 0, 1, 2, 3, 5, 3, 10, 1, 0, 1100)"
            )

        # Goalie
        conn.execute(
            "INSERT INTO goalie_stats "
            "(player_id, game_date, season, is_season_total, "
            "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            "VALUES (8477424, NULL, '20252026', 1, 20, 10, 5, 3, 1500, 80, 1580, 108000)"
        )
        for i in range(10):
            gd = f"2025-10-{(10 + i):02d}"
            w = 1 if i % 3 != 2 else 0
            conn.execute(
                f"INSERT INTO goalie_stats "
                f"(player_id, game_date, season, is_season_total, "
                f"wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
                f"VALUES (8477424, '{gd}', '20252026', 0, {w}, {1 - w}, 0, 0, 30, 2, 32, 3600)"
            )

        # Fantasy setup
        conn.execute(
            "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
            "VALUES ('team1', 'lg1', 'My Team', 'MT')"
        )
        conn.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Connor McDavid', 'C', 'active', 12500000)"
        )
        conn.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Juuse Saros', 'G', 'active', 5000000)"
        )
        conn.execute(
            "INSERT INTO fantasy_standings "
            "(league_id, team_id, rank, wins, losses, points, "
            "points_for, points_against, streak, games_played, fantasy_points_per_game) "
            "VALUES ('lg1', 'team1', 1, 50, 20, 100, 5000.5, 4200.0, 'W3', 70, 71.4)"
        )

        # Injuries
        conn.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (8471675, 'rotowire', 'Upper Body', 'Day-to-Day', '2026-02-18')"
        )

        # News
        conn.execute(
            "INSERT INTO player_news (rotowire_news_id, player_id, headline, content, published_at) "
            "VALUES ('news001', 8478402, 'McDavid: Hat Trick', 'Scored three goals.', '2026-02-18')"
        )

        # Team games
        conn.execute(
            "INSERT INTO team_games (team, season, game_date, opponent, home_away) "
            "VALUES ('EDM', '20252026', '2026-02-25', 'CGY', 'home')"
        )

        conn.commit()
        yield conn
        conn.close()

    @pytest.fixture
    def e2e_ctx(self, e2e_db):
        return SessionContext(conn=e2e_db, team_id="team1", season="20252026")

    def _tool_response(self, tool_name, tool_input, tool_id="tool_01"):
        """Create a mock API response containing a tool_use block."""
        return _MockResponse(
            content=[_MockContentBlock(
                type="tool_use", name=tool_name,
                input=tool_input, id=tool_id,
            )],
            stop_reason="tool_use",
        )

    def _text_response(self, text):
        """Create a mock API response containing a text block."""
        return _MockResponse(
            content=[_MockContentBlock(type="text", text=text)],
            stop_reason="end_turn",
        )

    def _tool_result_content(self, mock_api, result_index=0):
        """Extract the Nth tool_result content from the conversation.

        The mock stores a *reference* to self.messages (not a snapshot), so
        we search the final list state for tool_result entries.
        """
        messages = mock_api.messages.stream.call_args.kwargs["messages"]
        found = 0
        for msg in messages:
            content = msg.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    if found == result_index:
                        return item["content"]
                    found += 1
        raise AssertionError(f"Tool result #{result_index} not found")

    # -- roster --------------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_roster_e2e(self, mock_cls, e2e_ctx):
        """get_my_roster → real query/format → tool result has real data."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_my_roster", {})),
            _make_stream_cm(self._text_response("Here's your roster!")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Show my roster")

        content = self._tool_result_content(mock_api)
        assert "Connor McDavid" in content
        assert "Juuse Saros" in content

    # -- player stats --------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_player_stats_e2e(self, mock_cls, e2e_ctx):
        """get_player_stats → real lookup → formatted card returned."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_player_stats", {"player_name": "Connor McDavid"})),
            _make_stream_cm(self._text_response("McDavid is on fire.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("How is Connor McDavid doing?")

        content = self._tool_result_content(mock_api)
        assert "Connor McDavid" in content
        assert "EDM" in content
        assert "30" in content  # 30 goals

    # -- player not found ----------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_player_not_found_e2e(self, mock_cls, e2e_ctx):
        """get_player_stats for unknown player → 'not found' in tool result."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_player_stats", {"player_name": "Wayne Gretzky"})),
            _make_stream_cm(self._text_response("I couldn't find Wayne Gretzky.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Show me Wayne Gretzky stats")

        content = self._tool_result_content(mock_api)
        assert "not found" in content.lower()

    # -- compare players -----------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_compare_players_e2e(self, mock_cls, e2e_ctx):
        """compare_players → side-by-side data from real DB."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("compare_players", {
                "player_names": ["Connor McDavid", "Sidney Crosby"],
            })),
            _make_stream_cm(self._text_response("McDavid has the edge.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Compare McDavid and Crosby")

        content = self._tool_result_content(mock_api)
        assert "Connor McDavid" in content
        assert "Sidney Crosby" in content

    # -- injuries ------------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_injuries_e2e(self, mock_cls, e2e_ctx):
        """get_injuries scope=all → real injury data."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_injuries", {"scope": "all"})),
            _make_stream_cm(self._text_response("Crosby is day-to-day.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Any injuries?")

        content = self._tool_result_content(mock_api)
        assert "Sidney Crosby" in content
        assert "Upper Body" in content

    # -- standings -----------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_standings_e2e(self, mock_cls, e2e_ctx):
        """get_league_standings → real standings data."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_league_standings", {})),
            _make_stream_cm(self._text_response("You're in first!")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("What are the standings?")

        content = self._tool_result_content(mock_api)
        assert "My Team" in content

    # -- news ----------------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_news_e2e(self, mock_cls, e2e_ctx):
        """get_news_briefing for a specific player → real news data."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_news_briefing", {"player_name": "Connor McDavid"})),
            _make_stream_cm(self._text_response("McDavid had a hat trick!")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Any news about McDavid?")

        content = self._tool_result_content(mock_api)
        assert "Hat Trick" in content

    # -- schedule ------------------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_schedule_e2e(self, mock_cls, e2e_ctx):
        """get_schedule_analysis → real schedule data."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_schedule_analysis", {
                "team_or_player": "EDM", "days_ahead": 30,
            })),
            _make_stream_cm(self._text_response("Edmonton has games coming up.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("What's EDM schedule like?")

        content = self._tool_result_content(mock_api)
        assert "EDM" in content

    # -- tool dispatch error -------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_tool_error_e2e(self, mock_cls, e2e_ctx):
        """Missing required param → dispatch error is sent back gracefully."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_player_stats", {})),
            _make_stream_cm(self._text_response("Sorry, I need a player name.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Show me player stats")

        content = self._tool_result_content(mock_api)
        assert "Error executing" in content

    # -- multi-turn tool use -------------------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_multi_tool_e2e(self, mock_cls, e2e_ctx):
        """Claude calls two tools in sequence before final response."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_my_roster", {}, "tool_01")),
            _make_stream_cm(self._tool_response("get_injuries", {"scope": "my_roster"}, "tool_02")),
            _make_stream_cm(self._text_response("Your roster looks solid. No injuries.")),
        ]

        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        response = client.chat("Give me a full team update")

        assert mock_api.messages.stream.call_count == 3
        # First tool result had roster data
        roster_content = self._tool_result_content(mock_api, result_index=0)
        assert "Connor McDavid" in roster_content

    # -- conversation history preserved --------------------------------

    @patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"})
    @patch("assistant.client.anthropic.Anthropic")
    def test_conversation_history_e2e(self, mock_cls, e2e_ctx):
        """Messages accumulate correctly across multiple chat turns."""
        mock_api = MagicMock()
        mock_cls.return_value = mock_api

        # Turn 1: simple text response (no tool)
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._text_response("I'm your fantasy hockey assistant!")),
        ]
        client = AssistantClient(context=e2e_ctx, team_name="My Team")
        client.chat("Hello")

        # Turn 2: one tool call
        mock_api.messages.stream.side_effect = [
            _make_stream_cm(self._tool_response("get_my_roster", {})),
            _make_stream_cm(self._text_response("Here's your roster!")),
        ]
        client.chat("Show my roster")

        # Turn 1: user + assistant = 2 messages
        # Turn 2: user + assistant(tool_use) + user(tool_result) + assistant(text) = 4
        assert len(client.messages) == 6


# ---------------------------------------------------------------------------
# Live API tests — real Anthropic API, real DB, no mocking
# ---------------------------------------------------------------------------


class TestLiveAPI:
    """E2E tests using the real Anthropic API.

    Run with: pytest --integration tests/test_main.py::TestLiveAPI -v
    Requires: ANTHROPIC_API_KEY environment variable.
    Uses claude-haiku for speed and cost efficiency.
    """

    @pytest.fixture(autouse=True)
    def _require_api_key(self):
        if not os.environ.get("ANTHROPIC_API_KEY"):
            pytest.skip("ANTHROPIC_API_KEY not set")

    @pytest.fixture
    def live_db(self, tmp_path):
        """Real DB with test data for live API tests."""
        db_path = tmp_path / "live.db"
        init_db(db_path)
        conn = get_db(db_path)

        upsert_player(conn, {
            "id": 8478402, "full_name": "Connor McDavid",
            "first_name": "Connor", "last_name": "McDavid",
            "team_abbrev": "EDM", "position": "C",
        })
        upsert_player(conn, {
            "id": 8477424, "full_name": "Juuse Saros",
            "first_name": "Juuse", "last_name": "Saros",
            "team_abbrev": "NSH", "position": "G",
        })

        # Season totals
        conn.execute(
            "INSERT INTO skater_stats "
            "(player_id, game_date, season, is_season_total, goals, assists, points, "
            "hits, blocks, shots, plus_minus, pim, toi) "
            "VALUES (8478402, NULL, '20252026', 1, 30, 40, 70, 150, 75, 200, 15, 10, 72000)"
        )

        # Per-game rows
        for i in range(15):
            gd = f"2025-10-{(10 + i):02d}"
            conn.execute(
                f"INSERT INTO skater_stats "
                f"(player_id, game_date, season, is_season_total, goals, assists, points, "
                f"hits, blocks, shots, plus_minus, pim, toi) "
                f"VALUES (8478402, '{gd}', '20252026', 0, 2, 3, 5, 10, 5, 14, 1, 0, 1200)"
            )

        # Goalie
        conn.execute(
            "INSERT INTO goalie_stats "
            "(player_id, game_date, season, is_season_total, "
            "wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
            "VALUES (8477424, NULL, '20252026', 1, 20, 10, 5, 3, 1500, 80, 1580, 108000)"
        )
        for i in range(10):
            gd = f"2025-10-{(10 + i):02d}"
            w = 1 if i % 3 != 2 else 0
            conn.execute(
                f"INSERT INTO goalie_stats "
                f"(player_id, game_date, season, is_season_total, "
                f"wins, losses, ot_losses, shutouts, saves, goals_against, shots_against, toi) "
                f"VALUES (8477424, '{gd}', '20252026', 0, {w}, {1 - w}, 0, 0, 30, 2, 32, 3600)"
            )

        # Fantasy setup
        conn.execute(
            "INSERT INTO fantasy_teams (id, league_id, name, short_name) "
            "VALUES ('team1', 'lg1', 'My Team', 'MT')"
        )
        conn.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Connor McDavid', 'C', 'active', 12500000)"
        )
        conn.execute(
            "INSERT INTO fantasy_roster_slots (team_id, player_name, position_short, status_id, salary) "
            "VALUES ('team1', 'Juuse Saros', 'G', 'active', 5000000)"
        )
        conn.execute(
            "INSERT INTO fantasy_standings "
            "(league_id, team_id, rank, wins, losses, points, "
            "points_for, points_against, streak, games_played, fantasy_points_per_game) "
            "VALUES ('lg1', 'team1', 1, 50, 20, 100, 5000.5, 4200.0, 'W3', 70, 71.4)"
        )

        # Injury on roster player
        conn.execute(
            "INSERT INTO player_injuries (player_id, source, injury_type, status, updated_at) "
            "VALUES (8478402, 'rotowire', 'Upper Body', 'Day-to-Day', '2026-02-18')"
        )

        conn.commit()
        yield conn
        conn.close()

    @pytest.fixture
    def live_ctx(self, live_db):
        return SessionContext(conn=live_db, team_id="team1", season="20252026")

    def _find_tool_results(self, client):
        """Extract all tool_result content strings from the conversation."""
        results = []
        for msg in client.messages:
            content = msg.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if isinstance(item, dict) and item.get("type") == "tool_result":
                    results.append(item["content"])
        return results

    @pytest.mark.integration
    @patch.dict(os.environ, {"ASSISTANT_MODEL": "claude-haiku-4-5-20251001"})
    def test_roster_live(self, live_ctx):
        """Real API: ask for roster → Claude calls get_my_roster → real data."""
        client = AssistantClient(context=live_ctx, team_name="My Team")
        response = client.chat("Show me my full roster with stats.")

        assert isinstance(response, str)
        assert len(response) > 0

        tool_results = self._find_tool_results(client)
        assert len(tool_results) >= 1
        assert "Connor McDavid" in tool_results[0]

    @pytest.mark.integration
    @patch.dict(os.environ, {"ASSISTANT_MODEL": "claude-haiku-4-5-20251001"})
    def test_player_stats_live(self, live_ctx):
        """Real API: ask about a player → Claude calls get_player_stats."""
        client = AssistantClient(context=live_ctx, team_name="My Team")
        response = client.chat("Give me Connor McDavid's detailed stats.")

        assert isinstance(response, str)
        assert len(response) > 0

        tool_results = self._find_tool_results(client)
        assert len(tool_results) >= 1
        all_results = "\n".join(tool_results)
        assert "McDavid" in all_results

    @pytest.mark.integration
    @patch.dict(os.environ, {"ASSISTANT_MODEL": "claude-haiku-4-5-20251001"})
    def test_injuries_live(self, live_ctx):
        """Real API: ask about injuries → Claude calls get_injuries."""
        client = AssistantClient(context=live_ctx, team_name="My Team")
        response = client.chat("Are any of my players injured right now?")

        assert isinstance(response, str)
        assert len(response) > 0

        tool_results = self._find_tool_results(client)
        assert len(tool_results) >= 1
        all_results = "\n".join(tool_results)
        assert "McDavid" in all_results or "Upper Body" in all_results

    @pytest.mark.integration
    @patch.dict(os.environ, {"ASSISTANT_MODEL": "claude-haiku-4-5-20251001"})
    def test_multi_turn_live(self, live_ctx):
        """Real API: two-turn conversation preserves context."""
        client = AssistantClient(context=live_ctx, team_name="My Team")

        r1 = client.chat("Show me my roster.")
        assert isinstance(r1, str) and len(r1) > 0

        r2 = client.chat("Which of those players has the most goals?")
        assert isinstance(r2, str) and len(r2) > 0

        # Should have at least 4 messages (2 user + 2 assistant minimum)
        assert len(client.messages) >= 4
