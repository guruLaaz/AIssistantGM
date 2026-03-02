"""Tests for assistant/client.py — Claude API client."""

import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from db.schema import init_db, get_db
from assistant.tools import SessionContext
from assistant.client import AssistantClient, MAX_TOKENS


def _make_stream_cm(response):
    """Create a mock context manager that mimics messages.stream()."""
    stream = MagicMock()
    stream.get_final_message.return_value = response
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=stream)
    cm.__exit__ = MagicMock(return_value=False)
    return cm


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def db(db_path: Path) -> sqlite3.Connection:
    init_db(db_path)
    conn = get_db(db_path)
    yield conn
    conn.close()


@pytest.fixture
def ctx(db: sqlite3.Connection) -> SessionContext:
    return SessionContext(conn=db, team_id="team1", season="20252026")


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestAssistantClientInit:
    """Tests for AssistantClient initialization."""

    def test_missing_api_key_raises(self, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            with pytest.raises(RuntimeError, match="ANTHROPIC_API_KEY"):
                AssistantClient(context=ctx, team_name="My Team")

    @patch("assistant.client.anthropic.Anthropic")
    def test_sets_default_model(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            os.environ.pop("ASSISTANT_MODEL", None)
            client = AssistantClient(context=ctx, team_name="My Team")
            assert client.model == "claude-opus-4-6"

    @patch("assistant.client.anthropic.Anthropic")
    def test_custom_model(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {
            "ANTHROPIC_API_KEY": "test-key",
            "ASSISTANT_MODEL": "claude-haiku-3-5-20241022",
        }):
            client = AssistantClient(context=ctx, team_name="My Team")
            assert client.model == "claude-haiku-3-5-20241022"

    @patch("assistant.client.anthropic.Anthropic")
    def test_system_prompt_filled(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Cool Team")
            assert "Cool Team" in client.system_prompt
            assert "team1" in client.system_prompt
            assert "20252026" in client.system_prompt

    @patch("assistant.client.anthropic.Anthropic")
    def test_empty_messages(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            assert client.messages == []


# ---------------------------------------------------------------------------
# _estimate_tokens
# ---------------------------------------------------------------------------


class TestEstimateTokens:
    """Tests for the token estimation method."""

    @patch("assistant.client.anthropic.Anthropic")
    def test_empty_conversation(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            # Should at least count the system prompt
            tokens = client._estimate_tokens()
            assert tokens > 0
            assert tokens == len(client.system_prompt) // 4

    @patch("assistant.client.anthropic.Anthropic")
    def test_string_messages(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            client.messages = [
                {"role": "user", "content": "a" * 400},
                {"role": "assistant", "content": "b" * 400},
            ]
            base = len(client.system_prompt) // 4
            assert client._estimate_tokens() == base + 200  # 800 chars / 4

    @patch("assistant.client.anthropic.Anthropic")
    def test_list_content_dict_blocks(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            client.messages = [
                {"role": "user", "content": [
                    {"type": "tool_result", "content": "x" * 100},
                ]},
            ]
            tokens = client._estimate_tokens()
            assert tokens > len(client.system_prompt) // 4

    @patch("assistant.client.anthropic.Anthropic")
    def test_list_content_sdk_blocks(self, mock_anthropic, ctx: SessionContext) -> None:
        """SDK content block objects with .text attribute."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            block = SimpleNamespace(text="y" * 200)
            client.messages = [
                {"role": "assistant", "content": [block]},
            ]
            tokens = client._estimate_tokens()
            assert tokens > len(client.system_prompt) // 4

    @patch("assistant.client.anthropic.Anthropic")
    def test_mixed_content_types(self, mock_anthropic, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")
            client.messages = [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": [
                    SimpleNamespace(text="response text"),
                    SimpleNamespace(input={"key": "value"}),
                ]},
                {"role": "user", "content": [
                    {"type": "tool_result", "content": "result"},
                ]},
            ]
            tokens = client._estimate_tokens()
            assert tokens > 0


# ---------------------------------------------------------------------------
# chat (mocked)
# ---------------------------------------------------------------------------


class TestChat:
    """Tests for the chat method with mocked API."""

    @patch("assistant.client.anthropic.Anthropic")
    def test_simple_text_response(self, mock_anthropic_cls, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")

            # Mock a simple text response (no tool use)
            text_block = SimpleNamespace(type="text", text="Here is your roster!")
            mock_response = SimpleNamespace(
                content=[text_block],
                stop_reason="end_turn",
            )
            client.client.messages.stream = MagicMock(
                return_value=_make_stream_cm(mock_response)
            )

            result = client.chat("Show me my roster")
            assert result == "Here is your roster!"
            assert len(client.messages) == 2  # user + assistant

    @patch("assistant.client.anthropic.Anthropic")
    def test_tool_use_then_text(self, mock_anthropic_cls, ctx: SessionContext) -> None:
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")

            # First response: tool_use
            tool_block = SimpleNamespace(
                type="tool_use", name="get_league_standings",
                input={}, id="toolu_123",
            )
            tool_response = SimpleNamespace(
                content=[tool_block],
                stop_reason="tool_use",
            )

            # Second response: text
            text_block = SimpleNamespace(type="text", text="Standings loaded.")
            text_response = SimpleNamespace(
                content=[text_block],
                stop_reason="end_turn",
            )

            client.client.messages.stream = MagicMock(
                side_effect=[
                    _make_stream_cm(tool_response),
                    _make_stream_cm(text_response),
                ]
            )

            result = client.chat("What are the standings?")
            assert result == "Standings loaded."
            assert len(client.messages) == 4  # user + asst(tool) + user(result) + asst(text)

    @patch("assistant.client.anthropic.Anthropic")
    def test_context_trimming(self, mock_anthropic_cls, ctx: SessionContext) -> None:
        """When token estimate exceeds 90k, old messages are trimmed."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")

            # Stuff messages to trigger trimming (90k tokens = 360k chars)
            client.messages = [{"role": "user", "content": "first"}]
            for i in range(25):
                client.messages.append({"role": "user", "content": "x" * 15000})
                client.messages.append({"role": "assistant", "content": "y" * 15000})

            text_block = SimpleNamespace(type="text", text="Done")
            mock_response = SimpleNamespace(
                content=[text_block], stop_reason="end_turn",
            )
            client.client.messages.stream = MagicMock(
                return_value=_make_stream_cm(mock_response)
            )

            client.chat("new message")

            # After trimming: first message + last 20 + new user + new assistant = 23
            # The trimming happens before the API call, so: [first] + last 20 from prior + new user
            # Then after the call: + new assistant
            assert len(client.messages) <= 25

    @patch("assistant.client.anthropic.Anthropic")
    def test_multiple_text_blocks(self, mock_anthropic_cls, ctx: SessionContext) -> None:
        """Multiple text blocks are joined with newlines."""
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}):
            client = AssistantClient(context=ctx, team_name="Test")

            blocks = [
                SimpleNamespace(type="text", text="Line 1"),
                SimpleNamespace(type="text", text="Line 2"),
            ]
            mock_response = SimpleNamespace(content=blocks, stop_reason="end_turn")
            client.client.messages.stream = MagicMock(
                return_value=_make_stream_cm(mock_response)
            )

            result = client.chat("Hello")
            assert result == "Line 1\nLine 2"


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_max_tokens(self) -> None:
        assert MAX_TOKENS == 48_000
