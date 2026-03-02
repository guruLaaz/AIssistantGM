"""Integration tests for web search — requires real API keys.

Run with:  pytest nhl-data-pipeline/tests/test_integration_web_search.py --integration -v
"""

import os
import sqlite3
from pathlib import Path

import pytest

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a, **kw):  # type: ignore[misc]
        pass

# Load .env from repo root so API keys are available
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(_ENV_PATH)

from db.schema import init_db, get_db
from assistant.tools import SessionContext, _web_search
from assistant.client import AssistantClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
    return SessionContext(conn=db, team_id="integration_test", season="20252026")


# ---------------------------------------------------------------------------
# Test 1: Brave Search API directly
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestBraveSearchLive:
    """Hit the real Brave Search API and verify response structure."""

    def test_brave_search_returns_results(self) -> None:
        """Call _web_search with a real query and verify formatted output."""
        api_key = os.environ.get("BRAVE_SEARCH_API_KEY")
        if not api_key:
            pytest.skip("BRAVE_SEARCH_API_KEY not set")

        result = _web_search("NHL standings 2026", num_results=3)

        # Should NOT be an error message
        assert "not configured" not in result
        assert "timed out" not in result
        assert "invalid" not in result.lower()

        # Should contain the formatted header and at least one result
        assert "=== Web Search:" in result
        assert "NHL" in result
        assert "1." in result  # at least one numbered result


# ---------------------------------------------------------------------------
# Test 2: Claude calls Brave Search via tool-use loop
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestClaudeWebSearchE2E:
    """Full round-trip: user question → Claude → web_search tool → answer."""

    def test_claude_uses_web_search_tool(self, ctx: SessionContext) -> None:
        """Ask Claude a question that requires current info, verify it searches."""
        api_key_anthropic = os.environ.get("ANTHROPIC_API_KEY")
        api_key_brave = os.environ.get("BRAVE_SEARCH_API_KEY")
        if not api_key_anthropic:
            pytest.skip("ANTHROPIC_API_KEY not set")
        if not api_key_brave:
            pytest.skip("BRAVE_SEARCH_API_KEY not set")

        client = AssistantClient(context=ctx, team_name="Integration Test Team")

        # Ask something that requires live web data — Claude should invoke web_search
        answer = client.chat(
            "Use your web_search tool to find who won the most recent NHL game "
            "and tell me the score. Keep it to one sentence."
        )

        # Claude should have returned a non-empty text answer
        assert isinstance(answer, str)
        assert len(answer) > 10

        # The conversation should have at least 4 messages:
        # user → assistant(tool_use) → user(tool_result) → assistant(text)
        assert len(client.messages) >= 4

        # Verify the tool was actually called by checking for a tool_result message
        tool_result_found = False
        for msg in client.messages:
            content = msg.get("content", "")
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        tool_result_found = True
                        break
        assert tool_result_found, "Expected Claude to call web_search tool"
