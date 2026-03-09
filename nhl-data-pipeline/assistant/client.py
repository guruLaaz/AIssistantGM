"""Claude API client with tool-use loop for the fantasy hockey assistant."""

from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import anthropic

from assistant.tools import TOOLS, SessionContext, dispatch_tool

# ---------------------------------------------------------------------------
# Rotating file logger for assistant requests / responses / tool calls
# ---------------------------------------------------------------------------
_LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

_log = logging.getLogger("assistant.client")
_log.setLevel(logging.DEBUG)
_log.propagate = False

# 2 MB max per file, keep 1 backup → ~4 MB total cap
_handler = RotatingFileHandler(
    _LOG_DIR / "assistant.log",
    maxBytes=2 * 1024 * 1024,
    backupCount=1,
    encoding="utf-8",
)
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_log.addHandler(_handler)


def _safe_json(obj: object) -> str:
    """Serialize obj to compact JSON, falling back to str() on failure."""
    try:
        return json.dumps(obj, default=str, ensure_ascii=False)
    except Exception:
        return str(obj)

from config.infra_constants import (
    MAX_TOKENS,
    DEFAULT_MODEL as _DEFAULT_MODEL,
    DEEP_MODEL as _DEEP_MODEL,
    DEFAULT_THINKING_BUDGET as _DEFAULT_THINKING_BUDGET,
)

_PROMPT_PATH = Path(__file__).parent / "system_prompt.txt"


class AssistantClient:
    """Manages conversation state and the Claude tool-use loop."""

    def __init__(
        self,
        context: SessionContext,
        team_name: str,
    ) -> None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY environment variable is not set. "
                "Set it before running the assistant."
            )

        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = os.environ.get("ASSISTANT_MODEL", _DEFAULT_MODEL)
        self.context = context
        self.messages: list[dict] = []

        # Load and fill system prompt template
        template = _PROMPT_PATH.read_text(encoding="utf-8")
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        _est = _tz(_td(hours=-5))
        _now_est = _dt.now(_est)
        self.system_prompt = template.format(
            team_name=team_name,
            team_id=context.team_id,
            season=context.season,
            today=_now_est.strftime("%B %d, %Y at %I:%M %p EST"),
        )

    def _estimate_tokens(self) -> int:
        """Estimate total token count in conversation history.

        Uses a rough heuristic of 4 characters per token.
        Counts system prompt and all messages.
        """
        total_chars = len(self.system_prompt)
        for msg in self.messages:
            content = msg.get("content", "")
            if isinstance(content, str):
                total_chars += len(content)
            elif isinstance(content, list):
                for block in content:
                    if isinstance(block, dict):
                        total_chars += len(str(block.get("content", "")))
                        total_chars += len(str(block.get("input", "")))
                    else:
                        # Anthropic SDK content block objects
                        if hasattr(block, "text"):
                            total_chars += len(block.text)
                        elif hasattr(block, "input"):
                            total_chars += len(str(block.input))
        return total_chars // 4

    def chat(self, user_message: str, deep: bool = False) -> str:
        """Send a user message and return the final assistant text.

        Handles the full tool-use loop: if Claude responds with tool_use
        blocks, executes them via dispatch_tool, sends results back, and
        continues until Claude returns a text response.

        Args:
            user_message: The user's input text.
            deep: If True, use Opus with higher thinking budget.

        Returns:
            The assistant's final text response.
        """
        self.messages.append({"role": "user", "content": user_message})
        _log.info("USER: %s", user_message)

        # Select model and thinking config
        if deep:
            model = _DEEP_MODEL
            thinking = {"type": "adaptive"}
        else:
            model = self.model
            thinking = {"type": "enabled", "budget_tokens": _DEFAULT_THINKING_BUDGET}

        _log.info("MODEL: %s | thinking: %s", model, _safe_json(thinking))

        iteration = 0
        while True:
            iteration += 1

            # Context management: trim if approaching 100k tokens
            if self._estimate_tokens() > 90_000 and len(self.messages) > 21:
                self.messages = [self.messages[0]] + self.messages[-20:]
                _log.info("Context trimmed (token estimate exceeded 90k)")
                print("[Context trimmed to stay within limits]")

            _log.debug("API request (iter %d): %d messages, ~%d tokens",
                       iteration, len(self.messages), self._estimate_tokens())

            for attempt in range(3):
                try:
                    with self.client.messages.stream(
                        model=model,
                        max_tokens=MAX_TOKENS,
                        thinking=thinking,
                        system=[{
                            "type": "text",
                            "text": self.system_prompt,
                            "cache_control": {"type": "ephemeral"},
                        }],
                        tools=TOOLS,
                        messages=self.messages,
                    ) as stream:
                        response = stream.get_final_message()
                    break
                except anthropic.RateLimitError:
                    wait = 30 * (attempt + 1)
                    _log.warning("Rate limited (attempt %d) — waiting %ds", attempt + 1, wait)
                    print(f"[Rate limited — waiting {wait}s before retrying...]")
                    time.sleep(wait)
                except (anthropic.APIConnectionError, ConnectionError, OSError) as e:
                    _log.warning("Connection error (attempt %d): %s", attempt + 1, e)
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        print(f"[Connection error: {e} — retrying in {wait}s...]")
                        time.sleep(wait)
                    else:
                        raise

            # Log response metadata
            _log.info("API response: stop_reason=%s, usage=%s",
                      response.stop_reason, _safe_json(getattr(response, "usage", None)))

            # Collect all content blocks from the response
            assistant_content = response.content

            # Log each content block
            for block in assistant_content:
                if block.type == "thinking":
                    _log.debug("THINKING: %s", block.thinking[:500] if hasattr(block, "thinking") else "(redacted)")
                elif block.type == "text":
                    _log.info("RESPONSE TEXT: %s", block.text)
                elif block.type == "tool_use":
                    _log.info("TOOL CALL: %s(%s)", block.name, _safe_json(block.input))

            # Append the full assistant response to history
            self.messages.append({"role": "assistant", "content": assistant_content})

            # If stop_reason is not tool_use, extract text and return
            if response.stop_reason != "tool_use":
                text_parts = [
                    block.text
                    for block in assistant_content
                    if block.type == "text"
                ]
                final = "\n".join(text_parts)
                _log.info("FINAL RESPONSE (%d chars):\n%s", len(final), final)
                return final

            # Process tool calls
            tool_results = []
            for block in assistant_content:
                if block.type != "tool_use":
                    continue
                _log.info("TOOL DISPATCH: %s", block.name)
                try:
                    result = dispatch_tool(block.name, block.input, self.context)
                    _log.info("TOOL RESULT [%s]: %s", block.name, result if result else "(empty)")
                except Exception as e:
                    result = f"Error: {e}"
                    _log.error("TOOL ERROR [%s]: %s", block.name, e, exc_info=True)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            # Send tool results back
            self.messages.append({"role": "user", "content": tool_results})
