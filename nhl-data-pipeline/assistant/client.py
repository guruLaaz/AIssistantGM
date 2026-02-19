"""Claude API client with tool-use loop for the fantasy hockey assistant."""

from __future__ import annotations

import os
from pathlib import Path

import anthropic

from assistant.tools import TOOLS, SessionContext, dispatch_tool

MAX_TOKENS = 4096
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
        self.model = os.environ.get("ASSISTANT_MODEL", "claude-sonnet-4-20250514")
        self.context = context
        self.messages: list[dict] = []

        # Load and fill system prompt template
        template = _PROMPT_PATH.read_text(encoding="utf-8")
        self.system_prompt = template.format(
            team_name=team_name,
            team_id=context.team_id,
            season=context.season,
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

    def chat(self, user_message: str) -> str:
        """Send a user message and return the final assistant text.

        Handles the full tool-use loop: if Claude responds with tool_use
        blocks, executes them via dispatch_tool, sends results back, and
        continues until Claude returns a text response.

        Args:
            user_message: The user's input text.

        Returns:
            The assistant's final text response.
        """
        self.messages.append({"role": "user", "content": user_message})

        while True:
            # Context management: trim if approaching 100k tokens
            if self._estimate_tokens() > 90_000 and len(self.messages) > 21:
                self.messages = [self.messages[0]] + self.messages[-20:]
                print("[Context trimmed to stay within limits]")

            response = self.client.messages.create(
                model=self.model,
                max_tokens=MAX_TOKENS,
                system=self.system_prompt,
                tools=TOOLS,
                messages=self.messages,
            )

            # Collect all content blocks from the response
            assistant_content = response.content

            # Append the full assistant response to history
            self.messages.append({"role": "assistant", "content": assistant_content})

            # If stop_reason is not tool_use, extract text and return
            if response.stop_reason != "tool_use":
                text_parts = [
                    block.text
                    for block in assistant_content
                    if block.type == "text"
                ]
                return "\n".join(text_parts)

            # Process tool calls
            tool_results = []
            for block in assistant_content:
                if block.type != "tool_use":
                    continue
                result = dispatch_tool(block.name, block.input, self.context)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })

            # Send tool results back
            self.messages.append({"role": "user", "content": tool_results})
