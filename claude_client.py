"""claude_client.py — shared Claude API client and tool-use loop.

Single home for model defaults/aliases and the Claude tool-use loop that was
previously duplicated across analysis_module.py and tabs/mcp_tab.py.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Callable

import anthropic

DEFAULT_MODEL = "claude-opus-4-8"
FALLBACK_MODEL = "claude-sonnet-4-6"

MODEL_ALIASES: dict[str, str] = {
    "opus": "claude-opus-4-8",
    "sonnet": "claude-sonnet-4-6",
    "haiku": "claude-haiku-4-5",
}

# Models that support thinking={"type": "adaptive"}; older models 400 on it.
_ADAPTIVE_THINKING_PREFIXES = (
    "claude-fable",
    "claude-opus-4-6",
    "claude-opus-4-7",
    "claude-opus-4-8",
    "claude-sonnet-4-6",
)


def resolve_model(model: str | None) -> str:
    name = (model or "").strip()
    if not name:
        return DEFAULT_MODEL
    return MODEL_ALIASES.get(name.lower(), name)


def _supports_adaptive_thinking(model: str) -> bool:
    return model.startswith(_ADAPTIVE_THINKING_PREFIXES)


@dataclass
class ToolLoopResult:
    text: str
    tool_results: dict[str, Any] = field(default_factory=dict)
    tool_summaries: list[str] = field(default_factory=list)


def run_tool_loop(
    messages: list[dict[str, Any]],
    *,
    system: str,
    tools: list[dict[str, Any]],
    execute_tool: Callable[[str, dict[str, Any]], Any],
    model: str = DEFAULT_MODEL,
    max_tokens: int = 8192,
    max_rounds: int = 10,
) -> ToolLoopResult:
    """Run a Claude tool-use loop until a final text response is produced.

    Raises RuntimeError if no API key is configured or no usable model is
    found; other anthropic.APIError exceptions propagate to the caller.
    """
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise RuntimeError("Missing ANTHROPIC_API_KEY in environment.")

    client = anthropic.Anthropic()

    candidates: list[str] = []
    for name in (resolve_model(model), FALLBACK_MODEL):
        if name and name not in candidates:
            candidates.append(name)

    last_error: Exception | None = None
    for candidate in candidates:
        extra: dict[str, Any] = {}
        if _supports_adaptive_thinking(candidate):
            extra["thinking"] = {"type": "adaptive"}

        local_messages = list(messages)
        tool_results: dict[str, Any] = {}
        summaries: list[str] = []

        try:
            for _ in range(max_rounds):
                response = client.messages.create(
                    model=candidate,
                    max_tokens=max_tokens,
                    system=system,
                    tools=tools,
                    messages=local_messages,
                    **extra,
                )

                if response.stop_reason != "tool_use":
                    text = "\n".join(
                        block.text
                        for block in response.content
                        if block.type == "text" and block.text
                    )
                    return ToolLoopResult(
                        text or "(no text in response)",
                        tool_results,
                        summaries,
                    )

                # Preserve the full content (incl. thinking blocks) so the
                # follow-up request is valid.
                local_messages.append(
                    {"role": "assistant", "content": response.content}
                )

                tool_result_content: list[dict[str, Any]] = []
                for block in response.content:
                    if block.type != "tool_use":
                        continue
                    result = execute_tool(block.name, dict(block.input or {}))
                    tool_results[block.name] = result
                    payload = json.dumps(result, default=str)
                    summaries.append(f"[{block.name}({block.input})] → {payload}")
                    tool_result_content.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": payload,
                        }
                    )
                local_messages.append(
                    {"role": "user", "content": tool_result_content}
                )

            return ToolLoopResult(
                "Tool-use loop exceeded maximum iterations.",
                tool_results,
                summaries,
            )
        except anthropic.NotFoundError as exc:
            last_error = exc
            continue

    raise RuntimeError(f"Claude API call failed: no usable model. {last_error}")
