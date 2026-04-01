"""tabs/mcp_tab.py — MCP Portfolio Chat tab (Priority 4: deduplicated tool-use loop)."""

from __future__ import annotations

import json
import os
import urllib.error as urlerror
import urllib.request as urlrequest
from typing import Any

from nicegui import ui

# ---------------------------------------------------------------------------
# Tool schemas
# ---------------------------------------------------------------------------

TOOLS: list[dict] = [
    {
        "name": "get_portfolio_positions",
        "description": "Return all positions from the most-recent portfolio snapshot in the local database, aggregated across accounts and sorted by market value.",
        "input_schema": {
            "type": "object",
            "properties": {
                "min_market_value": {"type": "number", "description": "Only include positions with market value >= this amount. Default 0."},
                "instrument_type": {"type": "string", "description": "Filter by type: 'equity', 'fund', 'cash', 'option'. Leave blank for all."},
            },
        },
    },
    {
        "name": "get_positions_by_account",
        "description": "Return positions broken out per account from the most-recent snapshot.",
        "input_schema": {
            "type": "object",
            "properties": {
                "account_name": {"type": "string", "description": "Partial account name to filter by (case-insensitive). Leave blank for all accounts."},
            },
        },
    },
    {
        "name": "get_portfolio_totals",
        "description": "Return daily total portfolio market value over the past N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of calendar days of history. Default 90."},
            },
        },
    },
    {
        "name": "get_account_totals",
        "description": "Return daily market value per account over the past N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of calendar days of history. Default 30."},
            },
        },
    },
    {
        "name": "get_financials",
        "description": "Return key financial metrics for a ticker: price, EPS, dividend yield%, P/E ratio, and cash-per-share from SEC EDGAR.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol, e.g. 'AAPL'."},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_insider_activity",
        "description": "Return insider buy/sell/10b5-1 plan transaction summary for a ticker from SEC EDGAR Form 4 filings.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol."},
                "lookback_days": {"type": "integer", "description": "Days of history to scan. Default 365."},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_institutional_holders",
        "description": "Return the top institutional holders for a ticker (% of shares outstanding).",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol."},
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_dividend_forecast",
        "description": "Return a bear/base/bull dividend income forecast for a position using SEC EDGAR dividend history.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol."},
                "shares": {"type": "number", "description": "Number of shares held."},
                "years": {"type": "integer", "description": "Forecast horizon in years. Default 3."},
            },
            "required": ["ticker", "shares"],
        },
    },
    {
        "name": "get_price_history",
        "description": "Return recent closing price history for a ticker from yfinance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {"type": "string", "description": "Stock ticker symbol."},
                "days": {"type": "integer", "description": "Number of calendar days of history. Default 90."},
            },
            "required": ["ticker"],
        },
    },
]

SYSTEM = (
    "You are a portfolio assistant with access to live portfolio and market data tools. "
    "Use the tools to answer questions accurately. When looking up data for multiple "
    "tickers, call the tools in sequence. Be concise and include concrete numbers."
)

_GATHER_SYSTEM = (
    "You are a data-gathering assistant. Call whatever tools are needed "
    "to collect the data required to answer the user's question. "
    "Do not write a final answer — just gather the data."
)

# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------

def _execute_tool(name: str, tool_input: dict) -> Any:
    import portfolio_mcp as _mcp
    fn = getattr(_mcp, name, None)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    return fn(**tool_input)


# ---------------------------------------------------------------------------
# Priority 4: deduplicated tool-use loop
# ---------------------------------------------------------------------------

def _claude_tool_use_loop(
    messages: list[dict],
    api_key: str,
    model: str,
    system: str,
    max_tokens: int = 2048,
    raise_on_http_error: bool = True,
    collect_summaries: bool = False,
    max_rounds: int = 10,
) -> tuple[str | None, list[str]]:
    """
    Run a Claude tool-use loop until a text response is produced.

    Returns (final_text_or_None, tool_summaries).
    If raise_on_http_error is False, HTTP errors silently break the loop.
    If collect_summaries is True, each tool call is recorded as a string.
    """
    headers = {
        "content-type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
    }
    local_messages = list(messages)
    summaries: list[str] = []

    for _ in range(max_rounds):
        payload = {
            "model": model,
            "max_tokens": max_tokens,
            "system": system,
            "messages": local_messages,
            "tools": TOOLS,
        }
        req = urlrequest.Request(
            url="https://api.anthropic.com/v1/messages",
            data=json.dumps(payload).encode(),
            headers=headers,
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode())
        except urlerror.HTTPError as exc:
            if not raise_on_http_error:
                break
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"Claude API error ({exc.code}): {detail}") from exc
        except Exception:
            if not raise_on_http_error:
                break
            raise

        stop_reason = body.get("stop_reason")
        content = body.get("content", [])

        if stop_reason != "tool_use":
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    return str(block.get("text", "")), summaries
            return None, summaries

        local_messages.append({"role": "assistant", "content": content})
        tool_results = []
        for block in content:
            if not isinstance(block, dict) or block.get("type") != "tool_use":
                continue
            result = _execute_tool(block["name"], block.get("input", {}))
            if collect_summaries:
                summaries.append(
                    f"[{block['name']}({block.get('input', {})})] → {json.dumps(result)}"
                )
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": block["id"],
                "content": json.dumps(result),
            })
        local_messages.append({"role": "user", "content": tool_results})

    return None, summaries


# ---------------------------------------------------------------------------
# LLM callers
# ---------------------------------------------------------------------------

def _call_claude(messages: list[dict], api_key: str, model: str = "claude-sonnet-4-6") -> str:
    text, _ = _claude_tool_use_loop(
        messages, api_key, model,
        system=SYSTEM,
        max_tokens=2048,
        raise_on_http_error=True,
        collect_summaries=False,
    )
    return text or "Tool-use loop exceeded maximum iterations."


def _call_perplexity(messages: list[dict], api_key: str, model: str) -> str:
    """Gather data via Claude tool-use, then ask Perplexity with context injected."""
    tool_context = ""
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    if anthropic_key:
        _, summaries = _claude_tool_use_loop(
            list(messages), anthropic_key, "claude-sonnet-4-6",
            system=_GATHER_SYSTEM,
            max_tokens=512,
            raise_on_http_error=False,
            collect_summaries=True,
        )
        if summaries:
            tool_context = "\n\nLive data gathered from portfolio tools:\n" + "\n".join(summaries)

    question = next(
        (m["content"] for m in reversed(messages) if m["role"] == "user"), ""
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM + tool_context},
            {"role": "user", "content": question},
        ],
        "temperature": 0.1,
    }
    req = urlrequest.Request(
        url="https://api.perplexity.ai/chat/completions",
        data=json.dumps(payload).encode(),
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {api_key}",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode())
            choices = body.get("choices") or []
            if choices:
                return str(choices[0].get("message", {}).get("content", ""))
            return str(body)
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Perplexity API error ({exc.code}): {detail}") from exc


# ---------------------------------------------------------------------------
# Tab build
# ---------------------------------------------------------------------------

def build(panel_ref) -> None:
    import asyncio

    history: list[dict] = []

    with ui.tab_panel(panel_ref):
        with ui.card().classes("w-full"):
            ui.label("MCP Portfolio Chat").classes("text-xl font-semibold")
            ui.label(
                "Ask anything — Claude will call live portfolio and market data tools to answer."
            ).classes("text-sm text-gray-400")

        with ui.card().classes("w-full flex-1"):
            chat_column = ui.column().classes("w-full gap-2")

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center gap-2"):
                provider_select = ui.select(
                    options=["claude", "perplexity"],
                    value="claude",
                    label="Provider",
                ).classes("w-36")
                model_input = ui.input("Model", value="claude-sonnet-4-6").classes("w-56")

            provider_select.on(
                "update:model-value",
                lambda _: model_input.__setattr__(
                    "value",
                    "sonar" if provider_select.value == "perplexity" else "claude-sonnet-4-6",
                ),
            )

            async def send_click() -> None:
                user_text = chat_input.value.strip()
                if not user_text:
                    return

                provider = provider_select.value or "claude"
                if provider == "perplexity":
                    api_key = os.getenv("PERPLEXITY_API_KEY")
                    if not api_key:
                        ui.notify("PERPLEXITY_API_KEY not set", color="negative")
                        return
                else:
                    api_key = os.getenv("ANTHROPIC_API_KEY")
                    if not api_key:
                        ui.notify("ANTHROPIC_API_KEY not set", color="negative")
                        return

                chat_input.value = ""
                with chat_column:
                    ui.chat_message(text=user_text, name="You", sent=True).classes("w-full")
                send_button.disable()
                send_button.text = "..."
                history.append({"role": "user", "content": user_text})

                try:
                    if provider == "perplexity":
                        model = model_input.value.strip() or "sonar"
                        answer = await asyncio.to_thread(
                            _call_perplexity, list(history), api_key, model
                        )
                    else:
                        model = model_input.value.strip() or "claude-sonnet-4-6"
                        answer = await asyncio.to_thread(
                            _call_claude, list(history), api_key, model
                        )
                    history.append({"role": "assistant", "content": answer})
                    with chat_column:
                        ui.chat_message(text=answer, name="Assistant", sent=False).classes("w-full")
                except Exception as exc:
                    ui.notify(f"MCP chat error: {exc}", color="negative")
                finally:
                    send_button.text = "Send"
                    send_button.enable()

            def clear_click() -> None:
                chat_column.clear()
                history.clear()

            with ui.row().classes("w-full items-center gap-2 mt-1"):
                chat_input = ui.input(
                    placeholder="e.g. What are my top 5 positions and insider activity for each?",
                ).classes("flex-1").on("keydown.enter", send_click)
                send_button = ui.button("Send", on_click=send_click)

            ui.button("Clear conversation", on_click=clear_click).props("flat dense")
