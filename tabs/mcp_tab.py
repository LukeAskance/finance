"""tabs/mcp_tab.py — MCP Portfolio Chat tab."""

from __future__ import annotations

import os
from typing import Any

from nicegui import ui

import claude_client

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
                "min_market_value": {
                    "type": "number",
                    "description": "Only include positions with market value >= this amount. Default 0.",
                },
                "instrument_type": {
                    "type": "string",
                    "description": "Filter by type: 'equity', 'fund', 'cash', 'option'. Leave blank for all.",
                },
            },
        },
    },
    {
        "name": "get_positions_by_account",
        "description": "Return positions broken out per account from the most-recent snapshot.",
        "input_schema": {
            "type": "object",
            "properties": {
                "account_name": {
                    "type": "string",
                    "description": "Partial account name to filter by (case-insensitive). Leave blank for all accounts.",
                },
            },
        },
    },
    {
        "name": "get_portfolio_totals",
        "description": "Return daily total portfolio market value over the past N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of calendar days of history. Default 90.",
                },
            },
        },
    },
    {
        "name": "get_position_price_history",
        "description": "Return daily last_price/market_value history for one held position from the local database (summed across accounts). Prefer this over get_price_history when comparing moves across held positions — one cheap DB query instead of a live fetch.",
        "input_schema": {
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Instrument symbol, e.g. 'AAPL'.",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of calendar days of history. Default 30.",
                },
            },
            "required": ["symbol"],
        },
    },
    {
        "name": "get_account_totals",
        "description": "Return daily market value per account over the past N days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of calendar days of history. Default 30.",
                },
            },
        },
    },
    {
        "name": "get_financials",
        "description": "Return key financial metrics for a ticker: price, EPS, dividend yield%, P/E ratio, and cash-per-share from SEC EDGAR.",
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker symbol, e.g. 'AAPL'.",
                },
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
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker symbol.",
                },
                "lookback_days": {
                    "type": "integer",
                    "description": "Days of history to scan. Default 365.",
                },
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
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker symbol.",
                },
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
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker symbol.",
                },
                "shares": {
                    "type": "number",
                    "description": "Number of shares held.",
                },
                "years": {
                    "type": "integer",
                    "description": "Forecast horizon in years. Default 3.",
                },
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
                "ticker": {
                    "type": "string",
                    "description": "Stock ticker symbol.",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of calendar days of history. Default 90.",
                },
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

# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------


def _execute_tool(name: str, tool_input: dict) -> Any:
    import portfolio_mcp as _mcp

    fn = getattr(_mcp, name, None)
    return (
        {"error": f"Unknown tool: {name}"} if fn is None else fn(**tool_input)
    )


# ---------------------------------------------------------------------------
# LLM callers (shared tool-use loop lives in claude_client.py)
# ---------------------------------------------------------------------------


def _call_claude(
    messages: list[dict], model: str = claude_client.DEFAULT_MODEL
) -> str:
    result = claude_client.run_tool_loop(
        list(messages),
        system=SYSTEM,
        tools=TOOLS,
        execute_tool=_execute_tool,
        model=model,
    )
    return result.text


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

            async def send_click() -> None:
                user_text = chat_input.value.strip()
                if not user_text:
                    return

                api_key = os.getenv("ANTHROPIC_API_KEY")
                if not api_key:
                    ui.notify("ANTHROPIC_API_KEY not set", color="negative")
                    return

                chat_input.value = ""
                with chat_column:
                    ui.chat_message(
                        text=user_text, name="You", sent=True
                    ).classes("w-full")
                send_button.disable()
                send_button.text = "..."
                history.append({"role": "user", "content": user_text})

                try:
                    answer = await asyncio.to_thread(
                        _call_claude,
                        list(history),
                        claude_client.DEFAULT_MODEL,
                    )
                    history.append({"role": "assistant", "content": answer})
                    with chat_column:
                        ui.chat_message(
                            text=answer, name="Assistant", sent=False
                        ).classes("w-full")
                except Exception as exc:
                    ui.notify(f"MCP chat error: {exc}", color="negative")
                finally:
                    send_button.text = "Send"
                    send_button.enable()

            def clear_click() -> None:
                chat_column.clear()
                history.clear()

            def list_tools_click() -> None:
                lines = [f"{t['name']} — {t['description']}" for t in TOOLS]
                with chat_column:
                    ui.chat_message(
                        text=lines, name="Available tools", sent=False
                    ).classes("w-full")

            with ui.row().classes("w-full items-center gap-2 mt-1"):
                chat_input = (
                    ui.input(
                        placeholder="e.g. What are my top 5 positions and insider activity for each?",
                    )
                    .classes("flex-1")
                    .on("keydown.enter", send_click)
                )
                send_button = ui.button("Send", on_click=send_click)

            with ui.row().classes("gap-2"):
                ui.button("Clear conversation", on_click=clear_click).props(
                    "flat dense"
                )
                ui.button("List tools", on_click=list_tools_click).props(
                    "flat dense"
                )
