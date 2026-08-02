"""tabs/mcp_tab.py — shared live-portfolio tool definitions and dispatcher.

No longer a standalone UI tab (folded into the Analysis tab's chat — see
tabs/analysis_tab.py). Kept here since alerts.py also classifies notes
against this same TOOLS/_execute_tool pair.
"""

from __future__ import annotations

from typing import Any

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
        "name": "get_market_cap",
        "description": "Return the current market capitalization for a stock ticker.",
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

# ---------------------------------------------------------------------------
# Tool dispatch
# ---------------------------------------------------------------------------


def _execute_tool(name: str, tool_input: dict) -> Any:
    import portfolio_mcp as _mcp

    fn = getattr(_mcp, name, None)
    return (
        {"error": f"Unknown tool: {name}"} if fn is None else fn(**tool_input)
    )
