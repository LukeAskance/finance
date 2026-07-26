"""alerts.py — classify free-text alert notes into checkable conditions.

Parsing happens once per note (an LLM call, via claude_client.run_tool_loop).
Checking an active_alert happens many times after that, and never calls the
LLM — it calls financials.get_financials() directly and compares numbers.
"""

from __future__ import annotations

import operator

import claude_client
import historicals_store
from financials import get_financials
from tabs import mcp_tab

_OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "==": operator.eq,
}

_AVAILABLE_METRICS = (
    "price, eps, dividend_yield_pct, pe_ratio, cash_per_share, "
    "otm_put_iv_pct (options-implied volatility on ~10%-OTM puts, nearest "
    "20+ day expiration — a Merton-model proxy for market-implied credit "
    "distress; a spike suggests the market is pricing in higher default risk)"
)

RECORD_ALERT_TOOL = {
    "name": "record_alert",
    "description": (
        "Record your classification of the user's note. Call this exactly "
        "once, as your final action."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["active_alert", "standing_question", "needs_tool"],
                "description": (
                    "active_alert: a simple numeric threshold check on one "
                    "of the available get_financials metrics. "
                    "standing_question: an open/comparative question the "
                    "available tools CAN answer, but not as a single "
                    "threshold — needs periodic re-analysis. "
                    "needs_tool: no available tool supplies the data this "
                    "note needs."
                ),
            },
            "ticker": {"type": "string", "description": "Required for active_alert."},
            "metric": {
                "type": "string",
                "description": f"Required for active_alert. One of: {_AVAILABLE_METRICS}.",
            },
            "op": {"type": "string", "enum": list(_OPS)},
            "threshold": {"type": "number"},
            "summary": {
                "type": "string",
                "description": "One sentence: what will be checked, or what capability is missing.",
            },
        },
        "required": ["status", "summary"],
    },
}

CLASSIFY_SYSTEM = (
    "You are triaging a user's alert/question against a fixed set of data tools. "
    "Try to satisfy it using ONLY the available tools. Never guess or fall back on "
    "prior/training knowledge for financial figures — if a tool can't supply a number, "
    "you don't have that number. "
    f"For a numeric threshold check, only these get_financials metrics exist: {_AVAILABLE_METRICS}. "
    "Call record_alert exactly once, as your final action, with your classification."
)


def _classify_execute_tool(name: str, tool_input: dict) -> dict:
    if name == "record_alert":
        return {"ok": True}
    return mcp_tab._execute_tool(name, tool_input)


def create_alert(note_text: str) -> int:
    """Classify a note via the tool loop and persist it. Returns the new alert id."""
    captured: dict = {}

    def execute_tool(name: str, tool_input: dict) -> dict:
        if name == "record_alert":
            captured.update(tool_input)
        return _classify_execute_tool(name, tool_input)

    result = claude_client.run_tool_loop(
        [{"role": "user", "content": note_text}],
        system=CLASSIFY_SYSTEM,
        tools=[*mcp_tab.TOOLS, RECORD_ALERT_TOOL],
        execute_tool=execute_tool,
    )

    status = captured.get("status") or "needs_review"
    summary = captured.get("summary") or result.text

    return historicals_store.add_alert(
        note_text=note_text,
        status=status,
        ticker=captured.get("ticker"),
        metric=captured.get("metric"),
        op=captured.get("op"),
        threshold=captured.get("threshold"),
        summary=summary,
    )


def check_alert(alert: dict) -> tuple[bool, str]:
    """Poll one active_alert directly (no LLM). Returns (triggered, result text)."""
    ticker, metric, op, threshold = (
        alert["ticker"],
        alert["metric"],
        alert["op"],
        alert["threshold"],
    )
    if not (ticker and metric and op in _OPS and threshold is not None):
        return False, "Alert is missing ticker/metric/op/threshold."

    value = getattr(get_financials(ticker), metric, None)
    if value is None:
        return False, f"{ticker}.{metric} unavailable."

    triggered = _OPS[op](value, threshold)
    return triggered, f"{ticker} {metric} = {value} (threshold: {op} {threshold})"


if __name__ == "__main__":
    # ponytail: pure-logic self-check for check_alert; create_alert needs a
    # live ANTHROPIC_API_KEY + network so it isn't covered here.
    from dataclasses import dataclass

    @dataclass
    class _FakeFinancials:
        dividend_yield_pct: float

    def _fake_get_financials(ticker: str) -> _FakeFinancials:
        return _FakeFinancials(dividend_yield_pct=6.2)

    get_financials = _fake_get_financials

    triggered, msg = check_alert(
        {"ticker": "GHF", "metric": "dividend_yield_pct", "op": ">", "threshold": 6.0}
    )
    assert triggered is True, msg

    triggered, msg = check_alert(
        {"ticker": "GHF", "metric": "dividend_yield_pct", "op": ">", "threshold": 7.0}
    )
    assert triggered is False, msg

    triggered, msg = check_alert({"ticker": None, "metric": None, "op": None, "threshold": None})
    assert triggered is False

    print("alerts.py self-check OK")
