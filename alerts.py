"""alerts.py — classify free-text alert notes into checkable conditions.

Parsing happens once per note (an LLM call, via claude_client.run_tool_loop).
Checking an active_alert happens many times after that, and never calls the
LLM — it calls financials.get_financials() directly and compares numbers.
"""

from __future__ import annotations

import operator
from concurrent.futures import ThreadPoolExecutor

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


def check_portfolio_iv_spikes(
    tickers: list[str],
    relative_multiple: float = 1.5,
    absolute_pp: float = 10.0,
) -> list[dict]:
    """Scan every ticker for options-market distress signals — no LLM, no
    per-ticker alert needed, this watches the whole portfolio at once. Two
    independent triggers, either one flags a ticker:

    - baseline spike: today's near-term OTM put IV exceeds BOTH 1.5x AND
      +10 percentage points over its own trailing 30-day median. Catches a
      gradual multi-week drift higher. Requires ~10 days of accumulated
      history, so a freshly-watched ticker won't trigger this on day one.
    - term-structure inversion: near-term (~20d+) IV > long-dated (~180d)
      IV. Catches an acute, same-day event-risk signal even for a ticker
      with no accumulated history yet — the market pricing near-term danger
      above the long-run picture, right now.
    """

    def _check_one(ticker: str) -> dict | None:
        financials = get_financials(ticker)
        reasons = []

        today = financials.otm_put_iv_pct
        baseline = historicals_store.get_iv_baseline(ticker)
        if today is not None and baseline is not None:
            if today > baseline * relative_multiple and (today - baseline) > absolute_pp:
                pct_over = round((today / baseline - 1) * 100, 1)
                reasons.append(
                    f"baseline spike: {today:.1f}% vs 30d median "
                    f"{baseline:.1f}% (+{pct_over}%)"
                )

        near, far = financials.otm_put_iv_pct, financials.otm_put_iv_180d_pct
        if near is not None and far is not None and near > far:
            reasons.append(
                f"term-structure inversion: near {near:.1f}% > 180d {far:.1f}%"
            )

        if not reasons:
            return None
        return {
            "ticker": ticker,
            "today": today,
            "baseline": round(baseline, 2) if baseline is not None else None,
            "reasons": reasons,
        }

    if not tickers:
        return []
    with ThreadPoolExecutor(max_workers=min(8, len(tickers))) as pool:
        results = list(pool.map(_check_one, tickers))
    return [r for r in results if r is not None]


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

    # check_portfolio_iv_spikes: fake the baseline lookup and per-ticker
    # fetch to exercise both independent triggers.
    @dataclass
    class _FakeIvFinancials:
        otm_put_iv_pct: float | None
        otm_put_iv_180d_pct: float | None = None

    _baselines = {
        "SPIKE": 20.0, "FLAT": 20.0, "NOHIST": None, "LOWIV": 4.0,
        "NEWEVENT": None, "BOTH": 20.0,
    }
    _financials = {
        "SPIKE": _FakeIvFinancials(45.0, None),       # baseline spike only
        "FLAT": _FakeIvFinancials(22.0, None),        # neither trigger
        "LOWIV": _FakeIvFinancials(7.0, None),        # relative trips, absolute doesn't
        "NEWEVENT": _FakeIvFinancials(40.0, 15.0),    # no history yet, but inverted
        "BOTH": _FakeIvFinancials(45.0, 15.0),        # both triggers fire
    }

    historicals_store.get_iv_baseline = lambda ticker, **kw: _baselines.get(ticker)
    get_financials = lambda ticker: _financials.get(ticker, _FakeIvFinancials(None, None))

    spikes = check_portfolio_iv_spikes(["SPIKE", "FLAT", "NOHIST", "LOWIV", "NEWEVENT", "BOTH"])
    flagged = {s["ticker"]: s["reasons"] for s in spikes}
    # SPIKE: 45 vs 20 baseline -> 2.25x AND +25pp -> flagged (baseline spike)
    # FLAT: 22 vs 20 -> below both guards -> not flagged
    # NOHIST: no financials/baseline at all -> skipped
    # LOWIV: 7 vs 4 -> 1.75x (relative trips) but only +3pp (absolute doesn't) -> not flagged
    # NEWEVENT: no baseline (can't spike-check) but near 40 > far 15 -> flagged (inversion),
    #           proving the acute signal works even with zero accumulated history
    # BOTH: baseline spike AND inversion -> flagged with two reasons
    assert set(flagged) == {"SPIKE", "NEWEVENT", "BOTH"}, flagged
    assert len(flagged["SPIKE"]) == 1 and "baseline spike" in flagged["SPIKE"][0]
    assert len(flagged["NEWEVENT"]) == 1 and "inversion" in flagged["NEWEVENT"][0]
    assert len(flagged["BOTH"]) == 2

    print("alerts.py self-check OK")
