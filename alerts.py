"""alerts.py — classify free-text alert notes into checkable conditions.

Parsing happens once per note (an LLM call, via claude_client.run_tool_loop).
Checking an active_alert happens many times after that, and never calls the
LLM — it calls financials.get_financials() directly and compares numbers.
"""

from __future__ import annotations

import datetime
import operator
from concurrent.futures import ThreadPoolExecutor
from functools import partial

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


# ponytail: thresholds below are tuned against one real scan's false
# positives, not derived from a model — revisit if a new false-positive
# pattern shows up that these don't cover.
_INVERSION_MIN_RATIO = 1.20  # near/far below this is bid-ask noise on illiquid far puts
_INVERSION_MIN_SPREAD_PP = 5.0  # e.g. TD 23.3/22.1 and MAIN 29.1/27.5 both fail this
_LARGE_CAP_MARKET_CAP = 50_000_000_000
_LARGE_CAP_MIN_NEAR_IV = 50.0  # mega-caps need a more extreme absolute IV to mean anything
_SECTOR_CLUSTER_MIN = 3  # >=N distress hits in one sector -> one sector-wide event


def check_ticker_for_iv_spike(
    ticker: str,
    relative_multiple: float = 1.5,
    absolute_pp: float = 10.0,
) -> dict | None:
    """One ticker's options-market distress check. Two independent triggers,
    either one flags it:

    - baseline spike: today's near-term OTM put IV exceeds BOTH
      relative_multiple AND +absolute_pp percentage points over its own
      trailing 30-day median. Catches a gradual multi-week drift higher.
      Requires ~10 days of accumulated history, so a freshly-watched ticker
      won't trigger this on day one.
    - term-structure inversion: near-term (~20d+) IV exceeds long-dated
      (~180d) IV by at least _INVERSION_MIN_RATIO *and*
      _INVERSION_MIN_SPREAD_PP — a bare backwardation (ratio 1.01-1.19) is
      within noise/bid-ask spread on illiquid far-dated puts, not a signal.

    A hit is then downgraded from "distress" to "earnings" if the ticker's
    next earnings date falls inside the near-term put's own window — earnings
    routinely doubles near-term IV on blue chips regardless of distress — and
    dropped entirely for large caps (>$50B) whose near-term IV doesn't clear
    an absolute floor, since a small relative move means little for a mega-cap.

    Standalone (not nested in check_portfolio_iv_spikes) so callers like the
    Alerts tab can report progress as each ticker resolves, instead of
    waiting for the whole portfolio to finish. Sector clustering (>=3 hits in
    one sector collapsed into a single event) is a portfolio-wide concern, so
    it isn't done here — see cluster_sector_hits.
    """
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
    if near is not None and far is not None and far > 0:
        ratio = near / far
        spread = near - far
        if ratio >= _INVERSION_MIN_RATIO and spread >= _INVERSION_MIN_SPREAD_PP:
            reasons.append(
                f"term-structure inversion: near {near:.1f}% vs 180d {far:.1f}% "
                f"({ratio:.2f}x, +{spread:.1f}pp)"
            )

    if not reasons:
        return None

    kind = "distress"
    near_expiration = financials.otm_put_iv_near_expiration
    earnings_date = financials.next_earnings_date
    if near_expiration and earnings_date and datetime.date.today() <= earnings_date <= near_expiration:
        kind = "earnings"

    if (
        kind == "distress"
        and financials.market_cap
        and financials.market_cap > _LARGE_CAP_MARKET_CAP
        and (today is None or today <= _LARGE_CAP_MIN_NEAR_IV)
    ):
        return None

    return {
        "ticker": ticker,
        "today": today,
        "baseline": round(baseline, 2) if baseline is not None else None,
        "reasons": reasons,
        "kind": kind,
        "sector": financials.sector,
    }


def cluster_sector_hits(hits: list[dict], min_cluster: int = _SECTOR_CLUSTER_MIN) -> list[dict]:
    """Collapse >=min_cluster "distress" hits sharing a sector into one
    sector-wide event (kind="sector") — a shared macro/sector IV move (e.g.
    the whole midstream-energy space), not N separate company-specific
    distress signals. "earnings" hits are left alone; an earnings-driven IV
    bump isn't a sector signal even if several happen to land the same week.
    """
    by_sector: dict[str, list[dict]] = {}
    for hit in hits:
        if hit.get("kind") == "distress" and hit.get("sector"):
            by_sector.setdefault(hit["sector"], []).append(hit)

    clustered = {s: hs for s, hs in by_sector.items() if len(hs) >= min_cluster}
    if not clustered:
        return hits

    clustered_tickers = {h["ticker"] for hs in clustered.values() for h in hs}
    result = [h for h in hits if h["ticker"] not in clustered_tickers]
    result.extend(
        {"kind": "sector", "sector": sector, "tickers": [h["ticker"] for h in hs]}
        for sector, hs in clustered.items()
    )
    return result


def check_portfolio_iv_spikes(
    tickers: list[str],
    relative_multiple: float = 1.5,
    absolute_pp: float = 10.0,
) -> list[dict]:
    """Scan every ticker for options-market distress signals — no LLM, no
    per-ticker alert needed, this watches the whole portfolio at once. See
    check_ticker_for_iv_spike for the per-ticker triggers and downgrades, and
    cluster_sector_hits for the portfolio-wide sector-clustering pass applied
    to the results.
    """
    if not tickers:
        return []
    check_one = partial(
        check_ticker_for_iv_spike,
        relative_multiple=relative_multiple,
        absolute_pp=absolute_pp,
    )
    with ThreadPoolExecutor(max_workers=min(8, len(tickers))) as pool:
        results = list(pool.map(check_one, tickers))
    hits = [r for r in results if r is not None]
    return cluster_sector_hits(hits)


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
        otm_put_iv_near_expiration: datetime.date | None = None
        market_cap: float | None = None
        sector: str | None = None
        next_earnings_date: datetime.date | None = None

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

    # refinements 1+2: ratio/spread thresholds on the inversion trigger.
    # TD-like: near 23.3 vs far 22.1 -> ratio 1.05, spread 1.2pp -> below
    # both guards, no longer a bare "near > far" false positive.
    _financials["NOISE"] = _FakeIvFinancials(23.3, 22.1)
    _baselines["NOISE"] = None
    assert check_ticker_for_iv_spike("NOISE") is None

    # refinement 3: earnings inside the near-term put's own window
    # reclassifies the hit instead of suppressing or treating it as distress.
    today = datetime.date.today()
    _financials["EARNINGS"] = _FakeIvFinancials(
        45.0, 15.0,
        otm_put_iv_near_expiration=today + datetime.timedelta(days=20),
        next_earnings_date=today + datetime.timedelta(days=10),
    )
    _baselines["EARNINGS"] = None
    result = check_ticker_for_iv_spike("EARNINGS")
    assert result is not None and result["kind"] == "earnings", result

    # same inversion, but earnings falls after the near-term put expires ->
    # still a distress hit, not an earnings artifact.
    _financials["NOTYET"] = _FakeIvFinancials(
        45.0, 15.0,
        otm_put_iv_near_expiration=today + datetime.timedelta(days=20),
        next_earnings_date=today + datetime.timedelta(days=40),
    )
    _baselines["NOTYET"] = None
    result = check_ticker_for_iv_spike("NOTYET")
    assert result is not None and result["kind"] == "distress", result

    # refinement 5: large-cap absolute IV floor. Same inversion math as
    # NEWEVENT/BOTH above, but a >$50B market cap needs near IV > 50% to count.
    _financials["MEGALOW"] = _FakeIvFinancials(35.0, 15.0, market_cap=100_000_000_000)
    _baselines["MEGALOW"] = None
    assert check_ticker_for_iv_spike("MEGALOW") is None

    _financials["MEGAHIGH"] = _FakeIvFinancials(55.0, 15.0, market_cap=100_000_000_000)
    _baselines["MEGAHIGH"] = None
    result = check_ticker_for_iv_spike("MEGAHIGH")
    assert result is not None and result["kind"] == "distress", result

    # refinement 4: >=3 distress hits sharing a sector collapse into one
    # sector-wide event; a same-magnitude hit in a different sector stays solo.
    for t in ("ENERGY1", "ENERGY2", "ENERGY3"):
        _financials[t] = _FakeIvFinancials(45.0, 15.0, sector="Energy Midstream")
        _baselines[t] = None
    _financials["LONER"] = _FakeIvFinancials(45.0, 15.0, sector="Tech")
    _baselines["LONER"] = None

    portfolio_spikes = check_portfolio_iv_spikes(["ENERGY1", "ENERGY2", "ENERGY3", "LONER"])
    sector_events = [s for s in portfolio_spikes if s["kind"] == "sector"]
    assert len(sector_events) == 1, portfolio_spikes
    assert set(sector_events[0]["tickers"]) == {"ENERGY1", "ENERGY2", "ENERGY3"}, sector_events
    assert any(s.get("ticker") == "LONER" for s in portfolio_spikes), portfolio_spikes

    print("alerts.py self-check OK")
