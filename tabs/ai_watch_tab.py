"""tabs/ai_watch_tab.py — AI Bubble Early Warning panel.

Three sections:
  Signal Tickers  — ORCL, NVDA, IGV, CRWV: price vs. 1-year high + 30d ATM put IV
  BDC Lens        — TRIN, OTF, SLRC: live price vs. user-entered NAV estimate
  Phase Tracker   — manual phase selector + notes
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Any

import yfinance as yf
from nicegui import ui


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# (symbol, short label, AI signal role)
_SIGNAL_TICKERS: list[tuple[str, str, str]] = [
    ("ORCL", "Oracle",        "AI infra debt — leading CDS proxy; $130B debt, -60% from Sep 2025 peak"),
    ("NVDA", "Nvidia",        "Circular financing anchor — Chanos: 'sellers subsidizing buyers'"),
    ("IGV",  "Software ETF",  "SaaSocalypse tracker — $285B loss Feb 3, 2026; recovery = risk-off reversal"),
    ("CRWV", "CoreWeave",     "Debt-backed AI infra operator — interlocked with Nvidia + OpenAI"),
    ("IESC", "IES Holdings",  "Specialty electrical contractor — most direct data-center buildout play; revenue follows hyperscaler capex"),
    ("ACM",  "AECOM",         "Data-center construction/engineering — diversified; capex-cut sensitivity"),
    ("J",    "Jacobs Solutions", "Data-center construction/engineering — diversified; capex-cut sensitivity"),
]

# (symbol, short label, risk note, default NAV estimate)
_BDC_ROWS: list[tuple[str, str, str, float]] = [
    ("TRIN", "Trinity Capital",        "Tech-focused BDC — highest immediacy exit", 15.50),
    ("OTF",  "Blue Owl Tech Finance",  "Software BDC — watch Q3 2026 non-accruals",  17.30),
    ("SLRC", "SLR Investment Corp",    "ABL BDC — lower risk; zero non-accruals",    18.40),
]

_PHASES = [
    "Phase 0 — No signal",
    "Phase 1 — AI Infrastructure Repricing (ORCL CDS; bank commitment limits) [underway Aug 2026]",
    "Phase 2 — Hyperscaler Capex Cuts (watch Nvidia order book; MSFT/AMZN guidance revisions)",
    "Phase 3 — Broad Demand Recession (hiring freezes → consumer; Fed cutting)",
]

_PHASE1_PCT  = -45.0  # % off 1yr high → Phase 1 Signal
_WATCH_PCT   = -25.0  # % off 1yr high → Watch
_HIGH_IV     = 45.0   # 30d put IV % → elevated credit stress
_WATCH_IV    = 28.0


# ---------------------------------------------------------------------------
# Data fetchers (blocking — call via asyncio.to_thread)
# ---------------------------------------------------------------------------

def _fetch_signal(symbol: str) -> dict[str, Any]:
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="1y")
        if hist.empty:
            return {"symbol": symbol, "error": "no history"}

        current = float(hist["Close"].iloc[-1])
        high_1y = float(hist["Close"].max())
        pct_off  = (current - high_1y) / high_1y * 100

        iv: float | None = None
        try:
            exps = t.options
            cutoff = date.today() + timedelta(days=20)
            exp = next((e for e in exps if date.fromisoformat(e) >= cutoff), None)
            if exp:
                puts = t.option_chain(exp).puts
                idx  = (puts["strike"] - current).abs().idxmin()
                iv   = float(puts.loc[idx, "impliedVolatility"]) * 100
        except Exception:
            pass

        return {"symbol": symbol, "current": current, "high_1y": high_1y, "pct_off": pct_off, "iv": iv}
    except Exception as exc:
        return {"symbol": symbol, "error": str(exc)}


def _fetch_price(symbol: str) -> float | None:
    try:
        hist = yf.Ticker(symbol).history(period="2d")
        return float(hist["Close"].iloc[-1]) if not hist.empty else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

def _signal_badge(pct_off: float, iv: float | None) -> str:
    if pct_off <= _PHASE1_PCT or (iv is not None and iv >= _HIGH_IV):
        return "🔴 Phase 1 Signal"
    if pct_off <= _WATCH_PCT or (iv is not None and iv >= _WATCH_IV):
        return "🟡 Watch"
    return "🟢 Normal"


def _bdc_badge(discount_pct: float) -> str:
    if discount_pct <= -30:
        return "🔴 Extreme discount"
    if discount_pct <= -15:
        return "🟡 Wide discount"
    if discount_pct > 5:
        return "🔵 Premium — reassess"
    return "🟢 Normal"


# ---------------------------------------------------------------------------
# Tab build
# ---------------------------------------------------------------------------

def build(panel_ref) -> dict:
    with ui.tab_panel(panel_ref):

        # ── Signal tickers ──────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center justify-between"):
                with ui.column().classes("gap-0"):
                    ui.label("AI Bubble Signal Tickers").classes("text-xl font-semibold")
                    ui.label(
                        "Price vs. 1-year high + nearest-30d ATM put IV as credit-stress proxy. "
                        "Oracle CDS (not publicly available) is approximated by ORCL put skew."
                    ).classes("text-xs text-gray-400 max-w-2xl")
                signal_refresh_btn = ui.button("Refresh", icon="refresh")

            signal_status = ui.label("").classes("text-xs text-gray-500 mt-1")

            signal_table = ui.table(
                columns=[
                    {"name": "symbol",  "label": "Ticker",     "field": "symbol",  "align": "left",  "sortable": True},
                    {"name": "role",    "label": "Signal Role", "field": "role",    "align": "left"},
                    {"name": "current", "label": "Price",       "field": "current", "align": "right"},
                    {"name": "high_1y", "label": "1yr High",   "field": "high_1y", "align": "right"},
                    {"name": "pct_off", "label": "vs. High",   "field": "pct_off", "align": "right", "sortable": True},
                    {"name": "iv",      "label": "30d Put IV", "field": "iv",      "align": "right"},
                    {"name": "status",  "label": "Status",      "field": "status",  "align": "left"},
                ],
                rows=[],
            ).classes("w-full")

        # ── BDC lens ────────────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center justify-between"):
                with ui.column().classes("gap-0"):
                    ui.label("BDC Software Credit Lens").classes("text-xl font-semibold")
                    ui.label(
                        "Software-lending BDC discounts widen before non-accruals appear in filings. "
                        "NAV estimates are entered manually — update after each quarterly report."
                    ).classes("text-xs text-gray-400 max-w-2xl")
                bdc_refresh_btn = ui.button("Refresh", icon="refresh")

            bdc_table = ui.table(
                columns=[
                    {"name": "symbol",   "label": "Ticker",    "field": "symbol",   "align": "left"},
                    {"name": "note",     "label": "Risk Note",  "field": "note",     "align": "left"},
                    {"name": "price",    "label": "Price",      "field": "price",    "align": "right"},
                    {"name": "nav",      "label": "Est. NAV",  "field": "nav",      "align": "right"},
                    {"name": "discount", "label": "Discount",   "field": "discount", "align": "right", "sortable": True},
                    {"name": "status",   "label": "Status",     "field": "status",   "align": "left"},
                ],
                rows=[],
            ).classes("w-full")

            ui.label("NAV estimates (update quarterly after earnings):").classes("text-sm text-gray-400 mt-3")
            nav_inputs: dict[str, ui.number] = {}
            with ui.row().classes("gap-6 mt-1"):
                for sym, label, _, default_nav in _BDC_ROWS:
                    with ui.column().classes("gap-0"):
                        ui.label(f"{sym}").classes("text-xs text-gray-400")
                        nav_inputs[sym] = ui.number(
                            label=f"{label[:12]} NAV",
                            value=default_nav,
                            format="%.2f",
                            step=0.01,
                        ).classes("w-36")

        # ── Phase tracker ───────────────────────────────────────────────────
        with ui.card().classes("w-full"):
            ui.label("Phase Tracker").classes("text-xl font-semibold")
            ui.label("Manual — advance the phase as confirming signals arrive.").classes("text-xs text-gray-400")

            phase_select = ui.select(_PHASES, value=_PHASES[1]).classes("w-full mt-2")

            with ui.column().classes("gap-1 mt-3"):
                ui.label("Phase thresholds:").classes("text-sm text-gray-400")
                ui.label("1 → 2  Hyperscaler capex guidance revised down; Nvidia order book miss on an earnings call").classes("text-xs text-gray-500")
                ui.label("2 → 3  Tech hiring freezes spread to supplier ecosystem; Fed begins cutting").classes("text-xs text-gray-500")
                ui.label("Deploy  BDC NAV discounts >30% AND MLP unit prices fall on AI-sentiment contagion").classes("text-xs text-gray-500")

            ui.label("Evidence log:").classes("text-sm text-gray-400 mt-3")
            ui.textarea(
                placeholder=(
                    "Log phase-progression evidence as it arrives.\n"
                    "e.g. 2026-10-15 — Nvidia Q3 call: $4B order deferral mentioned\n"
                    "     2026-10-22 — MSFT revises Azure capex guidance down 15%"
                )
            ).classes("w-full")

        # ── Event handlers ──────────────────────────────────────────────────

        async def _refresh_signals() -> None:
            signal_refresh_btn.disable()
            signal_status.text = "Fetching…"
            try:
                results = await asyncio.gather(
                    *[asyncio.to_thread(_fetch_signal, sym) for sym, *_ in _SIGNAL_TICKERS]
                )
                rows = []
                for (sym, _label, role), data in zip(_SIGNAL_TICKERS, results):
                    if "error" in data:
                        rows.append({
                            "symbol": sym, "role": role,
                            "current": "—", "high_1y": "—", "pct_off": "—",
                            "iv": "—", "status": f"⚠ {data['error']}",
                        })
                        continue
                    pct = data["pct_off"]
                    iv  = data["iv"]
                    rows.append({
                        "symbol":  sym,
                        "role":    role,
                        "current": f"${data['current']:.2f}",
                        "high_1y": f"${data['high_1y']:.2f}",
                        "pct_off": f"{pct:+.1f}%",
                        "iv":      f"{iv:.1f}%" if iv is not None else "—",
                        "status":  _signal_badge(pct, iv),
                    })
                signal_table.rows = rows
                signal_table.update()
                signal_status.text = f"Last refreshed: {date.today().isoformat()}"
            except Exception as exc:
                signal_status.text = f"Error: {exc}"
            finally:
                signal_refresh_btn.enable()

        async def _refresh_bdc() -> None:
            bdc_refresh_btn.disable()
            try:
                prices = await asyncio.gather(
                    *[asyncio.to_thread(_fetch_price, sym) for sym, *_ in _BDC_ROWS]
                )
                rows = []
                for (sym, _label, note, _), price in zip(_BDC_ROWS, prices):
                    nav = float(nav_inputs[sym].value or 0)
                    if price is None or nav == 0:
                        rows.append({"symbol": sym, "note": note, "price": "—", "nav": f"${nav:.2f}", "discount": "—", "status": "—"})
                        continue
                    discount_pct = (price - nav) / nav * 100
                    rows.append({
                        "symbol":   sym,
                        "note":     note,
                        "price":    f"${price:.2f}",
                        "nav":      f"${nav:.2f}",
                        "discount": f"{discount_pct:+.1f}%",
                        "status":   _bdc_badge(discount_pct),
                    })
                bdc_table.rows = rows
                bdc_table.update()
            finally:
                bdc_refresh_btn.enable()

        signal_refresh_btn.on_click(_refresh_signals)
        bdc_refresh_btn.on_click(_refresh_bdc)

    return {}
