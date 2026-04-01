#! /Users/george/code/money/.venv/bin/python3

import asyncio
import os
import subprocess
import sys
import time
from contextlib import suppress
from datetime import datetime
from typing import Any

import yfinance as yf
from dotenv import load_dotenv
from nicegui import ui

import historicals_store
import options
import utilities
from analysis_module import PortfolioAnalysisEngine
from positions import load_portfolio_positions
from schwab_api import SchwabAPI

import tabs.analysis_tab as analysis_tab_mod
import tabs.historicals_tab as historicals_tab_mod
import tabs.income_tab as income_tab_mod
import tabs.mcp_tab as mcp_tab_mod
import tabs.options_tab as options_tab_mod
import tabs.portfolio_tab as portfolio_tab_mod

try:
    from dividend_prediction import DividendForecaster

    _dividend_prediction_import_error = None
except ImportError as exc:
    DividendForecaster = None
    _dividend_prediction_import_error = exc

try:
    from schwabdev.client import Client as _SchwabClient

    _schwab_import_error = None
except ImportError as exc:
    _SchwabClient = None
    _schwab_import_error = exc

SchwabClient: Any = _SchwabClient

load_dotenv()
historicals_store.init_db("/Users/george/code/money/portfolio.db")

dark_mode = ui.dark_mode()
dark_mode.enable()

ui.add_head_html(
    """
<style>
.portfolio-table-wrap .q-table__middle {
    max-height: 75vh;
    overflow: auto;
}

.portfolio-table-wrap thead tr th {
    position: sticky;
    top: 0;
    z-index: 2;
    background: var(--q-dark-page);
}
</style>
"""
)


def getClient():
    if SchwabClient is None:
        raise RuntimeError(f"schwabdev import failed: {_schwab_import_error}")

    return SchwabClient(
        os.getenv("SCHWAB_APP_KEY"),
        os.getenv("SCHWAB_SECRET"),
        os.getenv("callback_url"),
        os.getenv("token_filename"),
    )


def generate_report():
    time.sleep(2)
    return "Report generated successfully"


def run_report():
    result = generate_report()
    ui.notify(result)


def run_task(script: str):
    subprocess.run([sys.executable, script])
    ui.notify(f"{script} finished")


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

api: SchwabAPI | None = None
portfolio_snapshot_positions: list[Any] = []
portfolio_snapshot_rows: list[dict[str, Any]] = []
portfolio_snapshot_as_of: datetime | None = None
analysis_engine = PortfolioAnalysisEngine()

EQUITY_INCOME_TYPES = {"EQUITY", "MUTUAL_FUND", "COLLECTIVE_INVESTMENT"}
INCOME_ACCOUNT_NAMES = [
    "GeorgeTrust",
    "DebRoth",
    "Investments",
    "DebTrust",
    "GrandKids",
    "GeorgeRoth",
    "FidelityRoth",
]


def get_api() -> SchwabAPI:
    global api
    if api is None:
        api = SchwabAPI(getClient())
    return api


def fetch_quote(symbol: str) -> dict[str, Any] | None:
    return get_api().get_quote(symbol.upper())


def fetch_chain(symbol: str, contract_type: str) -> dict[str, Any]:
    return options.getChain(
        get_api(),
        name=symbol.upper(),
        put_or_call=contract_type,
    )


# ---------------------------------------------------------------------------
# Portfolio snapshot helpers
# ---------------------------------------------------------------------------

def _pe_ratio_for_row(p: Any) -> Any:
    if p.position_type in {"CALL", "PUT", "OPTION", "Cash"}:
        return "--"
    return round(float(p.pe_ratio), 2) if p.pe_ratio else "--"


def _underlying_symbol(value: Any) -> str:
    if isinstance(value, str):
        return value
    return str(getattr(value, "symbol", value))


def _build_portfolio_rows_from_positions(
    positions: list[Any],
) -> list[dict[str, Any]]:
    def _pct_pl(pl: float, market_value: float) -> float:
        cost_basis = market_value - pl
        return round((pl / cost_basis) * 100, 2) if cost_basis != 0 else 0.0

    rows = [
        {
            "symbol": p.symbol,
            "type": (
                "CEF/ETF"
                if p.position_type == "COLLECTIVE_INVESTMENT"
                else p.position_type
            ),
            "account": p.account_name,
            "underlying": _underlying_symbol(p.underlying),
            "quantity": round(float(p.quantity), 4),
            "last": round(float(p.last_price), 4),
            "market_value": round(float(p.market_value), 2),
            "pl": round(float(p.pl_total), 2),
            "pct_pl": _pct_pl(float(p.pl_total), float(p.market_value)),
            "pe_ratio": _pe_ratio_for_row(p),
        }
        for p in positions
    ]
    rows.sort(key=lambda r: float(r.get("market_value", 0.0)), reverse=True)
    return rows


async def ensure_portfolio_snapshot(
    force_refresh: bool = False,
) -> tuple[list[Any], list[dict[str, Any]]]:
    global portfolio_snapshot_positions, portfolio_snapshot_rows, portfolio_snapshot_as_of

    if portfolio_snapshot_positions and not force_refresh:
        return portfolio_snapshot_positions, portfolio_snapshot_rows

    positions = await asyncio.to_thread(
        load_portfolio_positions,
        get_api(),
        include_fidelity=True,
        include_options=True,
        include_cash=True,
    )
    rows = _build_portfolio_rows_from_positions(list(positions))

    portfolio_snapshot_positions = list(positions)
    portfolio_snapshot_rows = rows
    portfolio_snapshot_as_of = datetime.now()

    return portfolio_snapshot_positions, portfolio_snapshot_rows


def aggregate_rows_by_symbol(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol:
            continue

        if symbol not in grouped:
            grouped[symbol] = {
                "symbol": symbol,
                "type": row.get("type", ""),
                "account": "Aggregated",
                "underlying": row.get("underlying", symbol),
                "quantity": 0.0,
                "last": 0.0,
                "market_value": 0.0,
                "pl": 0.0,
                "pe_ratio": row.get("pe_ratio", "--"),
            }

        grouped[symbol]["quantity"] += float(row.get("quantity", 0.0))
        grouped[symbol]["market_value"] += float(row.get("market_value", 0.0))
        grouped[symbol]["pl"] += float(row.get("pl", 0.0))
        grouped[symbol]["last"] = float(row.get("last", 0.0))

    aggregated = list(grouped.values())
    for row in aggregated:
        row["quantity"] = round(float(row["quantity"]), 4)
        row["market_value"] = round(float(row["market_value"]), 2)
        row["pl"] = round(float(row["pl"]), 2)
        row["last"] = round(float(row["last"]), 4)
        pl = float(row["pl"])
        mv = float(row["market_value"])
        cost_basis = mv - pl
        row["pct_pl"] = round((pl / cost_basis) * 100, 2) if cost_basis != 0 else 0.0

    aggregated.sort(
        key=lambda row: float(row.get("market_value", 0.0)),
        reverse=True,
    )
    return aggregated


async def fetch_market_indicators() -> None:
    def _fetch():
        results = {}
        for ticker, key in [("^VIX", "vix"), ("^GSPC", "sp500")]:
            try:
                info = yf.Ticker(ticker).info
                results[key] = info.get("regularMarketPrice")
            except Exception:
                results[key] = None
        return results

    data = await asyncio.get_event_loop().run_in_executor(None, _fetch)
    vix_val = data.get("vix")
    sp500_val = data.get("sp500")
    vix_label.set_text(f"{vix_val:.2f}" if vix_val else "—")
    sp500_label.set_text(f"{sp500_val:,.2f}" if sp500_val else "—")


async def exit_app_click():
    ui.notify("Closing browser tab and exiting...", color="warning")
    with suppress(Exception):
        await ui.run_javascript('window.open("", "_self");window.close();')

    loop = asyncio.get_running_loop()
    loop.call_later(0.2, lambda: os._exit(0))


# ---------------------------------------------------------------------------
# Cross-tab refs — populated during build(), used by refresh handler
# ---------------------------------------------------------------------------

_portfolio_refs: dict[str, Any] = {}
_analysis_refs: dict[str, Any] = {}


async def refresh_portfolio_snapshot_click() -> None:
    refresh_portfolio_snapshot_button.disable()
    refresh_portfolio_snapshot_button.text = "Refreshing..."
    portfolio_snapshot_status_value.text = "Refreshing shared portfolio snapshot..."
    try:
        positions, rows = await ensure_portfolio_snapshot(force_refresh=True)

        set_rows = _portfolio_refs.get("set_snapshot_rows")
        if set_rows:
            set_rows(rows)

        analysis_table = _analysis_refs.get("analysis_rows_table")
        analysis_ans = _analysis_refs.get("analysis_answer")
        if analysis_table is not None:
            analysis_table.rows = []
            analysis_table.update()
        if analysis_ans is not None:
            analysis_ans.value = "Shared portfolio snapshot refreshed."

        portfolio_snapshot_status_value.text = (
            f"Refresh successful: {len(positions)} positions, "
            f"{len(rows)} rows @ "
            f'{portfolio_snapshot_as_of.strftime("%Y-%m-%d %H:%M:%S")}'
        )
        ui.notify(
            f"Refreshed shared snapshot: {len(positions)} positions",
            color="positive",
        )
        await fetch_market_indicators()
    except Exception as exc:
        portfolio_snapshot_status_value.text = f"Refresh failed: {exc}"
        ui.notify(f"Portfolio snapshot refresh failed: {exc}", color="negative")
    finally:
        refresh_portfolio_snapshot_button.text = "Refresh Portfolio Snapshot"
        refresh_portfolio_snapshot_button.enable()


# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------

with ui.tabs().classes("w-full") as tabs:
    dashboard_tab = ui.tab("Dashboard")
    portfolio_tab = ui.tab("Portfolio")
    options_tab = ui.tab("Options")
    historicals_tab = ui.tab("Historicals")
    income_tab = ui.tab("Income")
    analysis_tab = ui.tab("Analysis")
    mcp_tab = ui.tab("MCP")

with ui.tab_panels(tabs, value=dashboard_tab).classes("w-full"):
    with ui.tab_panel(dashboard_tab):
        with ui.card().classes("w-full"):
            ui.label("Dashboard").classes("text-xl font-semibold")
            with ui.row().classes("items-center gap-4"):
                ui.label("VIX:").classes("font-semibold")
                vix_label = ui.label("…").classes("text-lg")
                ui.label("S&P 500:").classes("font-semibold ml-4")
                sp500_label = ui.label("…").classes("text-lg")
            with ui.row().classes("items-center gap-3"):
                refresh_portfolio_snapshot_button = ui.button(
                    "Refresh Portfolio Snapshot",
                    on_click=refresh_portfolio_snapshot_click,
                )
                portfolio_snapshot_status_value = ui.label(
                    "No shared snapshot loaded"
                ).classes("text-sm")
            ui.button("Exit Application", on_click=exit_app_click)

    _portfolio_refs.update(
        portfolio_tab_mod.build(
            portfolio_tab,
            ensure_portfolio_snapshot,
            fetch_quote,
            aggregate_rows_by_symbol,
        )
    )

    options_tab_mod.build(options_tab, fetch_chain)

    historicals_tab_mod.build(
        historicals_tab,
        ensure_portfolio_snapshot,
        get_api,
        historicals_store,
        utilities,
    )

    income_tab_mod.build(
        income_tab,
        ensure_portfolio_snapshot,
        utilities,
        EQUITY_INCOME_TYPES,
        INCOME_ACCOUNT_NAMES,
        DividendForecaster,
        _dividend_prediction_import_error,
    )

    _analysis_refs.update(
        analysis_tab_mod.build(
            analysis_tab,
            analysis_engine,
            ensure_portfolio_snapshot,
        )
    )

    mcp_tab_mod.build(mcp_tab)

ui.run(port=8000, reload=False)
