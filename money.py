#! /Users/george/code/money/.venv/bin/python3

import asyncio
import csv
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
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
from positions import discover_equity_names, load_portfolio_positions
from schwab_api import SchwabAPI, get_shared_api

import tabs.alerts_tab as alerts_tab_mod
import tabs.analysis_tab as analysis_tab_mod
import tabs.fred_tab as fred_tab_mod
import tabs.historicals_tab as historicals_tab_mod
import tabs.income_tab as income_tab_mod
import tabs.mcp_tab as mcp_tab_mod
import tabs.options_tab as options_tab_mod
import tabs.portfolio_tab as portfolio_tab_mod


try:
    from dividend_prediction import DividendForecaster

    _dividend_prediction_import_error = None
except ImportError as exc:
    DividendForecaster = None  # type: ignore[assignment]
    _dividend_prediction_import_error = exc

load_dotenv()
historicals_store.init_db("/Users/george/code/money/portfolio.db")
_prev_vix, _prev_sp500, _prev_gld, _prev_fedfunds = historicals_store.get_last_market_indicators()

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
    return get_shared_api()


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
            "description": p.description,
            "underlying": _underlying_symbol(p.underlying),
            "quantity": round(float(p.quantity), 4),
            "last": round(float(p.last_price), 4),
            "market_value": round(float(p.market_value), 2),
            "pl": round(float(p.pl_total), 2),
            "pct_pl": _pct_pl(float(p.pl_total), float(p.market_value)),
            "pe_ratio": _pe_ratio_for_row(p),
            "div_yield": round(float(p.div_yield), 2) if p.div_yield else None,
        }
        for p in positions
    ]
    rows.sort(key=lambda r: float(r.get("market_value", 0.0)), reverse=True)
    return rows


def _fetch_company_names(symbols: list[str]) -> dict[str, str]:
    equity_symbols = [s for s in symbols if len(s) <= 5 and s.isalpha()]
    if not equity_symbols:
        return {}

    cached = historicals_store.get_cached_company_names(equity_symbols)
    missing = [s for s in equity_symbols if s not in cached]

    fetched: dict[str, str] = {}
    if missing:
        def fetch_name(sym: str) -> tuple[str, str]:
            try:
                info = yf.Ticker(sym).info
                return sym, info.get("longName") or info.get("shortName") or ""
            except Exception:
                return sym, ""

        with ThreadPoolExecutor(max_workers=min(8, len(missing))) as pool:
            fetched = dict(pool.map(fetch_name, missing))
        new_names = {sym: name for sym, name in fetched.items() if name}
        if new_names:
            historicals_store.set_company_names(new_names)

    return {**cached, **fetched}


async def ensure_portfolio_snapshot(
    force_refresh: bool = False,
) -> tuple[list[Any], list[dict[str, Any]]]:
    global portfolio_snapshot_positions, portfolio_snapshot_rows
    global portfolio_snapshot_as_of

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

    datecode = datetime.now().strftime("%y%m%d")
    csv_dir = os.getenv("JiminyFinanceDir", os.path.dirname(__file__))
    csv_path = os.path.join(csv_dir, f"Portfolio-{datecode}.csv")
    symbols = [r["symbol"] for r in rows]
    company_names = await asyncio.to_thread(_fetch_company_names, symbols)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "company_name", "account", "share_quantity", "latest_price", "market_value", "cost_basis"])
        for row in rows:
            cost_basis = round(float(row.get("market_value", 0.0)) - float(row.get("pl", 0.0)), 2)
            writer.writerow([row["symbol"], company_names.get(row["symbol"], ""), row.get("account", ""), row["quantity"], row["last"], row["market_value"], cost_basis])

    return portfolio_snapshot_positions, portfolio_snapshot_rows


async def get_portfolio_equity_tickers() -> list[str]:
    positions, _ = await ensure_portfolio_snapshot()
    return discover_equity_names(list(positions))


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
                "description": row.get("description", ""),
                "type": row.get("type", ""),
                "account": "Aggregated",
                "underlying": row.get("underlying", symbol),
                "quantity": 0.0,
                "last": 0.0,
                "market_value": 0.0,
                "pl": 0.0,
                "pe_ratio": row.get("pe_ratio", "--"),
                "div_yield": row.get("div_yield"),
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
    global _prev_vix, _prev_sp500, _prev_gld, _prev_fedfunds

    def _fetch():
        import httpx
        results: dict[str, Any] = {}

        def fetch_price(pair: tuple[str, str]) -> tuple[str, Any]:
            ticker, key = pair
            try:
                return key, yf.Ticker(ticker).info.get("regularMarketPrice")
            except Exception:
                return key, None

        pairs = [("^VIX", "vix"), ("^GSPC", "sp500"), ("GLD", "gld")]
        with ThreadPoolExecutor(max_workers=len(pairs)) as pool:
            results.update(dict(pool.map(fetch_price, pairs)))
        try:
            r = httpx.get(
                "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
                "/v2/accounting/od/debt_to_penny",
                params={"sort": "-record_date", "page[size]": 1},
                timeout=10,
            )
            r.raise_for_status()
            rows = r.json().get("data") or []
            if rows:
                results["debt"] = {
                    "date":   rows[0]["record_date"],
                    "amount": float(rows[0]["tot_pub_debt_out_amt"]),
                }
        except Exception:
            results["debt"] = None
        try:
            r = httpx.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": "EFFR",
                    "api_key": os.getenv("FRED_API_KEY"),
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 1,
                },
                timeout=10,
            )
            r.raise_for_status()
            observations = r.json().get("observations") or []
            results["fedfunds"] = float(observations[0]["value"])
        except Exception:
            results["fedfunds"] = None
        return results

    data = await asyncio.get_event_loop().run_in_executor(None, _fetch)
    vix_val   = data.get("vix")
    sp500_val = data.get("sp500")
    gld_val   = data.get("gld")
    debt_data = data.get("debt")
    fedfunds_val = data.get("fedfunds")

    # VIX — higher is worse, so red = up
    vix_label.set_text(f"{vix_val:.2f}" if vix_val else "—")
    if vix_val is not None and _prev_vix is not None:
        d = vix_val - _prev_vix
        sign = "+" if d >= 0 else ""
        vix_change_label.set_text(f"({sign}{d:.2f})")
        vix_change_label.classes(
            remove="text-green-400 text-red-400",
            add="text-red-400" if d >= 0 else "text-green-400",
        )
    else:
        vix_change_label.set_text("")
    if vix_val is not None:
        _prev_vix = vix_val

    # S&P 500 — higher is better, so green = up
    sp500_label.set_text(f"{sp500_val:,.2f}" if sp500_val else "—")
    if sp500_val is not None and _prev_sp500 is not None:
        d = sp500_val - _prev_sp500
        sign = "+" if d >= 0 else ""
        sp500_change_label.set_text(f"({sign}{d:,.2f})")
        sp500_change_label.classes(
            remove="text-green-400 text-red-400",
            add="text-green-400" if d >= 0 else "text-red-400",
        )
    else:
        sp500_change_label.set_text("")
    if sp500_val is not None:
        _prev_sp500 = sp500_val

    # GLD — higher is better, green = up
    gld_label.set_text(f"{gld_val:,.2f}" if gld_val else "—")
    if gld_val is not None and _prev_gld is not None:
        d = gld_val - _prev_gld
        sign = "+" if d >= 0 else ""
        gld_change_label.set_text(f"({sign}{d:,.2f})")
        gld_change_label.classes(
            remove="text-green-400 text-red-400",
            add="text-green-400" if d >= 0 else "text-red-400",
        )
    else:
        gld_change_label.set_text("")
    if gld_val is not None:
        _prev_gld = gld_val

    # Fed Funds Rate — higher is worse, so red = up
    fedfunds_label.set_text(f"{fedfunds_val:.2f}%" if fedfunds_val is not None else "—")
    if fedfunds_val is not None and _prev_fedfunds is not None:
        d = fedfunds_val - _prev_fedfunds
        sign = "+" if d >= 0 else ""
        fedfunds_change_label.set_text(f"({sign}{d:.2f})")
        fedfunds_change_label.classes(
            remove="text-green-400 text-red-400",
            add="text-red-400" if d >= 0 else "text-green-400",
        )
    else:
        fedfunds_change_label.set_text("")
    if fedfunds_val is not None:
        _prev_fedfunds = fedfunds_val

    # Persist for next session
    if vix_val is not None or sp500_val is not None or gld_val is not None or fedfunds_val is not None:
        await asyncio.to_thread(
            historicals_store.save_market_indicators,
            vix_val, sp500_val, gld_val, fedfunds_val
        )

    # Debt to the Penny
    if debt_data:
        amt = debt_data["amount"]
        debt_value_label.set_text(f"${amt:,.2f}")
        debt_date_label.set_text(debt_data["date"])
    else:
        debt_value_label.set_text("—")
        debt_date_label.set_text("")


async def check_alerts_click():
    triggered = False
    if (check_now := _alerts_refs.get("check_now")) is not None:
        triggered = await check_now() or triggered
    if (check_portfolio := _alerts_refs.get("check_portfolio_iv_spikes")) is not None:
        triggered = await check_portfolio() or triggered
    if triggered:
        tabs.value = alerts_tab


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
_historicals_refs: dict[str, Any] = {}
_alerts_refs: dict[str, Any] = {}


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

        # Portfolio value summary
        live_total = sum(float(r.get("market_value", 0.0)) for r in rows)
        portfolio_live_value.text = f"${live_total:,.2f}"

        prev = await asyncio.to_thread(historicals_store.get_latest_portfolio_total)
        if prev:
            prev_date, prev_total = prev
            delta = live_total - prev_total
            pct = (delta / prev_total * 100) if prev_total else 0.0
            sign = "+" if delta >= 0 else ""
            portfolio_prev_value.text = f"${prev_total:,.2f} ({prev_date})"
            portfolio_change_value.text = f"{sign}${delta:,.2f} ({sign}{pct:.2f}%)"
            portfolio_change_value.classes(
                remove="text-green-400 text-red-400",
                add="text-green-400" if delta >= 0 else "text-red-400",
            )
        else:
            portfolio_prev_value.text = "no snapshot in DB"
            portfolio_change_value.text = "—"

        # Persist today's snapshot to the historicals DB (after the
        # day-over-day comparison above, which reads the prior snapshot).
        capture = await asyncio.to_thread(
            historicals_store.capture_snapshot_from_loaded_positions,
            list(positions),
        )

        # Re-render the Historicals totals charts now that a new snapshot exists.
        refresh_totals = _historicals_refs.get("refresh_totals")
        if refresh_totals:
            await refresh_totals()

        portfolio_snapshot_status_value.text = (
            f"Refresh successful: {len(positions)} positions, "
            f"{len(rows)} rows @ "
            f'{portfolio_snapshot_as_of.strftime("%Y-%m-%d %H:%M:%S")}'
            f" (DB: {capture['positions']} positions saved for {capture['date']})"
        )
        ui.notify(
            f"Refreshed shared snapshot: {len(positions)} positions",
            color="positive",
        )
        await fetch_market_indicators()
    except RuntimeError:
        pass  # client disconnected before UI could be updated
    except Exception as exc:
        try:
            portfolio_snapshot_status_value.text = f"Refresh failed: {exc}"
            ui.notify(f"Portfolio snapshot refresh failed: {exc}", color="negative")
        except RuntimeError:
            pass
    finally:
        try:
            refresh_portfolio_snapshot_button.text = "Refresh Portfolio Snapshot"
            refresh_portfolio_snapshot_button.enable()
        except RuntimeError:
            pass


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
    fred_tab = ui.tab("Fred")
    mcp_tab = ui.tab("MCP")
    alerts_tab = ui.tab("Alerts")

with ui.tab_panels(tabs, value=dashboard_tab).classes("w-full"):
    with ui.tab_panel(dashboard_tab):
        with ui.card().classes("w-full"):
            ui.label("Dashboard").classes("text-xl font-semibold")
            with ui.row().classes("items-center gap-4"):
                ui.label("VIX:").classes("font-semibold")
                vix_label = ui.label("…").classes("text-lg")
                vix_change_label = ui.label("").classes("text-sm")
                ui.label("S&P 500:").classes("font-semibold ml-4")
                sp500_label = ui.label("…").classes("text-lg")
                sp500_change_label = ui.label("").classes("text-sm")
                ui.label("GLD:").classes("font-semibold ml-4")
                gld_label = ui.label("…").classes("text-lg")
                gld_change_label = ui.label("").classes("text-sm")
                ui.label("Fed Funds Rate:").classes("font-semibold ml-4")
                fedfunds_label = ui.label("…").classes("text-lg")
                fedfunds_change_label = ui.label("").classes("text-sm")
            with ui.row().classes("items-center gap-6 mt-1"):
                with ui.column().classes("gap-0"):
                    ui.label("Portfolio (live)").classes("text-xs text-gray-400")
                    portfolio_live_value = ui.label("—").classes(
                        "text-lg font-semibold"
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Previous snapshot").classes("text-xs text-gray-400")
                    portfolio_prev_value = ui.label("—").classes("text-lg")
                with ui.column().classes("gap-0"):
                    ui.label("Change").classes("text-xs text-gray-400")
                    portfolio_change_value = ui.label("—").classes("text-lg")
                with ui.column().classes("gap-0"):
                    ui.label("Debt to the Penny").classes("text-xs text-gray-400")
                    debt_value_label = ui.label("—").classes("text-lg font-semibold")
                    debt_date_label = ui.label("").classes("text-xs text-gray-500")
            with ui.row().classes("items-center gap-3"):
                refresh_portfolio_snapshot_button = ui.button(
                    "Refresh Portfolio Snapshot",
                    on_click=refresh_portfolio_snapshot_click,
                )
                portfolio_snapshot_status_value = ui.label(
                    "No shared snapshot loaded"
                ).classes("text-sm")
            ui.button("Check Alerts", on_click=check_alerts_click)
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

    _historicals_refs.update(
        historicals_tab_mod.build(
            historicals_tab,
            get_api,
            historicals_store,
            utilities,
        )
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

    fred_tab_mod.build(fred_tab)

    mcp_tab_mod.build(mcp_tab)

    _alerts_refs.update(alerts_tab_mod.build(alerts_tab, get_portfolio_equity_tickers))

ui.run(port=8000, reload=False)
