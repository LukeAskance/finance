"""tabs/historicals_tab.py — Historical prices and portfolio snapshots tab."""

from __future__ import annotations

import asyncio
from typing import Any

from nicegui import ui


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        text = str(value).strip()
        return int(float(text)) if text else None
    except (TypeError, ValueError):
        return None


def build(panel_ref, get_api_fn, historicals_store, utilities) -> dict[str, Any]:
    """Build the Historicals tab UI.

    Args:
        panel_ref: The ui.tab widget returned by ui.tab("Historicals").
        get_api_fn: callable() -> SchwabAPI.
        historicals_store: the historicals_store module.
        utilities: the utilities module.

    Returns:
        A dict of cross-tab hooks, e.g. ``refresh_totals`` — an async callable
        that re-renders the portfolio/account totals charts. The Portfolio-tab
        refresh handler calls this after persisting a new snapshot.
    """
    _refs: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Plot renderers
    # ------------------------------------------------------------------

    def _render_historicals_plot(symbol_series: dict, normalize: bool) -> None:
        _refs["plot_host"].clear()
        with _refs["plot_host"]:
            if not symbol_series:
                ui.label("No historical data found for the selected symbols.").classes(
                    "text-sm text-orange"
                )
                return
            with ui.pyplot(figsize=(16, 7), close=False).classes("w-full"):
                utilities.draw_historicals_series(
                    symbol_series,
                    normalize=normalize,
                    title="Historical Stock Prices",
                )

    def _render_portfolio_totals_plot(rows: list[dict]) -> None:
        import plotly.graph_objects as go

        _refs["totals_host"].clear()
        with _refs["totals_host"]:
            if not rows:
                ui.label("No portfolio totals yet. Capture a daily snapshot.").classes(
                    "text-sm text-orange"
                )
                return
            xs = [r["date"] for r in rows]
            ys = [float(r["total_market_value"]) for r in rows]
            fig = go.Figure(
                go.Scatter(
                    x=xs, y=ys, mode="lines+markers", line={"width": 2},
                    hovertemplate="%{x}<br>$%{y:,.2f}<extra></extra>",
                )
            )
            fig.update_layout(
                title="Portfolio Total Over Time",
                xaxis_title="UTC Date", yaxis_title="Portfolio Total ($)",
                yaxis_tickformat="$,.0f", hovermode="x unified",
                margin={"l": 60, "r": 20, "t": 40, "b": 40},
            )
            ui.plotly(fig).classes("w-full")

    def _render_account_totals_plot(series: dict) -> None:
        import plotly.graph_objects as go

        _refs["accounts_host"].clear()
        with _refs["accounts_host"]:
            if not series:
                ui.label("No account totals yet.").classes("text-sm text-orange")
                return
            fig = go.Figure()
            for account, points in series.items():
                xs = [d for d, _ in points]
                ys = [float(v) for _, v in points]
                fig.add_trace(
                    go.Scatter(
                        x=xs, y=ys, mode="lines+markers", name=account,
                        hovertemplate=f"{account}<br>%{{x}}<br>$%{{y:,.2f}}<extra></extra>",
                    )
                )
            fig.update_layout(
                title="Account Totals Over Time",
                xaxis_title="UTC Date", yaxis_title="Account Total ($)",
                yaxis_tickformat="$,.0f", hovermode="x unified",
                margin={"l": 60, "r": 20, "t": 40, "b": 40},
            )
            ui.plotly(fig).classes("w-full")

    # ------------------------------------------------------------------
    # Click handlers
    # ------------------------------------------------------------------

    async def plot_historicals_click(silent_if_incomplete: bool = False) -> None:
        raw_symbols = (_refs["symbols_input"].value or "").strip()
        symbols = utilities.parse_symbols(raw_symbols)
        if not symbols:
            if not silent_if_incomplete:
                ui.notify("Enter one or more ticker symbols", color="warning")
            return
        days = utilities.coerce_positive_int(_refs["days_input"].value)
        if days is None:
            if not silent_if_incomplete:
                ui.notify("Enter a valid positive number of days", color="warning")
            return
        mode = (_refs["mode_select"].value or "denormalize").strip().lower()
        normalize = mode == "normalize"
        _refs["plot_button"].disable()
        _refs["plot_button"].text = "Plotting..."
        try:
            symbol_series = await asyncio.to_thread(
                utilities.collect_historical_series, get_api_fn(), symbols, days
            )
            _render_historicals_plot(symbol_series, normalize)
        except Exception as exc:
            ui.notify(f"Unable to plot historicals: {exc}", color="negative")
        finally:
            _refs["plot_button"].text = "Plot"
            _refs["plot_button"].enable()

    async def on_mode_change(_: Any = None) -> None:
        await plot_historicals_click(silent_if_incomplete=True)

    async def refresh_totals_click() -> None:
        days = _coerce_int(_refs["totals_days_input"].value)
        if days is None or days <= 0:
            ui.notify("Days must be a positive integer", color="warning")
            return
        _refs["refresh_button"].disable()
        _refs["refresh_button"].text = "Refreshing..."
        try:
            payload = await asyncio.to_thread(historicals_store.get_totals_payload, days)
            portfolio_rows = payload.get("portfolio_rows", [])
            account_rows = payload.get("account_rows", [])
            account_series = payload.get("account_series", {})
            _render_portfolio_totals_plot(portfolio_rows)
            _render_account_totals_plot(account_series)
            _refs["status_value"].text = (
                f"Loaded {len(portfolio_rows)} portfolio points, "
                f"{len(account_rows)} account rows"
            )
        except Exception as exc:
            _refs["status_value"].text = f"Historical refresh error: {exc}"
        finally:
            _refs["refresh_button"].text = "Display Totals"
            _refs["refresh_button"].enable()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    with ui.tab_panel(panel_ref):
        with ui.column().classes("w-full gap-4"):
            with ui.card().classes("w-full"):
                ui.label("Historical Stock Prices").classes("text-xl font-semibold")
                _refs["symbols_input"] = ui.input(
                    "Ticker symbols", placeholder="AAPL or AAPL,MSFT,GOOG"
                ).props('spellcheck=false').classes("w-full")
                with ui.row().classes("items-center gap-3 w-full"):
                    _refs["days_input"] = (
                        ui.input("Days", value="1825", on_change=on_mode_change)
                        .props("type=number min=1")
                        .classes("w-32")
                    )
                    _refs["mode_select"] = ui.select(
                        options=["denormalize", "normalize"],
                        value="denormalize",
                        label="Mode",
                        on_change=on_mode_change,
                    ).classes("w-48")
                    _refs["plot_button"] = ui.button("Plot", on_click=plot_historicals_click)

            with ui.card().classes("w-full"):
                _refs["plot_host"] = ui.column().classes("w-full")
                ui.label("Click Plot to render chart").classes("text-sm text-gray")

            with ui.card().classes("w-full"):
                ui.label("Portfolio History (UTC Daily Snapshots)").classes("text-xl font-semibold")
                with ui.row().classes("items-center gap-3 w-full"):
                    _refs["totals_days_input"] = (
                        ui.input("Lookback days", value="365")
                        .props("type=number min=1")
                        .classes("w-32")
                    )
                    _refs["refresh_button"] = ui.button(
                        "Display Totals", on_click=refresh_totals_click
                    )
                    _refs["status_value"] = ui.label("No snapshots yet").classes("text-sm")

                _refs["totals_host"] = ui.column().classes("w-full")
                _refs["accounts_host"] = ui.column().classes("w-full")

    return {"refresh_totals": refresh_totals_click}
