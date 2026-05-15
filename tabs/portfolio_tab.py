"""tabs/portfolio_tab.py — Portfolio positions and quote tab."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from nicegui import ui


def build(
    panel_ref,
    ensure_snapshot_fn,
    fetch_quote_fn,
    aggregate_rows_by_symbol_fn,
) -> dict[str, Any]:
    """Build the Portfolio tab UI.

    Args:
        panel_ref: The ui.tab widget returned by ui.tab("Portfolio").
        ensure_snapshot_fn: async callable(force_refresh) -> (positions, rows).
        fetch_quote_fn: callable(symbol) -> quote dict or None.
        aggregate_rows_by_symbol_fn: callable(rows) -> aggregated rows.

    Returns:
        dict with:
            "portfolio_table": the portfolio table widget.
            "set_snapshot_rows": callable(rows) to update the table from a fresh snapshot.
    """
    _refs: dict[str, Any] = {}
    _state = {"original_rows": [], "is_aggregated": False}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _set_quote_summary(
        last: str = "-",
        bid: str = "-",
        ask: str = "-",
        open_price: str = "-",
        high: str = "-",
        low: str = "-",
        close: str = "-",
    ) -> None:
        _refs["last_value"].text = last
        _refs["bid_value"].text = bid
        _refs["ask_value"].text = ask
        _refs["open_value"].text = open_price
        _refs["high_value"].text = high
        _refs["low_value"].text = low
        _refs["close_value"].text = close

    def _quote_number(quote_data: dict[str, Any], *keys: str) -> str:
        for key in keys:
            value = quote_data.get(key)
            if value is not None:
                return f"{value}"
        return "-"

    def set_snapshot_rows(rows: list[dict[str, Any]]) -> None:
        _state["original_rows"] = [dict(r) for r in rows]
        _state["is_aggregated"] = False
        _refs["aggregate_button"].text = "Aggregate"
        _refs["portfolio_table"].rows = [dict(r) for r in rows]
        _refs["portfolio_table"].update()

    # ------------------------------------------------------------------
    # Click handlers
    # ------------------------------------------------------------------

    async def get_quote_click() -> None:
        symbol = _refs["symbol_input"].value.strip()
        if not symbol:
            ui.notify("Enter a ticker symbol first", color="warning")
            return

        _set_quote_summary("Loading...")
        _refs["quote_output"].value = "Loading..."
        try:
            result = await asyncio.to_thread(fetch_quote_fn, symbol)
            if not result:
                _set_quote_summary()
                _refs["quote_output"].value = (
                    f"No quote returned for {symbol.upper()}"
                )
                return

            symbol_key = symbol.upper()
            quote_data = result.get(symbol_key, {}).get("quote", {})
            _set_quote_summary(
                _quote_number(quote_data, "lastPrice", "mark"),
                _quote_number(quote_data, "bidPrice", "bid"),
                _quote_number(quote_data, "askPrice", "ask"),
                _quote_number(quote_data, "openPrice", "open"),
                _quote_number(quote_data, "highPrice", "high"),
                _quote_number(quote_data, "lowPrice", "low"),
                _quote_number(quote_data, "closePrice", "close"),
            )
            _refs["quote_output"].value = json.dumps(result, indent=2)
        except Exception as exc:
            _set_quote_summary()
            _refs["quote_output"].value = f"Quote error: {exc}"

    def toggle_aggregate_click() -> None:
        if not _state["is_aggregated"]:
            rows = list(_refs["portfolio_table"].rows or [])
            if not rows:
                ui.notify("Load portfolio rows first", color="warning")
                return
            aggregated_rows = aggregate_rows_by_symbol_fn(rows)
            _refs["portfolio_table"].rows = aggregated_rows
            _refs["portfolio_table"].update()
            _state["is_aggregated"] = True
            _refs["aggregate_button"].text = "Separate"
            ui.notify(
                f"Aggregated to {len(aggregated_rows)} symbols", color="positive"
            )
        else:
            if not _state["original_rows"]:
                ui.notify("No original rows to restore yet", color="warning")
                return
            _refs["portfolio_table"].rows = [
                dict(r) for r in _state["original_rows"]
            ]
            _refs["portfolio_table"].update()
            _state["is_aggregated"] = False
            _refs["aggregate_button"].text = "Aggregate"
            ui.notify(
                f"Restored {len(_state['original_rows'])} original rows",
                color="positive",
            )

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    with ui.tab_panel(panel_ref):
        with ui.row().classes("w-full items-start gap-4 no-wrap"):
            with ui.column().classes("w-1/5 min-w-[220px]"):
                with ui.card().classes("w-full"):
                    ui.label("Schwab Quote").classes("text-xl font-semibold")
                    _refs["symbol_input"] = (
                        ui.input("Symbol").props('clearable spellcheck=false').classes("w-40")
                    )
                    ui.button("Get Quote", on_click=get_quote_click)

                    with ui.row():
                        ui.label("Last:")
                        _refs["last_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Bid:")
                        _refs["bid_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Ask:")
                        _refs["ask_value"] = ui.label("-").classes("font-semibold")

                    with ui.row():
                        ui.label("Open:")
                        _refs["open_value"] = ui.label("-").classes("font-semibold")
                        ui.label("High:")
                        _refs["high_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Low:")
                        _refs["low_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Close:")
                        _refs["close_value"] = ui.label("-").classes("font-semibold")

                    _refs["quote_output"] = ui.textarea(label="Quote JSON")
                    _refs["quote_output"].props("readonly").classes("w-full")

                with ui.card().classes("w-full"):
                    ui.label("Portfolio Actions").classes("text-xl font-semibold")
                    _refs["aggregate_button"] = ui.button(
                        "Aggregate", on_click=toggle_aggregate_click
                    )

            with ui.column().classes("flex-1 min-w-0"):
                with ui.card().classes("w-full"):
                    ui.label("Portfolio").classes("text-xl font-semibold")
                    portfolio_columns = [
                        {
                            "name": "symbol",
                            "label": "Symbol",
                            "field": "symbol",
                            "sortable": True,
                            "style": "width: 10ch; max-width: 10ch;",
                        },
                        {
                            "name": "type",
                            "label": "Type",
                            "field": "type",
                            "sortable": True,
                            "style": "width: 7ch; max-width: 7ch;",
                        },
                        {
                            "name": "account",
                            "label": "Account",
                            "field": "account",
                            "sortable": True,
                            "style": "width: 9ch; max-width: 9ch;",
                        },
                        {
                            "name": "underlying",
                            "label": "Underlying",
                            "field": "underlying",
                            "sortable": True,
                            "style": "width: 9ch; max-width: 9ch;",
                        },
                        {
                            "name": "quantity",
                            "label": "Qty",
                            "field": "quantity",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 8ch; max-width: 8ch;",
                        },
                        {
                            "name": "last",
                            "label": "Last",
                            "field": "last",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 8ch; max-width: 8ch;",
                        },
                        {
                            "name": "market_value",
                            "label": "Mkt Val",
                            "field": "market_value",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 9ch; max-width: 9ch;",
                        },
                        {
                            "name": "pl",
                            "label": "P/L",
                            "field": "pl",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 8ch; max-width: 8ch;",
                        },
                        {
                            "name": "pct_pl",
                            "label": "%P/L",
                            "field": "pct_pl",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 7ch; max-width: 7ch;",
                        },
                        {
                            "name": "pe_ratio",
                            "label": "P/E",
                            "field": "pe_ratio",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 6ch; max-width: 6ch;",
                        },
                        {
                            "name": "div_yield",
                            "label": "Div Yld%",
                            "field": "div_yield",
                            "sortable": True,
                            "align": "right",
                            "style": "width: 8ch; max-width: 8ch;",
                        },
                    ]
                    with ui.element("div").classes("w-full portfolio-table-wrap"):
                        _refs["portfolio_table"] = ui.table(
                            columns=portfolio_columns,
                            rows=[],
                        ).classes("w-max min-w-full")
                    _refs["portfolio_table"].props(
                        'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
                    )

    return {
        "portfolio_table": _refs["portfolio_table"],
        "set_snapshot_rows": set_snapshot_rows,
    }
