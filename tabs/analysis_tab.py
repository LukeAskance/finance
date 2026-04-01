"""tabs/analysis_tab.py — Portfolio Analysis tab."""

from __future__ import annotations

import asyncio
from typing import Any

from nicegui import ui

from financials import get_financials, get_insider_transactions
from institutional import get_institutional_ownership

_DEFAULT_MODELS = {
    "claude": "claude-sonnet-4-20250514",
    "perplexity": "sonar",
}


def build(panel_ref, analysis_engine, ensure_snapshot_fn) -> dict[str, Any]:
    """Build the Analysis tab UI.

    Args:
        panel_ref: The ui.tab widget returned by ui.tab("Analysis").
        analysis_engine: PortfolioAnalysisEngine instance.
        ensure_snapshot_fn: async callable(force_refresh) -> (positions, rows).

    Returns:
        dict with "analysis_rows_table" and "analysis_answer" widgets for cross-tab use.
    """
    _refs: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    async def refresh_analysis_snapshot_click() -> None:
        _refs["refresh_button"].disable()
        _refs["refresh_button"].text = "Rebuilding..."
        try:
            positions, _ = await ensure_snapshot_fn(force_refresh=False)
            count = await asyncio.to_thread(
                analysis_engine.refresh_snapshot_from_positions,
                positions,
            )
            as_of = (
                analysis_engine.as_of.strftime("%Y-%m-%d %H:%M:%S")
                if analysis_engine.as_of
                else "-"
            )
            _refs["status_value"].text = (
                f"{count} aggregated positions loaded @ {as_of}"
            )
            _refs["rows_table"].rows = []
            _refs["rows_table"].update()
            _refs["answer"].value = "Analysis rebuilt from shared portfolio snapshot."
        except Exception as exc:
            _refs["answer"].value = f"Analysis refresh error: {exc}"
        finally:
            _refs["refresh_button"].text = "Rebuild Analysis"
            _refs["refresh_button"].enable()

    async def ask_analysis_click() -> None:
        question = _refs["question_input"].value.strip()
        if not question:
            ui.notify("Enter a question first", color="warning")
            return
        _refs["ask_button"].disable()
        _refs["ask_button"].text = "Thinking..."
        try:
            answer_text, rows = await asyncio.to_thread(
                analysis_engine.answer_question,
                question,
            )
            _refs["answer"].value = answer_text
            _refs["rows_table"].rows = rows
            _refs["rows_table"].update()
        except Exception as exc:
            _refs["answer"].value = f"Analysis error: {exc}"
        finally:
            _refs["ask_button"].text = "Ask"
            _refs["ask_button"].enable()

    async def ask_analysis_llm_click() -> None:
        question = _refs["question_input"].value.strip()
        if not question:
            ui.notify("Enter a question first", color="warning")
            return
        _refs["llm_button"].disable()
        _refs["llm_button"].text = "Querying LLM..."
        try:
            answer_text, rows, tool_results = await asyncio.to_thread(
                analysis_engine.ask_llm,
                question,
                _refs["provider_select"].value or "claude",
                _refs["model_input"].value or "",
                bool(_refs["grounded_toggle"].value),
                bool(_refs["general_mode_toggle"].value),
            )
            _refs["answer"].value = answer_text
            _refs["rows_table"].rows = rows
            _refs["rows_table"].update()
            if "get_institutional_ownership" in tool_results:
                _refs["inst_ownership_table"].rows = tool_results[
                    "get_institutional_ownership"
                ]
                _refs["inst_ownership_table"].update()
        except Exception as exc:
            _refs["answer"].value = f"LLM analysis error: {exc}"
        finally:
            _refs["llm_button"].text = "Ask LLM"
            _refs["llm_button"].enable()

    async def get_financials_click() -> None:
        ticker = _refs["financials_input"].value.strip().upper()
        if not ticker:
            ui.notify("Enter a ticker symbol first", color="warning")
            return
        _refs["financials_button"].disable()
        _refs["financials_button"].text = "Fetching..."
        try:
            f, ins = await asyncio.gather(
                asyncio.to_thread(get_financials, ticker),
                asyncio.to_thread(get_insider_transactions, ticker),
            )
            _refs["financials_price"].value = (
                f"${f.price:.2f}" if f.price is not None else "—"
            )
            _refs["financials_eps"].value = (
                f"${f.eps:.2f}" if f.eps is not None else "—"
            )
            _refs["financials_yield"].value = (
                f"{f.dividend_yield_pct:.2f}%"
                if f.dividend_yield_pct is not None
                else "—"
            )
            _refs["financials_pe"].value = (
                str(f.pe_ratio) if f.pe_ratio is not None else "—"
            )
            if f.cash_per_share is not None:
                _refs["financials_cash_per_share"].value = f"${f.cash_per_share:.2f}"
            else:
                _refs["financials_cash_per_share"].value = "—"
                ui.notify(
                    f"Cash/share unavailable: {f.cash_per_share_error}",
                    color="warning",
                )
            _refs["financials_ins_buys"].value = f"{ins.buys} / {ins.buys_shares:,}"
            _refs["financials_ins_sells"].value = (
                f"{ins.sells} / {ins.sells_shares:,}"
            )
            _refs["financials_ins_10b51"].value = (
                f"{ins.sells_10b51} / {ins.sells_10b51_shares:,}"
            )
        except Exception as exc:
            ui.notify(f"Financials fetch error: {exc}", color="negative")
        finally:
            _refs["financials_button"].text = "Fetch"
            _refs["financials_button"].enable()

    async def get_inst_ownership_click() -> None:
        ticker = _refs["inst_ownership_input"].value.strip().upper()
        if not ticker:
            ui.notify("Enter a ticker symbol first", color="warning")
            return
        _refs["inst_ownership_button"].disable()
        _refs["inst_ownership_button"].text = "Fetching..."
        try:
            rows = await asyncio.to_thread(get_institutional_ownership, ticker)
            _refs["inst_ownership_table"].rows = rows
            _refs["inst_ownership_table"].update()
        except Exception as exc:
            ui.notify(f"Ownership fetch error: {exc}", color="negative")
        finally:
            _refs["inst_ownership_button"].text = "Get Ownership"
            _refs["inst_ownership_button"].enable()

    def on_provider_change(_: Any = None) -> None:
        provider = (_refs["provider_select"].value or "claude").strip().lower()
        _refs["model_input"].value = _DEFAULT_MODELS.get(
            provider, _DEFAULT_MODELS["claude"]
        )

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    with ui.tab_panel(panel_ref):
        with ui.card().classes("w-full"):
            ui.label("Portfolio Analysis").classes("text-xl font-semibold")
            with ui.row().classes("items-center gap-2"):
                _refs["refresh_button"] = ui.button(
                    "Rebuild Analysis", on_click=refresh_analysis_snapshot_click
                )
                _refs["status_value"] = ui.label("No snapshot loaded").classes("text-sm")

            _refs["question_input"] = ui.input(
                "Ask about portfolio data",
                placeholder="e.g., Which positions have more than 100 shares?",
            ).classes("w-full")

            with ui.row().classes("items-center gap-2"):
                _refs["provider_select"] = ui.select(
                    options=["claude", "perplexity"],
                    value="claude",
                    label="Provider",
                    on_change=on_provider_change,
                ).classes("w-40")
                _refs["model_input"] = ui.input(
                    "Model",
                    value=_DEFAULT_MODELS["claude"],
                ).classes("w-64")
                _refs["grounded_toggle"] = ui.checkbox("Grounded only", value=True)
                _refs["general_mode_toggle"] = ui.checkbox(
                    "General assistant mode", value=False
                )

            with ui.row().classes("items-center gap-2"):
                _refs["ask_button"] = ui.button("Ask", on_click=ask_analysis_click)
                _refs["llm_button"] = ui.button(
                    "Ask LLM", on_click=ask_analysis_llm_click
                )

            _refs["answer"] = ui.textarea(label="Answer")
            _refs["answer"].props("readonly").classes("w-full")

            analysis_columns = [
                {"name": "symbol", "label": "Symbol", "field": "symbol"},
                {"name": "account", "label": "Account", "field": "account"},
                {"name": "type", "label": "Type", "field": "type"},
                {
                    "name": "quantity",
                    "label": "Qty",
                    "field": "quantity",
                    "align": "right",
                },
                {
                    "name": "market_value",
                    "label": "Mkt Value",
                    "field": "market_value",
                    "align": "right",
                },
                {"name": "sector", "label": "Sector", "field": "sector"},
                {"name": "industry", "label": "Industry", "field": "industry"},
            ]
            with ui.element("div").classes("w-full max-h-[45vh] overflow-auto"):
                _refs["rows_table"] = ui.table(
                    columns=analysis_columns,
                    rows=[],
                ).classes("w-max min-w-full")
            _refs["rows_table"].props(
                'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
            )

        with ui.card().classes("w-full"):
            ui.label("Financials").classes("text-xl font-semibold")
            with ui.row().classes("items-center gap-2"):
                _refs["financials_input"] = ui.input(
                    "Ticker", placeholder="e.g. AAPL"
                ).classes("w-32")
                _refs["financials_button"] = ui.button(
                    "Fetch", on_click=get_financials_click
                )
            with ui.row().classes("items-center gap-4 mt-1 flex-wrap"):
                with ui.column().classes("gap-0"):
                    ui.label("Price").classes("text-xs text-gray-400")
                    _refs["financials_price"] = (
                        ui.input(value="—").props("readonly").classes("w-24")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("EPS").classes("text-xs text-gray-400")
                    _refs["financials_eps"] = (
                        ui.input(value="—").props("readonly").classes("w-20")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Yield%").classes("text-xs text-gray-400")
                    _refs["financials_yield"] = (
                        ui.input(value="—").props("readonly").classes("w-20")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("P/E").classes("text-xs text-gray-400")
                    _refs["financials_pe"] = (
                        ui.input(value="—").props("readonly").classes("w-20")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Cash/Share").classes("text-xs text-gray-400")
                    _refs["financials_cash_per_share"] = (
                        ui.input(value="—").props("readonly").classes("w-24")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Ins Buys (txns/sh)").classes("text-xs text-gray-400")
                    _refs["financials_ins_buys"] = (
                        ui.input(value="—").props("readonly").classes("w-32")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("Ins Sells (txns/sh)").classes("text-xs text-gray-400")
                    _refs["financials_ins_sells"] = (
                        ui.input(value="—").props("readonly").classes("w-32")
                    )
                with ui.column().classes("gap-0"):
                    ui.label("10b5-1 Sells (txns/sh)").classes("text-xs text-gray-400")
                    _refs["financials_ins_10b51"] = (
                        ui.input(value="—").props("readonly").classes("w-36")
                    )

        with ui.card().classes("w-full"):
            ui.label("Institutional Ownership").classes("text-xl font-semibold")
            with ui.row().classes("items-center gap-2"):
                _refs["inst_ownership_input"] = ui.input(
                    "Ticker", placeholder="e.g. AAPL"
                ).classes("w-32")
                _refs["inst_ownership_button"] = ui.button(
                    "Get Ownership", on_click=get_inst_ownership_click
                )
            inst_ownership_columns = [
                {
                    "name": "holder",
                    "label": "Institution",
                    "field": "holder",
                    "sortable": True,
                },
                {
                    "name": "shares",
                    "label": "Shares",
                    "field": "shares",
                    "sortable": True,
                    "align": "right",
                },
                {
                    "name": "pct_out",
                    "label": "% Owned",
                    "field": "pct_out",
                    "sortable": True,
                    "align": "right",
                },
                {
                    "name": "value",
                    "label": "Value ($)",
                    "field": "value",
                    "sortable": True,
                    "align": "right",
                },
                {
                    "name": "date_reported",
                    "label": "Date Reported",
                    "field": "date_reported",
                    "sortable": True,
                },
            ]
            with ui.element("div").classes("w-full overflow-auto"):
                _refs["inst_ownership_table"] = ui.table(
                    columns=inst_ownership_columns,
                    rows=[],
                ).classes("w-max min-w-full")
            _refs["inst_ownership_table"].props(
                'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
            )
            _refs["inst_ownership_table"].add_slot(
                "body-cell-pct_out",
                '<q-td :props="props">{{ props.value }}%</q-td>',
            )

    return {
        "analysis_rows_table": _refs["rows_table"],
        "analysis_answer": _refs["answer"],
    }
