"""tabs/alerts_tab.py — Alerts tab: plain-English notes, classified once by
Claude into a live check (or a flagged capability gap), then polled cheaply."""

from __future__ import annotations

import asyncio

from nicegui import ui

import alerts
import historicals_store

_STATUS_COLOR = {
    "active_alert": "primary",
    "standing_question": "purple",
    "needs_tool": "warning",
    "needs_review": "negative",
}


def _fmt_dt(value) -> str:
    if not value:
        return "—"
    return value.strftime("%Y-%m-%d %H:%M")


def build(panel_ref, get_portfolio_tickers) -> dict:
    with ui.tab_panel(panel_ref):
        with ui.card().classes("w-full"):
            ui.label("Alerts").classes("text-xl font-semibold")
            ui.label(
                "Describe what you want to watch for, in plain English. Claude "
                "maps it to a live data check once — or tells you no data source "
                "exists yet, rather than guessing."
            ).classes("text-sm text-gray-400")

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center gap-2"):
                note_input = (
                    ui.input(
                        placeholder="e.g. Let me know when the dividend yield of KO rises above 4%"
                    )
                    .classes("flex-1")
                    .on("keydown.enter", lambda: add_click())
                )
                add_button = ui.button("Add", on_click=lambda: add_click())
            add_status_label = ui.label("").classes("text-sm text-gray-400")

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center justify-between"):
                ui.label("Watching").classes("text-lg font-semibold")
                check_all_button = ui.button(
                    "Check now", on_click=lambda: check_all_click()
                )
            list_column = ui.column().classes("w-full gap-2 mt-2")

        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center justify-between"):
                with ui.column().classes("gap-1"):
                    ui.label("Portfolio-wide: IV Spikes").classes("text-lg font-semibold")
                    ui.label(
                        "Scans every current holding for OTM put IV spiking above its "
                        "own trailing 30-day baseline — a Merton-model distress proxy, "
                        "no note needed."
                    ).classes("text-sm text-gray-400")
                check_portfolio_button = ui.button(
                    "Check Portfolio for IV Spikes",
                    on_click=lambda: check_portfolio_click(),
                )
            portfolio_status_label = ui.label("").classes("text-sm text-gray-400")
            portfolio_results_column = ui.column().classes("w-full gap-1 mt-2")

        def refresh_list() -> None:
            list_column.clear()
            rows = historicals_store.list_alerts()
            with list_column:
                if not rows:
                    ui.label("No alerts yet.").classes("text-sm text-gray-500")
                for row in rows:
                    _render_row(row)

        def _render_row(row: dict) -> None:
            with ui.card().classes("w-full").props("flat bordered"):
                with ui.row().classes("w-full items-start justify-between"):
                    with ui.column().classes("gap-1"):
                        ui.label(row["note_text"]).classes("font-medium")
                        ui.badge(
                            row["status"], color=_STATUS_COLOR.get(row["status"], "grey")
                        )
                        if row["summary"]:
                            ui.label(row["summary"]).classes("text-sm text-gray-400")
                        if row["last_result"]:
                            triggered = row["triggered_at"] is not None
                            ui.label(
                                ("🔔 " if triggered else "") + row["last_result"]
                            ).classes(
                                "text-sm "
                                + ("text-positive font-semibold" if triggered else "text-gray-500")
                            )
                        ui.label(
                            f"Added {_fmt_dt(row['created_at'])}"
                            + (
                                f" · last checked {_fmt_dt(row['last_checked_at'])}"
                                if row["last_checked_at"]
                                else ""
                            )
                        ).classes("text-xs text-gray-600")
                    ui.button(
                        icon="delete",
                        on_click=lambda alert_id=row["id"]: delete_click(alert_id),
                    ).props("flat dense round color=grey")

        async def add_click() -> None:
            note_text = note_input.value.strip()
            if not note_text:
                return
            note_input.value = ""
            add_button.disable()
            add_status_label.text = "Classifying against available tools…"
            try:
                await asyncio.to_thread(alerts.create_alert, note_text)
                add_status_label.text = ""
                refresh_list()
            except Exception as exc:
                ui.notify(f"Could not classify note: {exc}", color="negative")
                add_status_label.text = ""
            finally:
                add_button.enable()

        async def check_all_click() -> bool:
            check_all_button.disable()
            check_all_button.text = "Checking…"
            try:
                triggered_notes = []
                for row in historicals_store.list_alerts():
                    if row["status"] != "active_alert":
                        continue
                    triggered, result = await asyncio.to_thread(alerts.check_alert, row)
                    historicals_store.record_alert_check(row["id"], result, triggered)
                    if triggered:
                        triggered_notes.append(row["note_text"])
                refresh_list()
                if triggered_notes:
                    ui.notify(
                        f"{len(triggered_notes)} alert(s) triggered", color="positive"
                    )
                else:
                    ui.notify("Checked — nothing triggered.")
                return bool(triggered_notes)
            finally:
                check_all_button.text = "Check now"
                check_all_button.enable()

        async def check_portfolio_click() -> bool:
            check_portfolio_button.disable()
            check_portfolio_button.text = "Checking…"
            portfolio_results_column.clear()
            try:
                tickers = await get_portfolio_tickers()
                portfolio_status_label.text = f"Scanning {len(tickers)} holdings…"
                spikes = await asyncio.to_thread(alerts.check_portfolio_iv_spikes, tickers)
                with portfolio_results_column:
                    if spikes:
                        for s in spikes:
                            with ui.column().classes("gap-0"):
                                ui.label(f"🔔 {s['ticker']}").classes(
                                    "text-sm text-positive font-semibold"
                                )
                                for reason in s["reasons"]:
                                    ui.label(f"  {reason}").classes(
                                        "text-xs text-gray-400"
                                    )
                    else:
                        ui.label("No spikes detected.").classes("text-sm text-gray-500")
                portfolio_status_label.text = f"Checked {len(tickers)} holdings."
                if spikes:
                    ui.notify(f"{len(spikes)} name(s) spiking", color="positive")
                return bool(spikes)
            finally:
                check_portfolio_button.text = "Check Portfolio for IV Spikes"
                check_portfolio_button.enable()

        def delete_click(alert_id: int) -> None:
            historicals_store.delete_alert(alert_id)
            refresh_list()

        refresh_list()

    return {
        "check_now": check_all_click,
        "check_portfolio_iv_spikes": check_portfolio_click,
    }
