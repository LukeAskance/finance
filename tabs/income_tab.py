"""tabs/income_tab.py — Dividend income analysis and portfolio projection tab."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

from nicegui import ui


def build(
    panel_ref,
    ensure_snapshot_fn,
    utilities,
    EQUITY_INCOME_TYPES: set[str],
    INCOME_ACCOUNT_NAMES: list[str],
    DividendForecaster,
    dividend_import_error,
) -> None:
    """Build the Income tab UI.

    Args:
        panel_ref: The ui.tab widget returned by ui.tab("Income").
        ensure_snapshot_fn: async callable(force_refresh) -> (positions, rows).
        utilities: the utilities module.
        EQUITY_INCOME_TYPES: set of position type strings for income positions.
        INCOME_ACCOUNT_NAMES: ordered list of account names for checkboxes.
        DividendForecaster: class or None if import failed.
        dividend_import_error: the ImportError if DividendForecaster is None.
    """
    _refs: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_iso_date(value: Any) -> date | None:
        if not value:
            return None
        text = str(value).strip()
        if not text or text == "UnknownDay":
            return None
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except ValueError:
            return None

    def _build_symbol_dividend_payload(
        symbol: str, shares: float, forecast_years: int
    ) -> dict[str, Any]:
        if DividendForecaster is None:
            raise RuntimeError(
                f"dividend_prediction unavailable: {dividend_import_error}"
            )
        forecaster = DividendForecaster.from_edgar(symbol, shares=shares)
        result = forecaster.project(years=forecast_years)

        history_series = forecaster._raw.sort_index()
        historical_points = [
            (pd_ts.to_pydatetime(), float(amount))
            for pd_ts, amount in history_series.items()
        ]
        historical_rows = [
            {
                "date": pd_ts.strftime("%Y-%m-%d"),
                "div_per_share": round(float(amount), 4),
            }
            for pd_ts, amount in history_series.tail(80).items()
        ]
        annual_rows = [
            {
                "year": int(pd_ts.year),
                "annual_div_per_share": round(float(total), 4),
                "annual_income": round(float(total) * shares, 2),
            }
            for pd_ts, total in forecaster.annual.tail(12).items()
        ]
        base = result.scenarios.get("base")
        bear = result.scenarios.get("bear")
        bull = result.scenarios.get("bull")
        projection_rows: list[dict[str, Any]] = []
        if base and bear and bull:
            projection_rows.extend(
                {
                    "year": year,
                    "bear_income": round(bear.annual_dividends[idx] * shares, 2),
                    "base_income": round(base.annual_dividends[idx] * shares, 2),
                    "bull_income": round(bull.annual_dividends[idx] * shares, 2),
                }
                for idx, year in enumerate(base.years)
            )
        return {
            "summary": result.summary(),
            "historical_points": historical_points,
            "historical_rows": historical_rows,
            "annual_rows": annual_rows,
            "projection_rows": projection_rows,
        }

    def _render_income_plot(points: list[tuple[datetime, float]]) -> None:
        import matplotlib.pyplot as plt

        _refs["plot_host"].clear()
        with _refs["plot_host"]:
            if not points:
                ui.label("No dividend history available.").classes("text-sm text-orange")
                return
            with ui.pyplot(figsize=(16, 5), close=False).classes("w-full"):
                xs = [item[0] for item in points]
                ys = [item[1] for item in points]
                plt.plot(xs, ys, marker="o", linewidth=1.5)
                plt.grid(True, linestyle="--", alpha=0.6)
                plt.xlabel("Payment Date")
                plt.ylabel("Dividend / Share ($)")
                plt.title("Historical Dividends")
                plt.tight_layout()

    def _selected_accounts() -> set[str]:
        return {
            name
            for name, cb in _refs["account_checkboxes"].items()
            if bool(cb.value)
        }

    def _portfolio_income_projection(
        selected_accounts: set[str], positions: list[Any]
    ) -> tuple[dict[str, float], list[dict[str, Any]]]:
        today = date.today()
        horizon = today + timedelta(days=365)
        events: list[dict[str, Any]] = []

        for pos in positions:
            if selected_accounts and pos.account_name not in selected_accounts:
                continue
            if pos.position_type not in EQUITY_INCOME_TYPES:
                continue
            if pos.quantity <= 0 or pos.div_pay_amount <= 0 or pos.div_freq <= 0:
                continue
            start = _parse_iso_date(pos.next_div_pay_date) or _parse_iso_date(pos.div_pay_date)
            if start is None:
                continue
            interval_days = max(1, int(round(365 / pos.div_freq)))
            pay_date = start
            amount = float(pos.quantity) * float(pos.div_pay_amount)
            while pay_date <= horizon:
                days_out = (pay_date - today).days
                if days_out >= 0:
                    events.append({
                        "date": pay_date.isoformat(),
                        "days_out": days_out,
                        "symbol": pos.symbol,
                        "account": pos.account_name,
                        "amount": round(amount, 2),
                    })
                pay_date = pay_date + timedelta(days=interval_days)

        events.sort(key=lambda row: (row["date"], row["symbol"], row["account"]))
        month_income = sum(row["amount"] for row in events if row["days_out"] <= 30)
        quarter_income = sum(row["amount"] for row in events if row["days_out"] <= 90)
        year_income = sum(row["amount"] for row in events if row["days_out"] <= 365)
        return (
            {"month": round(month_income, 2), "quarter": round(quarter_income, 2), "year": round(year_income, 2)},
            events,
        )

    # ------------------------------------------------------------------
    # Click handlers
    # ------------------------------------------------------------------

    async def analyze_click() -> None:
        symbol = (_refs["symbol_input"].value or "").strip().upper()
        if not symbol:
            ui.notify("Enter a ticker symbol", color="warning")
            return
        shares_value = _refs["shares_input"].value
        try:
            shares = float(shares_value) if shares_value not in (None, "") else 1.0
        except ValueError:
            ui.notify("Shares must be a valid number", color="warning")
            return
        if shares <= 0:
            ui.notify("Shares must be greater than zero", color="warning")
            return
        forecast_years = utilities.coerce_positive_int(_refs["years_input"].value)
        if forecast_years is None:
            ui.notify("Forecast years must be a positive integer", color="warning")
            return

        _refs["analyze_button"].disable()
        _refs["analyze_button"].text = "Analyzing..."
        try:
            payload = await asyncio.to_thread(
                _build_symbol_dividend_payload, symbol, shares, forecast_years
            )
            _refs["summary"].value = payload["summary"]
            _refs["history_table"].rows = payload["historical_rows"]
            _refs["history_table"].update()
            _refs["annual_table"].rows = payload["annual_rows"]
            _refs["annual_table"].update()
            _refs["projection_table"].rows = payload["projection_rows"]
            _refs["projection_table"].update()
            _render_income_plot(payload["historical_points"])
        except Exception as exc:
            ui.notify(f"Income analysis error: {exc}", color="negative")
        finally:
            _refs["analyze_button"].text = "Analyze"
            _refs["analyze_button"].enable()

    async def refresh_income_click() -> None:
        _refs["income_button"].disable()
        _refs["income_button"].text = "Refreshing..."
        try:
            selected_accounts = _selected_accounts()
            if not selected_accounts:
                _refs["month_value"].text = "$0.00"
                _refs["quarter_value"].text = "$0.00"
                _refs["year_value"].text = "$0.00"
                _refs["events_table"].rows = []
                _refs["events_table"].update()
                ui.notify("Select at least one account", color="warning")
                return
            positions, _ = await ensure_snapshot_fn(force_refresh=False)
            totals, events = await asyncio.to_thread(
                _portfolio_income_projection, selected_accounts, positions
            )
            _refs["month_value"].text = f"${totals['month']:,.2f}"
            _refs["quarter_value"].text = f"${totals['quarter']:,.2f}"
            _refs["year_value"].text = f"${totals['year']:,.2f}"
            _refs["events_table"].rows = events[:500]
            _refs["events_table"].update()
        except Exception as exc:
            ui.notify(f"Portfolio income refresh failed: {exc}", color="negative")
        finally:
            _refs["income_button"].text = "Refresh Portfolio Income"
            _refs["income_button"].enable()

    def on_account_change(_: Any = None) -> None:
        asyncio.create_task(refresh_income_click())

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    with ui.tab_panel(panel_ref):
        with ui.column().classes("w-full gap-4"):
            with ui.card().classes("w-full"):
                ui.label("Stock Dividend History & Forecast").classes("text-xl font-semibold")
                with ui.row().classes("items-center gap-3 w-full"):
                    _refs["symbol_input"] = ui.input("Ticker", placeholder="AAPL").classes("w-32")
                    _refs["shares_input"] = (
                        ui.input("Shares", value="100")
                        .props("type=number min=0.0001 step=0.0001")
                        .classes("w-32")
                    )
                    _refs["years_input"] = (
                        ui.input("Forecast years", value="5")
                        .props("type=number min=1")
                        .classes("w-32")
                    )
                    _refs["analyze_button"] = ui.button("Analyze", on_click=analyze_click)

                _refs["summary"] = ui.textarea(label="Summary").classes("w-full")
                _refs["summary"].props("readonly")

                with ui.card().classes("w-full"):
                    _refs["plot_host"] = ui.column().classes("w-full")
                    ui.label("Run Analyze to render dividend history").classes("text-sm text-gray")

                with ui.row().classes("w-full gap-4 items-start no-wrap"):
                    with ui.column().classes("w-1/2 min-w-0"):
                        _refs["history_table"] = ui.table(
                            columns=[
                                {"name": "date", "label": "Date", "field": "date"},
                                {"name": "div_per_share", "label": "Div/Share", "field": "div_per_share", "align": "right"},
                            ],
                            rows=[],
                        ).classes("w-full")
                        _refs["history_table"].props('pagination={"rowsPerPage":10}')
                    with ui.column().classes("w-1/2 min-w-0"):
                        _refs["annual_table"] = ui.table(
                            columns=[
                                {"name": "year", "label": "Year", "field": "year"},
                                {"name": "annual_div_per_share", "label": "Annual Div/Share", "field": "annual_div_per_share", "align": "right"},
                                {"name": "annual_income", "label": "Annual Income", "field": "annual_income", "align": "right"},
                            ],
                            rows=[],
                        ).classes("w-full")
                        _refs["annual_table"].props('pagination={"rowsPerPage":10}')

                _refs["projection_table"] = ui.table(
                    columns=[
                        {"name": "year", "label": "Year", "field": "year"},
                        {"name": "bear_income", "label": "Bear Income", "field": "bear_income", "align": "right"},
                        {"name": "base_income", "label": "Base Income", "field": "base_income", "align": "right"},
                        {"name": "bull_income", "label": "Bull Income", "field": "bull_income", "align": "right"},
                    ],
                    rows=[],
                ).classes("w-full")
                _refs["projection_table"].props('pagination={"rowsPerPage":10}')

            with ui.card().classes("w-full"):
                ui.label("Portfolio Income Projection").classes("text-xl font-semibold")
                with ui.row().classes("items-center gap-3 w-full"):
                    ui.label("Accounts:").classes("text-sm font-semibold")
                    _refs["account_checkboxes"] = {
                        name: ui.checkbox(name, value=True, on_change=on_account_change)
                        for name in INCOME_ACCOUNT_NAMES
                    }
                _refs["income_button"] = ui.button(
                    "Refresh Portfolio Income", on_click=refresh_income_click
                )
                with ui.row().classes("items-center gap-8"):
                    with ui.column().classes("gap-1"):
                        ui.label("Next 30 days").classes("text-sm")
                        _refs["month_value"] = ui.label("$0.00").classes("text-lg font-semibold")
                    with ui.column().classes("gap-1"):
                        ui.label("Next 90 days").classes("text-sm")
                        _refs["quarter_value"] = ui.label("$0.00").classes("text-lg font-semibold")
                    with ui.column().classes("gap-1"):
                        ui.label("Next 12 months").classes("text-sm")
                        _refs["year_value"] = ui.label("$0.00").classes("text-lg font-semibold")

                _refs["events_table"] = ui.table(
                    columns=[
                        {"name": "date", "label": "Pay Date", "field": "date"},
                        {"name": "days_out", "label": "Days Out", "field": "days_out", "align": "right"},
                        {"name": "symbol", "label": "Symbol", "field": "symbol"},
                        {"name": "account", "label": "Account", "field": "account"},
                        {"name": "amount", "label": "Amount", "field": "amount", "align": "right"},
                    ],
                    rows=[],
                ).classes("w-full")
                _refs["events_table"].props('pagination={"rowsPerPage":12}')
