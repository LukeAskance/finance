"""tabs/fred_tab.py — FRED Economic Data plots tab."""

from __future__ import annotations

import asyncio
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
from nicegui import ui


# ---------------------------------------------------------------------------
# Render helpers (operate on a fig; no plt.show())
# ---------------------------------------------------------------------------

def _render_timeseries(fig: Any, df: pd.DataFrame, start_date: str, title: str) -> None:
    start = pd.to_datetime(start_date)
    data = df[df["date"] >= start].copy()
    data["mom"] = data["value"].pct_change() * 100
    data["yoy"] = data["value"].pct_change(periods=12) * 100

    ax1, ax2, ax3 = fig.subplots(3, 1, height_ratios=[1.5, 1, 1])

    ax1.plot(data["date"], data["value"], marker="o", markersize=3)
    ax1.set_title(title)
    ax1.set_ylabel("Value")
    ax1.tick_params(axis="x", rotation=45)
    ax1.grid(True, linestyle="--", alpha=0.7)

    ax2.plot(data["date"][1:], data["mom"][1:], marker="o", markersize=3, color="tab:blue")
    ax2.axhline(0, color="black", linewidth=0.5)
    ax2.set_title("Month-over-Month %")
    ax2.set_ylabel("MoM (%)")
    ax2.tick_params(axis="x", rotation=45)
    ax2.grid(True, linestyle="--", alpha=0.7)

    ax3.plot(data["date"][12:], data["yoy"][12:], marker="o", markersize=3, color="tab:green")
    ax3.axhline(0, color="black", linewidth=0.5)
    ax3.set_title("Year-over-Year %")
    ax3.set_xlabel("Date")
    ax3.set_ylabel("YoY (%)")
    ax3.tick_params(axis="x", rotation=45)
    ax3.grid(True, linestyle="--", alpha=0.7)

    fig.tight_layout()


def _render_dual(
    fig: Any,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    title1: str,
    title2: str,
    months: int = 60,
) -> None:
    end = df1["date"].max()
    start = end - pd.DateOffset(months=months)
    d1 = df1[df1["date"] >= start]
    d2 = df2[df2["date"] >= start]

    ax1 = fig.add_subplot(111)
    ax1.set_ylabel(title1, color="tab:blue")
    ax1.plot(d1["date"], d1["value"], color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.grid(True, linestyle="-", alpha=0.2)

    ax2 = ax1.twinx()
    ax2.set_ylabel(title2, color="tab:orange")
    ax2.plot(d2["date"], d2["value"], color="tab:orange")
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    fig.autofmt_xdate()
    fig.tight_layout()


def _render_quad(
    fig: Any,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
    title1: str,
    title2: str,
    title3: str,
    title4: str,
    months: int = 72,
) -> None:
    end = df1["date"].max()
    start = end - pd.DateOffset(months=months - 1)
    dfs = [df[df["date"] >= start] for df in (df1, df2, df3, df4)]

    ax1 = fig.add_subplot(111)
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax4 = ax1.twinx()
    ax3.spines["right"].set_position(("outward", 60))
    ax4.spines["right"].set_position(("outward", 120))

    colors = ["tab:blue", "tab:orange", "tab:green", "tab:red"]
    axes = [ax1, ax2, ax3, ax4]
    titles = [title1, title2, title3, title4]

    for ax, df, color, t in zip(axes, dfs, colors, titles):
        ax.plot(df["date"], df["value"], color=color)
        ax.set_ylabel(t, color=color)
        ax.tick_params(axis="y", labelcolor=color)

    ax1.grid(True, linestyle="-", alpha=0.2)
    fig.autofmt_xdate()
    fig.tight_layout()


# ---------------------------------------------------------------------------
# Plot catalogue: (label, fetch_fn, render_fn, figsize)
# fetch_fn  → returns whatever render_fn expects as extra args after fig
# render_fn → (fig, *fetched_data) → None
# ---------------------------------------------------------------------------

def _build_catalogue() -> list[tuple[str, Any, Any, tuple[int, int]]]:
    from pyfredapi import get_series  # type: ignore[attr-defined]

    def dual(s1, s2, t1, t2, months=60):
        def fetch():
            return get_series(s1), get_series(s2)
        def render(fig, d1, d2):
            _render_dual(fig, d1, d2, t1, t2, months)
        return fetch, render, (12, 6)

    def ts(series_id, title, start="2015-01-01"):
        def fetch():
            return (get_series(series_id),)
        def render(fig, df):
            _render_timeseries(fig, df, start, title)
        return fetch, render, (8, 8)

    def quad(s1, s2, s3, s4, t1, t2, t3, t4, months=72):
        def fetch():
            return get_series(s1), get_series(s2), get_series(s3), get_series(s4)
        def render(fig, d1, d2, d3, d4):
            _render_quad(fig, d1, d2, d3, d4, t1, t2, t3, t4, months)
        return fetch, render, (15, 8)

    entries: list[tuple[str, Any, Any, tuple[int, int]]] = [
        ("M2 Real vs Govt Expenditures",
         *dual("M2REAL", "W068RCQ027SBEA", "M2 Real", "Govt Expenditures")),
        ("10-Year Market Yield vs CPI",
         *dual("DFII10", "CPIAUCSL", "10-Year Market Yield", "CPI All Urban")),
        ("10-Year Market Yield vs Expected Inflation",
         *dual("DFII10", "EXPINF10YR", "10-Year Market Yield", "10-Year Expected Inflation")),
        ("GDP Now vs Consumer Sentiment (36 mo)",
         *dual("GDPNOW", "UMCSENT", "GDP Now (Atlanta Fed)", "Consumer Sentiment", 36)),
        ("Hourly Earnings: Production vs All Employees",
         *dual("AHETPI", "AHEMAN", "Earnings – Production", "Earnings – All")),
        ("Hourly Earnings (Production) vs Food CPI",
         *dual("AHETPI", "CUSR0000SAF11", "Avg Hourly Earnings", "CPI Food at Home")),
        ("M2 Real vs CPI",
         *dual("M2REAL", "CPIAUCSL", "M2 Real", "CPI All Urban")),
        ("Quad: M2 / CPI / Biz Confidence / Consumer Sentiment",
         *quad("M2REAL", "CPIAUCSL", "BSCICP03USM665S", "UMCSENT",
               "M2 Real", "CPI", "Biz Confidence", "Consumer Sentiment")),
        ("Total US Debt (time series)",      *ts("GFDEBTN",           "Total US Debt")),
        ("Real M2 (time series)",            *ts("M2REAL",            "Real M2 Money Supply")),
        ("CPI (time series)",                *ts("CPIAUCSL",          "US Consumer CPI")),
        ("Business Confidence (time series)",*ts("BSCICP03USM665S",   "Business Confidence Indicator")),
        ("Consumer Sentiment (time series)", *ts("UMCSENT",           "Consumer Sentiment")),
        ("Unemployment Rate (time series)",  *ts("UNRATE",            "US Unemployment Rate")),
    ]
    return entries


# ---------------------------------------------------------------------------
# Tab build
# ---------------------------------------------------------------------------

def build(panel_ref) -> None:
    catalogue = _build_catalogue()
    labels = [entry[0] for entry in catalogue]
    label_to_entry = {entry[0]: entry for entry in catalogue}

    with ui.tab_panel(panel_ref):
        with ui.card().classes("w-full"):
            ui.label("FRED Economic Data").classes("text-xl font-semibold")
            ui.label(
                "Select a chart and click Plot — data is fetched live from FRED."
            ).classes("text-sm text-gray-400")

        with ui.card().classes("w-full"):
            with ui.row().classes("items-center gap-4 w-full"):
                plot_select = ui.select(
                    options=labels,
                    value=labels[0],
                    label="Chart",
                ).classes("flex-1")
                plot_button = ui.button("Plot")
                status_label = ui.label("").classes("text-sm text-gray-400")

        plot_area = ui.column().classes("w-full")

        async def on_plot() -> None:
            label = plot_select.value
            if not label:
                return

            _, fetch_fn, render_fn, figsize = label_to_entry[label]

            plot_button.disable()
            plot_button.text = "Fetching…"
            status_label.text = "Fetching from FRED…"
            plot_area.clear()

            try:
                fetched = await asyncio.to_thread(fetch_fn)
            except Exception as exc:
                ui.notify(f"FRED fetch error: {exc}", color="negative")
                status_label.text = f"Error: {exc}"
                plot_button.text = "Plot"
                plot_button.enable()
                return

            status_label.text = "Rendering…"
            with plot_area:
                with ui.pyplot(figsize=figsize) as p:
                    render_fn(p.fig, *fetched)

            status_label.text = "Done"
            plot_button.text = "Plot"
            plot_button.enable()

        plot_button.on_click(on_plot)
