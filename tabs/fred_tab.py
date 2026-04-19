"""tabs/fred_tab.py — FRED Economic Data plots tab."""

from __future__ import annotations

import asyncio
from typing import Any

import matplotlib.patches as mpatches
import pandas as pd
from nicegui import ui


# ---------------------------------------------------------------------------
# Render helpers (operate on a fig; no plt.show())
# ---------------------------------------------------------------------------

def _shade_recessions(ax: Any, df_rec: pd.DataFrame) -> None:
    """Shade NBER recession periods (USREC series) on an axis."""
    in_rec = False
    rec_start = None
    for _, row in df_rec.iterrows():
        if row["value"] == 1 and not in_rec:
            in_rec = True
            rec_start = row["date"]
        elif row["value"] == 0 and in_rec:
            in_rec = False
            ax.axvspan(rec_start, row["date"], color="gray", alpha=0.25, label="_nolegend_")
    if in_rec and rec_start is not None:
        ax.axvspan(rec_start, df_rec["date"].max(), color="gray", alpha=0.25, label="_nolegend_")


def _render_yield_spread(
    fig: Any,
    df_spread: pd.DataFrame,
    df_rec: pd.DataFrame,
    title: str,
    months: int = 360,
) -> None:
    """Yield curve spread with zero line and NBER recession shading."""
    end = df_spread["date"].max()
    start = end - pd.DateOffset(months=months)
    data = df_spread[df_spread["date"] >= start].copy()
    rec = df_rec[df_rec["date"] >= start].copy()

    ax = fig.add_subplot(111)
    ax.plot(data["date"], data["value"], color="tab:blue", linewidth=1.2)
    ax.axhline(0, color="red", linewidth=1.0, linestyle="--", label="Zero")
    ax.fill_between(
        data["date"],
        data["value"],
        0,
        where=(data["value"] < 0),
        color="red",
        alpha=0.25,
        label="Inverted",
    )
    ax.fill_between(
        data["date"],
        data["value"],
        0,
        where=(data["value"] >= 0),
        color="tab:blue",
        alpha=0.12,
    )
    _shade_recessions(ax, rec)

    rec_patch = mpatches.Patch(color="gray", alpha=0.35, label="NBER Recession")
    inv_patch = mpatches.Patch(color="red", alpha=0.4, label="Inverted")
    ax.legend(handles=[rec_patch, inv_patch], fontsize=8)

    ax.set_title(title)
    ax.set_ylabel("Spread (percentage points)")
    ax.set_xlabel("Date")
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.autofmt_xdate()
    fig.tight_layout()


def _render_zscore_comparison(
    fig: Any,
    series_data: list[tuple[pd.DataFrame, str, str]],
    df_rec: pd.DataFrame,
    months: int = 420,
) -> None:
    """Plot multiple FRED series normalized to z-scores on a single axis."""
    ax = fig.add_subplot(111)

    if not series_data:
        ax.text(
            0.5, 0.5, "No series selected",
            ha="center", va="center", transform=ax.transAxes,
            fontsize=14, color="gray",
        )
        return

    end = max(df["date"].max() for df, _, _ in series_data)
    start = end - pd.DateOffset(months=months)
    rec = df_rec[df_rec["date"] >= start].copy()

    for df, label, color in series_data:
        monthly = (
            df.set_index("date")["value"]
            .resample("ME")
            .mean()
            .dropna()
        )
        zscore = (monthly - monthly.mean()) / monthly.std()
        visible = zscore[zscore.index >= pd.Timestamp(start)]
        ax.plot(visible.index, visible.values, label=label, color=color, linewidth=1.5)

    ax.axhline(0, color="gray", linewidth=0.8, linestyle="--", alpha=0.7)
    _shade_recessions(ax, rec)

    rec_patch = mpatches.Patch(color="gray", alpha=0.35, label="NBER Recession")
    existing_handles, existing_labels = ax.get_legend_handles_labels()
    ax.legend(handles=existing_handles + [rec_patch], fontsize=9)

    ax.set_title("Leading Indicators — Z-Score Normalized (σ)")
    ax.set_ylabel("Z-score (σ)")
    ax.set_xlabel("Date")
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.autofmt_xdate()
    fig.tight_layout()


def _render_sahm_rule(
    fig: Any,
    df_sahm: pd.DataFrame,
    df_other: pd.DataFrame | None,
    other_label: str,
    months: int = 300,
) -> None:
    """Plot Sahm Rule indicator with 0.5 threshold; optionally overlay another series."""
    end = df_sahm["date"].max()
    start = end - pd.DateOffset(months=months)
    sahm = df_sahm[df_sahm["date"] >= start].copy()

    if df_other is not None:
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twinx()
    else:
        ax1 = fig.add_subplot(111)
        ax2 = None

    ax1.plot(sahm["date"], sahm["value"], color="tab:red", linewidth=1.4, label="Sahm Rule")
    ax1.axhline(0.5, color="red", linewidth=1.0, linestyle="--", label="Trigger (0.5)")
    ax1.fill_between(
        sahm["date"], sahm["value"], 0.5,
        where=(sahm["value"] >= 0.5),
        color="red", alpha=0.25, label="Signal active",
    )
    ax1.set_ylabel("Sahm Rule (pp)", color="tab:red")
    ax1.tick_params(axis="y", labelcolor="tab:red")
    ax1.set_title("Sahm Rule Recession Indicator" + (f" vs {other_label}" if df_other is not None else ""))
    ax1.grid(True, linestyle="--", alpha=0.4)

    if ax2 is not None and df_other is not None:
        other = df_other[df_other["date"] >= start].copy()
        ax2.plot(other["date"], other["value"], color="tab:blue", linewidth=1.2, label=other_label)
        ax2.set_ylabel(other_label, color="tab:blue")
        ax2.tick_params(axis="y", labelcolor="tab:blue")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=8)
    else:
        ax1.legend(fontsize=8)

    ax1.set_xlabel("Date")
    fig.autofmt_xdate()
    fig.tight_layout()


def _render_timeseries(fig: Any, df: pd.DataFrame, start_date: str, title: str) -> None:
    start = pd.to_datetime(start_date)
    data = df[df["date"] >= start].copy()
    data["mom"] = data["value"].pct_change(fill_method=None) * 100
    data["yoy"] = data["value"].pct_change(periods=12, fill_method=None) * 100

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

    def yield_spread(series_id, title, months=360):
        """Yield spread with NBER recession shading."""
        def fetch():
            return get_series(series_id), get_series("USREC")
        def render(fig, df_spread, df_rec):
            _render_yield_spread(fig, df_spread, df_rec, title, months)
        return fetch, render, (14, 5)

    def sahm(other_id=None, other_label="", months=300):
        """Sahm Rule with optional overlay series."""
        def fetch():
            other = get_series(other_id) if other_id else None
            return get_series("SAHMREALTIME"), other
        def render(fig, df_sahm, df_other):
            _render_sahm_rule(fig, df_sahm, df_other, other_label, months)
        return fetch, render, (14, 5)

    entries: list[tuple[str, Any, Any, tuple[int, int]]] = [
        # ── Yield Curve Analysis ──────────────────────────────────────────────
        ("Yield Curve ▸ 10Y-3M Spread + Recessions",
         *yield_spread("T10Y3M", "10-Year minus 3-Month Treasury Yield (T10Y3M)")),
        ("Yield Curve ▸ 10Y-2Y Spread + Recessions",
         *yield_spread("T10Y2Y", "10-Year minus 2-Year Treasury Yield (T10Y2Y)")),
        ("Yield Curve ▸ vs Jobless Claims",
         *dual("T10Y3M", "ICSA", "10Y-3M Spread (pp)", "Initial Claims (thousands)", 300)),
        ("Yield Curve ▸ Recession Probability",
         *ts("RECPROUSM156N", "Smoothed US Recession Probability (%)", "1967-01-01")),
        ("Yield Curve ▸ OECD Leading Indicator (LEI)",
         *ts("USALOLITONOSTSAM", "OECD Composite Leading Indicator", "2000-01-01")),
        ("Yield Curve ▸ Leading Economic Index vs Consumer Sentiment",
         *dual("USALOLITONOSTSAM", "UMCSENT", "OECD Leading Economic Index", "Consumer Sentiment (Mich.)", 180)),
        ("Yield Curve ▸ All Key Indicators",
         *quad("T10Y3M", "ICSA", "UMCSENT", "RECPROUSM156N",
               "10Y-3M Spread", "Jobless Claims", "Consumer Sentiment", "Recession Prob (%)", 120)),
        # ── Macro / Money ─────────────────────────────────────────────────────
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
        # ── Labor Market ──────────────────────────────────────────────────────
        ("Nonfarm Payrolls (time series)",   *ts("PAYEMS",            "Nonfarm Payrolls (thousands)", "2000-01-01")),
        ("Job Openings – JOLTS (time series)", *ts("JTSJOL",          "Job Openings: Total Nonfarm (JOLTS)", "2001-01-01")),
        ("Payrolls vs Unemployment",         *dual("PAYEMS", "UNRATE", "Nonfarm Payrolls", "Unemployment Rate (%)", 120)),
        # ── Activity / Output ─────────────────────────────────────────────────
        ("Industrial Production (time series)", *ts("INDPRO",         "Industrial Production Index", "2000-01-01")),
        ("Weekly Economic Index (time series)",  *ts("WEI",           "Weekly Economic Index (WEI)", "2008-01-01")),
        ("Industrial Production vs Payrolls", *dual("INDPRO", "PAYEMS", "Industrial Production", "Nonfarm Payrolls", 120)),
        # ── Inflation / Rates ─────────────────────────────────────────────────
        ("Core CPI ex Food & Energy (time series)", *ts("CPILFESL",   "CPI Less Food & Energy (Core CPI)", "2000-01-01")),
        ("Federal Funds Rate (time series)", *ts("FEDFUNDS",          "Federal Funds Effective Rate (%)", "1990-01-01")),
        ("Fed Funds vs Core CPI",            *dual("FEDFUNDS", "CPILFESL", "Fed Funds Rate (%)", "Core CPI", 240)),
        ("Fed Funds vs Unemployment",        *dual("FEDFUNDS", "UNRATE", "Fed Funds Rate (%)", "Unemployment Rate (%)", 240)),
        # ── Market Volatility ─────────────────────────────────────────────────
        ("VIX (time series)",                *ts("VIXCLS",            "CBOE Volatility Index (VIX)", "2000-01-01")),
        ("VIX vs Yield Spread",              *dual("VIXCLS", "T10Y2Y", "VIX", "10Y-2Y Spread (pp)", 120)),
        # ── Sahm Rule ─────────────────────────────────────────────────────────
        ("Sahm Rule – Recession Indicator",  *sahm()),
        ("Sahm Rule vs Unemployment Rate",   *sahm("UNRATE",  "Unemployment Rate (%)", 360)),
        ("Sahm Rule vs Initial Claims",      *sahm("ICSA",    "Initial Jobless Claims", 360)),
        ("Sahm Rule vs Nonfarm Payrolls",    *sahm("PAYEMS",  "Nonfarm Payrolls", 300)),
        ("Sahm Rule vs Fed Funds Rate",      *sahm("FEDFUNDS","Fed Funds Rate (%)", 360)),
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

        # ── Normalized Comparison (Z-Score) ──────────────────────────────────
        ui.separator().classes("my-4")

        with ui.card().classes("w-full"):
            ui.label("Normalized Comparison (Z-Score)").classes("text-lg font-semibold")
            ui.label(
                "All series resampled to monthly and z-score normalized for direct "
                "comparison. Plot fetches from FRED once; checkboxes redraw instantly."
            ).classes("text-sm text-gray-400")

        with ui.row().classes("gap-3 w-full"):
            for card_label, card_value in [
                ("Yield curve lead time", "12–18 mo"),
                ("Credit spread lead",    "3–6 mo"),
                ("Claims behavior",       "Coincident"),
            ]:
                with ui.card().classes("flex-1"):
                    ui.label(card_label).classes("text-xs text-gray-400")
                    ui.label(card_value).classes("text-xl font-semibold mt-1")

        _zscore_cache: dict[str, pd.DataFrame | None] = {
            "T10Y2Y": None, "BAMLH0A0HYM2": None, "ICSA": None, "USREC": None,
            "WEI": None, "PAYEMS": None, "VIXCLS": None, "FEDFUNDS": None,
        }

        _ZSCORE_SERIES = [
            ("T10Y2Y",       "Yield curve (T10Y2Y)",   "#3b82f6"),
            ("BAMLH0A0HYM2", "Credit spread (HY OAS)", "#f97316"),
            ("ICSA",         "Jobless claims (ICSA)",  "#10b981"),
            ("WEI",          "Weekly Econ Index (WEI)","#8b5cf6"),
            ("PAYEMS",       "Nonfarm Payrolls",        "#ec4899"),
            ("VIXCLS",       "VIX",                    "#ef4444"),
            ("FEDFUNDS",     "Fed Funds Rate",          "#f59e0b"),
        ]

        with ui.card().classes("w-full"):
            with ui.row().classes("items-center gap-6 flex-wrap"):
                cb_yield    = ui.checkbox("Yield curve (T10Y2Y)",         value=True, on_change=lambda _: _zscore_redraw())
                cb_credit   = ui.checkbox("Credit spread (BAMLH0A0HYM2)", value=True, on_change=lambda _: _zscore_redraw())
                cb_claims   = ui.checkbox("Jobless claims (ICSA)",         value=True, on_change=lambda _: _zscore_redraw())
                cb_wei      = ui.checkbox("Weekly Econ Index (WEI)",       value=False, on_change=lambda _: _zscore_redraw())
                cb_payems   = ui.checkbox("Nonfarm Payrolls (PAYEMS)",     value=False, on_change=lambda _: _zscore_redraw())
                cb_vix      = ui.checkbox("VIX (VIXCLS)",                  value=False, on_change=lambda _: _zscore_redraw())
                cb_fedfunds = ui.checkbox("Fed Funds Rate (FEDFUNDS)",     value=False, on_change=lambda _: _zscore_redraw())
                zscore_button = ui.button("Plot")
                zscore_status = ui.label("").classes("text-sm text-gray-400")

        zscore_plot_area = ui.column().classes("w-full")

        _CB_VALS = {
            "T10Y2Y":       lambda: cb_yield.value,
            "BAMLH0A0HYM2": lambda: cb_credit.value,
            "ICSA":         lambda: cb_claims.value,
            "WEI":          lambda: cb_wei.value,
            "PAYEMS":       lambda: cb_payems.value,
            "VIXCLS":       lambda: cb_vix.value,
            "FEDFUNDS":     lambda: cb_fedfunds.value,
        }

        def _zscore_redraw() -> None:
            if _zscore_cache["USREC"] is None:
                zscore_status.text = "Click Plot to fetch data first."
                return
            active = []
            for sid, label, color in _ZSCORE_SERIES:
                if _CB_VALS[sid]() and _zscore_cache[sid] is not None:
                    active.append((_zscore_cache[sid], label, color))
            zscore_plot_area.clear()
            with zscore_plot_area:
                with ui.pyplot(figsize=(14, 6)) as p:
                    _render_zscore_comparison(p.fig, active, _zscore_cache["USREC"])
            zscore_status.text = "Done"

        async def on_zscore_plot() -> None:
            from pyfredapi import get_series  # type: ignore[attr-defined]

            zscore_button.disable()
            zscore_button.text = "Fetching…"
            zscore_status.text = "Fetching 8 series from FRED…"
            zscore_plot_area.clear()

            def _fetch_all():
                return {
                    "T10Y2Y":       get_series("T10Y2Y"),
                    "BAMLH0A0HYM2": get_series("BAMLH0A0HYM2"),
                    "ICSA":         get_series("ICSA"),
                    "USREC":        get_series("USREC"),
                    "WEI":          get_series("WEI"),
                    "PAYEMS":       get_series("PAYEMS"),
                    "VIXCLS":       get_series("VIXCLS"),
                    "FEDFUNDS":     get_series("FEDFUNDS"),
                }

            try:
                fetched = await asyncio.to_thread(_fetch_all)
            except Exception as exc:
                ui.notify(f"FRED fetch error: {exc}", color="negative")
                zscore_status.text = f"Error: {exc}"
                zscore_button.text = "Plot"
                zscore_button.enable()
                return

            _zscore_cache.update(fetched)
            zscore_status.text = "Rendering…"
            _zscore_redraw()
            zscore_button.text = "Plot"
            zscore_button.enable()

        zscore_button.on_click(on_zscore_plot)

        with ui.card().classes("w-full"):
            ui.label("Key correlations").classes("font-semibold")
            for bold, rest in [
                ("Yield curve inverts",   "(goes negative) → recession follows in 12–18 months"),
                ("Credit spreads spike",  "(go high) → credit stress, recession often 3–6 months out"),
                ("Jobless claims surge",  "→ coincident with recession start / middle"),
                ("WEI drops sharply",     "→ real-time GDP slowdown signal (weekly frequency)"),
                ("Payrolls slow/fall",    "→ coincident recession indicator; negative = contraction"),
                ("VIX spikes",            "→ market fear; often coincident or slightly leading"),
                ("Fed Funds high/rising", "→ tightening cycle; historically precedes slowdowns"),
                ("Anti-correlation:",     "yield curve DOWN while credit/claims/VIX UP signals trouble"),
            ]:
                with ui.row().classes("gap-1 items-start mt-1"):
                    ui.label(bold).classes("font-semibold text-sm")
                    ui.label(rest).classes("text-sm text-gray-400")

        with ui.row().classes("text-xs text-gray-500 mt-1 gap-2"):
            ui.label("FRED series:").classes("font-medium")
            ui.label("T10Y2Y · BAMLH0A0HYM2 · ICSA · WEI · PAYEMS · VIXCLS · FEDFUNDS · USREC")
