import io
import textwrap
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import requests
from PIL import Image


WORKDIR = Path.home() / "code" / "money"
OUT_PNG = WORKDIR / "shiller_pe_recession_overlay.png"
OUT_CSV = WORKDIR / "shiller_pe_recession_overlay_data.csv"

SHILLER_TABLE_URL = "https://www.multpl.com/shiller-pe/table/by-month"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=JHDUSRGDPBR"


def fetch_shiller_pe() -> pd.DataFrame:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }
    response = requests.get(SHILLER_TABLE_URL, headers=headers, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        raise RuntimeError("No tables found on Shiller P/E page.")

    df = tables[0].copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    date_col = next(c for c in df.columns if "date" in c)
    value_col = next(c for c in df.columns if "value" in c or "shiller" in c)

    out = df[[date_col, value_col]].rename(columns={date_col: "date", value_col: "shiller_pe"})
    out["date"] = pd.to_datetime(out["date"], format="mixed")
    out["shiller_pe"] = pd.to_numeric(out["shiller_pe"].astype(str).str.extract(r"([-+]?\d*\.?\d+)")[0])
    out = out.dropna().sort_values("date").reset_index(drop=True)
    return out


def fetch_recession_indicator() -> pd.DataFrame:
    df = pd.read_csv(FRED_CSV_URL)
    df = df.rename(columns={"observation_date": "date", "JHDUSRGDPBR": "recession"})
    df["date"] = pd.to_datetime(df["date"])
    df["recession"] = pd.to_numeric(df["recession"], errors="coerce")
    return df.dropna().sort_values("date").reset_index(drop=True)


def recession_spans(recession_df: pd.DataFrame):
    spans = []
    in_span = False
    start = None
    prev_date = None

    for row in recession_df.itertuples(index=False):
        is_recession = row.recession >= 0.5
        if is_recession and not in_span:
            start = row.date
            in_span = True
        elif not is_recession and in_span:
            spans.append((start, prev_date))
            in_span = False
        prev_date = row.date

    if in_span and start is not None:
        spans.append((start, prev_date))

    return spans


def plot_chart(shiller: pd.DataFrame, recession: pd.DataFrame) -> None:
    plt.rcdefaults()
    plt.rcParams.update(
        {
            "figure.dpi": 180,
            "font.family": "DejaVu Sans",
            "axes.edgecolor": "#D4D1CA",
            "axes.labelcolor": "#28251D",
            "xtick.color": "#7A7974",
            "ytick.color": "#7A7974",
            "text.color": "#28251D",
        }
    )

    fig, ax = plt.subplots(figsize=(14, 7.5), constrained_layout=True)
    fig.patch.set_facecolor("#F7F6F2")
    ax.set_facecolor("#FBFBF9")

    # Draw recession shading first so the Shiller line stays dominant.
    first_label = True
    for start, end in recession_spans(recession):
        if end < shiller["date"].min() or start > shiller["date"].max():
            continue
        ax.axvspan(
            max(start, shiller["date"].min()),
            min(end, shiller["date"].max()),
            facecolor="#BAB9B4",
            alpha=0.42,
            lw=0,
            label="Real GDP recession period" if first_label else None,
        )
        first_label = False

    ax.plot(
        shiller["date"],
        shiller["shiller_pe"],
        color="#20808D",
        lw=2.2,
        label="Shiller P/E ratio",
        zorder=3,
    )

    latest = shiller.iloc[-1]
    ax.scatter([latest["date"]], [latest["shiller_pe"]], color="#20808D", s=42, zorder=4)
    ax.annotate(
        f"Latest: {latest['shiller_pe']:.1f}",
        xy=(latest["date"], latest["shiller_pe"]),
        xytext=(-76, 20),
        textcoords="offset points",
        fontsize=10,
        color="#1B474D",
        arrowprops=dict(arrowstyle="-", color="#1B474D", lw=0.8),
        bbox=dict(boxstyle="round,pad=0.3", fc="#F9F8F5", ec="#D4D1CA", alpha=0.95),
    )

    ax.set_title(
        "Shiller P/E tends to compress around U.S. recession periods\n"
        "Monthly Shiller P/E ratio with FRED JHDUSRGDPBR recession-date overlay",
        loc="left",
        fontsize=15,
        fontweight="bold",
        pad=14,
    )
    ax.set_ylabel("Shiller P/E ratio")
    ax.set_xlabel("")

    ax.xaxis.set_major_locator(mdates.YearLocator(10))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_minor_locator(mdates.YearLocator(5))
    ax.grid(axis="y", color="#D4D1CA", lw=0.8, alpha=0.55)
    ax.grid(axis="x", visible=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#D4D1CA")
    ax.spines["bottom"].set_color("#D4D1CA")

    leg = ax.legend(
        loc="upper left",
        frameon=True,
        framealpha=0.95,
        facecolor="#F9F8F5",
        edgecolor="#D4D1CA",
        fontsize=10,
    )
    for line in leg.get_lines():
        line.set_linewidth(2.5)

    note = textwrap.fill(
        "Sources: Shiller P/E Ratio by Month, multpl.com; "
        "FRED JHDUSRGDPBR recession indicator. Recession shading marks observations where "
        "the FRED series equals 1.",
        width=130,
    )
    fig.text(0.01, -0.025, note, ha="left", va="top", fontsize=8.5, color="#7A7974")

    fig.savefig(OUT_PNG, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> None:
    shiller = fetch_shiller_pe()
    recession = fetch_recession_indicator()

    monthly = pd.merge_asof(
        shiller.sort_values("date"),
        recession.sort_values("date"),
        on="date",
        direction="nearest",
        tolerance=pd.Timedelta(days=20),
    )
    monthly.to_csv(OUT_CSV, index=False)

    plot_chart(shiller, recession)

    # Basic render sanity check: make sure image exists and is non-empty/readable.
    with Image.open(OUT_PNG) as img:
        width, height = img.size
    print(f"Saved chart: {OUT_PNG} ({width}x{height})")
    print(f"Saved aligned data: {OUT_CSV}")
    print(f"Shiller rows: {len(shiller):,}; recession rows: {len(recession):,}")
    print(f"Date range: {shiller['date'].min().date()} to {shiller['date'].max().date()}")


if __name__ == "__main__":
    main()
