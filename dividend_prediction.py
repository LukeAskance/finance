"""
dividend_forecast.py
--------------------
Predict future dividend income from historical data.

Sources supported:
  - SEC EDGAR XBRL company facts API (auto-fetch by ticker)
  - Schwab transaction history CSV export

Usage:
    from dividend_forecast import DividendForecaster

    # From SEC EDGAR
    f = DividendForecaster.from_edgar("AAPL")

    # From Schwab CSV
    f = DividendForecaster.from_schwab_csv("schwab_transactions.csv", ticker="AAPL")

    # From both (merged + deduplicated)
    f = DividendForecaster.from_combined("AAPL", schwab_csv="schwab_transactions.csv")

    # Run analysis
    print(f.summary())
    projections = f.project(years=5)
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import requests

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ScenarioProjection:
    """Single scenario (bear / base / bull) projection."""
    label: str
    growth_rate: float          # annual growth rate used
    annual_dividends: list[float]
    cumulative_income: list[float]
    years: list[int]

    def as_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame({
            "year": self.years,
            "annual_dividend": self.annual_dividends,
            "cumulative_income": self.cumulative_income,
        })


@dataclass
class ForecastResult:
    ticker: str
    shares: float
    last_annual_dividend: float     # per share
    cagr_3yr: Optional[float]
    cagr_5yr: Optional[float]
    cagr_10yr: Optional[float]
    scenarios: dict[str, ScenarioProjection] = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            f"Ticker          : {self.ticker}",
            f"Shares held     : {self.shares:,.2f}",
            f"Last annual div : ${self.last_annual_dividend:.4f}/share",
            f"  → Annual income: ${self.last_annual_dividend * self.shares:,.2f}",
            "",
            "Historical CAGR:",
            f"  3-year  : {self._fmt_pct(self.cagr_3yr)}",
            f"  5-year  : {self._fmt_pct(self.cagr_5yr)}",
            f"  10-year : {self._fmt_pct(self.cagr_10yr)}",
            "",
            "Projections (year 1 → last year):",
        ]
        for label, s in self.scenarios.items():
            first = s.annual_dividends[0] * self.shares
            last  = s.annual_dividends[-1] * self.shares
            lines.append(
                f"  {label:<6}: ${first:,.2f} → ${last:,.2f}/yr  "
                f"(g={s.growth_rate*100:.1f}%)"
            )
        return "\n".join(lines)

    @staticmethod
    def _fmt_pct(v: Optional[float]) -> str:
        return f"{v*100:.2f}%" if v is not None else "n/a (insufficient history)"


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class DividendForecaster:
    """
    Forecast future dividend income using CAGR-based scenario projections.

    Parameters
    ----------
    ticker : str
    dividends : pd.Series
        Index = pd.DatetimeIndex, values = per-share dividend amounts.
    shares : float
        Number of shares held (used for income projections).
    """

    def __init__(
        self,
        ticker: str,
        dividends: pd.Series,
        shares: float = 1.0,
    ):
        self.ticker = ticker.upper()
        self.shares = shares
        self._raw = self._clean(dividends)

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_edgar(
        cls,
        ticker: str,
        shares: float = 1.0,
    ) -> "DividendForecaster":
        """Fetch dividend-per-share history from SEC EDGAR XBRL company facts."""
        divs = _fetch_edgar_dividends(ticker)
        if divs.empty:
            raise ValueError(f"No dividend history found for {ticker} via SEC EDGAR.")
        return cls(ticker, divs, shares=shares)

    @classmethod
    def from_schwab_csv(
        cls,
        csv_path: str | Path,
        ticker: str,
        shares: float = 1.0,
    ) -> "DividendForecaster":
        """
        Load dividend history from a Schwab transaction history CSV export.

        Schwab CSV columns used:
          Date, Action, Symbol, Amount
        Dividend action keywords matched (case-insensitive):
          'dividend', 'div reinvest', 'qual div'
        """
        divs = _parse_schwab_csv(csv_path, ticker)
        if divs.empty:
            raise ValueError(
                f"No dividend transactions found for {ticker} in {csv_path}."
            )
        return cls(ticker, divs, shares=shares)

    @classmethod
    def from_combined(
        cls,
        ticker: str,
        schwab_csv: str | Path,
        shares: float = 1.0,
        prefer: str = "schwab",
    ) -> "DividendForecaster":
        """
        Merge SEC EDGAR + Schwab history. Deduplicates on date (within 5 days).

        Parameters
        ----------
        prefer : 'schwab' | 'edgar'
            Which source wins when the same payment date appears in both.
        """
        try:
            edgar_divs = _fetch_edgar_dividends(ticker)
        except Exception as e:
            warnings.warn(f"EDGAR fetch failed ({e}); using Schwab only.")
            edgar_divs = pd.Series(dtype=float)

        try:
            sw_divs = _parse_schwab_csv(schwab_csv, ticker)
        except Exception as e:
            warnings.warn(f"Schwab CSV parse failed ({e}); using EDGAR only.")
            sw_divs = pd.Series(dtype=float)

        merged = _merge_dividend_series(edgar_divs, sw_divs, prefer=prefer)
        if merged.empty:
            raise ValueError("No dividend data found from either source.")
        return cls(ticker, merged, shares=shares)

    # ------------------------------------------------------------------
    # Core analytics
    # ------------------------------------------------------------------

    @property
    def annual(self) -> pd.Series:
        """Per-share dividends summed by calendar year."""
        return self._raw.resample("YE").sum()

    def cagr(self, years: int) -> Optional[float]:
        """
        Compute CAGR over the most recent `years` full calendar years.
        Returns None if insufficient history.
        """
        ann = self.annual.copy()
        # drop current partial year
        ann = ann[ann.index.year < date.today().year]
        if len(ann) < years + 1:
            return None
        tail = ann.iloc[-(years + 1):]
        start, end = tail.iloc[0], tail.iloc[-1]
        if start <= 0:
            return None
        return (end / start) ** (1 / years) - 1

    def project(
        self,
        years: int = 5,
        bear_rate: Optional[float] = None,
        base_rate: Optional[float] = None,
        bull_rate: Optional[float] = None,
    ) -> ForecastResult:
        """
        Build bear / base / bull projections.

        If rates are not supplied they are derived automatically:
          base  = 5-yr CAGR  (fallback: 3-yr → 10-yr → 3%)
          bear  = base - 2%
          bull  = base + 2%
        """
        base = base_rate if base_rate is not None else self._auto_base_rate()
        bear = bear_rate if bear_rate is not None else max(0.0, base - 0.02)
        bull = bull_rate if bull_rate is not None else base + 0.02

        last_div = self._last_annual_div_per_share()

        scenarios: dict[str, ScenarioProjection] = {}
        for label, g in [("bear", bear), ("base", base), ("bull", bull)]:
            ann_divs, cum = [], []
            running = 0.0
            for yr in range(1, years + 1):
                d = last_div * (1 + g) ** yr
                running += d
                ann_divs.append(d)
                cum.append(running)
            scenarios[label] = ScenarioProjection(
                label=label,
                growth_rate=g,
                annual_dividends=ann_divs,
                cumulative_income=cum,
                years=list(range(date.today().year + 1,
                                 date.today().year + 1 + years)),
            )

        return ForecastResult(
            ticker=self.ticker,
            shares=self.shares,
            last_annual_dividend=last_div,
            cagr_3yr=self.cagr(3),
            cagr_5yr=self.cagr(5),
            cagr_10yr=self.cagr(10),
            scenarios=scenarios,
        )

    def summary(self, projection_years: int = 5) -> str:
        """Convenience: project + return formatted summary string."""
        return self.project(years=projection_years).summary()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _auto_base_rate(self) -> float:
        """Pick the best available CAGR as the base growth rate."""
        for yrs in (5, 3, 10):
            g = self.cagr(yrs)
            if g is not None:
                return g
        warnings.warn("Insufficient CAGR history; defaulting base rate to 3%.")
        return 0.03

    def _last_annual_div_per_share(self) -> float:
        """Most recent full-year per-share dividend total."""
        ann = self.annual
        ann = ann[ann.index.year < date.today().year]
        if ann.empty:
            raise ValueError("No complete annual dividend data available.")
        return float(ann.iloc[-1])

    @staticmethod
    def _clean(s: pd.Series) -> pd.Series:
        """Normalize index to UTC-naive DatetimeIndex, drop zeros/NaN, sort."""
        s = s.copy().dropna()
        s = s[s > 0]
        if hasattr(s.index, "tz") and s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        s.index = pd.to_datetime(s.index)
        return s.sort_index()


# ---------------------------------------------------------------------------
# SEC EDGAR helpers
# ---------------------------------------------------------------------------

_EDGAR_HEADERS = {"User-Agent": "money-app contact@example.com"}
_EDGAR_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_EDGAR_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
_EDGAR_TIMEOUT = 15  # seconds


def _ticker_to_cik(ticker: str) -> str:
    """Resolve a ticker symbol to a zero-padded 10-digit SEC CIK string."""
    r = requests.get(_EDGAR_TICKERS_URL, headers=_EDGAR_HEADERS, timeout=_EDGAR_TIMEOUT)
    r.raise_for_status()
    upper = ticker.upper()
    for entry in r.json().values():
        if entry["ticker"].upper() == upper:
            return str(entry["cik_str"]).zfill(10)
    raise ValueError(f"Could not find SEC CIK for ticker {ticker!r}")


def _edgar_entries_to_series(entries: list[dict]) -> pd.Series:
    """
    Convert EDGAR XBRL fact entries to a deduplicated per-share dividend Series.

    Prefers entries that carry a quarterly frame tag (CY????Q?); falls back to
    deduplicating by accession number when no framed entries exist.
    """
    quarterly = [e for e in entries if re.match(r"CY\d{4}Q\d$", e.get("frame", ""))]
    source = quarterly or entries

    seen: set[tuple] = set()
    records: list[tuple[str, float]] = []
    for e in source:
        key = (e["end"], e["val"]) if quarterly else (e.get("accn", e["end"]), e["val"])
        if key not in seen:
            seen.add(key)
            records.append((e["end"], float(e["val"])))

    if not records:
        return pd.Series(dtype=float)

    s = pd.Series(
        [v for _, v in records],
        index=pd.to_datetime([d for d, _ in records]),
    )
    return s[s > 0].sort_index()


def _fetch_edgar_dividends(ticker: str) -> pd.Series:
    """
    Fetch dividend-per-share history for *ticker* from SEC EDGAR XBRL facts.

    Tries CommonStockDividendsPerShareDeclared first, then falls back to
    CommonStockDividendsPerShareCashPaid.
    """
    cik = _ticker_to_cik(ticker)
    url = _EDGAR_FACTS_URL.format(cik=cik)
    r = requests.get(url, headers=_EDGAR_HEADERS, timeout=_EDGAR_TIMEOUT)
    r.raise_for_status()

    us_gaap = r.json().get("facts", {}).get("us-gaap", {})
    for concept in (
        # Common stock dividends per share
        "CommonStockDividendsPerShareDeclared",
        "CommonStockDividendsPerShareCashPaid",
        # MLP / limited-partnership distributions per unit
        "DistributionMadeToLimitedPartnerDistributionsDeclaredPerUnit",
        "DistributionMadeToLimitedPartnerCashDistributionsPerUnit",
        "DistributionMadeToMemberOrLimitedPartnerDistributionsDeclaredPerUnit",
    ):
        entries = us_gaap.get(concept, {}).get("units", {}).get("USD/shares", [])
        if entries:
            series = _edgar_entries_to_series(entries)
            if not series.empty:
                return series

    return pd.Series(dtype=float)


# ---------------------------------------------------------------------------
# Schwab CSV parser
# ---------------------------------------------------------------------------

def _parse_schwab_csv(path: str | Path, ticker: str) -> pd.Series:
    """
    Parse a Schwab transaction history CSV and extract per-share dividend amounts.

    Schwab exports two common formats:
      1. Full transaction history  — columns include Date, Action, Symbol, Amount
      2. Income history            — similar structure

    Because Schwab CSVs often have a multi-line header with account info,
    we scan for the actual column header row first.
    """
    path = Path(path)
    raw = path.read_text(errors="replace")
    lines = raw.splitlines()

    # Find header row (contains "Date" and "Action")
    header_idx = None
    for i, line in enumerate(lines):
        lower = line.lower()
        if "date" in lower and ("action" in lower or "description" in lower):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Could not locate header row in Schwab CSV.")

    df = pd.read_csv(
        path,
        skiprows=header_idx,
        thousands=",",
        on_bad_lines="skip",
    )
    df.columns = df.columns.str.strip().str.lower()

    # Normalize column names
    col_map = {}
    for c in df.columns:
        if c.startswith("date"):
            col_map[c] = "date"
        elif c in ("action", "description"):
            col_map[c] = "action"
        elif c in ("symbol", "ticker"):
            col_map[c] = "symbol"
        elif c in ("amount", "dividends", "income"):
            col_map[c] = "amount"
        elif c in ("price", "dividend/share", "div/share"):
            col_map[c] = "price"
    df = df.rename(columns=col_map)

    required = {"date", "action"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Schwab CSV missing expected columns: {missing}")

    # Filter to the target ticker
    if "symbol" in df.columns:
        df = df[df["symbol"].str.upper().str.strip() == ticker.upper()]

    # Filter to dividend rows
    div_keywords = ("dividend", "div reinvest", "qual div", "non-qual div",
                    "ordinary div", "special div", "income")
    mask = df["action"].str.lower().str.contains(
        "|".join(div_keywords), na=False
    )
    df = df[mask].copy()

    if df.empty:
        return pd.Series(dtype=float)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # Prefer per-share price column; fall back to total amount
    if "price" in df.columns:
        df["div_per_share"] = pd.to_numeric(
            df["price"].astype(str).str.replace(r"[$,]", "", regex=True),
            errors="coerce",
        )
    elif "amount" in df.columns:
        # Total amount — we don't know shares here, store as-is with a warning
        warnings.warn(
            "Schwab CSV has no per-share price column; "
            "using total Amount. Pass shares= for income projection."
        )
        df["div_per_share"] = pd.to_numeric(
            df["amount"].astype(str).str.replace(r"[$,()-]", "", regex=True),
            errors="coerce",
        )
    else:
        raise ValueError("Schwab CSV has neither Amount nor Price column.")

    df = df.dropna(subset=["div_per_share"])
    df = df[df["div_per_share"] > 0]

    series = pd.Series(df["div_per_share"].values, index=df["date"].values)
    return series.sort_index()


# ---------------------------------------------------------------------------
# Merge helper
# ---------------------------------------------------------------------------

def _merge_dividend_series(
    yf_divs: pd.Series,
    sw_divs: pd.Series,
    prefer: str = "schwab",
    window_days: int = 5,
) -> pd.Series:
    """
    Combine two dividend series, deduplicating payments within `window_days`.
    `prefer` controls which source's value wins on conflict.
    """
    if yf_divs.empty:
        return sw_divs
    if sw_divs.empty:
        return yf_divs

    primary   = sw_divs if prefer == "schwab" else yf_divs
    secondary = yf_divs if prefer == "schwab" else sw_divs

    # Normalize both to DatetimeIndex
    def to_dt(s):
        s = s.copy()
        s.index = pd.to_datetime(s.index)
        if hasattr(s.index, "tz") and s.index.tz is not None:
            s.index = s.index.tz_localize(None)
        return s.sort_index()

    primary   = to_dt(primary)
    secondary = to_dt(secondary)

    combined = primary.copy()
    for dt, val in secondary.items():
        # Check if a "close enough" date already exists in combined
        diffs = abs((combined.index - dt).days)
        if diffs.min() > window_days:
            combined.loc[dt] = val

    return combined.sort_index()


# ---------------------------------------------------------------------------
# Quick CLI demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    shares = float(sys.argv[2]) if len(sys.argv) > 2 else 100.0

    print(f"Fetching dividend history for {ticker} via SEC EDGAR...")
    forecaster = DividendForecaster.from_edgar(ticker, shares=shares)
    result = forecaster.project(years=5)
    print(result.summary())

    print("\nBase scenario detail:")
    print(result.scenarios["base"].as_dataframe().to_string(index=False))
