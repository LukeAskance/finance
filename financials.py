"""financials.py — Company financial metrics fetched from SEC EDGAR and yfinance."""

from __future__ import annotations

import datetime
import os
import re
from dataclasses import dataclass
from typing import Optional

import requests
import yfinance as yf

from dividend_prediction import (
    _EDGAR_FACTS_URL,
    _EDGAR_HEADERS,
    _EDGAR_TIMEOUT,
    _ticker_to_cik,
)


@dataclass
class CompanyFinancials:
    ticker: str
    price: Optional[float]
    eps: Optional[float]
    dividend_yield_pct: Optional[float]
    pe_ratio: Optional[float]
    cash_per_share: Optional[float]
    cash_per_share_error: Optional[str] = None


@dataclass
class InsiderTransactions:
    ticker: str
    buys: int
    buys_shares: int
    sells: int
    sells_shares: int
    sells_10b51: int
    sells_10b51_shares: int


def get_cash_per_share(ticker: str) -> float:
    """
    Return cash-per-share (most recent 10-K) for *ticker* via SEC EDGAR.

    Raises ValueError if the required XBRL concepts are not found.
    """
    cik = _ticker_to_cik(ticker)
    url = _EDGAR_FACTS_URL.format(cik=cik)
    r = requests.get(url, headers=_EDGAR_HEADERS, timeout=_EDGAR_TIMEOUT)
    r.raise_for_status()
    facts = r.json().get("facts", {}).get("us-gaap", {})

    def latest_annual(concept: str, unit: str) -> float:
        entries = facts.get(concept, {}).get("units", {}).get(unit, [])
        annual = [x for x in entries if x.get("form") == "10-K"]
        if not annual:
            raise ValueError(
                f"No annual 10-K data for EDGAR concept '{concept}' ({ticker})"
            )
        return float(sorted(annual, key=lambda x: x["end"])[-1]["val"])

    cash = latest_annual("CashAndCashEquivalentsAtCarryingValue", "USD")
    shares = latest_annual("CommonStockSharesOutstanding", "shares")

    if shares == 0:
        raise ValueError(f"Shares outstanding is zero for {ticker}")
    return cash / shares


def get_financials(ticker: str) -> CompanyFinancials:
    """
    Fetch price, EPS, dividend yield%, P/E from yfinance and
    cash/share from SEC EDGAR for *ticker*.
    """
    info = yf.Ticker(ticker).info

    price = info.get("regularMarketPrice") or info.get("currentPrice")
    eps = info.get("trailingEps")
    raw_yield = info.get("dividendYield")
    dividend_yield_pct = round(raw_yield, 2) if raw_yield is not None else None
    pe_ratio = info.get("trailingPE")

    cash_ps: Optional[float] = None
    cash_ps_error: Optional[str] = None
    try:
        cash_ps = get_cash_per_share(ticker)
    except Exception as exc:
        cash_ps_error = str(exc)

    return CompanyFinancials(
        ticker=ticker.upper(),
        price=price,
        eps=eps,
        dividend_yield_pct=dividend_yield_pct,
        pe_ratio=round(pe_ratio, 2) if pe_ratio is not None else None,
        cash_per_share=round(cash_ps, 2) if cash_ps is not None else None,
        cash_per_share_error=cash_ps_error,
    )


def get_insider_transactions(
    ticker: str, n_filings: int = 50, lookback_days: int = 365
) -> InsiderTransactions:
    """
    Return insider buy/sell/10b5-1 summary for *ticker* from SEC EDGAR Form 4 filings.

    Scans up to *n_filings* most-recent Form 4s, restricted to the past
    *lookback_days* days.  10b5-1 status is derived from the <aff10b5One>
    XML element in each filing.
    """
    from edgar import Company, set_identity  # type: ignore[import]

    identity = os.getenv("EDGAR_IDENTITY", "portfolio-app user@example.com")
    set_identity(identity)

    cutoff = (
        datetime.date.today() - datetime.timedelta(days=lookback_days)
    ).isoformat()

    company = Company(ticker.upper())
    filings = company.get_filings(form="4")

    buys = sells = buys_shares = sells_shares = sells_10b51 = sells_10b51_shares = 0

    for f in filings[:n_filings]:
        if str(f.filing_date) < cutoff:
            break  # filings are sorted newest-first

        obj = f.obj()
        mt = obj.market_trades
        if mt is None or mt.empty:
            continue

        # Determine if this filing was executed under a Rule 10b5-1 plan
        is_10b51 = False
        try:
            content = f.homepage.primary_xml_document.content or ""
            m = re.search(r"<aff10b5One>(\d+)</aff10b5One>", content)
            if m and m.group(1) == "1":
                is_10b51 = True
        except Exception:
            pass

        for _, row in mt.iterrows():
            shares = abs(int(row.get("Shares") or 0))
            if row.get("AcquiredDisposed") == "A":
                buys += 1
                buys_shares += shares
            elif row.get("AcquiredDisposed") == "D":
                sells += 1
                sells_shares += shares
                if is_10b51:
                    sells_10b51 += 1
                    sells_10b51_shares += shares

    return InsiderTransactions(
        ticker=ticker.upper(),
        buys=buys,
        buys_shares=buys_shares,
        sells=sells,
        sells_shares=sells_shares,
        sells_10b51=sells_10b51,
        sells_10b51_shares=sells_10b51_shares,
    )
