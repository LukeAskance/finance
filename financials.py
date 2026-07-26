"""financials.py — Company financial metrics fetched from SEC EDGAR and yfinance."""

from __future__ import annotations

import datetime
import os
import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Optional

import requests
import yfinance as yf

import historicals_store
from schwab_api import get_shared_api
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
    otm_put_iv_pct: Optional[float] = None
    otm_put_iv_error: Optional[str] = None


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


def get_otm_put_iv(api, ticker: str, target_moneyness: float = 0.90) -> float:
    """OTM put implied volatility (%): nearest expiration >= 20 days out, put
    strike closest to target_moneyness * spot, straight from Schwab's own
    option chain (paid market data, so — unlike yfinance — it's available for
    thinly-traded names too; volatility is already a plain percentage, no
    unit conversion needed). Merton model treats equity as a call on the
    firm's assets (debt = strike); a spike in this IV is a real-time proxy
    for the market pricing in distress / a bond downgrade.

    # ponytail: fixed 10%-OTM / nearest-20+-DTE definition, not configurable —
    # add a moneyness/tenor knob if a specific alert ever needs a different one.
    """
    chain = api.get_option_chain(
        ticker,
        contractType="PUT",
        strikeCount=40,
        includeUnderlyingQuote=True,
        strategy="SINGLE",
    )

    price = (chain.get("underlying") or {}).get("last")
    if not price:
        raise ValueError(f"No underlying price in option chain for {ticker}")

    put_map = chain.get("putExpDateMap") or {}
    if not put_map:
        raise ValueError(f"No put chain available for {ticker}")

    def dte(exp_key: str) -> int:
        return int(exp_key.rsplit(":", 1)[-1])

    candidates = sorted((k for k in put_map if dte(k) >= 20), key=dte)
    expiration = candidates[0] if candidates else min(put_map, key=dte)

    strikes = put_map[expiration]
    target_strike = price * target_moneyness
    closest_strike = min(strikes, key=lambda k: abs(float(k) - target_strike))
    iv = strikes[closest_strike][0].get("volatility")
    if not iv:
        raise ValueError(f"Implied volatility not available for {ticker} {expiration}")
    return round(float(iv), 2)


def get_financials(ticker: str) -> CompanyFinancials:
    """
    Fetch price, EPS, dividend yield%, P/E from yfinance, cash/share from SEC
    EDGAR, and OTM put IV% (options chain, distress proxy) for *ticker*.
    """
    t = yf.Ticker(ticker)
    info = t.info

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

    otm_put_iv_pct: Optional[float] = None
    otm_put_iv_error: Optional[str] = None
    try:
        otm_put_iv_pct = get_otm_put_iv(get_shared_api(), ticker)
    except Exception as exc:
        otm_put_iv_error = str(exc)

    if otm_put_iv_pct is not None:
        # history is best-effort — e.g. a standalone process (portfolio_mcp.py)
        # that never called historicals_store.init_db() shouldn't break the reading
        with suppress(Exception):
            historicals_store.record_iv(ticker, otm_put_iv_pct)

    return CompanyFinancials(
        ticker=ticker.upper(),
        price=price,
        eps=eps,
        dividend_yield_pct=dividend_yield_pct,
        pe_ratio=round(pe_ratio, 2) if pe_ratio is not None else None,
        cash_per_share=round(cash_ps, 2) if cash_ps is not None else None,
        cash_per_share_error=cash_ps_error,
        otm_put_iv_pct=otm_put_iv_pct,
        otm_put_iv_error=otm_put_iv_error,
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


if __name__ == "__main__":
    # ponytail: pure-logic self-check for get_otm_put_iv's expiration/strike
    # selection; get_financials/get_cash_per_share/get_insider_transactions
    # need live network so they aren't covered here.
    class _FakeSchwabAPI:
        def __init__(self, chain):
            self._chain = chain

        def get_option_chain(self, ticker, **kwargs):
            return self._chain

    today = datetime.date.today()
    near_key = f"{(today + datetime.timedelta(days=5)).isoformat()}:5"
    far_key = f"{(today + datetime.timedelta(days=30)).isoformat()}:30"

    chain = {
        "underlying": {"last": 100.0},
        "putExpDateMap": {
            near_key: {"90.0": [{"volatility": 99.0}]},
            far_key: {
                "80.0": [{"volatility": 55.0}],
                "90.0": [{"volatility": 40.0}],
                "100.0": [{"volatility": 35.0}],
            },
        },
    }

    # skips the too-near expiration, picks the strike closest to 10% OTM (90)
    iv = get_otm_put_iv(_FakeSchwabAPI(chain), "TEST", target_moneyness=0.90)
    assert iv == 40.0, iv

    # falls back to the only available expiration when none are 20+ days out
    chain_near_only = {
        "underlying": {"last": 100.0},
        "putExpDateMap": {near_key: {"90.0": [{"volatility": 99.0}]}},
    }
    iv = get_otm_put_iv(_FakeSchwabAPI(chain_near_only), "TEST")
    assert iv == 99.0, iv

    # no put chain at all -> raises rather than silently returning junk
    empty_chain = {"underlying": {"last": 100.0}, "putExpDateMap": {}}
    try:
        get_otm_put_iv(_FakeSchwabAPI(empty_chain), "TEST")
        raise AssertionError("expected ValueError for empty chain")
    except ValueError:
        pass

    print("financials.py self-check OK")
