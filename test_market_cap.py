#! /Users/george/code/money/.venv/bin/python3
"""test_market_cap.py — standalone test for the yfinance market-cap fetch.

Usage:
    ./test_market_cap.py            # tests AAPL, MSFT, NVDA
    ./test_market_cap.py TSLA BRK-B
"""

from __future__ import annotations

import asyncio
import datetime
import sys

import yfinance as yf

DEFAULT_TICKERS = ["AAPL", "MSFT", "NVDA"]


async def get_market_cap(ticker: str) -> dict:
    info = await asyncio.to_thread(lambda: yf.Ticker(ticker).info)
    if not (mc := info.get("marketCap")):
        raise ValueError(f"No marketCap returned for {ticker}")
    return {
        "symbol":    ticker,
        "date":      datetime.date.today().isoformat(),
        "marketCap": mc,
    }


def _fmt(market_cap: int) -> str:
    if market_cap >= 1_000_000_000_000:
        return f"${market_cap / 1_000_000_000_000:.2f}T"
    if market_cap >= 1_000_000_000:
        return f"${market_cap / 1_000_000_000:.2f}B"
    return f"${market_cap / 1_000_000:.2f}M"


async def main(tickers: list[str]) -> None:
    print(f"{'Ticker':<10} {'Date':<14} {'Market Cap':>14}  Raw")
    print("-" * 60)

    results = await asyncio.gather(
        *[get_market_cap(t.upper()) for t in tickers],
        return_exceptions=True,
    )

    for ticker, result in zip(tickers, results):
        if isinstance(result, Exception):
            print(f"{ticker.upper():<10}  ERROR: {result}")
        else:
            mc = result["marketCap"]
            print(
                f"{result['symbol']:<10} {result['date']:<14}"
                f" {_fmt(mc):>14}  {mc:,}"
            )


if __name__ == "__main__":
    tickers = sys.argv[1:] or DEFAULT_TICKERS
    asyncio.run(main(tickers))
