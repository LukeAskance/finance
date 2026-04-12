"""portfolio_mcp.py — MCP server exposing portfolio data and market tools to LLMs.

Run standalone (Claude Code / Claude Desktop):
    python portfolio_mcp.py

Registration in .mcp.json:
    {
      "mcpServers": {
        "portfolio": {
          "command": "/Users/george/code/money/.venv/bin/python",
          "args": ["/Users/george/code/money/portfolio_mcp.py"]
        }
      }
    }
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from pathlib import Path

from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Logging — stderr only (stdout is reserved for the MCP stdio protocol)
# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s [portfolio-mcp] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("portfolio_mcp")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_DB_PATH = Path(__file__).parent / "portfolio.db"
_EDGAR_IDENTITY = os.getenv("EDGAR_IDENTITY", "portfolio-mcp gflammer@icloud.com")

mcp = FastMCP("portfolio")
log.info("Server initialised — DB: %s", _DB_PATH)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _latest_snapshot_date(conn: sqlite3.Connection) -> str | None:
    row = conn.execute("SELECT MAX(date) FROM daily_position_snapshots").fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Tools — Portfolio / Database
# ---------------------------------------------------------------------------

@mcp.tool()
def get_portfolio_positions(
    min_market_value: float = 0.0,
    instrument_type: str = "",
) -> list[dict]:
    """
    Return all positions from the most-recent portfolio snapshot stored in the
    local SQLite database, aggregated across accounts, sorted by market value.

    Args:
        min_market_value: Only return positions with market value >= this amount.
        instrument_type: Filter by type: 'equity', 'fund', 'cash', 'option'.
                         Leave blank for all types.
    """
    log.info("get_portfolio_positions min_value=%s type=%r", min_market_value, instrument_type)
    with _db() as conn:
        snap_date = _latest_snapshot_date(conn)
        if not snap_date:
            log.warning("get_portfolio_positions: no snapshot found in DB")
            return []

        sql = """
            SELECT
                i.symbol,
                i.instrument_type,
                i.name        AS description,
                SUM(s.market_value) AS market_value,
                SUM(s.quantity)     AS quantity,
                s.last_price
            FROM daily_position_snapshots s
            JOIN instruments i ON i.id = s.instrument_id
            WHERE s.date = ?
            GROUP BY i.symbol, i.instrument_type, i.name, s.last_price
            HAVING SUM(s.market_value) >= ?
            ORDER BY SUM(s.market_value) DESC
        """
        rows = conn.execute(sql, (snap_date, min_market_value)).fetchall()

        result = [dict(r) for r in rows]
        if instrument_type:
            result = [r for r in result if r["instrument_type"] == instrument_type.lower()]

        for r in result:
            r["as_of"] = snap_date
            r["market_value"] = round(r["market_value"], 2)
            r["quantity"] = round(r["quantity"], 4)

        log.info("get_portfolio_positions → %d rows (as_of %s)", len(result), snap_date)
        return result


@mcp.tool()
def get_positions_by_account(account_name: str = "") -> list[dict]:
    """
    Return positions broken out per account from the most-recent snapshot.
    Optionally filter to a single account (partial name match, case-insensitive).

    Args:
        account_name: Partial account name to filter by, e.g. 'Roth'. Leave
                      blank to return all accounts.
    """
    log.info("get_positions_by_account account=%r", account_name)
    with _db() as conn:
        snap_date = _latest_snapshot_date(conn)
        if not snap_date:
            log.warning("get_positions_by_account: no snapshot found in DB")
            return []

        sql = """
            SELECT
                a.name          AS account,
                i.symbol,
                i.instrument_type,
                s.quantity,
                s.market_value,
                s.last_price
            FROM daily_position_snapshots s
            JOIN instruments i ON i.id = s.instrument_id
            JOIN accounts   a ON a.id = s.account_id
            WHERE s.date = ?
            ORDER BY a.name, s.market_value DESC
        """
        rows = conn.execute(sql, (snap_date,)).fetchall()
        result = [dict(r) for r in rows]

        if account_name:
            needle = account_name.lower()
            result = [r for r in result if needle in r["account"].lower()]

        for r in result:
            r["as_of"] = snap_date
            r["market_value"] = round(r["market_value"], 2)
            r["quantity"] = round(r["quantity"], 4)

        log.info("get_positions_by_account → %d rows (as_of %s)", len(result), snap_date)
        return result


@mcp.tool()
def get_portfolio_totals(days: int = 90) -> list[dict]:
    """
    Return daily total portfolio market value over the past N days from the
    local SQLite database.

    Args:
        days: Number of calendar days of history to return (default 90).
    """
    log.info("get_portfolio_totals days=%d", days)
    with _db() as conn:
        sql = """
            SELECT
                s.date,
                SUM(s.total_market_value) AS total_market_value
            FROM daily_account_snapshots s
            WHERE s.date >= date('now', ? || ' days')
            GROUP BY s.date
            ORDER BY s.date ASC
        """
        rows = conn.execute(sql, (f"-{days}",)).fetchall()
        result = [
            {"date": r["date"], "total_market_value": round(r["total_market_value"], 2)}
            for r in rows
        ]
        log.info("get_portfolio_totals → %d days of data", len(result))
        return result


@mcp.tool()
def get_account_totals(days: int = 30) -> list[dict]:
    """
    Return daily market value per account over the past N days.

    Args:
        days: Number of calendar days of history (default 30).
    """
    log.info("get_account_totals days=%d", days)
    with _db() as conn:
        sql = """
            SELECT
                s.date,
                a.name  AS account,
                s.total_market_value,
                s.cash_balance
            FROM daily_account_snapshots s
            JOIN accounts a ON a.id = s.account_id
            WHERE s.date >= date('now', ? || ' days')
            ORDER BY s.date ASC, a.name ASC
        """
        rows = conn.execute(sql, (f"-{days}",)).fetchall()
        result = [
            {
                "date": r["date"],
                "account": r["account"],
                "total_market_value": round(r["total_market_value"], 2),
                "cash_balance": round(r["cash_balance"], 2),
            }
            for r in rows
        ]
        log.info("get_account_totals → %d rows", len(result))
        return result


# ---------------------------------------------------------------------------
# Tools — Market Data (live, via existing modules)
# ---------------------------------------------------------------------------

def _fmt_market_cap(market_cap: int) -> str:
    if market_cap >= 1_000_000_000_000:
        return f"${market_cap / 1_000_000_000_000:.2f}T"
    if market_cap >= 1_000_000_000:
        return f"${market_cap / 1_000_000_000:.2f}B"
    return f"${market_cap / 1_000_000:.2f}M"


@mcp.tool()
def get_market_cap(ticker: str) -> dict:
    """
    Return the current market capitalisation for a ticker via yfinance.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.

    Returns a dict with keys:
        symbol, date, market_cap (raw integer), market_cap_str (human-readable).
    """
    import datetime
    import yfinance as yf

    ticker = ticker.upper()
    log.info("get_market_cap ticker=%s", ticker)

    info = yf.Ticker(ticker).info
    if not (market_cap := info.get("marketCap")):
        raise ValueError(f"No market cap data available for {ticker}")

    log.info("get_market_cap %s → %s", ticker, _fmt_market_cap(market_cap))
    return {
        "symbol":         ticker,
        "date":           datetime.date.today().isoformat(),
        "market_cap":     market_cap,
        "market_cap_str": _fmt_market_cap(market_cap),
    }

@mcp.tool()
def get_financials(ticker: str) -> dict:
    """
    Return key financial metrics for a ticker: price, EPS, dividend yield%,
    P/E ratio, and cash-per-share (from SEC EDGAR).

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.
    """
    log.info("get_financials ticker=%s", ticker)
    from financials import get_financials as _get  # local module
    f = _get(ticker.upper())
    log.info("get_financials %s → price=%s pe=%s", ticker, f.price, f.pe_ratio)
    return {
        "ticker": f.ticker,
        "price": f.price,
        "eps": f.eps,
        "dividend_yield_pct": f.dividend_yield_pct,
        "pe_ratio": f.pe_ratio,
        "cash_per_share": f.cash_per_share,
        "cash_per_share_error": f.cash_per_share_error,
    }


@mcp.tool()
def get_insider_activity(ticker: str, lookback_days: int = 365) -> dict:
    """
    Return insider buy/sell/10b5-1 plan transaction summary for a ticker,
    sourced from SEC EDGAR Form 4 filings.

    Args:
        ticker: Stock ticker symbol, e.g. 'NVDA'.
        lookback_days: How many days of history to scan (default 365).
    """
    log.info("get_insider_activity ticker=%s lookback_days=%d", ticker, lookback_days)
    from financials import get_insider_transactions
    ins = get_insider_transactions(ticker.upper(), lookback_days=lookback_days)
    log.info("get_insider_activity %s → buys=%d sells=%d 10b51=%d", ticker, ins.buys, ins.sells, ins.sells_10b51)
    return {
        "ticker": ins.ticker,
        "period_days": lookback_days,
        "buys": ins.buys,
        "buys_shares": ins.buys_shares,
        "sells": ins.sells,
        "sells_shares": ins.sells_shares,
        "sells_10b51": ins.sells_10b51,
        "sells_10b51_shares": ins.sells_10b51_shares,
    }


@mcp.tool()
def get_institutional_holders(ticker: str) -> list[dict]:
    """
    Return the top institutional holders for a ticker (% of shares outstanding),
    sourced from yfinance.

    Args:
        ticker: Stock ticker symbol, e.g. 'AAPL'.
    """
    log.info("get_institutional_holders ticker=%s", ticker)
    from institutional import get_institutional_ownership
    result = get_institutional_ownership(ticker.upper())
    log.info("get_institutional_holders %s → %d holders", ticker, len(result))
    return result


@mcp.tool()
def get_dividend_forecast(ticker: str, shares: float, years: int = 3) -> dict:
    """
    Return a bear/base/bull dividend income forecast for a position,
    using SEC EDGAR dividend history.

    Args:
        ticker: Stock ticker symbol.
        shares: Number of shares held.
        years: Forecast horizon in years (default 3).
    """
    log.info("get_dividend_forecast ticker=%s shares=%s years=%d", ticker, shares, years)
    from dividend_prediction import DividendForecaster
    f = DividendForecaster.from_edgar(ticker.upper(), shares=shares)
    result = f.project(years=years)
    log.info("get_dividend_forecast %s → scenarios: %s", ticker, list(result.scenarios))
    return {
        scenario: {
            "annual_dividends": s.annual_dividends,
            "growth_rate": s.growth_rate,
        }
        for scenario, s in result.scenarios.items()
    }


@mcp.tool()
def get_price_history(ticker: str, days: int = 90) -> list[dict]:
    """
    Return recent closing price history for a ticker from yfinance.

    Args:
        ticker: Stock ticker symbol.
        days: Number of calendar days of history (default 90).
    """
    log.info("get_price_history ticker=%s days=%d", ticker, days)
    import datetime
    import yfinance as yf

    end = datetime.date.today()
    start = end - datetime.timedelta(days=days)
    hist = yf.Ticker(ticker.upper()).history(start=start.isoformat(), end=end.isoformat())
    if hist.empty:
        log.warning("get_price_history %s → no data returned", ticker)
        return []
    result = [
        {"date": str(d.date()), "close": round(float(row["Close"]), 4)}
        for d, row in hist.iterrows()
    ]
    log.info("get_price_history %s → %d records", ticker, len(result))
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting portfolio MCP server (stdio transport)")
    try:
        mcp.run()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("portfolio MCP server shutting down")
        os._exit(0)
