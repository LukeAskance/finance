"""institutional.py — Top-10 institutional holders via yfinance (SEC 13F data)."""

from __future__ import annotations

import yfinance as yf  # type: ignore[import-untyped]


def get_institutional_ownership(ticker: str) -> list[dict]:
    """Return the top 10 institutional holders for *ticker*, sorted by
    percentage of shares outstanding (descending).

    Each dict contains:
        holder        - institution name
        shares        - number of shares held
        pct_out       - % of shares outstanding (e.g. 7.35 for 7.35%)
        value         - market value of holding in USD
        date_reported - date of the most recent 13F filing (YYYY-MM-DD)
    """
    ticker = ticker.strip().upper()
    if not ticker:
        return []

    try:
        t = yf.Ticker(ticker)
        df = t.institutional_holders
        if df is None or df.empty:
            return []

        # Normalise column names - yfinance capitalisation varies by version
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        col = {c.lower(): c for c in df.columns}

        def _col(*candidates: str) -> str | None:
            for name in candidates:
                if name in col:
                    return col[name]
            return None

        h_col = _col("holder")
        sh_col = _col("shares")
        dt_col = _col("date reported", "datereported")
        # yfinance >= 0.2 uses "pctHeld"; older versions used "% Out"
        pc_col = _col("pctheld", "% out", "pctout", "% outstanding")
        vl_col = _col("value")

        rows: list[dict] = []
        for _, row in df.iterrows():
            holder = str(row[h_col]) if h_col else ""
            shares = int(row[sh_col] or 0) if sh_col else 0
            date_rep = str(row[dt_col])[:10] if dt_col else ""
            pct_raw = float(row[pc_col] or 0) if pc_col else 0.0
            # Values are decimal fractions (0.0972 = 9.72%)
            pct_out = round(pct_raw * 100, 2)
            value = int(row[vl_col] or 0) if vl_col else 0
            rows.append({
                "holder": holder,
                "shares": shares,
                "pct_out": pct_out,
                "value": value,
                "date_reported": date_rep,
            })

        rows.sort(key=lambda r: r["pct_out"], reverse=True)
        return rows[:10]

    except Exception as exc:
        return [{
            "holder": f"Error: {exc}",
            "shares": 0,
            "pct_out": 0.0,
            "value": 0,
            "date_reported": "",
        }]
