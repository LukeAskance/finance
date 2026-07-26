# * (c) 1066-2050 George... Flammer All Rights Reserved
"""schwab_api.py — single point of contact for
    all Schwab REST + streaming calls.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def _call(request_fn, retries: int = 3, backoff: float = 1.0):
    """Call request_fn() -> requests.Response, retrying on 429 (rate limit)
    with backoff. The 16-way concurrent quote fetch in positions.py routinely
    bursts past Schwab's per-minute limit; retrying here covers every caller.
    """
    for attempt in range(retries):
        r = request_fn()
        if r.status_code != 429:
            return r
        wait = float(r.headers.get("Retry-After", backoff * (2**attempt)))
        logger.warning("Schwab API rate limited (429); retrying in %.1fs", wait)
        time.sleep(wait)
    return request_fn()


def _json(r):
    """r.json(), but on failure raise with status + body so the real
    cause (expired session, rate limit, HTML error page) isn't hidden
    behind a bare JSONDecodeError.
    """
    try:
        return r.json()
    except ValueError as exc:
        raise ValueError(
            f"Schwab API returned non-JSON (status {r.status_code}): "
            f"{r.text[:300]!r}"
        ) from exc


class SchwabAPI:
    """Wraps schwabdev.Client, consolidating all direct Schwab API calls."""

    def __init__(self, client):
        self.client = client

    # ── Account Domain ──────────────────────────────

    def get_linked_accounts(self) -> list:
        """Get all linked account numbers and hashes."""
        return _json(_call(self.client.account_linked))

    def get_account_details(
        self, account_hash: str, fields: str = "positions"
    ) -> dict:
        """Get account details (positions, balances)
        for a single account hash.
        """
        return _json(
            _call(lambda: self.client.account_details(account_hash, fields=fields))
        )

    # ── Quote Domain ────────────────────────────────

    def get_quote(self, symbol: str, gabby: bool = False) -> Optional[dict]:
        """
        Get raw quote response for a symbol.
        Returns the full response dict (keyed by symbol) or {} on 404.
        """
        if "-" in symbol:
            return {}

        r = _call(lambda: self.client.quote(symbol))
        if r.status_code == 404:
            if gabby:
                logger.warning("get_quote(%s) got 404", symbol)
            return {}

        return _json(r)

    def get_quote_and_fundamentals(
        self, symbol: str, gabby: bool = False
    ) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Get both quote data and fundamentals in a single API call.
        Returns (qDict, funDict) — both can be None if quote fails.
        """
        if "-" in symbol:
            return None, None

        symbol = symbol.upper()
        r = _call(lambda: self.client.quote(symbol))
        if r.status_code == 404:
            if gabby:
                logger.warning(
                    "get_quote_and_fundamentals: 404 response to quote(%s)",
                    symbol,
                )
            return None, None

        response_data = _json(r)

        if symbol not in response_data:
            if gabby:
                logger.warning(
                    "get_quote_and_fundamentals: symbol %s not in response",
                    symbol,
                )
            return None, None

        if gabby:
            logger.info("Quote payload for %s", symbol)
            logger.debug(json.dumps(response_data[symbol], indent=4))
            logger.debug(
                "Quote data:\n%s",
                json.dumps(response_data[symbol]["quote"], indent=4),
            )

        qDict = response_data[symbol]["quote"]
        funDict = response_data[symbol].get("fundamental", {})

        return qDict, funDict

    # ── History Domain ──────────────────────────────

    def get_price_history(
        self,
        symbol: str,
        periodType: str = "month",
        period: int = 1,
        frequencyType: str = "daily",
        frequency: int = 1,
        gabby: bool = False,
    ) -> Optional[dict]:
        """Get historical price candles for a symbol. Returns None on 404."""
        r = _call(
            lambda: self.client.price_history(
                symbol,
                periodType=periodType,
                period=period,
                frequencyType=frequencyType,
                frequency=frequency,
            )
        )
        if r.status_code == 404:
            if gabby:
                logger.warning("get_price_history(%s) got 404", symbol)
            return None

        return _json(r)

    def get_transactions(
        self,
        account_hash: str,
        start: datetime,
        end: datetime,
        types: str = "TRADE",
    ) -> list:
        """Get transactions for an account within a date range."""
        r = _call(lambda: self.client.transactions(account_hash, start, end, types))
        return _json(r)

    # ── Options Domain ──────────────────────────────

    def get_expiration_dates(self, symbol: str) -> list:
        """Get option expiration chain for a symbol."""
        return _json(_call(lambda: self.client.option_expiration_chain(symbol)))

    def get_option_chain(self, symbol: str, **kwargs) -> dict:
        """Get option chain data. Pass any option_chains() kwargs through."""
        return _json(_call(lambda: self.client.option_chains(symbol, **kwargs)))

    # ── Streaming ───────────────────────────────────

    @property
    def stream(self):
        """Access the streaming client."""
        return self.client.stream


_shared_api: Optional["SchwabAPI"] = None
_shared_api_error: Optional[str] = None


def get_shared_api() -> "SchwabAPI":
    """Lazily build the one shared SchwabAPI instance from .env credentials,
    so every caller (money.py's UI, financials.py's alert metrics, ...)
    reuses a single schwabdev.Client / token-refresh thread instead of each
    spinning up its own.
    """
    global _shared_api, _shared_api_error
    if _shared_api is not None:
        return _shared_api
    if _shared_api_error is not None:
        raise RuntimeError(_shared_api_error)

    try:
        from schwabdev.client import Client as _SchwabClient
    except ImportError as exc:
        _shared_api_error = f"schwabdev import failed: {exc}"
        raise RuntimeError(_shared_api_error) from exc

    client = _SchwabClient(
        os.getenv("SCHWAB_APP_KEY"),
        os.getenv("SCHWAB_SECRET"),
        os.getenv("callback_url"),
        os.getenv("token_filename"),
    )
    _shared_api = SchwabAPI(client)
    return _shared_api
