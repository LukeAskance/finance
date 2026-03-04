# * (c) 1066-2050 George... Flammer All Rights Reserved
"""schwab_api.py — single point of contact for
    all Schwab REST + streaming calls.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Tuple


logger = logging.getLogger(__name__)


class SchwabAPI:
    """Wraps schwabdev.Client, consolidating all direct Schwab API calls."""

    def __init__(self, client):
        self.client = client

    # ── Account Domain ──────────────────────────────

    def get_linked_accounts(self) -> list:
        """Get all linked account numbers and hashes."""
        return self.client.account_linked().json()

    def get_account_details(
        self, account_hash: str, fields: str = "positions"
    ) -> dict:
        """Get account details (positions, balances)
        for a single account hash.
        """
        return self.client.account_details(account_hash, fields=fields).json()

    # ── Quote Domain ────────────────────────────────

    def get_quote(self, symbol: str, gabby: bool = False) -> Optional[dict]:
        """
        Get raw quote response for a symbol.
        Returns the full response dict (keyed by symbol) or {} on 404.
        """
        if "-" in symbol:
            return {}

        r = self.client.quote(symbol)
        if r.status_code == 404:
            if gabby:
                logger.warning("get_quote(%s) got 404", symbol)
            return {}

        return r.json()

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
        r = self.client.quote(symbol)
        if r.status_code == 404:
            if gabby:
                logger.warning(
                    "get_quote_and_fundamentals: 404 response to quote(%s)",
                    symbol,
                )
            return None, None

        response_data = r.json()

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
        r = self.client.price_history(
            symbol,
            periodType=periodType,
            period=period,
            frequencyType=frequencyType,
            frequency=frequency,
        )
        if r.status_code == 404:
            if gabby:
                logger.warning("get_price_history(%s) got 404", symbol)
            return None

        return r.json()

    def get_transactions(
        self,
        account_hash: str,
        start: datetime,
        end: datetime,
        types: str = "TRADE",
    ) -> list:
        """Get transactions for an account within a date range."""
        r = self.client.transactions(account_hash, start, end, types)
        return r.json()

    # ── Options Domain ──────────────────────────────

    def get_expiration_dates(self, symbol: str) -> list:
        """Get option expiration chain for a symbol."""
        return self.client.option_expiration_chain(symbol).json()

    def get_option_chain(self, symbol: str, **kwargs) -> dict:
        """Get option chain data. Pass any option_chains() kwargs through."""
        return self.client.option_chains(symbol, **kwargs).json()

    # ── Streaming ───────────────────────────────────

    @property
    def stream(self):
        """Access the streaming client."""
        return self.client.stream
