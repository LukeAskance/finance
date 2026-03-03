#!/usr/bin/env python3

import argparse
import json
import os

from schwab_api import SchwabAPI


try:
    import schwabdev
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: schwabdev. "
        "Install it first (e.g. `pip install schwabdev`)."
    ) from exc


def build_client() -> "schwabdev.Client":
    app_key = os.getenv("SCHWAB_APP_KEY")
    app_secret = os.getenv("SCHWAB_APP_SECRET")
    callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1")
    tokens_file = os.getenv("SCHWAB_TOKENS_FILE", "tokens.json")

    if not app_key or not app_secret:
        raise SystemExit(
            "Set SCHWAB_APP_KEY and SCHWAB_APP_SECRET "
            "in your environment first."
        )

    return schwabdev.Client(
        app_key,
        app_secret,
        callback_url,
        tokens_file=tokens_file,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch one stock quote from Schwab"
    )
    parser.add_argument("symbol", help="Ticker symbol, e.g. AAPL")
    args = parser.parse_args()

    api = SchwabAPI(build_client())
    if not (quote_response := api.get_quote(args.symbol.upper())):
        raise SystemExit(f"No quote returned for symbol: {args.symbol}")

    print(json.dumps(quote_response, indent=2))


if __name__ == "__main__":
    main()
