#! /Users/george/code/money/.venv/bin/python3

import asyncio
import json
import os
import sys
import time
import subprocess
from typing import Any
from dotenv import load_dotenv

from nicegui import ui
from schwab_api import SchwabAPI
from positions import load_portfolio_positions

try:
    from schwabdev.client import Client as _SchwabClient
    _schwab_import_error = None
except ImportError as exc:
    _SchwabClient = None
    _schwab_import_error = exc

SchwabClient: Any = _SchwabClient

load_dotenv()


def getClient():
    if SchwabClient is None:
        raise RuntimeError(
            f'schwabdev import failed: {_schwab_import_error}'
        )

    return SchwabClient(
        os.getenv('SCHWAB_APP_KEY'),
        os.getenv('SCHWAB_SECRET'),
        os.getenv('callback_url'),
        os.getenv('token_filename'),
    )


def generate_report():
    time.sleep(2)
    return 'Report generated successfully'


def run_report():
    result = generate_report()
    ui.notify(result)


def run_task(script: str):
    subprocess.run([sys.executable, script])
    ui.notify(f'{script} finished')




api: SchwabAPI | None = None


def get_api() -> SchwabAPI:
    global api
    if api is None:
        api = SchwabAPI(getClient())
    return api


def fetch_quote(symbol: str) -> dict[str, Any] | None:
    return get_api().get_quote(symbol.upper())


def fetch_portfolio_rows() -> list[dict[str, Any]]:
    positions = load_portfolio_positions(get_api())

    def _underlying_symbol(value: Any) -> str:
        if isinstance(value, str):
            return value
        return str(getattr(value, 'symbol', value))

    rows = [
        {
            'symbol': p.symbol,
            'type': p.position_type,
            'broker': p.broker,
            'account': p.account_name,
            'underlying': _underlying_symbol(p.underlying),
            'quantity': round(float(p.quantity), 4),
            'last': round(float(p.last_price), 4),
            'market_value': round(float(p.market_value), 2),
            'pl': round(float(p.pl_total), 2),
        }
        for p in positions
    ]

    def _market_value_for_sort(row: dict[str, Any]) -> float:
        value = row.get('market_value', 0.0)
        if isinstance(value, (int, float, str)):
            try:
                return float(value)
            except ValueError:
                return 0.0
        return 0.0

    rows.sort(
        key=_market_value_for_sort,
        reverse=True,
    )
    return rows


def set_quote_summary(
    symbol: str,
    last: str = '-',
    bid: str = '-',
    ask: str = '-',
    open_price: str = '-',
    high: str = '-',
    low: str = '-',
    close: str = '-',
):
    symbol_value.text = symbol
    last_value.text = last
    bid_value.text = bid
    ask_value.text = ask
    open_value.text = open_price
    high_value.text = high
    low_value.text = low
    close_value.text = close


def quote_number(quote_data: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = quote_data.get(key)
        if value is not None:
            return f'{value}'
    return '-'


async def get_quote_click():
    symbol = symbol_input.value.strip()
    if not symbol:
        ui.notify('Enter a ticker symbol first', color='warning')
        return

    set_quote_summary(
        symbol.upper(),
        'Loading...',
        '-',
        '-',
        '-',
        '-',
        '-',
        '-',
    )
    quote_output.value = 'Loading...'
    try:
        result = await asyncio.to_thread(fetch_quote, symbol)
        if not result:
            set_quote_summary(symbol.upper())
            quote_output.value = f'No quote returned for {symbol.upper()}'
            return

        symbol_key = symbol.upper()
        quote_data = result.get(symbol_key, {}).get('quote', {})

        set_quote_summary(
            symbol_key,
            quote_number(quote_data, 'lastPrice', 'mark'),
            quote_number(quote_data, 'bidPrice', 'bid'),
            quote_number(quote_data, 'askPrice', 'ask'),
            quote_number(quote_data, 'openPrice', 'open'),
            quote_number(quote_data, 'highPrice', 'high'),
            quote_number(quote_data, 'lowPrice', 'low'),
            quote_number(quote_data, 'closePrice', 'close'),
        )
        quote_output.value = json.dumps(result, indent=2)
    except Exception as exc:
        set_quote_summary(symbol.upper())
        quote_output.value = f'Quote error: {exc}'


async def load_portfolio_click():
    load_portfolio_button.disable()
    load_portfolio_button.text = 'Loading...'
    try:
        rows = await asyncio.to_thread(fetch_portfolio_rows)
        portfolio_table.rows = rows
        portfolio_table.update()
        ui.notify(f'Loaded {len(rows)} portfolio rows', color='positive')
    except Exception as exc:
        ui.notify(f'Portfolio load failed: {exc}', color='negative')
    finally:
        load_portfolio_button.text = 'Load Portfolio'
        load_portfolio_button.enable()


""" with ui.card():
    ui.label('Automation Dashboard')
    ui.button('Clean Files', on_click=lambda: run_task('clean.py'))
    ui.button('Backup Data', on_click=lambda: run_task('backup.py'))
    ui.button('Generate Reports', on_click=lambda: run_task('report.py'))

with ui.card():
    ui.label('System Status').classes('text-xl font-semibold')
    ui.button('Run Job').classes('bg-green-600 text-white px-4 py-2')
 """


with ui.card():
    ui.label('Schwab Quote').classes('text-xl font-semibold')
    symbol_input = ui.input('Symbol').props('clearable').classes('w-40')
    ui.button('Get Quote', on_click=get_quote_click)
    load_portfolio_button = ui.button(
        'Load Portfolio',
        on_click=load_portfolio_click,
    )

    with ui.row():
        ui.label('Symbol:')
        symbol_value = ui.label('-').classes('font-semibold')
        ui.label('Last:')
        last_value = ui.label('-').classes('font-semibold')
        ui.label('Bid:')
        bid_value = ui.label('-').classes('font-semibold')
        ui.label('Ask:')
        ask_value = ui.label('-').classes('font-semibold')

    with ui.row():
        ui.label('Open:')
        open_value = ui.label('-').classes('font-semibold')
        ui.label('High:')
        high_value = ui.label('-').classes('font-semibold')
        ui.label('Low:')
        low_value = ui.label('-').classes('font-semibold')
        ui.label('Close:')
        close_value = ui.label('-').classes('font-semibold')

    quote_output = ui.textarea(label='Quote JSON')
    quote_output.props('readonly').classes('w-full')

    portfolio_columns = [
        {
            'name': 'symbol',
            'label': 'Symbol',
            'field': 'symbol',
            'sortable': True,
        },
        {'name': 'type', 'label': 'Type', 'field': 'type', 'sortable': True},
        {
            'name': 'broker',
            'label': 'Broker',
            'field': 'broker',
            'sortable': True,
        },
        {
            'name': 'account',
            'label': 'Account',
            'field': 'account',
            'sortable': True,
        },
        {
            'name': 'underlying',
            'label': 'Underlying',
            'field': 'underlying',
            'sortable': True,
        },
        {
            'name': 'quantity',
            'label': 'Qty',
            'field': 'quantity',
            'sortable': True,
            'align': 'right',
        },
        {
            'name': 'last',
            'label': 'Last',
            'field': 'last',
            'sortable': True,
            'align': 'right',
        },
        {
            'name': 'market_value',
            'label': 'Mkt Value',
            'field': 'market_value',
            'sortable': True,
            'align': 'right',
        },
        {
            'name': 'pl',
            'label': 'P/L',
            'field': 'pl',
            'sortable': True,
            'align': 'right',
        },
    ]
    with ui.element('div').classes('w-full max-h-96 overflow-auto'):
        portfolio_table = ui.table(
            columns=portfolio_columns,
            rows=[],
        ).classes('w-max min-w-full')
    portfolio_table.props(
        'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
    )


ui.run(port=8000, reload=False, )
