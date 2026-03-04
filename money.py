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
original_portfolio_rows: list[dict[str, Any]] = []


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


def aggregate_rows_by_symbol(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for row in rows:
        symbol = str(row.get('symbol', '')).upper()
        if not symbol:
            continue

        if symbol not in grouped:
            grouped[symbol] = {
                'symbol': symbol,
                'type': row.get('type', ''),
                'account': 'Aggregated',
                'underlying': row.get('underlying', symbol),
                'quantity': 0.0,
                'last': 0.0,
                'market_value': 0.0,
                'pl': 0.0,
            }

        grouped[symbol]['quantity'] += float(row.get('quantity', 0.0))
        grouped[symbol]['market_value'] += float(row.get('market_value', 0.0))
        grouped[symbol]['pl'] += float(row.get('pl', 0.0))
        grouped[symbol]['last'] = float(row.get('last', 0.0))

    aggregated = list(grouped.values())
    for row in aggregated:
        row['quantity'] = round(float(row['quantity']), 4)
        row['market_value'] = round(float(row['market_value']), 2)
        row['pl'] = round(float(row['pl']), 2)
        row['last'] = round(float(row['last']), 4)

    aggregated.sort(
        key=lambda row: float(row.get('market_value', 0.0)),
        reverse=True,
    )
    return aggregated


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
    global original_portfolio_rows
    load_portfolio_button.disable()
    load_portfolio_button.text = 'Loading...'
    try:
        rows = await asyncio.to_thread(fetch_portfolio_rows)
        original_portfolio_rows = [dict(row) for row in rows]
        portfolio_table.rows = rows
        portfolio_table.update()
        ui.notify(f'Loaded {len(rows)} portfolio rows', color='positive')
    except Exception as exc:
        ui.notify(f'Portfolio load failed: {exc}', color='negative')
    finally:
        load_portfolio_button.text = 'Load Portfolio'
        load_portfolio_button.enable()


def aggregate_click():
    rows = list(portfolio_table.rows or [])
    if not rows:
        ui.notify('Load portfolio rows first', color='warning')
        return

    aggregated_rows = aggregate_rows_by_symbol(rows)
    portfolio_table.rows = aggregated_rows
    portfolio_table.update()
    ui.notify(
        f'Aggregated to {len(aggregated_rows)} symbols',
        color='positive',
    )


def unaggregate_click():
    if not original_portfolio_rows:
        ui.notify('No original rows to restore yet', color='warning')
        return

    restored_rows = [dict(row) for row in original_portfolio_rows]
    portfolio_table.rows = restored_rows
    portfolio_table.update()
    ui.notify(
        f'Restored {len(restored_rows)} original rows',
        color='positive',
    )


""" with ui.card():
    ui.label('Automation Dashboard')
    ui.button('Clean Files', on_click=lambda: run_task('clean.py'))
    ui.button('Backup Data', on_click=lambda: run_task('backup.py'))
    ui.button('Generate Reports', on_click=lambda: run_task('report.py'))

with ui.card():
    ui.label('System Status').classes('text-xl font-semibold')
    ui.button('Run Job').classes('bg-green-600 text-white px-4 py-2')
 """


with ui.row().classes('w-full items-start gap-4 no-wrap'):
    with ui.column().classes('w-1/3 min-w-[360px]'):
        with ui.card().classes('w-full'):
            ui.label('Schwab Quote').classes('text-xl font-semibold')
            symbol_input = ui.input('Symbol').props('clearable').classes('w-40')
            ui.button('Get Quote', on_click=get_quote_click)
            load_portfolio_button = ui.button(
                'Load Portfolio',
                on_click=load_portfolio_click,
            )
            ui.button('aggregate', on_click=aggregate_click)
            ui.button('unaggregate', on_click=unaggregate_click)

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

    with ui.column().classes('flex-1 min-w-0'):
        with ui.card().classes('w-full'):
            ui.label('Portfolio').classes('text-xl font-semibold')
            portfolio_columns = [
                {
                    'name': 'symbol',
                    'label': 'Symbol',
                    'field': 'symbol',
                    'sortable': True,
                    'style': 'width: 10ch; max-width: 10ch;',
                },
                {
                    'name': 'type',
                    'label': 'Type',
                    'field': 'type',
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
            with ui.element('div').classes('w-full max-h-[75vh] overflow-auto'):
                portfolio_table = ui.table(
                    columns=portfolio_columns,
                    rows=[],
                ).classes('w-max min-w-full')
            portfolio_table.props(
                'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
            )


ui.run(port=8000, reload=False, )
