#! /Users/george/code/money/.venv/bin/python3

import asyncio
from datetime import datetime
import json
import os
import re
import sys
import time
import subprocess
from contextlib import suppress
from typing import Any
from dotenv import load_dotenv

from nicegui import ui
from schwab_api import SchwabAPI
from positions import load_portfolio_positions
from analysis_module import PortfolioAnalysisEngine
import options
import fundamentals

try:
    from schwabdev.client import Client as _SchwabClient
    _schwab_import_error = None
except ImportError as exc:
    _SchwabClient = None
    _schwab_import_error = exc

SchwabClient: Any = _SchwabClient

load_dotenv()

dark_mode = ui.dark_mode()
dark_mode.enable()

ui.add_head_html('''
<style>
.portfolio-table-wrap .q-table__middle {
    max-height: 75vh;
    overflow: auto;
}

.portfolio-table-wrap thead tr th {
    position: sticky;
    top: 0;
    z-index: 2;
    background: var(--q-dark-page);
}
</style>
''')


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


def _collect_historical_series(
    symbols: list[str],
    days: int,
) -> dict[str, list[tuple[datetime, float]]]:
    api_client = get_api()
    series: dict[str, list[tuple[datetime, float]]] = {}
    for symbol in symbols:
        raw_points = fundamentals.get_historicals(
            api=api_client,
            name=symbol,
            days=days,
            gabby=False,
        )
        parsed_points: list[tuple[datetime, float]] = []
        for date_text, close_value in raw_points or []:
            try:
                parsed_points.append(
                    (
                        datetime.strptime(str(date_text), '%Y-%m-%d'),
                        float(close_value),
                    )
                )
            except (TypeError, ValueError):
                continue
        if parsed_points:
            series[symbol] = parsed_points
    return series


def _render_historicals_plot(
    symbol_series: dict[str, list[tuple[datetime, float]]],
    normalize: bool,
) -> None:
    import matplotlib.pyplot as plt

    historicals_plot_host.clear()
    with historicals_plot_host:
        if not symbol_series:
            ui.label('No historical data found for the selected symbols.').classes(
                'text-sm text-orange'
            )
            return

        with ui.pyplot(figsize=(16, 7), close=False).classes('w-full'):
            for symbol, points in symbol_series.items():
                points_sorted = sorted(points, key=lambda item: item[0])
                x_values = [point[0] for point in points_sorted]
                y_values = [point[1] for point in points_sorted]
                if not y_values:
                    continue

                if normalize and y_values[0] != 0:
                    base_value = y_values[0]
                    y_values = [
                        ((value / base_value) - 1.0) * 100.0
                        for value in y_values
                    ]

                plt.plot(x_values, y_values, linewidth=2, label=symbol)

            plt.grid(True, alpha=0.3)
            plt.legend()
            plt.xlabel('Date')
            plt.ylabel('Change %' if normalize else 'Price ($)')
            plt.title('Historical Stock Prices')
            plt.tight_layout()




api: SchwabAPI | None = None
original_portfolio_rows: list[dict[str, Any]] = []
raw_chain_data: dict[str, Any] | None = None
pending_chain_render_task: asyncio.Task[Any] | None = None
chain_dte_min: int | None = None
chain_dte_max: int | None = None
chain_step_contracts: list[dict[str, Any]] = []
chain_step_index: int = 0
filtered_chain_data: dict[str, Any] | None = None
analysis_engine = PortfolioAnalysisEngine()
analysis_default_models = {
    'claude': 'claude-sonnet-4-20250514',
    'perplexity': 'sonar',
}


def get_api() -> SchwabAPI:
    global api
    if api is None:
        api = SchwabAPI(getClient())
    return api


def fetch_quote(symbol: str) -> dict[str, Any] | None:
    return get_api().get_quote(symbol.upper())


def fetch_chain(symbol: str, contract_type: str) -> dict[str, Any]:
    return options.getChain(
        get_api(),
        name=symbol.upper(),
        put_or_call=contract_type,
    )


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _dte_from_exp_key(exp_key: str) -> int | None:
    if ':' not in exp_key:
        return None
    return _coerce_int(exp_key.rsplit(':', 1)[-1])


def _extract_chain_dte_values(chain: dict[str, Any]) -> list[int]:
    dte_values: list[int] = []
    for map_name in ('callExpDateMap', 'putExpDateMap'):
        exp_map = chain.get(map_name, {}) or {}
        for exp_key, strikes in exp_map.items():
            if (exp_key_dte := _dte_from_exp_key(exp_key)) is not None:
                dte_values.append(exp_key_dte)
            for _, contracts in (strikes or {}).items():
                for contract in contracts or []:
                    dte = _coerce_int(contract.get('daysToExpiration'))
                    if dte is not None:
                        dte_values.append(dte)
    return dte_values


def _filter_chain_by_dte(
    chain: dict[str, Any],
    dte_limit: int,
) -> dict[str, Any]:
    filtered: dict[str, Any] = {
        key: value
        for key, value in chain.items()
        if key not in {'callExpDateMap', 'putExpDateMap'}
    }

    for map_name in ('callExpDateMap', 'putExpDateMap'):
        exp_map = chain.get(map_name, {}) or {}
        new_exp_map: dict[str, Any] = {}
        for exp_key, strikes in exp_map.items():
            exp_key_dte = _dte_from_exp_key(exp_key)
            new_strikes: dict[str, Any] = {}
            for strike_key, contracts in (strikes or {}).items():
                kept = [
                    contract
                    for contract in (contracts or [])
                    if (
                        (
                            dte := _coerce_int(contract.get('daysToExpiration'))
                            or exp_key_dte
                        )
                        is not None
                        and dte <= dte_limit
                    )
                ]
                if kept:
                    new_strikes[strike_key] = kept
            if new_strikes:
                new_exp_map[exp_key] = new_strikes
        filtered[map_name] = new_exp_map

    return filtered


def _price_text(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f'{float(value):.2f}'
    if value is None:
        return '-'
    return str(value)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def _extract_step_contracts(chain: dict[str, Any]) -> list[dict[str, Any]]:
    contracts: list[dict[str, Any]] = []
    underlying = chain.get('underlying', {}) or {}
    underlying_price: float | None = None
    if isinstance(underlying, dict):
        underlying_last = underlying.get('last')
        if isinstance(underlying_last, (int, float)):
            underlying_price = float(underlying_last)
        else:
            try:
                if underlying_last is not None:
                    underlying_price = float(str(underlying_last).strip())
            except ValueError:
                underlying_price = None

    show_itm = bool(chain_step_itm_checkbox.value)
    show_ntm = bool(chain_step_ntm_checkbox.value)
    show_otm = bool(chain_step_otm_checkbox.value)

    def _matches_moneyness(contract: dict[str, Any]) -> bool:
        in_the_money = bool(contract.get('inTheMoney'))
        strike_value = contract.get('strikePrice')
        strike = None
        if isinstance(strike_value, (int, float)):
            strike = float(strike_value)
        else:
            try:
                if strike_value is not None:
                    strike = float(str(strike_value).strip())
            except ValueError:
                strike = None

        is_ntm = False
        if strike is not None and underlying_price is not None:
            is_ntm = abs(strike - underlying_price) <= 1.5

        if show_itm and in_the_money:
            return True
        if show_otm and not in_the_money:
            return True
        if show_ntm and is_ntm:
            return True
        return False

    for map_name in ('callExpDateMap', 'putExpDateMap'):
        exp_map = chain.get(map_name, {}) or {}
        for exp_key, strikes in exp_map.items():
            exp_key_dte = _dte_from_exp_key(exp_key)
            for _, contract_list in (strikes or {}).items():
                for contract in contract_list or []:
                    if not _matches_moneyness(contract):
                        continue
                    contract_dte = _coerce_int(
                        contract.get('daysToExpiration')
                    )
                    dte = contract_dte if contract_dte is not None else exp_key_dte
                    if dte is None:
                        continue
                    contracts.append(
                        {
                            'description': str(
                                contract.get('description')
                                or contract.get('symbol')
                                or '-'
                            ),
                            'bid': contract.get('bid') or contract.get('bidPrice'),
                            'ask': contract.get('ask') or contract.get('askPrice'),
                            'last': contract.get('last') or contract.get('lastPrice'),
                            'mark': (
                                contract.get('mark')
                                or contract.get('markPrice')
                                or contract.get('last')
                                or contract.get('lastPrice')
                            ),
                            'dte': dte,
                            'symbol': str(contract.get('symbol') or ''),
                            'inTheMoney': bool(
                                contract.get('inTheMoney')
                            ),
                            'strikePrice': contract.get('strikePrice'),
                        }
                    )

    contracts.sort(
        key=lambda row: (
            int(row.get('dte') or 0),
            str(row.get('symbol') or ''),
        )
    )
    return contracts


def _update_chain_step_display() -> None:
    if not chain_step_contracts:
        chain_step_position_label.text = '0 / 0'
        chain_step_description_value.text = '-'
        chain_step_bid_value.text = '-'
        chain_step_ask_value.text = '-'
        chain_step_last_value.text = '-'
        chain_step_mark_value.text = '-'
        chain_step_dte_value.text = '-'
        chain_step_premium_value.text = '-'
        chain_step_annualized_value.text = '-'
        chain_step_up_button.disable()
        chain_step_down_button.disable()
        return

    contract = chain_step_contracts[chain_step_index]
    chain_step_position_label.text = (
        f'{chain_step_index + 1} / {len(chain_step_contracts)}'
    )
    symbol = str(contract.get('symbol') or '-')
    dte = _coerce_int(contract.get('dte'))
    dte_text = str(dte) if dte is not None else '-'
    chain_step_description_value.text = symbol
    chain_step_dte_value.text = dte_text
    chain_step_bid_value.text = _price_text(contract.get('bid'))
    chain_step_ask_value.text = _price_text(contract.get('ask'))
    chain_step_last_value.text = _price_text(contract.get('last'))
    chain_step_mark_value.text = _price_text(contract.get('mark'))

    mark = _coerce_float(contract.get('mark'))
    strike = _coerce_float(contract.get('strikePrice'))
    premium_percent: float | None = None
    annualized_percent: float | None = None
    if mark is not None and strike and strike > 0:
        premium_percent = (mark / strike) * 100.0
    if premium_percent is not None and dte and dte > 0:
        annualized_percent = premium_percent * (364.0 / dte)

    chain_step_premium_value.text = (
        f'{premium_percent:.2f}%'
        if premium_percent is not None
        else '-'
    )
    chain_step_annualized_value.text = (
        f'{annualized_percent:.2f}%'
        if annualized_percent is not None
        else '-'
    )

    if chain_step_index <= 0:
        chain_step_up_button.disable()
    else:
        chain_step_up_button.enable()

    if chain_step_index >= len(chain_step_contracts) - 1:
        chain_step_down_button.disable()
    else:
        chain_step_down_button.enable()


def _set_chain_step_contracts(chain: dict[str, Any]) -> None:
    global chain_step_contracts, chain_step_index, filtered_chain_data
    filtered_chain_data = chain
    chain_step_contracts = _extract_step_contracts(chain)
    chain_step_index = 0
    _update_chain_step_display()


def on_chain_step_filter_change(_: Any = None) -> None:
    if filtered_chain_data is None:
        _set_chain_step_contracts({})
        return
    _set_chain_step_contracts(filtered_chain_data)


def on_chain_step_up() -> None:
    global chain_step_index
    if chain_step_index <= 0:
        return
    chain_step_index -= 1
    _update_chain_step_display()


def on_chain_step_down() -> None:
    global chain_step_index
    if chain_step_index >= len(chain_step_contracts) - 1:
        return
    chain_step_index += 1
    _update_chain_step_display()


async def _render_filtered_chain() -> None:
    if raw_chain_data is None:
        return

    dte_limit = _coerce_int(chain_dte_input.value)
    if dte_limit is None:
        return

    if chain_dte_min is not None and dte_limit < chain_dte_min:
        chain_output.value = (
            f'DTE must be >= {chain_dte_min} '
            f'and <= {chain_dte_max}'
        )
        return

    if chain_dte_max is not None and dte_limit > chain_dte_max:
        chain_output.value = (
            f'DTE must be >= {chain_dte_min} '
            f'and <= {chain_dte_max}'
        )
        return

    chain_dte_value_label.text = f'DTE <= {dte_limit}'
    filtered = await asyncio.to_thread(
        _filter_chain_by_dte,
        raw_chain_data,
        dte_limit,
    )
    _set_chain_step_contracts(filtered)
    chain_output.value = await asyncio.to_thread(
        lambda: json.dumps(filtered, indent=2)
    )


def schedule_chain_render() -> None:
    global pending_chain_render_task

    if (
        pending_chain_render_task is not None
        and not pending_chain_render_task.done()
    ):
        pending_chain_render_task.cancel()

    async def _debounced_render() -> None:
        try:
            await asyncio.sleep(0.12)
            await _render_filtered_chain()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            chain_output.value = f'Chain render error: {exc}'

    pending_chain_render_task = asyncio.create_task(_debounced_render())


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


async def get_chain_click():
    global raw_chain_data, pending_chain_render_task, chain_dte_min, chain_dte_max, filtered_chain_data
    symbol = chain_symbol_input.value.strip()
    if not symbol:
        ui.notify('Enter a ticker symbol first', color='warning')
        return

    contract_type = chain_contract_type.value or 'ALL'
    chain_output.value = 'Loading...'
    try:
        chain = await asyncio.to_thread(fetch_chain, symbol, contract_type)
        if not isinstance(chain, dict):
            raw_chain_data = None
            filtered_chain_data = None
            chain_dte_min = None
            chain_dte_max = None
            chain_dte_value_label.text = 'DTE <= -'
            _set_chain_step_contracts({})
            chain_output.value = f'Chain error: unexpected response type {type(chain).__name__}'
            return

        raw_chain_data = chain

        dte_values = _extract_chain_dte_values(chain)
        if dte_values:
            chain_dte_min = min(dte_values)
            chain_dte_max = max(dte_values)
            chain_dte_input.value = str(chain_dte_max)
            chain_dte_value_label.text = f'DTE <= {chain_dte_max}'
            schedule_chain_render()
        else:
            chain_dte_min = None
            chain_dte_max = None
            chain_dte_input.value = '365'
            chain_dte_value_label.text = 'DTE <= -'
            _set_chain_step_contracts(chain)
            chain_output.value = json.dumps(chain, indent=2)
    except Exception as exc:
        raw_chain_data = None
        filtered_chain_data = None
        chain_dte_min = None
        chain_dte_max = None
        if (
            pending_chain_render_task is not None
            and not pending_chain_render_task.done()
        ):
            pending_chain_render_task.cancel()
        chain_dte_input.value = '365'
        chain_dte_value_label.text = 'DTE <= -'
        _set_chain_step_contracts({})
        chain_output.value = f'Chain error: {exc}'


def on_chain_dte_change(_: Any = None) -> None:
    value = _coerce_int(chain_dte_input.value)
    if value is None:
        chain_dte_value_label.text = 'Enter a valid integer DTE'
        return

    if chain_dte_min is not None and chain_dte_max is not None:
        if value < chain_dte_min or value > chain_dte_max:
            chain_dte_value_label.text = (
                f'Enter {chain_dte_min}..{chain_dte_max}'
            )
            return

    schedule_chain_render()


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


async def exit_app_click():
    ui.notify('Closing browser tab and exiting...', color='warning')
    with suppress(Exception):
        await ui.run_javascript('window.open("", "_self");window.close();')

    loop = asyncio.get_running_loop()
    loop.call_later(0.2, lambda: os._exit(0))


async def refresh_analysis_snapshot_click() -> None:
    analysis_refresh_button.disable()
    analysis_refresh_button.text = 'Refreshing...'
    try:
        count = await asyncio.to_thread(
            analysis_engine.refresh_snapshot,
            get_api(),
            True,
            True,
        )
        as_of = analysis_engine.as_of.strftime('%Y-%m-%d %H:%M:%S') if analysis_engine.as_of else '-'
        analysis_status_value.text = (
            f'{count} aggregated positions loaded @ {as_of}'
        )
        analysis_rows_table.rows = []
        analysis_rows_table.update()
        analysis_answer.value = 'Snapshot refreshed. Ask a question below.'
    except Exception as exc:
        analysis_answer.value = f'Analysis refresh error: {exc}'
    finally:
        analysis_refresh_button.text = 'Refresh Snapshot'
        analysis_refresh_button.enable()


async def ask_analysis_click() -> None:
    question = analysis_question_input.value.strip()
    if not question:
        ui.notify('Enter a question first', color='warning')
        return

    analysis_ask_button.disable()
    analysis_ask_button.text = 'Thinking...'
    try:
        answer_text, rows = await asyncio.to_thread(
            analysis_engine.answer_question,
            question,
        )
        analysis_answer.value = answer_text
        analysis_rows_table.rows = rows
        analysis_rows_table.update()
    except Exception as exc:
        analysis_answer.value = f'Analysis error: {exc}'
    finally:
        analysis_ask_button.text = 'Ask'
        analysis_ask_button.enable()


async def ask_analysis_llm_click() -> None:
    question = analysis_question_input.value.strip()
    if not question:
        ui.notify('Enter a question first', color='warning')
        return

    analysis_llm_button.disable()
    analysis_llm_button.text = 'Querying LLM...'
    try:
        answer_text, rows = await asyncio.to_thread(
            analysis_engine.ask_llm,
            question,
            analysis_provider_select.value or 'claude',
            analysis_model_input.value or '',
            bool(analysis_grounded_toggle.value),
        )
        analysis_answer.value = answer_text
        analysis_rows_table.rows = rows
        analysis_rows_table.update()
    except Exception as exc:
        analysis_answer.value = f'LLM analysis error: {exc}'
    finally:
        analysis_llm_button.text = 'Ask LLM'
        analysis_llm_button.enable()


async def plot_historicals_click(silent_if_incomplete: bool = False) -> None:
    raw_symbols = (historicals_symbols_input.value or '').strip()
    symbols = [
        token.strip().upper()
        for token in re.split(r'[\s,]+', raw_symbols)
        if token.strip()
    ]
    if not symbols:
        if not silent_if_incomplete:
            ui.notify('Enter one or more ticker symbols', color='warning')
        return

    days = _coerce_int(historicals_days_input.value)
    if days is None or days <= 0:
        if not silent_if_incomplete:
            ui.notify('Enter a valid positive number of days', color='warning')
        return

    mode = (historicals_mode_select.value or 'denormalize').strip().lower()
    normalize = mode == 'normalize'

    historicals_plot_button.disable()
    historicals_plot_button.text = 'Plotting...'
    try:
        symbol_series = await asyncio.to_thread(
            _collect_historical_series,
            symbols,
            days,
        )
        _render_historicals_plot(symbol_series, normalize)
    except Exception as exc:
        ui.notify(f'Unable to plot historicals: {exc}', color='negative')
    finally:
        historicals_plot_button.text = 'Plot'
        historicals_plot_button.enable()


async def on_historicals_mode_change(_: Any = None) -> None:
    await plot_historicals_click(silent_if_incomplete=True)


def on_analysis_provider_change(_: Any = None) -> None:
    provider = (analysis_provider_select.value or 'claude').strip().lower()
    default_model = analysis_default_models.get(
        provider,
        analysis_default_models['claude'],
    )
    analysis_model_input.value = default_model


with ui.tabs().classes('w-full') as tabs:
    dashboard_tab = ui.tab('Dashboard')
    portfolio_tab = ui.tab('Portfolio')
    options_tab = ui.tab('Options')
    historicals_tab = ui.tab('Historicals')
    analysis_tab = ui.tab('Analysis')

with ui.tab_panels(tabs, value=portfolio_tab).classes('w-full'):
    with ui.tab_panel(dashboard_tab):
        with ui.card().classes('w-full'):
            ui.label('Dashboard').classes('text-xl font-semibold')
            ui.button('Exit Application', on_click=exit_app_click)

    with ui.tab_panel(portfolio_tab):
        with ui.row().classes('w-full items-start gap-4 no-wrap'):
            with ui.column().classes('w-1/3 min-w-[360px]'):
                with ui.card().classes('w-full'):
                    ui.label('Schwab Quote').classes('text-xl font-semibold')
                    symbol_input = ui.input('Symbol').props(
                        'clearable'
                    ).classes('w-40')
                    ui.button('Get Quote', on_click=get_quote_click)

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

                with ui.card().classes('w-full'):
                    ui.label('Portfolio Actions').classes('text-xl font-semibold')
                    load_portfolio_button = ui.button(
                        'Load Portfolio',
                        on_click=load_portfolio_click,
                    )
                    ui.button('aggregate', on_click=aggregate_click)
                    ui.button('unaggregate', on_click=unaggregate_click)

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
                    with ui.element('div').classes(
                        'w-full portfolio-table-wrap'
                    ):
                        portfolio_table = ui.table(
                            columns=portfolio_columns,
                            rows=[],
                        ).classes('w-max min-w-full')
                    portfolio_table.props(
                        (
                            'pagination={"rowsPerPage":0} '
                            'rows-per-page-options="[0]"'
                        )
                    )

    with ui.tab_panel(options_tab):
        with ui.row().classes('w-full items-start gap-4 no-wrap'):
            with ui.column().classes('w-1/3 min-w-[320px]'):
                with ui.card().classes('w-full'):
                    ui.label('Options Chain Controls').classes(
                        'text-xl font-semibold'
                    )
                    chain_symbol_input = ui.input('Symbol').props(
                        'clearable'
                    ).classes('w-40')
                    chain_contract_type = ui.select(
                        options=['ALL', 'CALL', 'PUT'],
                        value='ALL',
                        label='Contract Type',
                    ).classes('w-40')
                    chain_dte_input = ui.input(
                        label='DTE',
                        value='365',
                        on_change=on_chain_dte_change,
                    ).props('type=number').classes('w-40')
                    chain_dte_value_label = ui.label('DTE <= -').classes(
                        'text-xs'
                    )
                    ui.button('Get Chain', on_click=get_chain_click)

                with ui.card().classes('w-full'):
                    ui.label('Chain Step').classes('text-lg font-semibold')
                    with ui.row().classes('items-center gap-4'):
                        chain_step_itm_checkbox = ui.checkbox(
                            'ITM',
                            value=True,
                            on_change=on_chain_step_filter_change,
                        )
                        chain_step_ntm_checkbox = ui.checkbox(
                            'NTM',
                            value=True,
                            on_change=on_chain_step_filter_change,
                        )
                        chain_step_otm_checkbox = ui.checkbox(
                            'OTM',
                            value=True,
                            on_change=on_chain_step_filter_change,
                        )
                    with ui.row().classes('items-center gap-2'):
                        chain_step_up_button = ui.button(
                            'Up',
                            on_click=on_chain_step_up,
                        )
                        chain_step_down_button = ui.button(
                            'Down',
                            on_click=on_chain_step_down,
                        )
                        chain_step_position_label = ui.label('0 / 0').classes(
                            'text-sm'
                        )

                    with ui.row().classes('items-center gap-2'):
                        ui.label('Description:')
                        chain_step_description_value = ui.label('-').classes(
                            'font-semibold'
                        )
                    with ui.row().classes('items-center gap-4'):
                        ui.label('Bid:')
                        chain_step_bid_value = ui.label('-').classes(
                            'font-semibold'
                        )
                        ui.label('Ask:')
                        chain_step_ask_value = ui.label('-').classes(
                            'font-semibold'
                        )
                        ui.label('Last:')
                        chain_step_last_value = ui.label('-').classes(
                            'font-semibold'
                        )
                        ui.label('Mark:')
                        chain_step_mark_value = ui.label('-').classes(
                            'font-semibold'
                        )
                    with ui.row().classes('items-center gap-4'):
                        ui.label('DTE:')
                        chain_step_dte_value = ui.label('-').classes(
                            'font-semibold'
                        )
                        ui.label('Premium %:')
                        chain_step_premium_value = ui.label('-').classes(
                            'font-semibold'
                        )
                        ui.label('Annualized %:')
                        chain_step_annualized_value = ui.label('-').classes(
                            'font-semibold'
                        )

                    chain_step_up_button.disable()
                    chain_step_down_button.disable()

            with ui.column().classes('flex-1 min-w-0'):
                with ui.card().classes('w-full'):
                    ui.label('Options Chain Display').classes(
                        'text-xl font-semibold'
                    )
                    chain_output = ui.textarea(label='Chain JSON')
                    chain_output.props('readonly').classes('w-full')
                    chain_output.style('height: 75vh;')

    with ui.tab_panel(historicals_tab):
        with ui.column().classes('w-full gap-4'):
            with ui.card().classes('w-full'):
                ui.label('Historical Stock Prices').classes('text-xl font-semibold')
                historicals_symbols_input = ui.input(
                    'Ticker symbols',
                    placeholder='AAPL or AAPL,MSFT,GOOG',
                ).classes('w-full')
                with ui.row().classes('items-center gap-3 w-full'):
                    historicals_days_input = ui.input(
                        'Days',
                        value='1825',
                        on_change=on_historicals_mode_change,
                    ).props('type=number min=1').classes('w-32')
                    historicals_mode_select = ui.select(
                        options=['denormalize', 'normalize'],
                        value='denormalize',
                        label='Mode',
                        on_change=on_historicals_mode_change,
                    ).classes('w-48')
                    historicals_plot_button = ui.button(
                        'Plot',
                        on_click=plot_historicals_click,
                    )

            with ui.card().classes('w-full'):
                historicals_plot_host = ui.column().classes('w-full')
                ui.label('Click Plot to render chart').classes('text-sm text-gray')

    with ui.tab_panel(analysis_tab):
        with ui.card().classes('w-full'):
            ui.label('Portfolio Analysis').classes('text-xl font-semibold')
            with ui.row().classes('items-center gap-2'):
                analysis_refresh_button = ui.button(
                    'Refresh Snapshot',
                    on_click=refresh_analysis_snapshot_click,
                )
                analysis_status_value = ui.label('No snapshot loaded').classes(
                    'text-sm'
                )

            analysis_question_input = ui.input(
                'Ask about portfolio data',
                placeholder='e.g., Which positions have more than 100 shares?',
            ).classes('w-full')
            with ui.row().classes('items-center gap-2'):
                analysis_provider_select = ui.select(
                    options=['claude', 'perplexity'],
                    value='claude',
                    label='Provider',
                    on_change=on_analysis_provider_change,
                ).classes('w-40')
                analysis_model_input = ui.input(
                    'Model',
                    value=analysis_default_models['claude'],
                ).classes('w-64')
                analysis_grounded_toggle = ui.checkbox(
                    'Grounded only',
                    value=True,
                )

            with ui.row().classes('items-center gap-2'):
                analysis_ask_button = ui.button(
                    'Ask',
                    on_click=ask_analysis_click,
                )
                analysis_llm_button = ui.button(
                    'Ask LLM',
                    on_click=ask_analysis_llm_click,
                )

            analysis_answer = ui.textarea(label='Answer')
            analysis_answer.props('readonly').classes('w-full')

            analysis_columns = [
                {'name': 'symbol', 'label': 'Symbol', 'field': 'symbol'},
                {'name': 'account', 'label': 'Account', 'field': 'account'},
                {'name': 'type', 'label': 'Type', 'field': 'type'},
                {
                    'name': 'quantity',
                    'label': 'Qty',
                    'field': 'quantity',
                    'align': 'right',
                },
                {
                    'name': 'market_value',
                    'label': 'Mkt Value',
                    'field': 'market_value',
                    'align': 'right',
                },
                {'name': 'sector', 'label': 'Sector', 'field': 'sector'},
                {
                    'name': 'industry',
                    'label': 'Industry',
                    'field': 'industry',
                },
            ]
            with ui.element('div').classes('w-full max-h-[45vh] overflow-auto'):
                analysis_rows_table = ui.table(
                    columns=analysis_columns,
                    rows=[],
                ).classes('w-max min-w-full')
            analysis_rows_table.props(
                'pagination={"rowsPerPage":0} rows-per-page-options="[0]"'
            )


ui.run(port=8000, reload=False, )
