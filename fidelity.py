# * (c) 1066-2050 George... Flammer All Rights Reserved
"""

    fidelity.py module designed to handle fidelity-specific data and formatting

"""

import os
import glob

from collections import namedtuple
import json
import csv

import c
import options
import fundamentals
from typing import Optional, Tuple


# * +++ Helper Functions +++


def _get_newest_csv_file(pattern: str,
                         search_dir: str = "/Users/george/Downloads/",
                         cleanup_old: bool = True) -> Optional[str]:
    """
    Find newest CSV file matching pattern and optionally delete older versions.

    Args:
        pattern: Glob pattern to match files (e.g., "Portfolio_*.csv")
        search_dir: Directory to search in
        cleanup_old: Whether to delete old files (keeps only newest)

    Returns:
        Path to newest file or None if no files found
    """
    # Filter out non-files (directories, symlinks)
    files = list(filter(os.path.isfile, glob.glob(f"{search_dir}{pattern}")))

    if not files:
        return None

    # Sort by modification time, newest first
    files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    # Optionally cleanup old files
    if cleanup_old and len(files) > 1:
        for file in files[1:]:
            c.orange(f'Deleting old file: {file}')
            os.remove(file)

    return files[0]


# Action classification constants
CASH_RECEIPT_ACTIONS = {'DIVIDEND', 'INTEREST', 'RETURN', 'CONTRIBUTION',
                        'LONG-TERM', 'SHORT-TERM'}
BUY_ACTIONS = {'BOUGHT', 'REINVESTMENT', 'MERGER'}
SELL_ACTIONS = {'SOLD'}
SKIP_ACTIONS = {'EXPIRED'}


def _classify_transaction_action(action_str: str) -> Tuple[str, Optional[str]]:
    """
    Classify Fidelity transaction action and determine type.

    Args:
        action_str: Action string from Fidelity transaction

    Returns:
        Tuple of (the_action, the_type) where the_type may be None
    """
    action = action_str.strip()

    # Check for cash receipt actions
    if any(keyword in action for keyword in CASH_RECEIPT_ACTIONS):
        return 'RECEIVED', 'CASH'

    # Check for buy actions
    if any(keyword in action for keyword in BUY_ACTIONS):
        return 'BOUGHT', None

    # Check for sell actions
    if any(keyword in action for keyword in SELL_ACTIONS):
        return 'SOLD', None

    # Check for actions to skip
    if any(keyword in action for keyword in SKIP_ACTIONS):
        return 'SKIP', None

    # Unknown action - return first word
    return action.split()[0] if action else 'UnknownAction', None


# * +++ Fidelity +++


def fidelityOptionDescToSchwabSymbol(desc: str) -> str:
    """
    Translates Fidelity option description to Schwab symbol format.

    Example: "CFR OCT 18 2024 $120 CALL" -> "CFR   24101800120000"

    Args:
        desc: Fidelity option description (e.g., "CFR OCT 18 2024 $120 CALL")

    Returns:
        Schwab-formatted option symbol

    Raises:
        ValueError: If description format is invalid
    """
    import calendar

    abbr_to_num = {name.upper(): num for num,
                   name in enumerate(calendar.month_abbr) if num}
    ol = desc.split(" ")

    # Validate description format (Issue #15)
    if len(ol) < 6:
        raise ValueError(f'Invalid option description format '
                         f'(expected 6+ parts): {desc}')

    # Validate month abbreviation exists
    if ol[1] not in abbr_to_num:
        raise ValueError(f'Invalid month abbreviation "{ol[1]}" in: {desc}')

    month_num = abbr_to_num[ol[1]]
    fidelity_strike = ol[4][1:]  # Remove leading $
    strikePrice = fidelity_strike.replace('.', "")[:3]

    return (
        f'{ol[0]:<6}{ol[3][2:]}{month_num:02}{ol[2]}{ol[5][0]}{strikePrice:>06}00'
        if float(fidelity_strike) % 1
        else f'{ol[0]:<6}{ol[3][2:]}{month_num:02}{ol[2]}{ol[5][0]}{strikePrice:>05}000'
        )


def ppFidelityPos(pos: dict):
    """ pretty pring a Fidelity position (line) """
    c.green(json.dumps(pos, indent=4, ))
    print()


def importFidelity(gabby=False,):
    """
    Import Fidelity portfolio positions from CSV file.
    Uses helper function to find and cleanup old CSV files (Issue #2).
    """
    # * Name our tuple
    posData = namedtuple('posData',
                         'symbol underlying '
                         'accountName accountCash description '
                         'quantity averageCost type dte strikePrice')

    # Use helper function to get newest portfolio file (Issue #2)
    newest_portfolio = _get_newest_csv_file("Portfolio_*.csv", cleanup_old=True)

    if newest_portfolio is None:
        c.red('No Portfolio*.csv file in ~/Downloads')
        return None

    c.green(f'Using: {newest_portfolio}')
    with open(newest_portfolio) as f:
        reader = csv.reader(f)
        headings = next(reader)  # * First line has the headings
        # ? c.bold(headings)
        for row in csv.DictReader(f, fieldnames=(headings)):
            cash_value = 0
            dte = 0
            strikePrice = 0.0

            if not row.get('Symbol') or (row.get('Symbol')
                                         in ['Pending activity', ]):
                # * print(row)
                break

            split_desc = row['Description'].split(" ")
            if row['Symbol']:
                if row.get('Symbol') in ['CORE**', 'FDRXX**', 'SPRXX']:
                    # ? print(json.dumps(row, indent=3, ))
                    cash_value = float(row.get('Current Value', )[1:])
                    # ? print(f'setting cash_value to: {cash_value}')
                    row['Quantity'] = float(row.get('Current Value')[1:])
                    row['Average Cost Basis'] = '$1'

                elif len(row['Symbol']) > 9:        # * Option
                    if split_desc[-1] in ['CALL', 'PUT']:
                        # * before_symbol = pos['Symbol']
                        row['Symbol'] = fidelityOptionDescToSchwabSymbol(row['Description'])
                        c.orange(row['Symbol'])
                        row['Type'] = split_desc[-1]

                        if gabby:
                            c.orange(json.dumps(row, indent=2))

                        option_data = options.deconstructOptionSymbol(row['Symbol'], )
                        dte = option_data.DTE
                        strikePrice = option_data.strikePrice
                    else:
                        row['Type'] = 'Money Market'

                else:
                    row['Type'] = 'EQUITY'

                # ! Special case for Fidelity "Jeeze we don't know" value
                if row.get('Average Cost Basis') == '--':
                    row['Average Cost Basis'] = '$0'

                if row.get('Quantity') == '':
                    row['Quantity'] = '0'

                if row.get('Average Cost Basis', None) is None:
                    row['Average Cost Basis'] = '$0'

                yield posData(row['Symbol'],
                              split_desc[0],
                              'FidelityRoth',
                              cash_value,
                              row['Description'],
                              float(row['Quantity']),
                              float(row['Average Cost Basis'][1:]),
                              row['Type'],
                              dte,
                              strikePrice,
                              )


def fidelityOptionSymbolToSchwabSymbol(symbol: str) -> str:
    """ translates: -VET241220C12.5 """
    """ to 'VET  240920C0012500' (see test routines at file end.) """

    name = ''
    for ch in symbol:
        if ch.isnumeric():
            break
        elif ch.isalpha():
            name += ch

    # * c.orange(symbol)

    # * Now have the name
    i = symbol.index(name)
    date_put_call_price = symbol[i:].removeprefix(name)

    date = date_put_call_price[:6]
    put_call = date_put_call_price[6:7]
    date_put_call = f'{date}{put_call}'
    strikePrice = date_put_call_price.removeprefix(date_put_call)

    # * deal with fractional strike prices (e.g. 7.5)
    fidelity_strike = strikePrice.replace('.', '')

    return (
        f'{name:<6}{date}{put_call}{fidelity_strike:>06}00'
        if float(strikePrice) % 1
        else f'{name:<6}{date}{put_call}{fidelity_strike:>05}000'
        )


def fidelityTransactions(gabby=False, ):
    """
    Import Fidelity transaction history from CSV file.
    Uses helper function to find and cleanup old CSV files (Issue #2).
    """
    # Use helper function to get newest transaction file (Issue #2)
    recent_history = _get_newest_csv_file("History_for_Account_218751762*.csv",
                                          cleanup_old=True)

    if recent_history is None:
        c.red('No "History_for_Account_218751762.csv" file in ~/Downloads')
        return None

    if gabby:
        c.green(f'Using: {recent_history}')

    with open(recent_history) as f:
        reader = csv.reader(f)

        # * First two lines are blank
        trash1 = next(reader)
        trash2 = next(reader)

        headings = next(reader)  # * Third line has the headings:

        """
        Run Date,Action,Symbol,Description,Type,Quantity,Price ($),
            Commission ($),Fees ($), Accrued Interest ($),Amount ($),
            Cash Balance ($),Settlement Date
        """
        # ? c.orange(headings)

        for trans in csv.DictReader(f, fieldnames=(headings)):
            # ? print(json.dumps(trans, indent=2))
            if trans['Symbol'] is None:
                continue

            symbol = trans['Symbol'].strip()

            if 'XX' in symbol:
                c.light_white(symbol)

            if len(symbol) > 8:
                if symbol[0] == '-':
                    # * print(trans['Symbol'])
                    trans['Symbol'] = fidelityOptionSymbolToSchwabSymbol(symbol)
                else:
                    if gabby:
                        c.bold(f'What transaction is this? {symbol}')
                    return None

            # * Name our tupl
            trans_data = fundamentals.transData

            run_date = trans['Run Date'][1:]
            date_list = run_date.split('/')
            iso_date = f'{date_list[2]}-{date_list[0]}-{date_list[1]}'
            the_type = None
            # ? c.yellow(iso_date)

            # Classify transaction action using helper function (Issue #3)
            the_action, action_type = _classify_transaction_action(trans['Action'])

            # Skip transactions marked for skipping
            if the_action == 'SKIP':
                continue

            # Use action_type if provided by classifier
            if action_type:
                the_type = action_type

            symbol = trans['Symbol'].strip()
            the_description = trans['Description']
            if the_type is None:
                if len(symbol) < 8:
                    the_type = 'EQUITY'
                elif 'CALL' in the_description:
                    the_type = 'CALL'
                elif 'PUT' in the_description:
                    the_type = 'PUT'
                else:
                    tt = trans.get('Action').split()[0]
                    the_type = trans.get('Type', f'UnknownType::Action:{tt}')
                    # ? print(the_type)
                    # ? c.yellow(json.dumps(trans, indent=3))

            # * c.orange(json.dumps(trans, indent=2))

            yield trans_data(
                'FidelityRoth',
                iso_date,
                the_action,
                symbol,
                the_description,
                the_type,
                trans['Quantity'],
                trans.get('Price ($)', 0),
                float(trans['Amount ($)']),
                )


def fidelityTransactionAppraisals(api, ):
    """
    Analyze Fidelity transactions and calculate performance metrics.
    Uses batched API calls for ~20x performance improvement (Issue #11).
    """
    import options
    import fundamentals

    # First pass: collect all transactions and identify symbols needing quotes
    all_transactions = []
    buy_transactions = []  # Track BOUGHT transactions needing quotes
    total_dividends = total_interest = total_return = 0

    for t in fidelityTransactions():
        if t is None:
            continue

        all_transactions.append(t)

        # Accumulate cash flows
        if 'DIVIDEND' in t.action:
            total_dividends += float(t.totalAmount)
        elif 'INTEREST' in t.action:
            total_interest += float(t.totalAmount)
        elif t.action in ['RETURN', 'SHORT-TERM', 'LONG-TERM', 'MERGER']:
            total_return += float(t.totalAmount)

        # Collect BOUGHT transactions that need quotes
        if ('BOUGHT' in t.action) and (t.symbol != 'CASH'):
            buy_transactions.append(t)

    # Batch fetch all quotes in parallel
    #   (Issue #11: ~20x faster than sequential)
    quote_map = {}
    if buy_transactions:
        # Create list of unique symbols to avoid duplicate API calls
        unique_symbols = {t.symbol for t in buy_transactions}

        # Fetch all quotes in parallel using existing threading infrastructure
        quote_results = fundamentals.threaded_schwab_details(
            api,
            unique_symbols,
            lambda a, symbol: fundamentals.do_quote(a, symbol)
        )

        # Build symbol -> quote mapping for O(1) lookup
        quote_map = {q.symbol: q for q in quote_results if q is not None}

    # Second pass: display results using pre-fetched quotes
    for t in buy_transactions:
        if qData := quote_map.get(t.symbol):
            elapsed = abs(options.dteFromYYYYmmdd(t.runDate))
            value = float(t.quantity) * qData.lastPrice
            percent = abs((1 - abs(value / float(t.totalAmount))) * 100)
            arr = (365 / elapsed) * percent if elapsed > 0 else 0

            color = c.orange if abs(value) - abs(t.totalAmount) < 0 else c.green
            color(f"{t.symbol} invested ${abs(float(t.totalAmount))} "
                  f"{elapsed} days ago.  "
                  f"Now it's {value:.2f} or {percent:.2f}% => Arr: {arr:.2f}")

    c.yellow(f'D: {total_dividends:.2f} I: {total_interest:.2f} '
             f'R: {total_return:.2f}')


if __name__ == '__main__':

    c.green('Wait for it...')

    c.green(fidelityOptionDescToSchwabSymbol("CFR OCT 18 2024 $120 CALL"))
    c.green(fidelityOptionDescToSchwabSymbol("VET DEC 20 2024 $12.50 CALL"))

    c.orange(fidelityOptionSymbolToSchwabSymbol('-VET241220C12.5'))

    c.bold('DONE')
