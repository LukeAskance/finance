#! /usr/local/bin/python3

# * (c) 1066-2050 George... Flammer All Rights Reserved
""" fundamentals.py module extracts and pretty prints equity fundamentals """

import os
import sys
import ast
import threading

import json
from datetime import date
from collections import namedtuple
from typing import Generator, NamedTuple, Tuple, Optional, Dict

from datetime import datetime, timedelta

import yfinance as yf

import c
import options


class equityData(NamedTuple):
    symbol: str
    underlying: str
    accountName: str
    accountCash: float
    description: str
    quantity: float
    averageCost: float
    type: str
    high52: float
    low52: float
    lastPrice: float
    divPayAmount: float
    divPayDate: str
    divFreq: float
    divExDate: str
    divYield: float
    eps: float
    peRatio: float
    lastEarningsDate: str
    nextDivExDate: str
    nextDivPayDate: str
    pl: float
    PL: float
    income: float
    marketValue: float
    percentPL: float
    longName: str


class transData(NamedTuple):
    accountName: str
    runDate: str
    action: str
    symbol: str
    description: str
    type: str
    quantity: float
    price: float
    totalAmount: float


class qData(NamedTuple):
    symbol: str
    close: float
    lastPrice: float
    high52: float
    low52: float
    bidPrice: float
    askPrice: float
    openInterest: float
    netChange: float
    netPercentageChange: float


# * vscode extension "Better Comments"... comments
# * Important
# ! Danger, danger, Will Robinson!!
# ? Any Idea why I did thin??
# TODO: Same ole... except orange

# Constants
ACCOUNT_MAPPING = {
    "11351369": "GeorgeTrust",
    "21178329": "DebRoth",
    "30969090": "Investments",
    "45521728": "DebTrust",
    "63568172": "GrandKids",
    "89958151": "GeorgeRoth",
}

# Cached reverse mapping for efficiency
_ACCOUNT_NAME_TO_NUMBER = {v: k for k, v in ACCOUNT_MAPPING.items()}


def gabbyDumps(desc: str, target: str, d: dict) -> None:
    """Pretty-print JSON data for debugging (when gabby=True)"""
    c.bold(f"{desc} {target}:")
    c.green(json.dumps(d, indent=4))


# * =====Schwab Client account stuff ============


def get_account_numbers(
    api,
) -> Generator[str, None, None,]:
    """generate account numbers"""

    for account in api.get_linked_accounts():
        yield account.get(
            "accountNumber",
        )


# * Put a name on the account number
def accountName(accountNumberStr: str) -> str | None:
    """Get account name from account number"""
    return ACCOUNT_MAPPING.get(accountNumberStr)


def acctNumFromName(accountName: str) -> str:
    """Get account number from account name, defaults to GeorgeRoth if not found"""
    return _ACCOUNT_NAME_TO_NUMBER.get(accountName, "89958151")


def shorty_to_name(
    linked_accounts: list,
    shorty: str,
):
    return next(
        (
            accountName(account["accountNumber"])
            for account in linked_accounts
            if shorty == account["accountNumber"][-3:]
        ),
        None,
    )


def accountHash(linked_accounts: str, acct_number: str):
    """
    passed [linked_accounts] and the account number...
    returns account 'hashValue'
    """

    return next(
        (
            account["hashValue"]
            for account in linked_accounts
            if acct_number == account["accountNumber"]
        ),
        None,
    )


# * =====Schwab Client account stuff ============


# * ============= Threading stuff =============


def threaded_schwab_details(api, query_list, schwab_query_function) -> list:
    """
    Generic threaded executor for Schwab API calls.

    Runs schwab_query_function in parallel for each item in query_list,
    significantly speeding up bulk operations
        (e.g., fetching data for 20+ positions).

    Args:
        api: SchwabAPI instance
        query_list: List of query parameters (e.g., position tuples, symbols)
        schwab_query_function: Function to call for each query
            (must accept api, query)

    Returns:
        List of results from successful function calls
            (None results are filtered out)

    Thread Safety:
        Uses lock to safely append results from multiple threads
    """
    results_list = []
    lock = threading.Lock()
    threads = []

    def thread_wrapper(api, query):
        """Execute query function and store result if not None"""
        if result := schwab_query_function(api, query):
            with lock:
                results_list.append(result)

    # Create and start a thread for each query
    for query in query_list:
        thread = threading.Thread(target=thread_wrapper, args=(api, query))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete before returning
    for thread in threads:
        thread.join()

    return results_list


# * ============= Threading stuff =============


def get_all_account_details_parallel(
    api, linked_accounts: list, fields: str = "positions"
) -> list:
    """
    Fetch account details for all accounts in parallel for performance.

    Replaces sequential API calls with parallel execution,
        significantly improving performance when fetching data
        for multiple accounts (e.g., 6 accounts @ 200ms each
    = 1200ms sequential vs ~300ms parallel).

    Args:
        api: SchwabAPI instance
        linked_accounts: List of account dictionaries from
            api.get_linked_accounts()
        fields: Fields to retrieve (default: "positions")

    Returns:
        List of account details dictionaries with account info enriched
    """

    def fetch_account_details(account: dict) -> Optional[dict]:
        """Fetch details for a single account and enrich with account metadata"""
        try:
            details = api.get_account_details(
                account["hashValue"], fields=fields
            )
            # Enrich with account metadata for easier processing
            details["_accountNumber"] = account["accountNumber"]
            details["_accountName"] = accountName(account["accountNumber"])
            details["_hashValue"] = account["hashValue"]
            return details
        except Exception as e:
            c.red(
                f"Error fetching details for account {account.get('accountNumber', 'unknown')}: {e}"
            )
            return None

    # Use existing threading infrastructure
    return threaded_schwab_details(
        api, linked_accounts, lambda a, acc: fetch_account_details(acc)
    )


def batch_enrich_positions(api, positions: list, gabby: bool = False) -> list:
    """
    Enrich multiple positions with real-time data in parallel.

    Instead of calling addRealtimeDataToPosTuple()
        sequentially for each position,
    this function batches the API calls for efficiency.

    Args:
        api: SchwabAPI instance
        positions: List of position namedtuples to enrich
        gabby: Debug output flag

    Returns:
        List of enriched position namedtuples
    """
    # Use existing threaded infrastructure to process positions in parallel
    return threaded_schwab_details(
        api,
        positions,
        lambda a, pos: addRealtimeDataToPosTuple(a, pos, gabby=gabby),
    )


# * ============= Threading stuff =============


# * +++ Profit / loss / ROI calculations +++


def annualized_pl_percent(
    premium: float,
    capital: float,
    to_date=None,
    gabby=False,
) -> float:
    """calculates annualized profit given:
    premium
    allocated "capital" (per share) for puts or "exercise price" for calls
    start_date (if "None" then uses "today")
    to_date: end date (generally date of expiry)
        OR "today" if we are thinking about BTC
    """
    from_date = date.today()

    to_date = date.fromisoformat(to_date)

    days_between = (to_date - from_date).days

    profit = premium / capital

    if gabby:
        c.bold(f"annualized_pl_percent: {profit * (360 / days_between)}")

    return profit * (360 / days_between) if days_between > 0 else 0


# * --- Profit / loss / ROI calculations ---


# * +++ getPositions +++


def getPosition(
    api,
    name: str,
    gabby=False,
) -> dict:
    """returns fundamentals for name in dictionary form"""

    result = api.get_quote(name, gabby=gabby)

    if gabby and result and name in result:
        c.red("getPosition")
        c.bold(json.dumps(result[name], indent=4))

    return result


def parsePosition(
    position,
) -> dict:

    return {
        "accountName": position.get("accountName"),
        "accountValue": position.get("accountValue"),
        "cash": position.get("cashBalance"),
        "symbol": position["instrument"].get("symbol"),
        "qty": position.get("longQuantity"),
        "value": position.get("marketValue"),
        "basis": position.get("averagePrice"),
    }


def getPositionsFromHash(
    api,
    account_hash,
    gabby=False,
) -> dict:
    """Returns positions for account"""

    detailsDict = api.get_account_details(account_hash)
    positions = detailsDict["securitiesAccount"]["positions"]

    if gabby:
        c.red(detailsDict)
        c.bold(positions)

    return positions


"""
getPosition position:
{
    "shortQuantity": 0.0,
    "averagePrice": 56.6843,
    "currentDayProfitLoss": -47.999999999999,
    "currentDayProfitLossPercentage": -0.8,
    "longQuantity": 100.0,
    "settledLongQuantity": 100.0,
    "settledShortQuantity": 0.0,
    "instrument": {
        "assetType": "EQUITY",
        "cusip": "92936U109",
        "symbol": "WPC",
        "netChange": -0.48
    },
    "marketValue": 5923.0,
    "maintenanceRequirement": 5923.0,
    "averageLongPrice": 56.684285,
    "taxLotAverageLongPrice": 56.6843,
    "longOpenProfitLoss": 254.57,
    "previousSessionLongQuantity": 100.0,
    "currentDayCost": 0.0,
    "acctLiquidationValue": 151462.12,
    "accountCash": 2086.68,
    "qty": 100.0,
    "accountNumber": "89958151",
    "accountName": "GeorgeRoth",
    "description": null,
    "symbol": "WPC"
}

"""


def getPositions(
    api,
    gabby=False,
) -> Generator[dict, None, None,]:
    """
    Generator for position dict of all Schwab positions.
    Uses parallel API calls for improved performance.
    """
    linked_accounts = api.get_linked_accounts()

    # Check for timeout errors
    for account in linked_accounts:
        if isinstance(account, (str,)):
            c.red(f"Schwab timeout??  account: {account}")
            return None

    # Fetch all account details in parallel (performance optimization)
    all_details = get_all_account_details_parallel(
        api, linked_accounts, fields="positions"
    )

    # Process each account's positions
    for detailsDict in all_details:
        if detailsDict is None:
            continue

        if gabby:
            c.bold("getPositions: detailsDict")
            c.red(json.dumps(detailsDict, indent=4))

        # Use enriched metadata from parallel fetch
        account_name = detailsDict.get("_accountName")
        account_number = detailsDict["securitiesAccount"]["accountNumber"]
        positions = detailsDict["securitiesAccount"]["positions"]

        for position in positions:
            position["acctLiquidationValue"] = detailsDict["securitiesAccount"][
                "initialBalances"
            ]["liquidationValue"]
            position["accountCash"] = detailsDict["securitiesAccount"][
                "currentBalances"
            ]["cashBalance"]
            position["qty"] = position.get("longQuantity") or position.get(
                "shortQuantity"
            )
            position["accountNumber"] = account_number
            position["accountName"] = account_name
            position["description"] = position.get("instrument").get(
                "description"
            )
            position["symbol"] = position.get("instrument").get("symbol")

            if gabby:
                gabbyDumps(
                    "getPosition",
                    "position",
                    position,
                )

            yield position


def getHistoricalPrices(
    api,
    name: str,
    periodType="month",
    numPeriods=1,
    frequencyType="daily",
    frequency=1,
    gabby=False,
) -> dict:
    """get historical prices for name"""

    result = api.get_price_history(
        name,
        periodType=periodType,
        period=numPeriods,
        frequencyType=frequencyType,
        frequency=frequency,
        gabby=gabby,
    )

    if gabby and result:
        c.red("getHistoricalPrices")
        c.bold(json.dumps(result, indent=4))

    return result


# * +++  TUPLE Stuff +++


def XppHistoricals(
    api,
    name: str,
    gabby=False,
):
    """pretty print historical prices for name"""
    h_dict = getHistoricalPrices(api, name)
    for x in h_dict["candles"]:
        # * c.green(x)
        dt = datetime.fromtimestamp((x["datetime"] / 1000))
        # * c.green(dt.strftime("%Y-%m-%d"))  # Output: 2025-02-26
        # * c.green(dt.strftime("%d/%m/%Y %H:%M:%S"))  # Output: 26/02/2025 14:30:45
        date_str = dt.strftime("%B %d, %Y")  # Output: February 26, 2025
        c.green(f'{date_str}  ${x["close"]}')

    # * c.orange(r.json())


def get_price_moves(
    api,
    name_tuple: tuple,
):
    """get price moves for name"""

    days_ago = name_tuple[5]

    h_dict = getHistoricalPrices(
        api,
        name_tuple[0],
        periodType="month",
        numPeriods=1,
        frequencyType="daily",
        frequency=1,
    )
    for h in h_dict["candles"]:
        h["datetime"] = datetime.fromtimestamp((h["datetime"] / 1000))

        # * Calculate number of days 'ago'
        h["days_ago"] = (datetime.now() - h["datetime"]).days
        # * print(h['days_ago'], h['datetime'].strftime("%B %d, %Y"), h['close'])

    move = next(
        (
            day["close"]
            for day in h_dict["candles"]
            if days_ago - 2 <= day["days_ago"] <= days_ago + 2
        ),
        0,
    )

    current_price = do_quote(api, name_tuple[0]).lastPrice

    # * c.lightBlue(f'{name_tuple[0]}: M: ${move} C: ${current_price}')

    percent = 100 * ((current_price - move) / move) if move else move

    return (
        name_tuple[0],
        name_tuple[1],
        name_tuple[2],
        name_tuple[3],
        name_tuple[4],
        name_tuple[5],
        percent,
    )


def pp_moves(
    name: str,
    per_month: float,
    per_week: float,
    per_day: float,
):
    """pretty print moves"""
    c.lightWhite(
        f"{name}: M {per_month:.2f}%  W {per_week:.2f}%  D {per_day:.2f}%"
    )


def load_python_list(filename):
    """
    Reads a file containing a list and returns the list.

    Args:
        filename (str): Name of file to read from
    Returns:
        list: List read from the file

    Raises:
        FileNotFoundError: If the file doesn't exist
        SyntaxError: If the file content cannot be parsed
        ValueError: If the content is not a list
    """
    if os.path.exists(filename) is False:
        return None

    try:
        with open(filename, "r") as file:
            content = file.read().strip()

        # Parse the string to Python object
        parsed_list = ast.literal_eval(content)

        # Verify result is a list
        if not isinstance(parsed_list, list):
            raise ValueError("File content must represent a list")

        return parsed_list

    except FileNotFoundError as e:
        raise FileNotFoundError(f"File '{filename}' not found") from e
    except SyntaxError as e:
        raise SyntaxError("File content is not a valid Python literal") from e


def get_movers(
    api,
    days_ago=30,
):
    """get movers from portfolio"""

    name_list = load_python_list("name_tuples.txt")
    new_name_list = []
    new_name_list.extend(
        (name[0], name[1], name[2], name[3], name[4], days_ago)
        for name in name_list
    )

    move_percentage_list = threaded_schwab_details(
        api, new_name_list, get_price_moves
    )

    c.green(
        f"{days_ago} days percentage move for "
        f"{len(move_percentage_list)} positions"
    )

    print(move_percentage_list[0])
    m_dict = {
        name_tuple[0]: (name_tuple[6], name_tuple[3])
        for name_tuple in move_percentage_list
    }

    sorted_dict = dict(sorted(m_dict.items(), key=lambda item: item[1][0]))

    for mover, pc in sorted_dict.items():
        c.bold(f"{mover:>7}: {pc[0]:6.2f}% - {pc[1]:>9.2f}")


def get_historicals(
    api,
    name="EPD",
    days=365 * 5,
    gabby=False,
):
    """get historicals for name"""

    h_dict = api.get_price_history(
        name, periodType="year", period=10, frequencyType="weekly", frequency=1
    )

    hist_list = []
    # * c.orange(h_dict)
    for h in h_dict["candles"]:
        that_date = datetime.fromtimestamp((h["datetime"] / 1000))

        # Calculate the difference
        difference = that_date - datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        diff = abs(difference.days)
        if diff < days:
            hist_list.append((that_date.strftime("%Y-%m-%d"), h["close"]))

    # * return [(h['date_str'], h['close']) for h in h_dict['candles']]
    return hist_list


def _handle_cash_position(pos: equityData) -> equityData:
    """
    Handle cash positions (CORE, FDRXX) from Fidelity accounts.
    These are money market funds treated as cash equivalents.
    """
    # Cash positions get simplified data with 3.5% annual yield estimate
    return equityData(
        pos.symbol,
        pos.underlying,
        pos.accountName,
        pos.accountCash,
        pos.description,
        pos.quantity,
        pos.averageCost,
        pos.type,
        1,
        1,
        1,
        0.003,
        "",
        12,
        "",
        3.5,
        0,
        0,
        "",
        "",
        "",
        0,
        0,
        (0.035 * pos.quantity),  # Annual income at 3.5%
        pos.quantity,  # Market value = quantity for cash
        0,  # No percentage change
        "",  # No long name needed
    )


def _handle_option_position(
    api, pos: equityData, gabby: bool = False
) -> namedtuple:
    """
    Handle option positions (symbols longer than 7 characters).
    Delegates to options.get_option_quote() for specialized processing.
    """
    return options.get_option_quote(api, pos, pos.symbol, gabby=gabby)


def _handle_equity_position(
    api, pos: equityData, gabby: bool = False
) -> namedtuple:
    """
    Handle standard equity (stock) positions.
    Fetches real-time quote and fundamental data, calculates P&L and income.
    """
    # Single API call for both quote and fundamental data
    #   (efficiency improvement)
    qDict, funDict = getQuoteAndFundamentals(api, pos.symbol, gabby=gabby)

    # Handle missing data gracefully
    if qDict is None:
        qDict = {}
    if not funDict:
        c.orange(f"Could not get fundamental data for {pos.symbol}")
        funDict = {}

    # Calculate profit/loss metrics
    pl = qDict.get("lastPrice", 0) - pos.averageCost  # Per-share P/L
    PL = pl * pos.quantity  # Total P/L
    percentPL = (100 * (pl / pos.averageCost)) if pos.averageCost else 100

    # Calculate annual dividend income
    income = (
        funDict.get("divPayAmount", 0)
        * funDict.get("divFreq", 0)
        * pos.quantity
    )
    marketValue = qDict.get("lastPrice", 0) * pos.quantity

    return equityData(
        pos.symbol,
        pos.underlying,
        pos.accountName,
        pos.accountCash,
        pos.description,
        pos.quantity,
        pos.averageCost,
        pos.type,
        # Price data
        qDict.get("52WeekHigh", 0),
        qDict.get("52WeekLow", 0),
        qDict.get("lastPrice", 0),
        # Dividend data
        funDict.get("divPayAmount", 0),
        funDict.get("divPayDate", "UnknownDay")[:10],
        funDict.get("divFreq", 0),
        funDict.get("divExDate", "UnknownDay")[:10],
        funDict.get("divYield", 0),
        # Fundamental data
        funDict.get("eps", 0),
        funDict.get("peRatio", 0),
        funDict.get("lastEarningsDate", "UnknownDay")[:10],
        funDict.get("nextDivExDate", "UnknownDay")[:10],
        funDict.get("nextDivPayDate", "UnknownDay")[:10],
        # Calculated fields
        pl,
        PL,
        income,
        marketValue,
        percentPL,
        pos.symbol,  # Use symbol as long name
    )


def addRealtimeDataToPosTuple(
    api, pos: namedtuple, gabby: bool = False
) -> namedtuple:
    """
    Enrich position data with real-time market data and fundamentals.
    Routes to specialized handlers based on position type.
    """
    if pos is None:
        return None

    # Route to appropriate handler based on position type
    if pos.symbol in ["CORE", "FDRXX**"]:
        return _handle_cash_position(pos)
    elif len(pos.symbol) > 7:
        return _handle_option_position(api, pos, gabby)
    else:
        return _handle_equity_position(api, pos, gabby)


def get_company_long_name(ticker: str) -> str:
    """
    Retrieve the long name of a company using its stock ticker.

    :param ticker: The stock ticker symbol (e.g., "VICI").
    :return: The long name of the company (e.g., "VICI Properties Inc.")
        or an error message.
    """
    try:
        # Fetch ticker info using yfinance
        ticker_info = yf.Ticker(ticker).info

        # Check if 'longName' exists in the returned data
        if "longName" in ticker_info:
            return ticker_info["longName"]
        else:
            return "Long name not found for the given ticker."
    except Exception as e:
        return f"An error occurred: {e}"


def getPosTuples(
    api,
    gabby=False,
) -> Generator[namedtuple, None, None,]:
    """
    Generator of namedTuples for all Schwab positions.
    Uses parallel API calls for improved performance.
    """
    # * Name our tuple
    pos_data = namedtuple(
        "posData",
        "symbol underlying accountName accountCash "
        "description quantity averageCost type dte "
        "current_pl, current_pl_percent longName",
    )

    linked_accounts = api.get_linked_accounts()

    # Fetch all account details in parallel (performance optimization)
    all_details = get_all_account_details_parallel(
        api, linked_accounts, fields="positions"
    )

    for detailsDict in all_details:
        if detailsDict is None:
            continue

        # Use enriched metadata from parallel fetch
        account_name = detailsDict.get("_accountName")

        if "securitiesAccount" not in detailsDict:
            c.bold(f"Chuck Schwab will not deliver details for {account_name}:")
            c.red(detailsDict.get("message", "Unknown error"))
            c.green("Might be just plain maintenance.. has to be done sometime")
            sys.exit()

        account_cash = detailsDict["securitiesAccount"]["currentBalances"][
            "cashBalance"
        ]

        if gabby:
            c.bold("getPositions: detailsDict")
            c.red(json.dumps(detailsDict, indent=4))
            sys.exit()

        dte = 0
        for position in detailsDict["securitiesAccount"].get("positions", []):
            if position is None:
                continue

            quantity = position.get("longQuantity") or position.get(
                "shortQuantity"
            )
            symbol = position.get("instrument").get("symbol")

            if len(symbol) > 9:
                split_symbol = symbol.split(" ")
                underlying = split_symbol[0]
                c.orange(symbol)
                dte = options.dteFromYYYYmmdd(split_symbol[-1][:6])
            elif len(symbol) == 9:
                c.yellow(symbol)
                continue
            else:
                underlying = symbol

            yield pos_data(
                symbol,
                underlying,
                account_name,
                account_cash,
                position.get("instrument").get("description", "EQUITY"),
                quantity,
                position.get("averageLongPrice"),
                position["instrument"].get("assetType"),
                dte,
                position.get("currentDayProfitLoss"),
                position.get("currentDayProfitLossPercentage"),
                "long_name",
            )


def pos_string(
    pos: equityData,
):
    return (
        f"{pos.accountName:>14}({pos.symbol:^6}): "
        f"Value: ${pos.lastPrice * pos.quantity:<12,.2f}  "
        f"Income: ${pos.divPayAmount * pos.divFreq * pos.quantity:<10,.2f}  "
        f"Shares: {pos.quantity:<10,.3f}  "
        f"Basis: ${pos.averageCost:<8,.2f} "
    )


# * +++ Total commands +++++


def total_qty(
    name: str,
    all_pos: list,
) -> float:
    """Returns the total share quantiity of 'name'"""
    return sum(
        position.quantity
        for position in all_pos
        if position.symbol == name.upper()
    )


def get_raw_schwab_transactions(
    api,
    account_hash,
    days=30,
    gabby=False,
) -> dict:
    """Returns orders for account"""

    result = api.get_transactions(
        account_hash, (datetime.now() - timedelta(days=days)), datetime.now()
    )
    if gabby:
        c.red(result)

    return result


def _process_option_trade(item: dict) -> tuple:
    """
    Process options trade (VANILLA type).
    Options quantities are in contracts (100 shares each).
    Returns: (type, description, quantity, action)
    """
    if isinstance(item, dict):
        the_type = item.get("instrument").get("putCall")  # 'CALL' or 'PUT'
        the_description = item.get("instrument").get("description", "Unknown?")
        the_quantity = item.get("amount") * 100  # Convert contracts to shares
        # Negative quantity for non-EQUITY types indicates a sale
        the_action = (
            "SOLD" if the_type != "EQUITY" and the_quantity < 0 else "BOUGHT"
        )

        return the_type, the_description, the_quantity, the_action

    return "UNKNOWN", "Unknown?", 0, "UNKNOWN"


def _process_equity_trade(item: dict) -> tuple:
    """
    Process standard equity (stock) trade.
    Returns: (type, description, quantity, action)
    """
    the_type = "EQUITY"
    the_description = item.get("instrument").get("type", "EQUITY")
    the_quantity = item.get("amount")
    # OPENING = buying, CLOSING = selling
    the_action = "BOUGHT" if item.get("positionEffect") == "OPENING" else "SOLD"

    return the_type, the_description, the_quantity, the_action


def schwabTransactions(
    api, accountName: str, days: int = 270, gabby: bool = False
):
    """
    Generator yielding transaction data for a specific account.
    Filters out system transfers and currency-only transactions.
    """
    # Validate account name against known accounts
    account_names = list(ACCOUNT_MAPPING.values())
    if accountName not in account_names:
        c.yellow(
            f"Was proudly walking down the {accountName} street... Dad was right: devious"
        )
        return None

    # Get account hash for API calls
    linked_accounts = api.get_linked_accounts()
    account_number = acctNumFromName(accountName)
    if (account_hash := accountHash(linked_accounts, account_number)) is None:
        c.red(
            f"No account hash found for {accountName} Account number: ({account_number})"
        )
        return None

    # Fetch raw transaction data
    trades = get_raw_schwab_transactions(
        api, account_hash, days=days, gabby=gabby
    )

    # Process each trade and yield transaction data
    for trade in trades:
        # Skip system transfers (internal movements, not trades)
        if trade.get("description") == "System transfer":
            continue

        for item in trade.get("transferItems"):
            # Only process non-currency items (stocks and options)
            if item.get("instrument").get("assetType") != "CURRENCY":

                # Route to appropriate handler based on instrument type
                if item.get("instrument").get("type") == "VANILLA":
                    (
                        the_type,
                        the_description,
                        the_quantity,
                        the_action,
                    ) = _process_option_trade(item)
                else:
                    (
                        the_type,
                        the_description,
                        the_quantity,
                        the_action,
                    ) = _process_equity_trade(item)

                trade_date = trade.get("tradeDate")[:10]
                yield transData(
                    accountName,
                    trade_date,
                    the_action,
                    item.get("instrument").get("symbol", "UnknownSymbol?"),
                    the_description,
                    the_type,
                    the_quantity,
                    item.get("price", 0),
                    item.get("cost"),
                )


def getQuoteAndFundamentals(
    api, name: str, gabby: bool = False
) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Get both quote data and fundamentals in a single API call for efficiency.
    Returns: (qDict, funDict) tuple - both can be None if quote fails
    """
    return api.get_quote_and_fundamentals(name, gabby=gabby)


def getQuoteData(api, name: str, gabby=False):
    """Gets all the current quote data - for equity ONLY"""
    qDict, _ = getQuoteAndFundamentals(api, name, gabby)
    return qDict


def do_quote(
    api,
    name: str,
    gabby=False,
) -> Optional[qData]:
    """Returns real-time quote information for SINGLE name in [av]"""

    # qData = namedtuple('qData', 'symbol close lastPrice high52 low52 bidPrice \
    #                    askPrice openInterest netChange netPercentageChange')

    if qDict := getQuoteData(
        api,
        name,
        gabby=gabby,
    ):
        # * gabbyDumps('fundamentals', 'do_quote', qDict, )

        return qData(
            name,
            qDict.get("closePrice"),
            qDict.get("lastPrice"),
            qDict.get("52WeekHigh"),
            qDict.get("52WeekLow"),
            qDict.get("bidPrice"),
            qDict.get("askPrice"),
            qDict.get("openInterest"),
            qDict.get("netChange"),
            qDict.get("netPercentChange"),
        )

    c.bold(f"No quote available for {name}")
    return None


def quote_string(
    qData: qData,
) -> str:
    return (
        f"{qData.symbol}: Last: ${qData.lastPrice}  Bid: ${qData.bidPrice}  "
        f"Ask: ${qData.askPrice}  Close: ${qData.close}  "
        f"52WeekHigh: ${qData.high52:.2f} "
        f"52WeekLow: ${qData.low52:.2f} +/-: ${qData.netChange:.2f} "
        f"or {qData.netPercentageChange:.2f}%"
    )


def getLiquidatedValue(
    api,
    account_hash,
    gabby=False,
):
    """getLiquidated Value of account"""

    if detailsDict := api.get_account_details(account_hash):
        return detailsDict["aggregatedBalance"]["liquidationValue"]


if __name__ == "__main__":
    """Standalone execution UX."""

    get_historicals(
        api=None,
        name="EPD",
        gabby=False,
    )

    sys.exit()
