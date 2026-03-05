# * (c) 1066-2050 George... Flammer All Rights Reserved
"""

    options.py module is designed to ease finding optimal option options
    Hard Work.

"""

import datetime
from pathlib import Path
from collections import namedtuple
from dataclasses import dataclass
from typing import List, Dict, Optional, Any

import json

# ? import getClientAccounts
import c
import fundamentals


# * vscode extension "Better Comments"... comments
# * Important
# ! Danger, danger, Will Robinson!!
# ? Any Idea why I did thin??
# TODO: Same ole... except orange

# Add Some Color ==========================

# Constants
OPTION_HISTORY_FILE = './optionHistory.json'


@dataclass
class Underlying:
    """Underlying stock/asset data"""
    symbol: str
    description: str
    bid: float
    ask: float
    last: float
    high52: float
    low52: float
    price: float
    change: float
    percentChange: float


@dataclass
class optionData:
    """Option data structure with pricing, Greeks, and metadata"""
    symbol: str
    underlying: Underlying
    accountName: str
    strikePrice: float
    quantity: int
    expirationDate: str
    putCall: str
    type: str
    description: str
    bid: float
    ask: float
    lastPrice: float
    volatility: float
    markChange: float
    markPercentageChange: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    openInterest: int
    timeValue: float
    theoreticalOptionValue: float
    interestRate: float
    daysToExpiration: int
    intrinsicValue: float
    extrinsicValue: float
    high52: float
    low52: float
    inTheMoney: bool


def helpGreeks():
    c.green("\nImplied volatility is a dynamic figure that changes based on activity in the options market place.")
    c.green("\tUsually, when implied volatility increases, the price of options will increase as well,")
    c.green("\tassuming all other things remain constant.")
    c.green("\tSo when implied volatility increases after a trade has been placed,")
    c.green("\tit’s good for the option owner and bad for the option seller.")
    c.green("\tConversely, if implied volatility decreases after your trade is placed,")
    c.green("\tthe price of options usually decreases.")
    c.green("\tThat’s good if you’re an option seller and bad if you’re an option owner.")
    c.green("\nDelta is the amount an option price is expected to move based on a $1 change in the underlying stock.")
    c.green("\tSo as expiration approaches, changes in the stock value will cause more dramatic changes in delta,"
            "\n\tdue to increased or decreased probability of finishing in-the-money.")
    c.green("\nGamma is the rate that delta will change based on a $1 change in the stock price."
            "\n\tSo if delta is the “speed” at which option prices change, you can think of gamma as the 'acceleration."
            "'\n\tOptions with the highest gamma are the most responsive to changes "
            "in the price of the underlying stock.")
    c.green("\nTheta measures time decay, the enemy number one for the option buyer."
            "\n\tOn the other hand, it’s usually the option seller’s best friend."
            "\n\tTheta is the amount the price of calls and puts will decrease (at least in theory)"
            "\n\tfor a one-day change in the time to expiration.")
    c.green("\nYou can think of vega as the Greek who’s a little shaky and over-caffeinated."
            "\n\tVega is the amount call and put prices will change, in theory,"
            "\n\tfor a corresponding one-point change in implied volatility."
            "\n\tVega does not have any effect on the intrinsic value of options;"
            "\n\tit only affects the 'time value' of an option’s price.")
    c.green("\n\tTypically, as implied volatility increases, the value of options will increase."
            "\n\tThat’s because an increase in implied volatility suggests an increased range"
            "\n\ttof potential movement for the stock.")
    c.green("\nIf you’re a more advanced option trader, you might have noticed we’re missing a Greek — rho."
            "\n\tThat’s the amount an option value will change in theory"
            "\n\tbased on a one percentage-point change in interest rates.")


def pr_option_help():
    """ Prints command line help for put and call commands """
    c.bold("Examples of option command options are:")
    c.bold("call ?  - this text")
    c.bold("call <name> -dte## -count## (default dte=90 count=7)")
    c.bold("ex: call epd -dte60 -count12")
    c.bold("ex put MAIN -dte180")


# * +++ Option related code +++

def _read_dictfile(file: str = OPTION_HISTORY_FILE, gabby: bool = False) -> Optional[Dict]:
    """ Read JSON file and return dictionary, or None if file doesn't exist """

    if Path(file).is_file():
        try:
            with open(file, 'r') as fp:
                return json.load(fp)
        except json.JSONDecodeError as e:
            c.red(f'Error reading JSON from {file}: {e}')
            raise
        except IOError as e:
            c.red(f'Error opening file {file}: {e}')
            raise
    return None


def _write_dictfile(d: Dict, file: str = OPTION_HISTORY_FILE, gabby: bool = False) -> None:
    """ Write dictionary to file in JSON format """

    try:
        with open(file, "w") as fp:
            json.dump(d, fp, indent=4, )  # encode dict into JSON
    except IOError as e:
        c.red(f'Error writing to file {file}: {e}')
        raise
    except TypeError as e:
        c.red(f'Error serializing data to JSON: {e}')
        raise


# * +++ EXP DATES +++


def getExpDates(api, name: str, gabby: bool = False) -> List[tuple]:

    expirationList = api.get_expiration_dates(name)

    retList = [(dates["expirationDate"], dates["daysToExpiration"])
               for dates in expirationList['expirationList']]

    if gabby:
        c.red(str(retList))

    return retList

# * --- EXP DATES ---


def option_quote_string_for_gui(api, symbol: str) -> str:

    if (qData := get_option_quote(api, None, symbol, )):
        return (
                f"{symbol}: {qData.description}  Qty: {qData.quantity} DTE: {qData.daysToExpiration} "
                f"bid: ${qData.bid} ask: ${qData.ask} last:$ {qData.lastPrice} "
                f'\t Underlying: ${qData.underlying.price:.2f}')
    else:
        return f"Cannot get quote for {symbol}"


def _save_option_to_history(qData: optionData, option_history: Dict) -> None:
    """Save option data to history dictionary (persistence logic only)"""

    new_entry = {
        'dte': qData.daysToExpiration,
        'bid': qData.bid,
        'ask': qData.ask,
        'close': qData.lastPrice,
        'theta': qData.theta,
        'delta': qData.delta,
        'underlying': qData.underlying.price,
        'volatility': qData.volatility,
        'theoreticalOptionValue': qData.theoreticalOptionValue
    }

    if (entry := option_history.get(qData.symbol)) is None:
        c.orange(f'Initial entry (new stuff!!): {json.dumps(new_entry, indent=2)} symbol: {qData.symbol}')
        option_history[qData.symbol] = [new_entry]
    else:
        days_list = [day.get('dte') for day in entry]
        if qData.daysToExpiration not in days_list:
            entry.append(new_entry)


def ppOptions(api, all_pos: List, gabby: bool = False) -> None:
    """ Pretty print options positions (display logic separated from persistence) """

    # * Read optionHistory for persistence
    if (option_history := _read_dictfile(file=OPTION_HISTORY_FILE, gabby=gabby)) is None:
        option_history = {}

    for position in all_pos:
        # * Look only at Options
        if position.type not in ['PUT', 'CALL']:
            continue

        basis = next(
            (
                pos.averageCost
                for pos in all_pos
                if pos.symbol == position.underlying.symbol
            ),
            0,
        )

        qData = get_option_quote(api, None, position.symbol, gabby=gabby, )

        option_quantity = position.quantity if position.accountName == 'FidelityRoth' else (-1 * position.quantity)

        c.green(position.symbol)
        c.bold(f"\t{qData.description}  Qty: {option_quantity} DTE: {qData.daysToExpiration} "
               f"bid: ${qData.bid} ask: ${qData.ask} last:$ {qData.lastPrice} ")

        u_str = c._green(f'\t Underlying: ${qData.underlying.price:.2f}')

        color = c._green if qData.strikePrice > basis else c._orange
        b_str = color(f'Basis: {basis:.2f}')

        diff = qData.strikePrice - qData.underlying.price
        if diff > 0:
            color = c._bold if qData.putCall == 'CALL' else c._red
        else:
            color = c._red if qData.putCall == 'CALL' else c._bold

        if qData.underlying.price:
            diff_percent = int(100 * diff / qData.underlying.price)
            d_str = color(f'  diff: ${diff:.2f}({diff_percent}%)  ')
        else:
            d_str = color('  No "Underlying Price" available.  ')

        rest_str = c._green(f' 52High: ${qData.high52} 52Low: '
                            f'${qData.low52} openInterest: {qData.openInterest}')
        print(u_str + d_str + b_str + rest_str)

        # *  Now the greeks...
        # ? gamma, rho, vega ??
        c.green(f'\t  theoreticalOptionValue: ${qData.theoreticalOptionValue:.3f} '
                f'Volatility: {qData.volatility:.3f} delta: {qData.delta:.3f} theta: {qData.theta:.3f}')

        # Save to history (persistence separated from display)
        _save_option_to_history(qData, option_history)

    # Write history file once after all positions processed
    _write_dictfile(option_history, file=OPTION_HISTORY_FILE, gabby=gabby)


# * +++ Chain +++


def ppOptionChain(chain: Dict, put_call: str, dte: Optional[int] = None, greeks: bool = False, gabby: bool = False) -> None:
    '''
    Pretty print the call option chain
    '''
    def header(exp_date: str):
        c.bold(f'{exp_date:<26}dte  last   bid   ask  price  volume   '
               f'intrinsicValue   extrinsicValue   theoreticalValue')

    c.bold(f'\n{chain["symbol"]}  '
           f'Current:: ${chain["underlying"]["last"]}   '
           f'52WeekHigh: ${chain["underlying"]["fiftyTwoWeekHigh"]} '
           f'52WeekLow: ${chain["underlying"]["fiftyTwoWeekLow"]} '
           f'Volatility: {chain.get("volatility")}\n'
           )

    expDateMap = 'callExpDateMap' if put_call == 'CALL' else 'putExpDateMap'

    for exp_date, singleOpt in chain[expDateMap].items():
        # ? c.red(exp_date)  # * The exp date
        if dteFromYYYYmmdd(exp_date[:10]) > dte:
            break

        header(exp_date[:10])

        for strike, contractList in singleOpt.items():
            strikePrice = float(strike)
            contract = contractList[0]  # * There is only one item in list

            dte_str = contract.get('expirationDate')[:10]

            # ? if gabby:
                # ? c.bold('Contract')
                # ? c.green(json.dumps(contract, indent=4,))

            # * c.lightBlue(json.dumps(contract, indent=3))
            price = contract.get('bid', 0) or contract.get('close', 0)
            color = c.green if contract.get('inTheMoney') else c.lightBlue

            color(f'{contract['symbol']:<21} {contract['daysToExpiration']:>7} {contract.get("last"):>5} '
                  f'{contract['bid']:>5} {contract['ask']:>5} {price:>6} '
                  f'{contract['totalVolume']:>5} {contract['intrinsicValue']:>15} '
                  f'{contract['extrinsicValue']:>15} {contract.get('theoreticalOptionValue'):>15}'
                  )

            # * Bottom line attempts at value
            if gabby:
                apparent_value = strikePrice + price
                premium = apparent_value - price
                profit_percent = 100 * (price / strikePrice)
                annualized = fundamentals.annualized_pl_percent(
                    premium,
                    strikePrice,
                    to_date=dte_str,
                    gabby=False, )

                c.bold(f'\tApparent Value: {apparent_value:.2f}  Premium: {premium:.2f} '
                       f'Profit %: {profit_percent:.2f}%  Annualized: {annualized:.2f}%'
                       )

            # ! Greeks
            if greeks:
                c.green(f'\tGreeks: vol:{contract.get('volatility')} '
                        f'delta:{contract.get('delta')} '
                        f'gamma:{contract.get('gamma')} '
                        f'theta:{contract.get('theta')} '
                        f'vega:{contract.get('vega')} '
                        f'rho:{contract.get('rho')}'
                        )
        print()


def getChain(api, name: str,
             put_or_call: str = 'ALL',
             strike: Optional[float] = None,
             strike_count: Optional[int] = None,
             toDate: Optional[str] = None,
             fromDate: Optional[str] = None,
             daysToExpiration: Optional[int] = None,
             option_type: Optional[str] = None,
             gabby: bool = False) -> Dict:
    ''' get option chain for name '''

    if gabby:
        c.bold(f'name: {name}  strike: {strike} type: {put_or_call} dte: {daysToExpiration} range: {strike_count}')

    chain = api.get_option_chain(name,
                                 contractType=put_or_call,
                                 strikeCount=strike_count,
                                 includeUnderlyingQuote=True,
                                 strategy="SINGLE",
                                 interval=None,
                                 strike=strike,
                                 range=None,
                                 fromDate=fromDate,
                                 toDate=toDate,
                                 volatility=None,
                                 underlyingPrice=None,
                                 interestRate=None,
                                 daysToExpiration=daysToExpiration,
                                 expMonth=None,
                                 optionType=option_type,
                                 entitlement=None,
                                 )
    if gabby:
        c.bold('options.get_chain:')
        c.lightBlue(chain)
        # ? ppOptionChain(chain, put_or_call, gabby=gabby, )

    return chain


def get_option_quote(api, pos: Optional[Any], name: str, gabby: bool = False) -> optionData:
    """
        called with an option name in the form: 'WPC   241115C00060000'
        or, if we have a position, the 'pos' namedtuple
    """

    symbol_data = deconstructOptionSymbol(pos.symbol) if pos else deconstructOptionSymbol(name)
    # * print(symbol_data)
    # * c.orange(name)

    opt_chain_dict = getChain(api,
                              name=symbol_data.underlying_name,
                              put_or_call=symbol_data.put_call,
                              strike=symbol_data.strikePrice,
                              strike_count=None,    # ! This is important!!
                              toDate=symbol_data.expDate,
                              fromDate=symbol_data.expDate,
                              )

    # * c.lightGreen(json.dumps(opt_chain_dict, indent=3))

    if (uDict := opt_chain_dict.get('underlying')) is None:
        error_msg = f'No underlying data found for: {symbol_data.underlying_name}'
        c.red(error_msg)
        raise ValueError(error_msg)

    underlying_data = Underlying(
        symbol=uDict.get('symbol'),
        description=uDict.get('description'),
        bid=uDict.get('bid'),
        ask=uDict.get('ask'),
        last=uDict.get('last'),
        high52=uDict.get('fiftyTwoWeekHigh'),
        low52=uDict.get('fiftyTwoWeekLow'),
        price=uDict.get('last'),
        change=uDict.get('change'),
        percentChange=uDict.get('percentChange'),
    )

    # * c.bold('get_option_data:: underlying_data')
    # * c.lightBlue(underlying_data)

    map_name = 'callExpDateMap' if symbol_data.put_call == 'CALL' else 'putExpDateMap'
    if oDict := opt_chain_dict.get(map_name):
        # * c.blue(json.dumps(oDict, indent=2))
        for _, v in oDict.items():
            for _, vv in v.items():
                # Extract contract data to eliminate repetition
                contract = vv[0]

                return optionData(
                    symbol=contract.get('symbol'),
                    underlying=underlying_data,
                    accountName=pos.accountName if pos else 'Schwab',
                    strikePrice=contract.get('strikePrice'),
                    quantity=pos.quantity if pos else 0,
                    expirationDate=contract.get('expirationDate')[:10],
                    putCall=contract.get('putCall'),
                    type=contract.get('putCall'),
                    description=contract.get('description'),
                    bid=contract.get('bid'),
                    ask=contract.get('ask'),
                    lastPrice=contract.get('last'),
                    volatility=contract.get('volatility'),
                    markChange=contract.get('markChange'),
                    markPercentageChange=contract.get('markChangePercentage'),
                    delta=contract.get('delta'),
                    gamma=contract.get('gamma'),
                    theta=contract.get('theta'),
                    vega=contract.get('vega'),
                    rho=contract.get('rho'),
                    openInterest=contract.get('openInterest'),
                    timeValue=contract.get('timeValue'),
                    theoreticalOptionValue=contract.get('theoreticalOptionValue'),
                    interestRate=uDict.get('interestRate'),
                    daysToExpiration=contract.get('daysToExpiration'),
                    intrinsicValue=contract.get('intrinsicValue'),
                    extrinsicValue=contract.get('extrinsicValue'),
                    high52=contract.get('high52Week'),
                    low52=contract.get('low52Week'),
                    inTheMoney=contract.get('inTheMoney'),
                )

    error_msg = f'No option data found in chain for {name}'
    c.red(error_msg)
    raise ValueError(error_msg)


def optionUX(api, av: List[str], put_call: str, gabby: bool = False) -> Dict:
    # Single pass over av to extract all parameters
    greeks = False
    count = 7  # default
    dte = 90   # default

    for a in av:
        if '?' in a:
            pr_option_help()
            return
        elif a == '-greeks':
            greeks = True
        elif a.startswith('-count'):
            count = int(a[6:])
        elif a.startswith('-dte'):
            dte = int(a[4:])

    name = av[1].upper()

    c.bold(f'Options for {name}  dte: {dte} count: {count}')

    opt_chain = getChain(api, name, put_call, strike_count=count, daysToExpiration=dte, gabby=gabby, )

    if not opt_chain:
        error_msg = f'No options found for {name} with dte: {dte} and count: {count}'
        c.red(error_msg)
        raise ValueError(error_msg)

    ppOptionChain(opt_chain, put_call, dte=dte, greeks=greeks, gabby=gabby, )

    if greeks:
        helpGreeks()

    return opt_chain


def ourOptionsUX(api, av: List[str], allPos: List, gabby: bool = False) -> None:

    if '?' in av:
        c.bold('Show the Schwab options we hold.')
        c.green('\t"options" - lists options')
        return

    ppOptions(api, allPos, gabby=gabby, )


# * --- Chain ---


# * +++ ROI +++

def dteFromYYYYmmdd(YYYYmmdd: str) -> int:
    """ Can handle YYmmdd or YYYY-mm-dd formats """

    from_date = datetime.date.today()

    YYYYmmdd = YYYYmmdd.replace('-', '') if '-' in YYYYmmdd else f'20{YYYYmmdd}'

    to_date = datetime.date.fromisoformat(YYYYmmdd)

    return (to_date - from_date).days


def deconstructOptionSymbol(name: str, gabby: bool = False) -> namedtuple:

    symbol_data = namedtuple('symbolData', 'underlying_name expDate DTE put_call strikePrice')

    try:
        split_string = name.split(" ")
        # * c.orange(split_string)
        underlying_name = split_string[0]
        exp_date = split_string[-1]
        exp_date = f'20{exp_date[:2]}-{exp_date[2:4]}-{exp_date[4:6]}'
        put_call = 'CALL' if split_string[-1][6:7] == 'C' else 'PUT'
        try:
            # ? strikePrice = float(split_string[-1][7:12] + '.' + split_string[-1][12:14])
            strikePrice = float(f'{split_string[-1][7:12]}.{split_string[-1][12:14]}')
        except ValueError as e:
            c.red(f'Error parsing strike price in option symbol {name}: {e}')
        try:
            strikePrice = float(split_string[-1][7:12])
        except ValueError as e:
            c.red(f'Error parsing strike price in option symbol {name}: {e}')
        dte = dteFromYYYYmmdd(exp_date)
    except Exception as e:
        c.red(f'Error deconstructing option symbol {name}: {e}')
        raise

    if gabby:
        c.bold(underlying_name, exp_date, put_call, strikePrice, dte)

    return symbol_data(underlying_name, exp_date, dte, put_call, strikePrice)


def constructOptionSymbols(name: str, strike: int | float, exps: tuple, put_call: str = 'P') -> List[str]:

    def _ISOtoOptionDate(ISOdate) -> str:
        return f'{ISOdate[2:4]}{ISOdate[5:7]:02}{ISOdate[8:10]:02}'

    return [f'{name:<6}{_ISOtoOptionDate(exp[0])}{put_call}{int(strike):05}000' for exp in exps]


def calcROIfromOptionSymbol(api, name: str, premium: float, marketValue: float, DTE: int,
                            strikePrice: float, dividend: float = 0, our_basis: Optional[float] = None, gabby: bool = False) -> float:
    """ returns ROI """

    if DTE <= 0:
        return 0

    option_data = deconstructOptionSymbol(name)

    dte_multiplier = (365 / DTE)

    if option_data.put_call == 'CALL':
        option_ROI = 100 * (((premium + dividend) / marketValue) * dte_multiplier)
        if gabby:
            print(premium, marketValue, dte_multiplier)
            c.bold(f'callROI: {option_ROI:.2f}%')

    elif option_data.put_call == 'PUT':
        option_ROI = 100 * ((premium / option_data.strikePrice) * dte_multiplier)
        if gabby:
            print(premium, strikePrice, dte_multiplier)
            c.bold(f'putROI: {option_ROI:.2f}%')

    return option_ROI


# * --- ROI ---


# * +++ Option Strategies plotting +++
# ? import opstrat as op


# * --- Option Strategies plotting ---


if __name__ == '__main__':

    c.green('Wait for it...')
    # ? op.single_plotter()

    print(deconstructOptionSymbol('MSFT  240913C00445000'))

    print(constructOptionSymbols('MSFT', 450,
                                 [('2024-09-06', 3),
                                  ('2024-09-13', 10),
                                  ('2024-09-20', 17), ], 'C', ))

    # ! op.single_plotter(spot=113, strike=115, op_type='c', tr_type='s', op_pr=3)

    c.bold('DONE')
