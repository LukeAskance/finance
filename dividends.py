#! /usr/local/bin/python3

# * (c) 1066-2050 George... Flammer All Rights Reserved
""" dividends.py module is designed to ease finding optimal option options """

from datetime import datetime
import json
from dataclasses import dataclass
from typing import Optional

import c
import fundamentals


@dataclass
class divData:
    """Dividend and earnings data for a security"""
    symbol: str
    lastPrice: float
    divPayAmount: float
    divFreq: int
    divYield: float
    divIncome: float
    divPayDate: str
    nextDivExDate: str
    nextDivPayDate: str
    lastEarningsDate: str
    eps: float
    peRatio: float


def daysFromYYmmdd(date):
    """ returns DTE given a date in YY-mm-dd format
        OR None if no date is given
    """

    if '-' in date:
        timedelta = datetime.strptime(date, "%Y-%m-%d") - datetime.now()
        return timedelta.days
    return 0


def dividendYield(api, name: str, gabby=False) -> Optional[divData]:
    """
    Fetch dividend and earnings data for a security.
    Returns None if data cannot be retrieved or is incomplete.
    """
    # Get position data from API
    if not (fullDict := fundamentals.getPosition(api, name, gabby=gabby)):
        return None

    # Validate that symbol exists in response
    if name not in fullDict:
        if gabby:
            c.red(f'Symbol {name} not found in API response')
        return None

    # Extract fundamental and quote data with defensive checks
    funDict = fullDict[name].get('fundamental', {})
    qDict = fullDict[name].get('quote', {})

    # Validate required data exists
    if not funDict or not qDict:
        if gabby:
            c.orange(f'Missing fundamental or quote data for {name}')
        return None

    if gabby:
        c.bold(json.dumps(fullDict, indent=2))

    # Calculate dividend income with safe defaults
    div_pay_amount = funDict.get('divPayAmount', 0.0)
    div_freq = funDict.get('divFreq', 0.0)

    return divData(
        symbol=name,
        lastPrice=qDict.get('lastPrice', 0.0),
        divPayAmount=div_pay_amount,
        divFreq=div_freq,
        divYield=funDict.get('divYield', 0.0),
        divIncome=div_pay_amount * div_freq,
        divPayDate=funDict.get('divPayDate', 'NoDivDates')[:10],
        nextDivExDate=funDict.get('nextDivExDate', 'NoDivDates')[:10],
        nextDivPayDate=funDict.get('nextDivPayDate', 'NoDivDates')[:10],
        lastEarningsDate=funDict.get('lastEarningsDate', 'NoEarningsDate')[:10],
        eps=funDict.get('eps', 0.0),
        peRatio=funDict.get('peRatio', 0.0),
    )


# * +++ ppDividends +++

def ppDividends(pos: divData) -> str:
    """Format dividend data as a string for display"""

    return (
        f"{pos.symbol} Price: ${pos.lastPrice} "
        f" divAmt: ${pos.divPayAmount} x "
        f"{pos.divFreq} times yearly = "
        f"${(pos.divPayAmount * pos.divFreq):.2f} "
        f"Yield: {pos.divYield:.2f}% "
        f"EPS: {pos.eps:.2f}  PE: {pos.peRatio:.2f}\n"
        f"\tPaid: {pos.divPayDate} "
        f"NextDivExDate: {pos.nextDivExDate} NextDivPayDate: "
        f"{pos.nextDivPayDate} LastEarnings: "
        f"{pos.lastEarningsDate}"
        )

# * --- ppDividends ---


def dividendUX(api, av: list, allPos: list, gabby=False, ):
    """ Handle command line input for accessing dividend quotations... """
    for a in av:
        if '?' in a:
            c.bold('ex: div name1 ... name2... (may be option symbol)')
            return True

    for name in av[1:]:
        done = False
        if (name := name.upper()).isalpha():
            # * Check first if we have the DIV info already loaded (in allPos)
            for pos in allPos:
                if name == pos.symbol:
                    c.green(ppDividends(pos))
                    done = True
                    break

        if not done:
            if (dData := dividendYield(api, name, gabby=gabby, )):
                c.bold(ppDividends(dData))
            else:
                c.orange(f'No dividend data for {name}')
