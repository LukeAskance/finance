from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional, Protocol, cast
from datetime import date, datetime
import csv
import glob
import os


DEFAULT_ACCOUNT_MAPPING = {
    "11351369": "GeorgeTrust",
    "21178329": "DebRoth",
    "30969090": "Investments",
    "45521728": "DebTrust",
    "63568172": "GrandKids",
    "89958151": "GeorgeRoth",
}


class QuoteFundamentalsAPI(Protocol):
    def get_linked_accounts(self) -> list[dict[str, Any]]:
        ...

    def get_account_details(
        self,
        account_hash: str,
        fields: str = "positions",
    ) -> dict[str, Any]:
        ...

    def get_quote_and_fundamentals(
        self,
        symbol: str,
        gabby: bool = False,
    ) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
        ...

    def get_quote(
        self,
        symbol: str,
        gabby: bool = False,
    ) -> Optional[dict[str, Any]]:
        ...


@dataclass(slots=True)
class UnderlyingQuote:
    symbol: str
    price: float = 0.0


@dataclass(slots=True)
class PortfolioPosition:
    symbol: str
    underlying: str | UnderlyingQuote
    broker: str
    account_name: str
    account_cash: float
    description: str
    quantity: float
    average_cost: float
    position_type: str

    high52: float = 0.0
    low52: float = 0.0
    last_price: float = 0.0

    div_pay_amount: float = 0.0
    div_pay_date: str = "UnknownDay"
    div_freq: float = 0.0
    div_ex_date: str = "UnknownDay"
    div_yield: float = 0.0

    eps: float = 0.0
    pe_ratio: float = 0.0
    last_earnings_date: str = "UnknownDay"
    next_div_ex_date: str = "UnknownDay"
    next_div_pay_date: str = "UnknownDay"

    pl: float = 0.0
    pl_total: float = 0.0
    income: float = 0.0
    market_value: float = 0.0
    percent_pl: float = 0.0
    long_name: str = ""

    strike_price: Optional[float] = None
    days_to_expiration: Optional[int] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark_percentage_change: Optional[float] = None

    @property
    def accountName(self) -> str:
        return self.account_name

    @property
    def accountCash(self) -> float:
        return self.account_cash

    @property
    def averageCost(self) -> float:
        return self.average_cost

    @property
    def type(self) -> str:
        return self.position_type

    @property
    def high52Week(self) -> float:
        return self.high52

    @property
    def low52Week(self) -> float:
        return self.low52

    @property
    def lastPrice(self) -> float:
        return self.last_price

    @property
    def divPayAmount(self) -> float:
        return self.div_pay_amount

    @property
    def divPayDate(self) -> str:
        return self.div_pay_date

    @property
    def divFreq(self) -> float:
        return self.div_freq

    @property
    def divExDate(self) -> str:
        return self.div_ex_date

    @property
    def divYield(self) -> float:
        return self.div_yield

    @property
    def peRatio(self) -> float:
        return self.pe_ratio

    @property
    def lastEarningsDate(self) -> str:
        return self.last_earnings_date

    @property
    def nextDivExDate(self) -> str:
        return self.next_div_ex_date

    @property
    def nextDivPayDate(self) -> str:
        return self.next_div_pay_date

    @property
    def PL(self) -> float:
        return self.pl_total

    @property
    def marketValue(self) -> float:
        return self.market_value

    @property
    def percentPL(self) -> float:
        return self.percent_pl

    @property
    def longName(self) -> str:
        return self.long_name

    @property
    def strikePrice(self) -> Optional[float]:
        return self.strike_price

    @property
    def daysToExpiration(self) -> Optional[int]:
        return self.days_to_expiration

    @property
    def markPercentageChange(self) -> Optional[float]:
        return self.mark_percentage_change


@dataclass(slots=True)
class _PositionSeed:
    symbol: str
    underlying: str
    broker: str
    account_name: str
    account_cash: float
    description: str
    quantity: float
    average_cost: float
    position_type: str
    strike_price: Optional[float] = None
    days_to_expiration: Optional[int] = None


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text == "--":
        return default
    text = text.replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return default


def _safe_optional_float(
    value: Any,
    default: Optional[float] = None,
) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text == "--":
        return default
    text = text.replace("$", "").replace(",", "")
    try:
        return float(text)
    except ValueError:
        return default


def _to_date10(value: Any, default: str = "UnknownDay") -> str:
    if not value:
        return default
    text = str(value)
    return text[:10] if len(text) >= 10 else text


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def _is_option_symbol(symbol: str) -> bool:
    return len(symbol) > 7


def _extract_underlying_from_option_symbol(symbol: str) -> str:
    return symbol.split(" ")[0].strip() if " " in symbol else symbol[:6].strip()


def _dte_from_yyyymmdd(yyyymmdd: str) -> int:
    try:
        expiry = datetime.strptime(yyyymmdd, "%y%m%d").date()
        return (expiry - date.today()).days
    except ValueError:
        return 0


def _extract_quote_payload(
    raw_quote: Optional[dict[str, Any]],
    symbol: str,
) -> dict[str, Any]:
    if not raw_quote:
        return {}

    node: Any = raw_quote.get(symbol) or raw_quote.get(symbol.upper())
    if node is None and raw_quote:
        first = next(iter(raw_quote.values()))
        if isinstance(first, dict):
            node = cast(dict[str, Any], first)

    if not isinstance(node, dict):
        return {}

    node = cast(dict[str, Any], node)

    quote = node.get("quote")
    if isinstance(quote, dict):
        return cast(dict[str, Any], quote)
    return node


def fidelity_option_desc_to_schwab_symbol(desc: str) -> str:
    import calendar

    abbr_to_num = {
        name.upper(): num
        for num, name in enumerate(calendar.month_abbr)
        if num
    }
    parts = desc.split(" ")
    if len(parts) < 6:
        raise ValueError(
            f"Invalid option description format (expected 6+ parts): {desc}"
        )
    if parts[1] not in abbr_to_num:
        raise ValueError(
            f'Invalid month abbreviation "{parts[1]}" in: {desc}'
        )

    month_num = abbr_to_num[parts[1]]
    strike_text = parts[4][1:]
    strike_digits = strike_text.replace(".", "")[:3]

    if float(strike_text) % 1:
        return (
            f"{parts[0]:<6}{parts[3][2:]}{month_num:02}{parts[2]}"
            f"{parts[5][0]}{strike_digits:>06}00"
        )

    return (
        f"{parts[0]:<6}{parts[3][2:]}{month_num:02}{parts[2]}"
        f"{parts[5][0]}{strike_digits:>05}000"
    )


def _discover_schwab_position_seeds(
    api: QuoteFundamentalsAPI,
    account_mapping: Optional[dict[str, str]] = None,
    include_options: bool = True,
    include_cash: bool = True,
) -> list[_PositionSeed]:
    mapping = account_mapping or DEFAULT_ACCOUNT_MAPPING
    linked_accounts = api.get_linked_accounts()

    def fetch_details(account: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        account_number = account.get("accountNumber", "")
        account_name = str(mapping.get(account_number, account_number))
        details = api.get_account_details(
            str(account.get("hashValue", "")),
            fields="positions",
        )
        return account_name, details

    seeds: list[_PositionSeed] = []
    max_workers = min(12, max(1, len(linked_accounts)))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(fetch_details, acct) for acct in linked_accounts]
        for fut in as_completed(futures):
            account_name, details = fut.result()
            securities_account = details.get("securitiesAccount", {})
            balances = securities_account.get("currentBalances", {})
            account_cash = _safe_float(balances.get("cashBalance"))

            if include_cash and account_cash:
                seeds.append(
                    _PositionSeed(
                        symbol="CASH",
                        underlying="CASH",
                        broker="Schwab",
                        account_name=account_name,
                        account_cash=account_cash,
                        description="Cash balance",
                        quantity=account_cash,
                        average_cost=1.0,
                        position_type="Cash",
                    )
                )

            for position in securities_account.get("positions", []):
                if not position:
                    continue

                instrument = position.get("instrument", {})
                symbol = str(instrument.get("symbol", "")).strip().upper()
                if not symbol:
                    continue

                if len(symbol) == 9:
                    continue

                is_option = _is_option_symbol(symbol)
                if is_option and not include_options:
                    continue

                quantity = (
                    position.get("longQuantity")
                    or position.get("shortQuantity")
                    or 0
                )
                underlying = (
                    _extract_underlying_from_option_symbol(symbol)
                    if is_option
                    else symbol
                )

                option_type = str(instrument.get("assetType", "EQUITY"))
                if is_option and option_type == "OPTION":
                    option_type = "OPTION"

                seeds.append(
                    _PositionSeed(
                        symbol=symbol,
                        underlying=underlying,
                        broker="Schwab",
                        account_name=account_name,
                        account_cash=account_cash,
                        description=instrument.get("description", "EQUITY"),
                        quantity=_safe_float(quantity),
                        average_cost=_safe_float(
                            position.get("averageLongPrice")
                        ),
                        position_type=option_type,
                        strike_price=_safe_optional_float(
                            position.get("averagePrice")
                        ),
                    )
                )

    return seeds


def _get_newest_csv_file(
    pattern: str,
    search_dir: str,
    cleanup_old: bool = True,
) -> Optional[str]:
    files = list(
        filter(
            os.path.isfile,
            glob.glob(f"{search_dir.rstrip('/')}/{pattern}"),
        )
    )
    if not files:
        return None

    files.sort(key=lambda path: os.path.getmtime(path), reverse=True)
    if cleanup_old and len(files) > 1:
        for old_file in files[1:]:
            try:
                os.remove(old_file)
            except OSError:
                continue

    return files[0]


def _discover_fidelity_position_seeds(
    search_dir: str = "~/Downloads",
    account_name: str = "FidelityRoth",
    include_options: bool = True,
    include_cash: bool = True,
) -> list[_PositionSeed]:
    portfolio_csv = _get_newest_csv_file(
        "Portfolio_*.csv",
        os.path.expanduser(search_dir),
        cleanup_old=True,
    )
    if portfolio_csv is None:
        return []

    seeds: list[_PositionSeed] = []
    fidelity_cash = 0.0
    with open(portfolio_csv) as f:
        reader = csv.reader(f)
        headings = next(reader)

        for row in csv.DictReader(f, fieldnames=headings):
            symbol = str(row.get("Symbol", "")).strip().upper()
            if not symbol:
                continue

            if symbol == "PENDING ACTIVITY":
                continue

            description = str(row.get("Description", "")).strip()

            if symbol in {"CORE**", "FDRXX**", "SPRXX"}:
                cash_value = _safe_float(row.get("Current Value"))
                fidelity_cash += cash_value
                if include_cash and cash_value > 0:
                    seeds.append(
                        _PositionSeed(
                            symbol=symbol,
                            underlying=symbol,
                            broker="Fidelity",
                            account_name=account_name,
                            account_cash=cash_value,
                            description=description or "Cash",
                            quantity=cash_value,
                            average_cost=1.0,
                            position_type="Cash",
                        )
                    )
                continue

            quantity = _safe_float(row.get("Quantity"))
            average_cost = _safe_float(row.get("Average Cost Basis"))
            option_type = None
            strike_price = None
            dte = None

            if len(symbol) > 9:
                if not include_options:
                    continue
                split_desc = description.split(" ")
                if not split_desc or split_desc[-1] not in {"CALL", "PUT"}:
                    continue
                option_type = split_desc[-1]
                symbol = fidelity_option_desc_to_schwab_symbol(description)
                strike_price = _safe_optional_float(split_desc[4][1:])
                dte = _dte_from_yyyymmdd(symbol[6:12])

            underlying = description.split(" ")[0] if description else symbol
            position_type = option_type or "EQUITY"

            seeds.append(
                _PositionSeed(
                    symbol=symbol,
                    underlying=underlying,
                    broker="Fidelity",
                    account_name=account_name,
                    account_cash=fidelity_cash,
                    description=description,
                    quantity=quantity,
                    average_cost=average_cost,
                    position_type=position_type,
                    strike_price=strike_price,
                    days_to_expiration=dte,
                )
            )

    if fidelity_cash:
        for seed in seeds:
            if seed.account_name == account_name:
                seed.account_cash = fidelity_cash

    return seeds


def _build_cash_position(seed: _PositionSeed) -> PortfolioPosition:
    market_value = seed.quantity
    income = 0.035 * market_value
    return PortfolioPosition(
        symbol=seed.symbol,
        underlying=seed.underlying,
        broker=seed.broker,
        account_name=seed.account_name,
        account_cash=seed.account_cash,
        description=seed.description,
        quantity=seed.quantity,
        average_cost=seed.average_cost,
        position_type="Cash",
        high52=1.0,
        low52=1.0,
        last_price=1.0,
        div_pay_amount=0.003,
        div_freq=12.0,
        div_yield=3.5,
        pl=0.0,
        pl_total=0.0,
        income=income,
        market_value=market_value,
        percent_pl=0.0,
        long_name=seed.description or seed.symbol,
    )


def _build_option_position_from_seed(
    api: QuoteFundamentalsAPI,
    seed: _PositionSeed,
    gabby: bool = False,
) -> PortfolioPosition:
    raw_quote = api.get_quote(seed.symbol, gabby=gabby)
    quote = _extract_quote_payload(raw_quote, seed.symbol)

    last_price = _safe_float(quote.get("lastPrice") or quote.get("mark"))
    mark = _safe_float(quote.get("mark"), last_price)
    bid = _safe_optional_float(quote.get("bidPrice"))
    ask = _safe_optional_float(quote.get("askPrice"))
    strike_price = _safe_optional_float(
        quote.get("strikePrice"),
        seed.strike_price,
    )
    dte = _to_int(quote.get("daysToExpiration"), seed.days_to_expiration or 0)

    underlying_price = _safe_float(quote.get("underlyingPrice"))
    underlying_quote = UnderlyingQuote(
        symbol=seed.underlying,
        price=underlying_price,
    )

    pl = last_price - seed.average_cost
    pl_total = pl * seed.quantity
    percent_pl = (
        (100 * (pl / seed.average_cost))
        if seed.average_cost
        else 100.0
    )
    market_value = mark * seed.quantity

    position_type = str(seed.position_type).upper()
    if position_type not in {"CALL", "PUT"}:
        contract_type = str(quote.get("putCall") or "").upper()
        if contract_type == "CALL":
            position_type = "CALL"
        elif contract_type == "PUT":
            position_type = "PUT"
        else:
            position_type = "OPTION"

    return PortfolioPosition(
        symbol=seed.symbol,
        underlying=underlying_quote,
        broker=seed.broker,
        account_name=seed.account_name,
        account_cash=seed.account_cash,
        description=seed.description,
        quantity=seed.quantity,
        average_cost=seed.average_cost,
        position_type=position_type,
        high52=_safe_float(quote.get("52WeekHigh")),
        low52=_safe_float(quote.get("52WeekLow")),
        last_price=last_price,
        pl=pl,
        pl_total=pl_total,
        income=0.0,
        market_value=market_value,
        percent_pl=percent_pl,
        long_name=seed.symbol,
        strike_price=strike_price,
        days_to_expiration=dte,
        bid=bid,
        ask=ask,
        mark_percentage_change=_safe_float(
            quote.get("markPercentChange"),
            0.0,
        ),
    )


def _build_position_from_seed(
    api: QuoteFundamentalsAPI,
    seed: _PositionSeed,
    gabby: bool = False,
) -> PortfolioPosition:
    if seed.position_type == "Cash":
        return _build_cash_position(seed)

    if _is_option_symbol(seed.symbol) or seed.position_type in {
        "CALL",
        "PUT",
        "OPTION",
    }:
        return _build_option_position_from_seed(api, seed, gabby=gabby)

    q_dict, fun_dict = api.get_quote_and_fundamentals(seed.symbol, gabby=gabby)
    q_dict = q_dict or {}
    fun_dict = fun_dict or {}

    last_price = _safe_float(q_dict.get("lastPrice"))
    pl = last_price - seed.average_cost
    pl_total = pl * seed.quantity
    percent_pl = (
        (100 * (pl / seed.average_cost))
        if seed.average_cost
        else 100.0
    )

    div_pay_amount = _safe_float(fun_dict.get("divPayAmount"))
    div_freq = _safe_float(fun_dict.get("divFreq"))
    income = div_pay_amount * div_freq * seed.quantity
    market_value = last_price * seed.quantity

    return PortfolioPosition(
        symbol=seed.symbol,
        underlying=seed.underlying,
        broker=seed.broker,
        account_name=seed.account_name,
        account_cash=seed.account_cash,
        description=seed.description,
        quantity=seed.quantity,
        average_cost=seed.average_cost,
        position_type=seed.position_type,
        high52=_safe_float(q_dict.get("52WeekHigh")),
        low52=_safe_float(q_dict.get("52WeekLow")),
        last_price=last_price,
        div_pay_amount=div_pay_amount,
        div_pay_date=_to_date10(fun_dict.get("divPayDate")),
        div_freq=div_freq,
        div_ex_date=_to_date10(fun_dict.get("divExDate")),
        div_yield=_safe_float(fun_dict.get("divYield")),
        eps=_safe_float(fun_dict.get("eps")),
        pe_ratio=_safe_float(fun_dict.get("peRatio")),
        last_earnings_date=_to_date10(fun_dict.get("lastEarningsDate")),
        next_div_ex_date=_to_date10(fun_dict.get("nextDivExDate")),
        next_div_pay_date=_to_date10(fun_dict.get("nextDivPayDate")),
        pl=pl,
        pl_total=pl_total,
        income=income,
        market_value=market_value,
        percent_pl=percent_pl,
        long_name=str(fun_dict.get("longName") or seed.symbol),
    )


def load_portfolio_positions(
    api: QuoteFundamentalsAPI,
    include_fidelity: bool = True,
    fidelity_search_dir: str = "~/Downloads",
    include_options: bool = True,
    include_cash: bool = True,
    gabby: bool = False,
    max_workers: int = 16,
) -> list[PortfolioPosition]:
    schwab_seeds = _discover_schwab_position_seeds(
        api,
        include_options=include_options,
        include_cash=include_cash,
    )
    fidelity_seeds = (
        _discover_fidelity_position_seeds(
            search_dir=fidelity_search_dir,
            include_options=include_options,
            include_cash=include_cash,
        )
        if include_fidelity
        else []
    )
    seeds = schwab_seeds + fidelity_seeds

    if not seeds:
        return []

    with ThreadPoolExecutor(max_workers=min(max_workers, len(seeds))) as pool:
        futures = [
            pool.submit(_build_position_from_seed, api, seed, gabby)
            for seed in seeds
        ]
        return [fut.result() for fut in as_completed(futures)]


def discover_equity_names(positions: list[PortfolioPosition]) -> list[str]:
    valid_types = {"EQUITY", "MUTUAL_FUND", "COLLECTIVE_INVESTMENT"}
    return sorted(
        {p.symbol for p in positions if p.position_type in valid_types}
    )


def discover_equity_names_from_sources(
    api: QuoteFundamentalsAPI,
    include_fidelity: bool = True,
    fidelity_search_dir: str = "~/Downloads",
) -> list[str]:
    schwab = _discover_schwab_position_seeds(
        api,
        include_options=False,
        include_cash=False,
    )
    fidelity = (
        _discover_fidelity_position_seeds(
            search_dir=fidelity_search_dir,
            include_options=False,
            include_cash=False,
        )
        if include_fidelity
        else []
    )
    return sorted({seed.symbol for seed in (schwab + fidelity)})


def as_dict(position: PortfolioPosition) -> dict[str, Any]:
    return {
        "symbol": position.symbol,
        "underlying": position.underlying,
        "broker": position.broker,
        "account_name": position.account_name,
        "account_cash": position.account_cash,
        "description": position.description,
        "quantity": position.quantity,
        "average_cost": position.average_cost,
        "position_type": position.position_type,
        "high52": position.high52,
        "low52": position.low52,
        "last_price": position.last_price,
        "div_pay_amount": position.div_pay_amount,
        "div_pay_date": position.div_pay_date,
        "div_freq": position.div_freq,
        "div_ex_date": position.div_ex_date,
        "div_yield": position.div_yield,
        "eps": position.eps,
        "pe_ratio": position.pe_ratio,
        "last_earnings_date": position.last_earnings_date,
        "next_div_ex_date": position.next_div_ex_date,
        "next_div_pay_date": position.next_div_pay_date,
        "pl": position.pl,
        "pl_total": position.pl_total,
        "income": position.income,
        "market_value": position.market_value,
        "percent_pl": position.percent_pl,
        "long_name": position.long_name,
        "strike_price": position.strike_price,
        "days_to_expiration": position.days_to_expiration,
        "bid": position.bid,
        "ask": position.ask,
        "mark_percentage_change": position.mark_percentage_change,
    }
