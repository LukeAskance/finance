from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    select,
    text,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

from positions import load_portfolio_positions


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    broker: Mapped[str | None] = mapped_column(String(64), nullable=True)
    account_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    base_currency: Mapped[str] = mapped_column(String(8), default="USD", nullable=False)
    metadata_json: Mapped[str | None] = mapped_column(Text, nullable=True)


class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    instrument_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    underlying_id: Mapped[int | None] = mapped_column(ForeignKey("instruments.id"), nullable=True)
    strike: Mapped[float | None] = mapped_column(Float, nullable=True)
    expiry: Mapped[date | None] = mapped_column(Date, nullable=True)
    option_type: Mapped[str | None] = mapped_column(String(8), nullable=True)
    currency: Mapped[str] = mapped_column(String(8), default="USD", nullable=False)
    sector: Mapped[str | None] = mapped_column(String(128), nullable=True)
    industry: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tags_json: Mapped[str | None] = mapped_column(Text, nullable=True)

    underlying: Mapped["Instrument | None"] = relationship(remote_side=[id])


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    fees: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    cash_effect: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class DailyPrice(Base):
    __tablename__ = "daily_prices"
    __table_args__ = (
        UniqueConstraint("date", "instrument_id", name="uq_daily_prices_date_instrument"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), nullable=False, index=True)
    close_price: Mapped[float] = mapped_column(Float, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), default="USD", nullable=False)
    price_source: Mapped[str] = mapped_column(String(64), default="schwab_api", nullable=False)


class Classification(Base):
    __tablename__ = "classifications"
    __table_args__ = (
        UniqueConstraint("instrument_id", "attribute_name", name="uq_classifications_instrument_attr"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), nullable=False, index=True)
    attribute_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    attribute_value: Mapped[str] = mapped_column(String(256), nullable=False)


class DailyAccountSnapshot(Base):
    __tablename__ = "daily_account_snapshots"
    __table_args__ = (
        UniqueConstraint("date", "account_id", name="uq_daily_account_snapshots_date_account"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    total_market_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    cash_balance: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    unrealized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    realized_pnl_to_date: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class DailyPositionSnapshot(Base):
    __tablename__ = "daily_position_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "date",
            "account_id",
            "instrument_id",
            name="uq_daily_position_snapshots_date_account_instrument",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), nullable=False, index=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), nullable=False, index=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    market_value: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    average_cost: Mapped[float | None] = mapped_column(Float, nullable=True)


class MarketIndicatorSnapshot(Base):
    __tablename__ = "market_indicator_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    vix: Mapped[float | None] = mapped_column(Float, nullable=True)
    sp500: Mapped[float | None] = mapped_column(Float, nullable=True)
    gld: Mapped[float | None] = mapped_column(Float, nullable=True)


_engine = None
_SessionLocal: sessionmaker[Session] | None = None


def init_db(db_path: str) -> None:
    global _engine, _SessionLocal
    _engine = create_engine(f"sqlite:///{db_path}", future=True)
    _SessionLocal = sessionmaker(bind=_engine, autoflush=False, autocommit=False, future=True)
    Base.metadata.create_all(_engine)
    with _engine.connect() as conn:
        cols = {row[1] for row in conn.execute(text("PRAGMA table_info(market_indicator_snapshots)"))}
        if "gld" not in cols:
            conn.execute(text("ALTER TABLE market_indicator_snapshots ADD COLUMN gld REAL"))
            conn.commit()


@contextmanager
def session_scope():
    if _SessionLocal is None:
        raise RuntimeError("Database not initialized; call init_db(...) first.")
    session = _SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _coerce_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _instrument_type_from_position_type(position_type: Any) -> str:
    text = str(position_type or "").strip().upper()
    if text == "CASH":
        return "cash"
    if text in {"OPTION", "OPTIONS"}:
        return "option"
    if text == "ETF":
        return "etf"
    if text in {"MUTUAL_FUND", "COLLECTIVE_INVESTMENT", "FUND"}:
        return "fund"
    return "equity"


def _get_or_create_account(session: Session, name: str) -> Account:
    row = session.execute(select(Account).where(Account.name == name)).scalar_one_or_none()
    if row:
        return row
    row = Account(name=name, base_currency="USD")
    session.add(row)
    session.flush()
    return row


def _get_or_create_instrument(session: Session, symbol: str, instrument_type: str) -> Instrument:
    row = session.execute(select(Instrument).where(Instrument.symbol == symbol)).scalar_one_or_none()
    if row:
        return row
    row = Instrument(symbol=symbol, instrument_type=instrument_type, currency="USD")
    session.add(row)
    session.flush()
    return row


def _write_snapshot_rows(
    session: Session,
    positions: list[Any],
    snapshot_date: date,
) -> dict[str, Any]:
    account_totals: dict[int, dict[str, float]] = {}
    rolled_positions: dict[tuple[int, int], dict[str, Any]] = {}
    position_count = 0

    for p in positions:
        symbol = str(getattr(p, "symbol", "") or "").strip().upper()
        account_name = str(getattr(p, "account_name", "") or "").strip()
        if not symbol or not account_name:
            continue

        instrument_type = _instrument_type_from_position_type(getattr(p, "position_type", ""))
        account = _get_or_create_account(session, account_name)
        instrument = _get_or_create_instrument(session, symbol, instrument_type)

        quantity = _coerce_float(getattr(p, "quantity", 0.0))
        market_value = _coerce_float(getattr(p, "market_value", 0.0))
        last_price = _coerce_float(getattr(p, "last_price", 0.0))

        key = (account.id, instrument.id)
        rolled = rolled_positions.setdefault(
            key,
            {
                "account_id": account.id,
                "instrument_id": instrument.id,
                "quantity": 0.0,
                "market_value": 0.0,
                "last_price": 0.0,
            },
        )
        rolled["quantity"] += quantity
        rolled["market_value"] += market_value
        rolled["last_price"] = last_price
        position_count += 1

        totals = account_totals.setdefault(
            account.id,
            {"total_market_value": 0.0, "cash_balance": 0.0},
        )
        totals["total_market_value"] += market_value
        if instrument_type == "cash":
            totals["cash_balance"] += market_value

    for rolled in rolled_positions.values():
        stmt = (
            sqlite_insert(DailyPositionSnapshot)
            .values(
                date=snapshot_date,
                account_id=rolled["account_id"],
                instrument_id=rolled["instrument_id"],
                quantity=rolled["quantity"],
                market_value=rolled["market_value"],
                last_price=rolled["last_price"],
                average_cost=None,
            )
            .on_conflict_do_update(
                index_elements=["date", "account_id", "instrument_id"],
                set_={
                    "quantity": rolled["quantity"],
                    "market_value": rolled["market_value"],
                    "last_price": rolled["last_price"],
                    "average_cost": None,
                },
            )
        )
        session.execute(stmt)

    for account_id, totals in account_totals.items():
        stmt = (
            sqlite_insert(DailyAccountSnapshot)
            .values(
                date=snapshot_date,
                account_id=account_id,
                total_market_value=totals["total_market_value"],
                cash_balance=totals["cash_balance"],
                unrealized_pnl=0.0,
                realized_pnl_to_date=0.0,
            )
            .on_conflict_do_update(
                index_elements=["date", "account_id"],
                set_={
                    "total_market_value": totals["total_market_value"],
                    "cash_balance": totals["cash_balance"],
                    "unrealized_pnl": 0.0,
                    "realized_pnl_to_date": 0.0,
                },
            )
        )
        session.execute(stmt)

    return {
        "date": snapshot_date.isoformat(),
        "positions": position_count,
        "accounts": len(account_totals),
    }


def capture_snapshot_from_loaded_positions(
    positions: list[Any],
    snapshot_utc_date: date | None = None,
) -> dict[str, Any]:
    snapshot_date = snapshot_utc_date or datetime.now(timezone.utc).date()

    with session_scope() as session:
        session.execute(delete(DailyPositionSnapshot).where(DailyPositionSnapshot.date == snapshot_date))
        session.execute(delete(DailyAccountSnapshot).where(DailyAccountSnapshot.date == snapshot_date))
        session.flush()
        return _write_snapshot_rows(session, list(positions), snapshot_date)


def capture_snapshot_from_positions(api: Any, snapshot_utc_date: date | None = None) -> dict[str, Any]:
    positions = load_portfolio_positions(
        api,
        include_fidelity=True,
        include_options=True,
        include_cash=True,
    )
    return capture_snapshot_from_loaded_positions(list(positions), snapshot_utc_date)


def get_latest_portfolio_total() -> tuple[str, float] | None:
    """Return (date_iso, total_market_value) for the most recent snapshot date in the DB, or None."""
    with session_scope() as session:
        row = session.execute(
            select(
                DailyAccountSnapshot.date,
                func.sum(DailyAccountSnapshot.total_market_value),
            )
            .group_by(DailyAccountSnapshot.date)
            .order_by(DailyAccountSnapshot.date.desc())
            .limit(1)
        ).one_or_none()
    if row is None:
        return None
    d, total = row
    return d.isoformat(), round(float(total or 0.0), 2)


def save_market_indicators(vix: float | None, sp500: float | None, gld: float | None = None) -> None:
    """Persist VIX, S&P 500, and GLD values with a UTC timestamp."""
    with session_scope() as session:
        session.add(
            MarketIndicatorSnapshot(
                timestamp=datetime.now(timezone.utc),
                vix=vix,
                sp500=sp500,
                gld=gld,
            )
        )


def get_last_market_indicators() -> tuple[float | None, float | None, float | None]:
    """Return (vix, sp500, gld) from the most recent saved row, or (None, None, None)."""
    with session_scope() as session:
        row = session.execute(
            select(MarketIndicatorSnapshot.vix, MarketIndicatorSnapshot.sp500, MarketIndicatorSnapshot.gld)
            .order_by(MarketIndicatorSnapshot.timestamp.desc())
            .limit(1)
        ).one_or_none()
    return (None, None, None) if row is None else (row.vix, row.sp500, row.gld)


def get_totals_payload(days: int = 365) -> dict[str, Any]:
    start_date = datetime.now(timezone.utc).date() - timedelta(days=max(1, int(days)) - 1)

    with session_scope() as session:
        portfolio_raw = session.execute(
            select(
                DailyAccountSnapshot.date,
                func.sum(DailyAccountSnapshot.total_market_value),
            )
            .where(DailyAccountSnapshot.date >= start_date)
            .group_by(DailyAccountSnapshot.date)
            .order_by(DailyAccountSnapshot.date.asc())
        ).all()

        account_raw = session.execute(
            select(
                DailyAccountSnapshot.date,
                Account.name,
                DailyAccountSnapshot.total_market_value,
                DailyAccountSnapshot.cash_balance,
            )
            .join(Account, Account.id == DailyAccountSnapshot.account_id)
            .where(DailyAccountSnapshot.date >= start_date)
            .order_by(DailyAccountSnapshot.date.asc(), Account.name.asc())
        ).all()

    portfolio_rows = [
        {"date": d.isoformat(), "total_market_value": round(float(v or 0.0), 2)}
        for d, v in portfolio_raw
    ]
    account_rows = [
        {
            "date": d.isoformat(),
            "account": name,
            "total_market_value": round(float(total or 0.0), 2),
            "cash_balance": round(float(cash or 0.0), 2),
        }
        for d, name, total, cash in account_raw
    ]

    account_series: dict[str, list[tuple[str, float]]] = {}
    for row in account_rows:
        account_series.setdefault(row["account"], []).append(
            (row["date"], float(row["total_market_value"]))
        )

    return {
        "portfolio_rows": portfolio_rows,
        "account_rows": account_rows,
        "account_series": account_series,
    }
