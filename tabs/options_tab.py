"""tabs/options_tab.py — Options Chain tab."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from nicegui import ui

# ---------------------------------------------------------------------------
# Module-level state (exclusively owned by this tab)
# ---------------------------------------------------------------------------
_raw_chain_data: dict[str, Any] | None = None
_pending_render_task: asyncio.Task[Any] | None = None
_chain_dte_min: int | None = None
_chain_dte_max: int | None = None
_chain_step_contracts: list[dict[str, Any]] = []
_chain_step_index: int = 0
_filtered_chain_data: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        text = str(value).strip()
        return int(float(text)) if text else None
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        text = str(value).strip()
        return float(text) if text else None
    except ValueError:
        return None


def _price_text(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{float(value):.2f}"
    return "-" if value is None else str(value)


def _dte_from_exp_key(exp_key: str) -> int | None:
    return (
        None if ":" not in exp_key else _coerce_int(exp_key.rsplit(":", 1)[-1])
    )


def _extract_chain_dte_values(chain: dict[str, Any]) -> list[int]:
    dte_values: list[int] = []
    for map_name in ("callExpDateMap", "putExpDateMap"):
        exp_map = chain.get(map_name, {}) or {}
        for exp_key, strikes in exp_map.items():
            if (exp_key_dte := _dte_from_exp_key(exp_key)) is not None:
                dte_values.append(exp_key_dte)
            for _, contracts in (strikes or {}).items():
                for contract in contracts or []:
                    dte = _coerce_int(contract.get("daysToExpiration"))
                    if dte is not None:
                        dte_values.append(dte)
    return dte_values


def _filter_chain_by_dte(chain: dict[str, Any], dte_limit: int) -> dict[str, Any]:
    filtered: dict[str, Any] = {
        key: value
        for key, value in chain.items()
        if key not in {"callExpDateMap", "putExpDateMap"}
    }
    for map_name in ("callExpDateMap", "putExpDateMap"):
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
                            dte := _coerce_int(contract.get("daysToExpiration"))
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


def build(panel_ref, fetch_chain_fn) -> None:
    """Build the Options tab UI.

    Args:
        panel_ref: The ui.tab widget returned by ui.tab("Options").
        fetch_chain_fn: Callable(symbol, contract_type) -> dict — from money.py.
    """
    global _raw_chain_data, _pending_render_task, _chain_dte_min
    global _chain_dte_max, _filtered_chain_data, _chain_step_contracts, _chain_step_index

    # Forward references filled in during UI construction
    _refs: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Inner helpers that reference UI widgets (defined as closures)
    # ------------------------------------------------------------------

    def _extract_step_contracts(chain: dict[str, Any]) -> list[dict[str, Any]]:
        contracts: list[dict[str, Any]] = []
        underlying = chain.get("underlying", {}) or {}
        underlying_price: float | None = None
        if isinstance(underlying, dict):
            underlying_last = underlying.get("last")
            if isinstance(underlying_last, (int, float)):
                underlying_price = float(underlying_last)
            else:
                try:
                    if underlying_last is not None:
                        underlying_price = float(str(underlying_last).strip())
                except ValueError:
                    underlying_price = None

        show_itm = bool(_refs["itm_cb"].value)
        show_ntm = bool(_refs["ntm_cb"].value)
        show_otm = bool(_refs["otm_cb"].value)

        def _matches_moneyness(contract: dict[str, Any]) -> bool:
            in_the_money = bool(contract.get("inTheMoney"))
            strike_value = contract.get("strikePrice")
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
            return True if show_otm and not in_the_money else show_ntm and is_ntm

        for map_name in ("callExpDateMap", "putExpDateMap"):
            exp_map = chain.get(map_name, {}) or {}
            for exp_key, strikes in exp_map.items():
                exp_key_dte = _dte_from_exp_key(exp_key)
                for _, contract_list in (strikes or {}).items():
                    for contract in contract_list or []:
                        if not _matches_moneyness(contract):
                            continue
                        contract_dte = _coerce_int(contract.get("daysToExpiration"))
                        dte = contract_dte if contract_dte is not None else exp_key_dte
                        if dte is None:
                            continue
                        contracts.append({
                            "description": str(
                                contract.get("description") or contract.get("symbol") or "-"
                            ),
                            "bid": contract.get("bid") or contract.get("bidPrice"),
                            "ask": contract.get("ask") or contract.get("askPrice"),
                            "last": contract.get("last") or contract.get("lastPrice"),
                            "mark": (
                                contract.get("mark") or contract.get("markPrice")
                                or contract.get("last") or contract.get("lastPrice")
                            ),
                            "dte": dte,
                            "symbol": str(contract.get("symbol") or ""),
                            "inTheMoney": bool(contract.get("inTheMoney")),
                            "strikePrice": contract.get("strikePrice"),
                        })

        contracts.sort(key=lambda row: (int(row.get("dte") or 0), str(row.get("symbol") or "")))
        return contracts

    def _update_display() -> None:
        global _chain_step_contracts, _chain_step_index
        if not _chain_step_contracts:
            _refs["pos_label"].text = "0 / 0"
            _refs["desc_value"].text = "-"
            _refs["bid_value"].text = "-"
            _refs["ask_value"].text = "-"
            _refs["last_value"].text = "-"
            _refs["mark_value"].text = "-"
            _refs["dte_value"].text = "-"
            _refs["premium_value"].text = "-"
            _refs["annualized_value"].text = "-"
            _refs["up_button"].disable()
            _refs["down_button"].disable()
            return

        contract = _chain_step_contracts[_chain_step_index]
        _refs["pos_label"].text = f"{_chain_step_index + 1} / {len(_chain_step_contracts)}"
        symbol = str(contract.get("symbol") or "-")
        dte = _coerce_int(contract.get("dte"))
        dte_text = str(dte) if dte is not None else "-"
        _refs["desc_value"].text = symbol
        _refs["dte_value"].text = dte_text
        _refs["bid_value"].text = _price_text(contract.get("bid"))
        _refs["ask_value"].text = _price_text(contract.get("ask"))
        _refs["last_value"].text = _price_text(contract.get("last"))
        _refs["mark_value"].text = _price_text(contract.get("mark"))

        mark = _coerce_float(contract.get("mark"))
        strike = _coerce_float(contract.get("strikePrice"))
        premium_percent: float | None = None
        annualized_percent: float | None = None
        if mark is not None and strike and strike > 0:
            premium_percent = (mark / strike) * 100.0
        if premium_percent is not None and dte and dte > 0:
            annualized_percent = premium_percent * (364.0 / dte)

        _refs["premium_value"].text = f"{premium_percent:.2f}%" if premium_percent is not None else "-"
        _refs["annualized_value"].text = f"{annualized_percent:.2f}%" if annualized_percent is not None else "-"

        if _chain_step_index <= 0:
            _refs["up_button"].disable()
        else:
            _refs["up_button"].enable()

        if _chain_step_index >= len(_chain_step_contracts) - 1:
            _refs["down_button"].disable()
        else:
            _refs["down_button"].enable()

    def _set_step_contracts(chain: dict[str, Any]) -> None:
        global _chain_step_contracts, _chain_step_index, _filtered_chain_data
        _filtered_chain_data = chain
        _chain_step_contracts = _extract_step_contracts(chain)
        _chain_step_index = 0
        _update_display()

    def on_filter_change(_: Any = None) -> None:
        if _filtered_chain_data is None:
            _set_step_contracts({})
            return
        _set_step_contracts(_filtered_chain_data)

    def on_step_up() -> None:
        global _chain_step_index
        if _chain_step_index <= 0:
            return
        _chain_step_index -= 1
        _update_display()

    def on_step_down() -> None:
        global _chain_step_index
        if _chain_step_index >= len(_chain_step_contracts) - 1:
            return
        _chain_step_index += 1
        _update_display()

    async def _render_filtered() -> None:
        if _raw_chain_data is None:
            return
        dte_limit = _coerce_int(_refs["dte_input"].value)
        if dte_limit is None:
            return
        if _chain_dte_min is not None and dte_limit < _chain_dte_min:
            _refs["chain_output"].value = f"DTE must be >= {_chain_dte_min} and <= {_chain_dte_max}"
            return
        if _chain_dte_max is not None and dte_limit > _chain_dte_max:
            _refs["chain_output"].value = f"DTE must be >= {_chain_dte_min} and <= {_chain_dte_max}"
            return
        _refs["dte_label"].text = f"DTE <= {dte_limit}"
        filtered = await asyncio.to_thread(_filter_chain_by_dte, _raw_chain_data, dte_limit)
        _set_step_contracts(filtered)
        _refs["chain_output"].value = await asyncio.to_thread(lambda: json.dumps(filtered, indent=2))

    def schedule_render() -> None:
        global _pending_render_task
        if _pending_render_task is not None and not _pending_render_task.done():
            _pending_render_task.cancel()

        async def _debounced() -> None:
            try:
                await asyncio.sleep(0.12)
                await _render_filtered()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _refs["chain_output"].value = f"Chain render error: {exc}"

        _pending_render_task = asyncio.create_task(_debounced())

    def on_dte_change(_: Any = None) -> None:
        value = _coerce_int(_refs["dte_input"].value)
        if value is None:
            _refs["dte_label"].text = "Enter a valid integer DTE"
            return
        if (
            _chain_dte_min is not None and _chain_dte_max is not None
            and (value < _chain_dte_min or value > _chain_dte_max)
        ):
            _refs["dte_label"].text = f"Enter {_chain_dte_min}..{_chain_dte_max}"
            return
        schedule_render()

    async def get_chain_click() -> None:
        global _raw_chain_data, _pending_render_task, _chain_dte_min, _chain_dte_max, _filtered_chain_data
        symbol = _refs["symbol_input"].value.strip()
        if not symbol:
            ui.notify("Enter a ticker symbol first", color="warning")
            return

        contract_type = _refs["contract_type"].value or "ALL"
        _refs["chain_output"].value = "Loading..."
        try:
            chain = await asyncio.to_thread(fetch_chain_fn, symbol, contract_type)
            if not isinstance(chain, dict):
                _raw_chain_data = None
                _filtered_chain_data = None
                _chain_dte_min = None
                _chain_dte_max = None
                _refs["dte_label"].text = "DTE <= -"
                _set_step_contracts({})
                _refs["chain_output"].value = f"Chain error: unexpected response type {type(chain).__name__}"
                return

            _raw_chain_data = chain
            dte_values = _extract_chain_dte_values(chain)
            if dte_values:
                _chain_dte_min = min(dte_values)
                _chain_dte_max = max(dte_values)
                _refs["dte_input"].value = str(_chain_dte_max)
                _refs["dte_label"].text = f"DTE <= {_chain_dte_max}"
                schedule_render()
            else:
                _chain_dte_min = None
                _chain_dte_max = None
                _refs["dte_input"].value = "365"
                _refs["dte_label"].text = "DTE <= -"
                _set_step_contracts(chain)
                _refs["chain_output"].value = json.dumps(chain, indent=2)
        except Exception as exc:
            _raw_chain_data = None
            _filtered_chain_data = None
            _chain_dte_min = None
            _chain_dte_max = None
            if _pending_render_task is not None and not _pending_render_task.done():
                _pending_render_task.cancel()
            _refs["dte_input"].value = "365"
            _refs["dte_label"].text = "DTE <= -"
            _set_step_contracts({})
            _refs["chain_output"].value = f"Chain error: {exc}"

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    with ui.tab_panel(panel_ref):
        with ui.row().classes("w-full items-start gap-4 no-wrap"):
            with ui.column().classes("w-1/3 min-w-[320px]"):
                with ui.card().classes("w-full"):
                    ui.label("Options Chain Controls").classes("text-xl font-semibold")
                    _refs["symbol_input"] = ui.input("Symbol").props('clearable spellcheck=false').classes("w-40")
                    _refs["contract_type"] = ui.select(
                        options=["ALL", "CALL", "PUT"],
                        value="ALL",
                        label="Contract Type",
                    ).classes("w-40")
                    _refs["dte_input"] = (
                        ui.input(label="DTE", value="365", on_change=on_dte_change)
                        .props("type=number")
                        .classes("w-40")
                    )
                    _refs["dte_label"] = ui.label("DTE <= -").classes("text-xs")
                    ui.button("Get Chain", on_click=get_chain_click)

                with ui.card().classes("w-full"):
                    ui.label("Chain Step").classes("text-lg font-semibold")
                    with ui.row().classes("items-center gap-4"):
                        _refs["itm_cb"] = ui.checkbox("ITM", value=True, on_change=on_filter_change)
                        _refs["ntm_cb"] = ui.checkbox("NTM", value=True, on_change=on_filter_change)
                        _refs["otm_cb"] = ui.checkbox("OTM", value=True, on_change=on_filter_change)
                    with ui.row().classes("items-center gap-2"):
                        _refs["up_button"] = ui.button("Up", on_click=on_step_up)
                        _refs["down_button"] = ui.button("Down", on_click=on_step_down)
                        _refs["pos_label"] = ui.label("0 / 0").classes("text-sm")

                    with ui.row().classes("items-center gap-2"):
                        ui.label("Description:")
                        _refs["desc_value"] = ui.label("-").classes("font-semibold")
                    with ui.row().classes("items-center gap-4"):
                        ui.label("Bid:")
                        _refs["bid_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Ask:")
                        _refs["ask_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Last:")
                        _refs["last_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Mark:")
                        _refs["mark_value"] = ui.label("-").classes("font-semibold")
                    with ui.row().classes("items-center gap-4"):
                        ui.label("DTE:")
                        _refs["dte_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Premium %:")
                        _refs["premium_value"] = ui.label("-").classes("font-semibold")
                        ui.label("Annualized %:")
                        _refs["annualized_value"] = ui.label("-").classes("font-semibold")

                    _refs["up_button"].disable()
                    _refs["down_button"].disable()

            with ui.column().classes("flex-1 min-w-0"):
                with ui.card().classes("w-full"):
                    ui.label("Options Chain Display").classes("text-xl font-semibold")
                    _refs["chain_output"] = ui.textarea(label="Chain JSON")
                    _refs["chain_output"].props("readonly").classes("w-full")
                    _refs["chain_output"].style("height: 75vh;")
