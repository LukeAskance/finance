from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

import yfinance as yf

import claude_client
from institutional import get_institutional_ownership
from positions import load_portfolio_positions


# ---------------------------------------------------------------------------
# Tool definitions exposed to the Claude tool-use API
# ---------------------------------------------------------------------------
_CLAUDE_TOOLS: list[dict] = [
    {
        "name": "get_institutional_ownership",
        "description": (
            "Get the top 10 institutional holders for a stock ticker, sorted by "
            "percentage of shares outstanding. Use this when the user asks about "
            "who owns a stock, institutional holders, major shareholders, or fund "
            "ownership of any company."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The stock ticker symbol, e.g. 'AAPL' or 'MSFT'.",
                }
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_market_cap",
        "description": (
            "Get the current market capitalisation for a stock ticker. "
            "Use this when the user asks about market cap, company size, "
            "large-cap vs small-cap classification, or wants to compare "
            "the size of companies in the portfolio."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The stock ticker symbol, e.g. 'AAPL' or 'WPC'.",
                }
            },
            "required": ["ticker"],
        },
    },
    {
        "name": "get_price_history",
        "description": (
            "Get recent closing price history for a stock ticker. "
            "Use this when the user asks about price performance, returns, "
            "how a stock has moved, 52-week high/low, or trend over time."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "ticker": {
                    "type": "string",
                    "description": "The stock ticker symbol, e.g. 'AAPL'.",
                },
                "days": {
                    "type": "integer",
                    "description": "Number of calendar days of history to return (default 90).",
                },
            },
            "required": ["ticker"],
        },
    },
]


@dataclass(slots=True)
class PortfolioRecord:
    symbol: str
    account: str
    position_type: str
    quantity: float
    market_value: float
    last_price: float
    description: str
    sector: str | None = None
    industry: str | None = None
    classification_status: str = "missing"


class PortfolioAnalysisEngine:
    def __init__(self) -> None:
        self._snapshot: list[PortfolioRecord] = []
        self._as_of: datetime | None = None
        self._class_cache: dict[str, tuple[str | None, str | None, str]] = {}

    @property
    def as_of(self) -> datetime | None:
        return self._as_of

    @property
    def snapshot(self) -> list[PortfolioRecord]:
        return self._snapshot

    def refresh_snapshot_from_positions(self, positions: list[Any]) -> int:
        # Move the existing body of refresh_snapshot here, starting from the
        # point after positions have already been loaded.
        loaded_positions = list(positions)

        records: list[PortfolioRecord] = []
        records.extend(
            PortfolioRecord(
                symbol=str(p.symbol),
                account=str(p.account_name),
                position_type=str(p.position_type),
                quantity=float(p.quantity),
                market_value=float(p.market_value),
                last_price=float(p.last_price),
                description=str(p.description),
            )
            for p in loaded_positions
        )

        # ! if aggregate_by_symbol:
        records = self._aggregate_records(records)

        self._snapshot = records
        self._as_of = datetime.now()
        return len(records)

    def refresh_snapshot(
        self,
        api: Any,
        include_fidelity: bool = True,
        include_options: bool = True,
        include_cash: bool = True,
        positions: list[Any] | None = None,
    ) -> int:
        loaded_positions = positions
        if loaded_positions is None:
            loaded_positions = load_portfolio_positions(
                api,
                include_fidelity=include_fidelity,
                include_options=include_options,
                include_cash=include_cash,
            )
        return self.refresh_snapshot_from_positions(list(loaded_positions))

    def _aggregate_records(
        self,
        records: list[PortfolioRecord],
    ) -> list[PortfolioRecord]:
        grouped: dict[tuple[str, str], PortfolioRecord] = {}

        for rec in records:
            key = (rec.symbol, rec.position_type)
            existing = grouped.get(key)
            if existing is None:
                grouped[key] = PortfolioRecord(
                    symbol=rec.symbol,
                    account="Aggregated",
                    position_type=rec.position_type,
                    quantity=rec.quantity,
                    market_value=rec.market_value,
                    last_price=rec.last_price,
                    description=rec.description,
                )
                continue

            existing.quantity += rec.quantity
            existing.market_value += rec.market_value
            if existing.quantity:
                existing.last_price = existing.market_value / existing.quantity

        aggregated = list(grouped.values())
        aggregated.sort(
            key=lambda rec: abs(rec.market_value),
            reverse=True,
        )
        return aggregated

    def _is_equity_like(self, rec: PortfolioRecord) -> bool:
        return rec.position_type in {
            "EQUITY",
            "MUTUAL_FUND",
            "COLLECTIVE_INVESTMENT",
        }

    def _classify_symbols(self, symbols: list[str]) -> None:
        """Fetch sector/industry for *symbols* concurrently and cache them."""
        missing = sorted({s for s in symbols if s not in self._class_cache})
        if not missing:
            return

        def fetch(symbol: str) -> tuple[str, tuple[str | None, str | None, str]]:
            try:
                info = yf.Ticker(symbol).info
                sector = info.get("sector")
                industry = info.get("industry")
                status = "ok" if sector or industry else "missing"
                return symbol, (
                    str(sector) if sector else None,
                    str(industry) if industry else None,
                    status,
                )
            except Exception:
                return symbol, (None, None, "error")

        with ThreadPoolExecutor(max_workers=min(8, len(missing))) as pool:
            for symbol, result in pool.map(fetch, missing):
                self._class_cache[symbol] = result

    def _enrich_classification(self) -> None:
        pending = [
            rec
            for rec in self._snapshot
            if self._is_equity_like(rec) and not (rec.sector or rec.industry)
        ]
        if not pending:
            return
        self._classify_symbols([rec.symbol for rec in pending])
        for rec in pending:
            sector, industry, status = self._class_cache.get(
                rec.symbol, (None, None, "error")
            )
            rec.sector = sector
            rec.industry = industry
            rec.classification_status = status

    def _rows(self, rows: list[PortfolioRecord]) -> list[dict[str, Any]]:
        return [
            {
                "symbol": r.symbol,
                "account": r.account,
                "type": r.position_type,
                "quantity": round(r.quantity, 4),
                "market_value": round(r.market_value, 2),
                "sector": r.sector or "-",
                "industry": r.industry or "-",
            }
            for r in rows
        ]

    def _apply_filters(
        self,
        rows: list[PortfolioRecord],
        question: str,
    ) -> list[PortfolioRecord]:
        q = question.lower()
        filtered = list(rows)

        more_than = re.search(
            r"(more than|over|greater than)\s+([0-9]+(?:\.[0-9]+)?)\s+shares",
            q,
        )
        less_than = re.search(
            r"(less than|under|fewer than)\s+([0-9]+(?:\.[0-9]+)?)\s+shares",
            q,
        )
        at_least = re.search(
            r"(at least|>=)\s+([0-9]+(?:\.[0-9]+)?)\s+shares",
            q,
        )

        if more_than:
            threshold = float(more_than.group(2))
            filtered = [r for r in filtered if r.quantity > threshold]
        if less_than:
            threshold = float(less_than.group(2))
            filtered = [r for r in filtered if r.quantity < threshold]
        if at_least:
            threshold = float(at_least.group(2))
            filtered = [r for r in filtered if r.quantity >= threshold]

        mv_more = re.search(
            r"(market value|value)\s+(more than|over|greater than)\s+\$?([0-9]+(?:\.[0-9]+)?)",
            q,
        )
        mv_less = re.search(
            r"(market value|value)\s+(less than|under|below)\s+\$?([0-9]+(?:\.[0-9]+)?)",
            q,
        )
        if mv_more:
            threshold = float(mv_more.group(3))
            filtered = [r for r in filtered if r.market_value > threshold]
        if mv_less:
            threshold = float(mv_less.group(3))
            filtered = [r for r in filtered if r.market_value < threshold]

        if "option" in q:
            filtered = [
                r
                for r in filtered
                if r.position_type in {"OPTION", "CALL", "PUT"}
            ]
        if "cash" in q:
            if "exclude cash" in q or "without cash" in q or "non-cash" in q:
                filtered = [r for r in filtered if r.position_type != "Cash"]
            else:
                filtered = [r for r in filtered if r.position_type == "Cash"]
        if "equity" in q or "stock" in q:
            filtered = [
                r
                for r in filtered
                if r.position_type
                in {"EQUITY", "MUTUAL_FUND", "COLLECTIVE_INVESTMENT"}
            ]

        if account_match := re.search(r"\bin\s+account\s+([a-z0-9_ -]+)", q):
            account = account_match.group(1).strip().lower()
            filtered = [r for r in filtered if account in r.account.lower()]

        industry_match = re.search(r"in\s+the\s+(.+?)\s+industry", q)
        sector_match = re.search(r"in\s+the\s+(.+?)\s+sector", q)
        if industry_match or sector_match or "industry" in q or "sector" in q:
            self._enrich_classification()
            target = (
                industry_match.group(1).strip()
                if industry_match
                else sector_match.group(1).strip()
                if sector_match
                else ""
            )
            if target:
                filtered = [
                    r
                    for r in filtered
                    if (
                        (r.industry and target in r.industry.lower())
                        or (r.sector and target in r.sector.lower())
                    )
                ]
            elif "energy" in q:
                filtered = [
                    r
                    for r in filtered
                    if (
                        (r.industry and "energy" in r.industry.lower())
                        or (r.sector and "energy" in r.sector.lower())
                    )
                ]

        return filtered

    def _summarize_filter_result(
        self,
        rows: list[PortfolioRecord],
        question: str,
    ) -> str:
        q = question.lower()
        if "largest" in q or "top" in q:
            return f"Showing {len(rows)} top positions by market value."
        if "industry" in q or "sector" in q:
            return f"{len(rows)} positions match requested industry/sector filters."
        if "shares" in q:
            return f"{len(rows)} positions match requested share filters."
        if "value" in q or "market value" in q:
            return (
                f"{len(rows)} positions match requested market value filters."
            )
        return f"{len(rows)} positions match your filters."

    def _snapshot_payload(self, limit: int = 120) -> dict[str, Any]:
        rows = self._rows(self._snapshot[:limit])
        return {
            "as_of": self._as_of.strftime("%Y-%m-%d %H:%M:%S")
            if self._as_of
            else None,
            "count": len(self._snapshot),
            "rows": rows,
        }

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def _execute_tool(self, name: str, tool_input: dict) -> Any:
        if name == "get_institutional_ownership":
            return get_institutional_ownership(tool_input.get("ticker", ""))
        if name == "get_market_cap":
            import datetime
            ticker = tool_input.get("ticker", "").upper()
            info = yf.Ticker(ticker).info
            if not (market_cap := info.get("marketCap")):
                return {"error": f"No market cap data available for {ticker}"}
            if market_cap >= 1_000_000_000_000:
                market_cap_str = f"${market_cap / 1_000_000_000_000:.2f}T"
            elif market_cap >= 1_000_000_000:
                market_cap_str = f"${market_cap / 1_000_000_000:.2f}B"
            else:
                market_cap_str = f"${market_cap / 1_000_000:.2f}M"
            return {
                "symbol": ticker,
                "date": datetime.date.today().isoformat(),
                "market_cap": market_cap,
                "market_cap_str": market_cap_str,
            }
        if name == "get_price_history":
            import datetime
            ticker = tool_input.get("ticker", "").upper()
            days = int(tool_input.get("days", 90))
            end = datetime.date.today()
            start = end - datetime.timedelta(days=days)
            hist = yf.Ticker(ticker).history(
                start=start.isoformat(), end=end.isoformat()
            )
            if hist.empty:
                return {"error": f"No price history returned for {ticker}"}
            return [
                {"date": str(d)[:10], "close": round(float(row["Close"]), 4)}
                for d, row in hist.iterrows()
            ]
        return {"error": f"Unknown tool: {name}"}

    # ------------------------------------------------------------------
    # Claude with tool-use (multi-turn loop)
    # ------------------------------------------------------------------

    def _call_claude_with_tools(
        self,
        question: str,
        model: str,
        grounded_only: bool,
        general_mode: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        """Call Claude with tool-use support.

        Returns (answer_text, tool_results) where tool_results maps
        tool name → raw result data (e.g. {"get_institutional_ownership": [...]}).
        """
        snapshot = self._snapshot_payload()

        if general_mode:
            system = (
                "You are a helpful financially aware assistant. You have access "
                "to the user's portfolio data for context when relevant. "
                "Be concise and include concrete symbols/values when available."
            )
        else:
            grounding = (
                "Answer ONLY using the provided portfolio snapshot data and any "
                "tool results. If the answer is not available, say so explicitly."
                if grounded_only
                else "You may answer generally, but prioritize the provided "
                "portfolio snapshot and any tool results when relevant."
            )
            system = (
                "You are a portfolio analysis assistant. "
                f"{grounding} "
                "Be concise and include concrete symbols/values when available."
            )

        user_text = (
            f"Here is my current portfolio for reference:\n"
            f"{json.dumps(snapshot, indent=2)}\n\n{question}"
            if general_mode
            else f"Portfolio snapshot JSON:\n"
            f"{json.dumps(snapshot, indent=2)}\n\nQuestion: {question}"
        )

        try:
            result = claude_client.run_tool_loop(
                [{"role": "user", "content": user_text}],
                system=system,
                tools=_CLAUDE_TOOLS,
                execute_tool=self._execute_tool,
                model=model,
            )
        except Exception as exc:
            return f"Claude API call failed: {exc}", {}

        return result.text, result.tool_results

    def _call_perplexity(
        self,
        question: str,
        model: str,
        grounded_only: bool,
        general_mode: bool = False,
    ) -> str:
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            return "Missing PERPLEXITY_API_KEY in environment."

        snapshot = self._snapshot_payload()

        if general_mode:
            system_text = (
                "You are a helpful assistant. You have access to the user's "
                "portfolio data for context when relevant. "
                "Be concise and include concrete symbols/values when available."
            )
        else:
            grounding = (
                "Answer ONLY using the provided portfolio snapshot data. "
                "If unavailable in that data, state that clearly."
                if grounded_only
                else "You may answer generally, but prioritize the "
                     "provided portfolio snapshot when relevant."
            )
            system_text = (
                "You are a portfolio analysis assistant. "
                f"{grounding} "
                "Be concise and include concrete symbols/values when available."
            )

        user_text = (
            f"Here is my current portfolio for reference:\n{json.dumps(snapshot, indent=2)}\n\n{question}"
            if general_mode
            else f"Portfolio snapshot JSON:\n{json.dumps(snapshot, indent=2)}\n\nQuestion: {question}"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0.1,
        }

        req = urlrequest.Request(
            url="https://api.perplexity.ai/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "content-type": "application/json",
                "authorization": f"Bearer {api_key}",
            },
            method="POST",
        )

        try:
            with urlrequest.urlopen(req, timeout=45) as response:
                body = json.loads(response.read().decode("utf-8"))
                choices = body.get("choices") or []
                if choices and isinstance(choices[0], dict):
                    message = choices[0].get("message") or {}
                    if content := message.get("content"):
                        return str(content)
                return str(body)
        except urlerror.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            return f"Perplexity API error ({exc.code}): {detail}"
        except Exception as exc:
            return f"Perplexity API call failed: {exc}"

    def ask_llm(
        self,
        question: str,
        provider: str = "claude",
        model: str = "",
        grounded_only: bool = True,
        general_mode: bool = False,
    ) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
        q = question.strip()
        if not q:
            return "Enter a question.", [], {}

        if not self._snapshot:
            return "Load a portfolio snapshot first.", [], {}

        tool_results: dict[str, Any] = {}
        provider_key = provider.strip().lower()
        if provider_key == "perplexity":
            use_model = model.strip() or "sonar"
            answer = self._call_perplexity(
                q, use_model, grounded_only, general_mode
            )
        else:
            use_model = model.strip() or claude_client.DEFAULT_MODEL
            answer, tool_results = self._call_claude_with_tools(
                q, use_model, grounded_only, general_mode
            )

        evidence_rows = self._rows(self._snapshot[:40]) if grounded_only else []
        return answer, evidence_rows, tool_results

    def answer_question(
        self, question: str
    ) -> tuple[str, list[dict[str, Any]]]:
        if not self._snapshot:
            return "Load a portfolio snapshot first.", []

        q = question.strip().lower()
        if not q:
            return "Enter a question.", []

        top_match = re.search(r"top\s+([0-9]+)", q)
        if "largest" in q or top_match:
            count = int(top_match.group(1)) if top_match else 5
            base = self._apply_filters(self._snapshot, question)
            base.sort(key=lambda r: abs(r.market_value), reverse=True)
            rows = base[:count]
            return self._summarize_filter_result(rows, question), self._rows(
                rows
            )

        filtered = self._apply_filters(self._snapshot, question)
        if filtered != self._snapshot:
            return (
                self._summarize_filter_result(filtered, question),
                self._rows(filtered),
            )

        return (
            "Question pattern not recognized yet. Try examples: "
            '"top 5 largest positions", '
            '"options with market value over 1000", '
            '"equity positions in the energy industry", or '
            '"positions with more than 100 shares".',
            [],
        )
