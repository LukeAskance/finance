# Money

`money.py` is a NiceGUI-based portfolio application for Schwab (+ optional Fidelity CSV import) with tabs for:

- Portfolio loading, display, aggregate/unaggregate
- Quotes and options chain exploration
- Historical price plotting in-app
- Income analysis (single-symbol dividend history/forecast + portfolio income projection)
- Portfolio analysis with local and LLM-backed Q&A

## Main Functionality (`money.py`)

- **Dashboard**: quick app controls (including exit).
- **Portfolio**:
	- load normalized portfolio positions (`positions.load_portfolio_positions`)
	- sortable table with symbol/type/account/qty/value/P&L
	- aggregate and unaggregate views.
- **Options**:
	- fetch options chains
	- filter by DTE
	- step through contracts with ITM/NTM/OTM filters.
- **Historicals**:
	- plot one or more ticker symbols directly inside the tab
	- days control
	- normalize/denormalize mode with auto-refresh.
- **Income**:
	- symbol-level dividend history + scenario forecast (bear/base/bull) using `dividend_prediction.py`
	- portfolio-level projected income for next 30/90/365 days from loaded position dividend fields.
- **Analysis**:
	- snapshot refresh + rule-based Q&A
	- optional LLM Q&A with provider/model controls.

## Requirements

- Python 3.12+
- A configured Schwab token/client setup (`schwabdev` + `.env` values)
- Optional: Fidelity CSV exports in `~/Downloads` for Fidelity position ingestion

## Environment Variables

Create a `.env` file in the project root (or ensure these vars are exported):

- `SCHWAB_APP_KEY`
- `SCHWAB_SECRET`
- `callback_url`
- `token_filename`
- `ANTHROPIC_API_KEY` (optional, Analysis tab)
- `PERPLEXITY_API_KEY` (optional, Analysis tab)

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Run

```bash
source .venv/bin/activate
./money.py
```

If `money.py` is not executable on your machine:

```bash
python money.py
```

The UI runs on port `8000` by default.

## Notes

- Historical/Income plotting uses Matplotlib rendered inside NiceGUI (`ui.pyplot`).
- Symbol dividend forecasting relies on market dividend history availability (via `yfinance`).
- Portfolio income projection uses current position dividend metadata and estimated payment cadence from `div_freq`.
