import os

from dotenv import load_dotenv

from pyfredapi import get_series, get_series_info  # type: ignore[attr-defined]
import c

import matplotlib.pyplot as plt
import pandas as pd

# Load environment variables from .env file
load_dotenv()

os.environ['FRED_API_KEY'] = str(os.getenv("FRED_API_KEY"))


# ---------------------------------------------------------------------------
# Utility plot functions
# ---------------------------------------------------------------------------

def plot_time_series_with_changes(df, start_date, title="Time Series Plot", figsize=(8, 8)):
    """
    Create three subplots: raw value, month-over-month %, and year-over-year %.
    Returns list of (date_str, value, month_pct_change, year_pct_change) tuples.
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)

    mask = df['date'] >= start_date
    filtered_df = df[mask].copy()

    filtered_df['month_pct_change'] = filtered_df['value'].pct_change() * 100
    filtered_df['year_pct_change'] = filtered_df['value'].pct_change(periods=12) * 100

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=figsize, height_ratios=[1.5, 1, 1])

    ax1.plot(filtered_df['date'], filtered_df['value'], marker='o')
    ax1.set_title(title)
    ax1.set_ylabel('Value')
    ax1.tick_params(axis='x', rotation=45)
    ax1.grid(True, linestyle='--', alpha=0.7)

    ax2.plot(filtered_df['date'][1:], filtered_df['month_pct_change'][1:],
             marker='o', color='blue', linestyle='-')
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax2.set_title('Month-over-Month Percentage Change')
    ax2.set_ylabel('MoM Change (%)')
    ax2.tick_params(axis='x', rotation=45)
    ax2.grid(True, linestyle='--', alpha=0.7)

    ax3.plot(filtered_df['date'][12:], filtered_df['year_pct_change'][12:],
             marker='o', color='green', linestyle='-')
    ax3.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    ax3.set_title('Year-over-Year Percentage Change')
    ax3.set_xlabel('Date')
    ax3.set_ylabel('YoY Change (%)')
    ax3.tick_params(axis='x', rotation=45)
    ax3.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.show()

    return [
        (
            row['date'].strftime('%Y-%m-%d'),
            row['value'],
            None if pd.isna(row['month_pct_change']) else row['month_pct_change'],
            None if pd.isna(row['year_pct_change']) else row['year_pct_change'],
        )
        for _, row in filtered_df.iterrows()
    ]


def plot_dual_dataframes(df1, df2, title1, title2, months_to_plot=60):
    """
    Plot two series with separate y-axes over a trailing time window.
    """
    def _set_axis(color, ax, title, data_frame):
        ax.set_ylabel(title, color=color)
        ax.plot(data_frame['date'], data_frame['value'], color=color)
        ax.tick_params(axis='y', labelcolor=color)

    end_date = df1['date'].max()
    start_date = end_date - pd.DateOffset(months=months_to_plot)

    df1_filtered = df1[df1['date'] >= start_date]
    df2_filtered = df2[df2['date'] >= start_date]

    fig, ax1 = plt.subplots(figsize=(12, 6))
    _set_axis('tab:blue', ax1, title1, df1_filtered)

    ax2 = ax1.twinx()
    _set_axis('tab:orange', ax2, title2, df2_filtered)

    ax1.grid(True, which='both', axis='both', linestyle='-', alpha=0.2)
    plt.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.show()

    return fig


def plot_quad_dataframes(df1, df2, df3, df4, title1, title2, title3, title4, months_to_plot):
    """
    Plot four series with separate y-axes over a trailing time window.
    """
    end_date = df1['date'].max()
    start_date = end_date - pd.DateOffset(months=months_to_plot - 1)
    filtered_dfs = [df[df['date'] >= start_date] for df in [df1, df2, df3, df4]]

    fig, ax1 = plt.subplots(figsize=(15, 10))
    ax2 = ax1.twinx()
    ax3 = ax1.twinx()
    ax4 = ax1.twinx()

    ax3.spines['right'].set_position(('outward', 60))
    ax4.spines['right'].set_position(('outward', 120))

    colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red']
    axes = [ax1, ax2, ax3, ax4]
    titles = [title1, title2, title3, title4]

    for ax, df, color, title in zip(axes, filtered_dfs, colors, titles):
        ax.plot(df['date'], df['value'], color=color)
        ax.set_ylabel(title, color=color)
        ax.tick_params(axis='y', labelcolor=color)

    ax1.grid(True, which='both', axis='both', linestyle='-', alpha=0.2)
    plt.tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.show()

    return fig


# ---------------------------------------------------------------------------
# Individual plot routines
# ---------------------------------------------------------------------------
# FRED series quick reference:
#   M2REAL              Real M2 Money Supply
#   M2V                 M2 Velocity
#   M2                  M2 Money Supply
#   GDP                 Gross Domestic Product
#   W068RCQ027SBEA      Government Total Expenditures
#   FYFSGDA188S         Federal Surplus/Deficit as % of GDP
#   A191RP1Q027SBEA     GDP % Change (SAAR)
#   DFII10              10-Year Market Yield (inflation-indexed)
#   EXPINF10YR          10-Year Expected Inflation
#   EXPINF1YR           1-Year Expected Inflation
#   A091RC1Q027SBEA     US Government Interest Payments
#   FDEFX               National Defense Spending
#   DEXUSEU             Euro/USD Exchange Rate
#   MEDCPIM158SFRBCLE   Median CPI
#   CPIAUCSL            CPI All Urban Consumers
#   CUSR0000SAF11       CPI Food at Home
#   CUSR0000SEFV        CPI Food Away from Home
#   CPIENGSL            CPI Energy
#   CUSR0000SASLE       CPI Services (less energy)
#   CUSR0000SAH1        CPI Shelter
#   GFDEGDQ188S         Total US Debt as % of GDP
#   GFDEBTN             Total US Debt
#   IRLTLT01USM156N     10-Year US Govt Bond Yields
#   CORESTICKM159SFRBATL  Core Sticky CPI
#   UMCSENT             Consumer Sentiment
#   PSAVERT             Personal Savings Rate
#   UNRATE              US Unemployment Rate
#   CAUR                California Unemployment Rate
#   TXUR                Texas Unemployment Rate
#   LNS14000024         Unemployment Rate Age 20+
#   LNS14027660         Unemployment Rate Age 25, HS diploma
#   BSCICP03USM665S     US Business Cycle Indicator
#   AHETPI              Avg Hourly Earnings - Production/Nonsupervisory
#   AHEMAN              Avg Hourly Earnings - All Employees
#   GDPNOW              GDP Now (Atlanta Fed)


def plot_m2_vs_govt():
    """Real M2 Money Supply vs Government Expenditures"""
    m2 = get_series("M2REAL")
    govt = get_series("W068RCQ027SBEA")
    plot_dual_dataframes(m2, govt, 'M2 Real', 'Govt Expenditures')


def plot_total_debt():
    """Total US Debt with MoM and YoY changes"""
    debt = get_series("GFDEBTN")
    plot_time_series_with_changes(debt, "2015-01-01", title="Total US Debt")


def plot_gdpnow_vs_sentiment():
    """GDP Now (Atlanta Fed) vs Consumer Sentiment (36 months)"""
    gdpnow = get_series("GDPNOW")
    sentiment = get_series("UMCSENT")
    plot_dual_dataframes(gdpnow, sentiment, 'GDP Now (Atlanta Fed)', 'Consumer Sentiment', months_to_plot=36)


def plot_market_yield_vs_cpi():
    """10-Year Market Yield vs CPI All Urban Consumers"""
    yield10 = get_series("DFII10")
    cpi = get_series("CPIAUCSL")
    plot_dual_dataframes(yield10, cpi, "10-Year Market Yield", 'CPI All Urban Consumers')


def plot_market_yield_vs_expected_inflation():
    """10-Year Market Yield vs 10-Year Expected Inflation"""
    yield10 = get_series("DFII10")
    exp_inf = get_series("EXPINF10YR")
    plot_dual_dataframes(yield10, exp_inf, "10-Year Market Yield", '10-Year Expected Inflation')


def plot_hourly_earnings_production_vs_all():
    """Avg Hourly Earnings: Production/Nonsupervisory vs All Employees"""
    prod = get_series("AHETPI")
    all_emp = get_series("AHEMAN")
    plot_dual_dataframes(prod, all_emp, "Hourly Earnings - Production", 'Hourly Earnings - All')


def plot_hourly_earnings_vs_food_cpi():
    """Avg Hourly Earnings (Production) vs CPI Food at Home"""
    prod = get_series("AHETPI")
    food_cpi = get_series("CUSR0000SAF11")
    plot_dual_dataframes(prod, food_cpi, "Avg Hourly Earnings", 'CPI Food at Home')


def plot_m2_vs_cpi():
    """Real M2 Money Supply vs CPI All Urban Consumers"""
    m2 = get_series("M2REAL")
    cpi = get_series("CPIAUCSL")
    plot_dual_dataframes(m2, cpi, 'M2 Real', 'CPI All Urban')


def plot_quad_m2_cpi_biz_sentiment():
    """Quad: Real M2 / CPI / Business Confidence / Consumer Sentiment (72 months)"""
    m2 = get_series("M2REAL")
    cpi = get_series("CPIAUCSL")
    biz = get_series("BSCICP03USM665S")
    sentiment = get_series("UMCSENT")
    plot_quad_dataframes(m2, cpi, biz, sentiment,
                         'M2 Real', 'CPI', 'Biz Sentiment', 'Consumer Sentiment', 72)


def plot_m2_timeseries():
    """Real M2 Money Supply with MoM and YoY changes"""
    data = get_series("M2REAL")
    plot_time_series_with_changes(data, "2015-01-01", title="Real M2 Money Supply")


def plot_cpi_timeseries():
    """US Consumer CPI with MoM and YoY changes"""
    data = get_series("CPIAUCSL")
    plot_time_series_with_changes(data, "2015-01-01", title="US Consumer CPI")


def plot_business_confidence_timeseries():
    """Business Confidence Indicator with MoM and YoY changes"""
    data = get_series("BSCICP03USM665S")
    plot_time_series_with_changes(data, "2015-01-01", title="Business Confidence Indicator")


def plot_consumer_sentiment_timeseries():
    """Consumer Sentiment with MoM and YoY changes"""
    data = get_series("UMCSENT")
    plot_time_series_with_changes(data, "2015-01-01", title="Consumer Sentiment")


def plot_unemployment_timeseries():
    """US Unemployment Rate with MoM and YoY changes"""
    data = get_series("UNRATE")
    plot_time_series_with_changes(data, "2015-01-01", title="US Unemployment Rate")


# ---------------------------------------------------------------------------
# Menu
# ---------------------------------------------------------------------------

MENU = [
    ("M2 Real vs Govt Expenditures",                    plot_m2_vs_govt),
    ("Total US Debt (time series)",                     plot_total_debt),
    ("GDP Now vs Consumer Sentiment (36 mo)",           plot_gdpnow_vs_sentiment),
    ("10-Year Market Yield vs CPI",                     plot_market_yield_vs_cpi),
    ("10-Year Market Yield vs Expected Inflation",      plot_market_yield_vs_expected_inflation),
    ("Hourly Earnings: Production vs All Employees",    plot_hourly_earnings_production_vs_all),
    ("Hourly Earnings (Production) vs Food CPI",        plot_hourly_earnings_vs_food_cpi),
    ("M2 Real vs CPI",                                  plot_m2_vs_cpi),
    ("Quad: M2 / CPI / Biz Confidence / Sentiment",    plot_quad_m2_cpi_biz_sentiment),
    ("Real M2 (time series)",                           plot_m2_timeseries),
    ("CPI (time series)",                               plot_cpi_timeseries),
    ("Business Confidence (time series)",               plot_business_confidence_timeseries),
    ("Consumer Sentiment (time series)",                plot_consumer_sentiment_timeseries),
    ("Unemployment Rate (time series)",                 plot_unemployment_timeseries),
]


def show_menu():
    print()
    c.lightBlue("=== FRED Economic Data Plots ===")
    for i, (label, _) in enumerate(MENU, 1):
        print(f"  {i:2}. {label}")
    print("   q. Quit")
    print()


def main():
    while True:
        show_menu()
        choice = input("Select a plot: ").strip().lower()

        if choice in ('q', 'quit', 'exit'):
            break

        try:
            idx = int(choice) - 1
        except ValueError:
            c.orange("Invalid selection — enter a number or 'q' to quit.")
            continue

        if not 0 <= idx < len(MENU):
            c.orange(f"Please enter a number between 1 and {len(MENU)}.")
            continue

        label, fn = MENU[idx]
        c.lightGreen(f"\nFetching data for: {label} …")
        try:
            fn()
        except Exception as e:
            c.orange(f"Error: {e}")


if __name__ == '__main__':
    main()
