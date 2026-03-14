from __future__ import annotations

from datetime import datetime
import re
from typing import Any

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import fundamentals


def parse_symbols(raw_symbols: str) -> list[str]:
    return [
        token.strip().upper()
        for token in re.split(r'[\s,]+', raw_symbols.strip())
        if token.strip()
    ]


def coerce_positive_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        ivalue = int(value)
        return ivalue if ivalue > 0 else None
    try:
        text = str(value).strip()
        if not text:
            return None
        ivalue = int(float(text))
        return ivalue if ivalue > 0 else None
    except (TypeError, ValueError):
        return None


def collect_historical_series(
    api: Any,
    symbols: list[str],
    days: int,
) -> dict[str, list[tuple[datetime, float]]]:
    series: dict[str, list[tuple[datetime, float]]] = {}
    for symbol in symbols:
        raw_points = fundamentals.get_historicals(
            api=api,
            name=symbol,
            days=days,
            gabby=False,
        )
        parsed_points: list[tuple[datetime, float]] = []
        for date_text, close_value in raw_points or []:
            try:
                parsed_points.append(
                    (
                        datetime.strptime(str(date_text), '%Y-%m-%d'),
                        float(close_value),
                    )
                )
            except (TypeError, ValueError):
                continue
        if parsed_points:
            series[symbol] = parsed_points
    return series


def draw_historical_series(
    symbol_series: dict[str, list[tuple[datetime, float]]],
    normalize: bool,
    title: str = 'Historical Stock Prices',
) -> None:
    colors = [
        'steelblue',
        'crimson',
        'forestgreen',
        'darkorange',
        'purple',
        'brown',
        'deeppink',
        'olive',
        'teal',
        'navy',
    ]

    for idx, (symbol, points) in enumerate(symbol_series.items()):
        points_sorted = sorted(points, key=lambda item: item[0])
        x_values = [point[0] for point in points_sorted]
        y_values = [point[1] for point in points_sorted]
        if not y_values:
            continue

        if normalize and y_values[0] != 0:
            base_value = y_values[0]
            y_values = [((value / base_value) - 1.0) * 100.0 for value in y_values]

        color = colors[idx % len(colors)]
        plt.plot(
            x_values,
            y_values,
            marker='.',
            color=color,
            linestyle='-',
            linewidth=1.5,
            label=symbol,
            markersize=3,
        )

    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend(loc='upper left', framealpha=0.9)
    plt.xlabel('Date')
    plt.ylabel('Change (%)' if normalize else 'Price ($)')
    plt.title(title)
    axis = plt.gca()
    axis.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)
    plt.tight_layout()
