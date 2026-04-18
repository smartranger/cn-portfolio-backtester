from __future__ import annotations

import math

import pandas as pd


def calculate_cagr(nav_series: pd.Series) -> float:
    if nav_series.empty:
        return 0.0

    clean_nav = nav_series.dropna()
    if clean_nav.empty:
        return 0.0

    start_date = pd.Timestamp(clean_nav.index[0])
    end_date = pd.Timestamp(clean_nav.index[-1])
    elapsed_days = max((end_date - start_date).days, 0)

    if elapsed_days == 0 or clean_nav.iloc[0] <= 0:
        return float(clean_nav.iloc[-1] / clean_nav.iloc[0] - 1.0) if clean_nav.iloc[0] > 0 else 0.0

    return float((clean_nav.iloc[-1] / clean_nav.iloc[0]) ** (365.25 / elapsed_days) - 1.0)


def calculate_max_drawdown(nav_series: pd.Series) -> float:
    if nav_series.empty:
        return 0.0

    clean_nav = nav_series.dropna()
    if clean_nav.empty:
        return 0.0

    running_max = clean_nav.cummax()
    drawdown = clean_nav / running_max - 1.0
    return float(abs(drawdown.min()))


def calculate_volatility(nav_series: pd.Series) -> float:
    if nav_series.empty:
        return 0.0

    daily_returns = nav_series.pct_change().dropna()
    if daily_returns.empty:
        return 0.0

    return float(daily_returns.std(ddof=0) * math.sqrt(252))


def summarize_performance(nav_series: pd.Series) -> dict[str, float]:
    return {
        "cagr": calculate_cagr(nav_series),
        "max_drawdown": calculate_max_drawdown(nav_series),
        "volatility": calculate_volatility(nav_series),
    }
