from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from src.data.asset_classifier import enrich_asset_config
from src.data.models import AssetConfig

from .metrics import summarize_performance


@dataclass
class BacktestResult:
    nav: pd.DataFrame
    metrics: dict[str, float]
    weights: pd.DataFrame


class PermanentPortfolioBacktester:
    def __init__(
        self,
        initial_capital: float = 1.0,
        lower_threshold: float = 0.15,
        upper_threshold: float = 0.35,
    ) -> None:
        self.initial_capital = initial_capital
        self.lower_threshold = lower_threshold
        self.upper_threshold = upper_threshold

    def run(self, prices: pd.DataFrame, assets: list[AssetConfig]) -> BacktestResult:
        if prices.empty:
            raise ValueError("没有可用于回测的价格数据")

        enriched_assets = [enrich_asset_config(asset) for asset in assets]
        target_weights = self._normalize_weights(enriched_assets)
        price_panel = self._build_price_panel(prices, [asset.code for asset in enriched_assets])

        first_prices = price_panel.iloc[0]
        shares = (self.initial_capital * target_weights) / first_prices

        nav_records: list[dict[str, object]] = []
        weight_records: list[dict[str, object]] = []

        for current_date, current_prices in price_panel.iterrows():
            asset_values = shares * current_prices
            portfolio_value = float(asset_values.sum())
            current_weights = asset_values / portfolio_value
            rebalanced = False

            if ((current_weights > self.upper_threshold) | (current_weights < self.lower_threshold)).any():
                target_values = portfolio_value * target_weights
                shares = target_values / current_prices
                asset_values = shares * current_prices
                portfolio_value = float(asset_values.sum())
                current_weights = asset_values / portfolio_value
                rebalanced = True

            nav_records.append(
                {
                    "date": current_date,
                    "nav": portfolio_value / self.initial_capital,
                    "portfolio_value": portfolio_value,
                    "rebalanced": rebalanced,
                }
            )

            weight_record = {"date": current_date}
            weight_record.update({code: float(weight) for code, weight in current_weights.items()})
            weight_records.append(weight_record)

        nav_frame = pd.DataFrame(nav_records)
        nav_frame["daily_return"] = nav_frame["nav"].pct_change().fillna(0.0)
        nav_frame["date"] = pd.to_datetime(nav_frame["date"])

        weight_frame = pd.DataFrame(weight_records)
        weight_frame["date"] = pd.to_datetime(weight_frame["date"])

        nav_series = nav_frame.set_index("date")["nav"]
        metrics = summarize_performance(nav_series)
        return BacktestResult(nav=nav_frame, metrics=metrics, weights=weight_frame)

    def _normalize_weights(self, assets: list[AssetConfig]) -> pd.Series:
        weight_map = pd.Series({asset.code: float(asset.weight) for asset in assets}, dtype="float64")
        total_weight = float(weight_map.sum())

        if total_weight <= 0:
            raise ValueError("资产权重之和必须大于 0")

        return weight_map / total_weight

    def _build_price_panel(self, prices: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
        prepared = prices.copy()
        prepared["date"] = pd.to_datetime(prepared["date"])
        prepared["code"] = prepared["code"].astype(str).str.strip()
        prepared["price"] = pd.to_numeric(prepared["price"], errors="coerce")
        prepared = prepared.dropna(subset=["date", "code", "price"])

        panel = (
            prepared.pivot_table(index="date", columns="code", values="price", aggfunc="last")
            .sort_index()
            .reindex(columns=codes)
            .ffill()
            .dropna(how="any")
        )

        if panel.empty:
            raise ValueError("价格数据无法对齐为有效的回测面板")

        return panel
