from __future__ import annotations

from datetime import date
from typing import Iterable

import akshare as ak
import pandas as pd

from .asset_classifier import enrich_asset_config
from .models import AssetConfig


class DataFetchError(RuntimeError):
    """Raised when an asset cannot be fetched from AkShare."""


class AkshareClient:
    def fetch_asset_history(
        self,
        asset: AssetConfig,
        start_date: date | str,
        end_date: date | str,
    ) -> pd.DataFrame:
        enriched_asset = enrich_asset_config(asset)
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)

        try:
            if enriched_asset.asset_type == "etf":
                history = self._fetch_etf_history(enriched_asset.code, start_ts, end_ts)
            elif enriched_asset.asset_type == "money_fund":
                history = self._fetch_money_fund_history(enriched_asset.code, start_ts, end_ts)
            else:
                history = self._fetch_fund_history(enriched_asset.code, start_ts, end_ts)
        except Exception as exc:  # pragma: no cover - network/runtime dependent
            raise DataFetchError(f"拉取 {enriched_asset.code} 失败: {exc}") from exc

        if history.empty:
            raise DataFetchError(f"{enriched_asset.code} 未返回可用历史数据")

        history["date"] = pd.to_datetime(history["date"]).dt.normalize()
        history["code"] = enriched_asset.code
        history["price"] = pd.to_numeric(history["price"], errors="coerce")
        history = history.dropna(subset=["date", "price"])
        history = history.loc[(history["date"] >= start_ts) & (history["date"] <= end_ts)]
        history = history.sort_values("date").drop_duplicates(subset=["date", "code"], keep="last")

        if history.empty:
            raise DataFetchError(f"{enriched_asset.code} 在所选区间内没有数据")

        return history[["date", "code", "price"]].reset_index(drop=True)

    def _fetch_fund_history(self, code: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        last_error: Exception | None = None

        for indicator in ("单位净值走势", "累计净值走势"):
            try:
                raw = ak.fund_open_fund_info_em(symbol=code, indicator=indicator)
                parsed = self._extract_date_price(
                    raw,
                    date_candidates=("净值日期", "日期", "x", "date"),
                    price_candidates=("单位净值", "累计净值", "净值", "y", "value"),
                )
                if not parsed.empty:
                    return parsed
            except Exception as exc:  # pragma: no cover - runtime dependent
                last_error = exc

        raise DataFetchError(f"开放式基金 {code} 拉取失败: {last_error}")

    def _fetch_etf_history(self, code: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        last_error: Exception | None = None
        start_str = start_ts.strftime("%Y%m%d")
        end_str = end_ts.strftime("%Y%m%d")

        try:
            raw = ak.fund_etf_hist_em(
                symbol=code,
                period="daily",
                start_date=start_str,
                end_date=end_str,
                adjust="qfq",
            )
            parsed = self._extract_date_price(
                raw,
                date_candidates=("日期", "date"),
                price_candidates=("收盘", "单位净值", "close", "price"),
            )
            if not parsed.empty:
                return parsed
        except Exception as exc:  # pragma: no cover - runtime dependent
            last_error = exc

        try:
            raw = ak.fund_etf_hist_sina(symbol=self._with_market_prefix(code))
            parsed = self._extract_date_price(
                raw,
                date_candidates=("date", "日期"),
                price_candidates=("close", "收盘", "单位净值"),
            )
            if not parsed.empty:
                return parsed
        except Exception as exc:  # pragma: no cover - runtime dependent
            last_error = exc

        raise DataFetchError(f"ETF {code} 拉取失败: {last_error}")

    def _fetch_money_fund_history(self, code: str, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
        benefit_data = self._try_money_fund_benefit_series(code)
        if not benefit_data.empty:
            benefit_data["price"] = (1.0 + benefit_data["benefit"] / 10000.0).cumprod()
            return benefit_data[["date", "price"]]

        annualized_data = self._try_money_fund_annualized_series(code)
        if not annualized_data.empty:
            annualized_data["price"] = (1.0 + annualized_data["annualized_rate"] / 100.0 / 365.0).cumprod()
            return annualized_data[["date", "price"]]

        business_days = pd.date_range(start=start_ts, end=end_ts, freq="B")
        return pd.DataFrame({"date": business_days, "price": 1.0})

    def _try_money_fund_benefit_series(self, code: str) -> pd.DataFrame:
        for indicator in ("每万份收益", "万份收益"):
            try:
                raw = ak.fund_open_fund_info_em(symbol=code, indicator=indicator)
                parsed = self._extract_date_price(
                    raw,
                    date_candidates=("净值日期", "日期", "x", "date"),
                    price_candidates=("每万份收益", "万份收益", "y", "value"),
                    value_name="benefit",
                )
                if not parsed.empty:
                    parsed["benefit"] = pd.to_numeric(parsed["benefit"], errors="coerce").fillna(0.0)
                    return parsed[["date", "benefit"]]
            except Exception:
                continue

        return pd.DataFrame(columns=["date", "benefit"])

    def _try_money_fund_annualized_series(self, code: str) -> pd.DataFrame:
        for indicator in ("七日年化收益率", "7日年化收益率"):
            try:
                raw = ak.fund_open_fund_info_em(symbol=code, indicator=indicator)
                parsed = self._extract_date_price(
                    raw,
                    date_candidates=("净值日期", "日期", "x", "date"),
                    price_candidates=("七日年化收益率", "7日年化收益率", "y", "value"),
                    value_name="annualized_rate",
                )
                if not parsed.empty:
                    parsed["annualized_rate"] = self._to_numeric(parsed["annualized_rate"])
                    return parsed[["date", "annualized_rate"]]
            except Exception:
                continue

        return pd.DataFrame(columns=["date", "annualized_rate"])

    def _extract_date_price(
        self,
        frame: pd.DataFrame,
        date_candidates: Iterable[str],
        price_candidates: Iterable[str],
        value_name: str = "price",
    ) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["date", value_name])

        date_column = self._find_column(frame, date_candidates)
        price_column = self._find_column(frame, price_candidates)

        if not date_column or not price_column:
            return pd.DataFrame(columns=["date", value_name])

        parsed = frame[[date_column, price_column]].copy()
        parsed.columns = ["date", value_name]
        parsed["date"] = pd.to_datetime(parsed["date"], errors="coerce")
        parsed = parsed.dropna(subset=["date"])
        return parsed

    def _find_column(self, frame: pd.DataFrame, candidates: Iterable[str]) -> str | None:
        for candidate in candidates:
            if candidate in frame.columns:
                return candidate
        return None

    def _to_numeric(self, series: pd.Series) -> pd.Series:
        cleaned = (
            series.astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        return pd.to_numeric(cleaned, errors="coerce")

    def _with_market_prefix(self, code: str) -> str:
        normalized_code = str(code).strip()
        if normalized_code.startswith(("5", "6", "9")):
            return f"sh{normalized_code}"
        return f"sz{normalized_code}"
