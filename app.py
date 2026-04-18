from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from src.backtest.engine import PermanentPortfolioBacktester
from src.data.akshare_client import AkshareClient, DataFetchError
from src.data.asset_classifier import SUPPORTED_ASSET_TYPES
from src.data.models import AssetConfig
from src.storage.sqlite_store import SQLiteStore
from src.ui.defaults import (
    DEFAULT_DB_PATH,
    DEFAULT_END_DATE,
    DEFAULT_INITIAL_CAPITAL,
    DEFAULT_START_DATE,
    default_asset_rows,
)


def normalize_assets(asset_frame: pd.DataFrame) -> list[AssetConfig]:
    cleaned = asset_frame.copy()
    cleaned["code"] = cleaned["code"].fillna("").astype(str).str.strip()
    cleaned["name"] = cleaned["name"].fillna("").astype(str).str.strip()
    cleaned["asset_type"] = cleaned["asset_type"].fillna("").astype(str).str.strip()
    cleaned["weight"] = pd.to_numeric(cleaned["weight"], errors="coerce")
    cleaned = cleaned.dropna(subset=["weight"])
    cleaned = cleaned.loc[cleaned["code"] != ""]

    if cleaned.empty:
        raise ValueError("请至少保留一个有效资产代码")

    if (cleaned["weight"] < 0).any():
        raise ValueError("权重不能为负数")

    total_weight = float(cleaned["weight"].sum())
    if total_weight <= 0:
        raise ValueError("权重之和必须大于 0")

    if abs(total_weight - 1.0) > 1e-6:
        st.info(f"检测到当前权重合计为 {total_weight:.4f}，已自动归一化到 1.0。")
        cleaned["weight"] = cleaned["weight"] / total_weight

    return [
        AssetConfig(
            code=row.code,
            name=row.name,
            asset_type=row.asset_type or None,
            weight=float(row.weight),
        )
        for row in cleaned.itertuples(index=False)
    ]


def render_metrics(metrics: dict[str, float]) -> None:
    cagr_col, drawdown_col, volatility_col = st.columns(3)
    cagr_col.metric("CAGR", f"{metrics['cagr']:.2%}")
    drawdown_col.metric("最大回撤", f"{metrics['max_drawdown']:.2%}")
    volatility_col.metric("波动率", f"{metrics['volatility']:.2%}")


def main() -> None:
    st.set_page_config(page_title="cn-permanent-portfolio", layout="wide")
    st.title("cn-permanent-portfolio")
    st.caption("中国市场永久投资组合回测工具")

    with st.sidebar:
        st.subheader("回测参数")
        start_date = st.date_input("开始日期", value=DEFAULT_START_DATE)
        end_date = st.date_input("结束日期", value=DEFAULT_END_DATE)
        initial_capital = st.number_input("初始资金", min_value=0.0001, value=DEFAULT_INITIAL_CAPITAL, step=0.1)

    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("请选择单个开始日期和结束日期。")
        return

    if start_date > end_date:
        st.error("开始日期不能晚于结束日期。")
        return

    st.subheader("资产配置")
    st.write("支持新增、删除代码，并调整资产类型与权重。")

    editor_frame = pd.DataFrame(default_asset_rows())
    edited_assets = st.data_editor(
        editor_frame,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "code": st.column_config.TextColumn("代码", required=True),
            "name": st.column_config.TextColumn("名称"),
            "asset_type": st.column_config.SelectboxColumn("类型", options=list(SUPPORTED_ASSET_TYPES)),
            "weight": st.column_config.NumberColumn("权重", min_value=0.0, max_value=1.0, step=0.01, format="%.4f"),
        },
    )

    if st.button("运行回测", type="primary", use_container_width=True):
        try:
            assets = normalize_assets(edited_assets)
        except ValueError as exc:
            st.error(str(exc))
            return

        client = AkshareClient()
        store = SQLiteStore(DEFAULT_DB_PATH)

        try:
            with st.spinner("正在拉取数据并执行回测..."):
                fetched_rows = 0
                for asset in assets:
                    history = client.fetch_asset_history(asset, start_date=start_date, end_date=end_date)
                    fetched_rows += store.upsert_prices(
                        history,
                        asset_type=asset.asset_type,
                        source="akshare",
                    )

                price_frame = store.load_prices(
                    codes=[asset.code for asset in assets],
                    start_date=start_date,
                    end_date=end_date,
                )

                backtester = PermanentPortfolioBacktester(initial_capital=initial_capital)
                result = backtester.run(price_frame, assets)
        except DataFetchError as exc:
            st.error(str(exc))
            return
        except Exception as exc:
            st.error(f"运行失败: {exc}")
            return

        st.success(f"回测完成，本次写入/更新 {fetched_rows} 条价格记录。")
        render_metrics(result.metrics)

        nav_display = result.nav.set_index("date")[["nav"]]
        st.subheader("净值曲线")
        st.line_chart(nav_display)

        with st.expander("查看净值数据"):
            st.dataframe(result.nav, use_container_width=True)

        with st.expander("查看每日权重"):
            st.dataframe(result.weights, use_container_width=True)


if __name__ == "__main__":
    main()
