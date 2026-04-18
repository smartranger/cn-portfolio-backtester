from __future__ import annotations

from datetime import date

import altair as alt
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


def asset_rows_from_assets(assets: list[AssetConfig]) -> list[dict[str, object]]:
    return [
        {
            "code": asset.code,
            "name": asset.name,
            "asset_type": asset.asset_type,
            "weight": asset.weight,
        }
        for asset in assets
    ]


def initialize_ui_state() -> None:
    if "asset_rows" not in st.session_state:
        st.session_state["asset_rows"] = default_asset_rows()
    if "start_date_input" not in st.session_state:
        st.session_state["start_date_input"] = DEFAULT_START_DATE
    if "end_date_input" not in st.session_state:
        st.session_state["end_date_input"] = DEFAULT_END_DATE
    if "pending_start_date_input" in st.session_state:
        st.session_state["start_date_input"] = st.session_state.pop("pending_start_date_input")


def clear_backtest_state() -> None:
    for key in ("backtest_result", "backtest_assets", "backtest_prices", "fetched_rows"):
        st.session_state.pop(key, None)


def build_config_comparison_frame(config_names: list[str], store: SQLiteStore) -> pd.DataFrame:
    comparison_rows: list[dict[str, object]] = []
    max_asset_count = 0

    loaded_configs: list[tuple[str, pd.Timestamp, list[AssetConfig]]] = []
    for config_name in config_names:
        start_date, assets = store.load_portfolio_config(config_name)
        loaded_configs.append((config_name, start_date, assets))
        max_asset_count = max(max_asset_count, len(assets))

    for config_name, start_date, assets in loaded_configs:
        row: dict[str, object] = {
            "配置名称": config_name,
            "起始投资时间": start_date.strftime("%Y-%m-%d"),
        }
        for index in range(max_asset_count):
            column_name = f"资产{index + 1}"
            if index < len(assets):
                asset = assets[index]
                row[column_name] = f"{asset.code} {asset.name} / {asset.asset_type} / {asset.weight:.2%}"
            else:
                row[column_name] = ""
        comparison_rows.append(row)

    return pd.DataFrame(comparison_rows)


def build_asset_curve_frame(price_frame: pd.DataFrame, assets: list[AssetConfig], nav_dates: pd.Series) -> pd.DataFrame:
    if price_frame.empty:
        return pd.DataFrame(columns=["date", "series", "value"])

    label_map = {asset.code: f"{asset.code} {asset.name or asset.code}" for asset in assets}
    panel = (
        price_frame.copy()
        .assign(date=lambda frame: pd.to_datetime(frame["date"]))
        .pivot_table(index="date", columns="code", values="price", aggfunc="last")
        .sort_index()
        .reindex(columns=[asset.code for asset in assets])
        .ffill()
    )
    panel = panel.reindex(pd.to_datetime(nav_dates)).ffill().dropna(how="any")

    normalized = panel.divide(panel.iloc[0])
    normalized = normalized.rename(columns=label_map)
    normalized = normalized.reset_index(names="date")
    return normalized.melt(id_vars="date", var_name="series", value_name="value")


def render_nav_chart(
    nav_frame: pd.DataFrame,
    price_frame: pd.DataFrame,
    assets: list[AssetConfig],
    selected_asset_series: list[str],
) -> None:
    nav_chart_data = nav_frame.copy()
    nav_chart_data["date"] = pd.to_datetime(nav_chart_data["date"])
    nav_chart_data["series"] = "组合净值"
    nav_chart_data["value"] = nav_chart_data["nav"]
    nav_chart_data["rebalance_label"] = nav_chart_data["rebalanced"].map({True: "是", False: "否"})

    asset_curve_data = build_asset_curve_frame(price_frame, assets, nav_chart_data["date"])
    if selected_asset_series:
        asset_curve_data = asset_curve_data.loc[asset_curve_data["series"].isin(selected_asset_series)]
    else:
        asset_curve_data = asset_curve_data.iloc[0:0]

    line_data = pd.concat(
        [
            nav_chart_data[["date", "series", "value"]],
            asset_curve_data[["date", "series", "value"]],
        ],
        ignore_index=True,
    )

    base = alt.Chart(line_data).encode(
        x=alt.X("date:T", title="日期"),
        y=alt.Y("value:Q", title="归一化净值"),
        color=alt.Color("series:N", title="曲线"),
        tooltip=[
            alt.Tooltip("date:T", title="日期"),
            alt.Tooltip("series:N", title="曲线"),
            alt.Tooltip("value:Q", title="数值", format=".4f"),
        ],
    )

    line_chart = base.mark_line()

    rebalance_points = (
        alt.Chart(nav_chart_data.loc[nav_chart_data["rebalanced"]])
        .mark_point(color="#d62728", filled=True, size=70)
        .encode(
            x=alt.X("date:T", title="日期"),
            y=alt.Y("value:Q", title="归一化净值"),
            tooltip=[
                alt.Tooltip("date:T", title="日期"),
                alt.Tooltip("value:Q", title="组合净值", format=".4f"),
                alt.Tooltip("rebalance_label:N", title="触发再平衡"),
            ],
        )
    )

    st.altair_chart((line_chart + rebalance_points).interactive(), use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="cn-permanent-portfolio", layout="wide")
    initialize_ui_state()

    store = SQLiteStore(DEFAULT_DB_PATH)

    st.title("cn-permanent-portfolio")
    st.caption("中国市场永久投资组合回测工具")

    with st.sidebar:
        st.subheader("回测参数")
        start_date = st.date_input("开始日期", key="start_date_input")
        end_date = st.date_input("结束日期", key="end_date_input")
        initial_capital = st.number_input("初始资金", min_value=0.0001, value=DEFAULT_INITIAL_CAPITAL, step=0.1)

    if isinstance(start_date, tuple) or isinstance(end_date, tuple):
        st.error("请选择单个开始日期和结束日期。")
        return

    if start_date > end_date:
        st.error("开始日期不能晚于结束日期。")
        return

    st.subheader("资产配置")
    st.write("支持新增、删除代码，并调整资产类型与权重。")

    editor_frame = pd.DataFrame(st.session_state["asset_rows"])
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
    st.session_state["asset_rows"] = edited_assets.to_dict(orient="records")

    st.subheader("配置存档")
    st.write("保存当前起始投资时间和基金组合，便于快速切换与比较。")

    archive_name = st.text_input("存档名称", placeholder="例如：经典永久组合")
    archive_col1, archive_col2, archive_col3 = st.columns(3)

    if archive_col1.button("保存当前配置", use_container_width=True):
        try:
            assets_to_save = normalize_assets(edited_assets)
            if not archive_name.strip():
                raise ValueError("请先输入存档名称")
            store.save_portfolio_config(archive_name, start_date, assets_to_save)
            st.success(f"已保存配置：{archive_name.strip()}")
        except ValueError as exc:
            st.error(str(exc))

    config_list = store.list_portfolio_configs()
    if config_list.empty:
        st.info("还没有配置存档，可以先保存一组当前配置。")
    else:
        config_names = config_list["name"].tolist()
        selected_config_name = archive_col2.selectbox("选择存档", options=config_names, label_visibility="collapsed")

        if st.button("加载选中配置", use_container_width=True):
            loaded_start_date, loaded_assets = store.load_portfolio_config(selected_config_name)
            st.session_state["asset_rows"] = asset_rows_from_assets(loaded_assets)
            st.session_state["pending_start_date_input"] = loaded_start_date.date()
            clear_backtest_state()
            st.rerun()

        if archive_col3.button("删除选中配置", use_container_width=True):
            store.delete_portfolio_config(selected_config_name)
            clear_backtest_state()
            st.success(f"已删除配置：{selected_config_name}")
            st.rerun()

        compare_names = st.multiselect(
            "比较已存档配置",
            options=config_names,
            help="展示多组配置的起始时间和基金明细，便于横向比较。",
        )
        if compare_names:
            comparison_frame = build_config_comparison_frame(compare_names, store)
            st.dataframe(comparison_frame, use_container_width=True)

    if st.button("运行回测", type="primary", use_container_width=True):
        try:
            assets = normalize_assets(edited_assets)
        except ValueError as exc:
            st.error(str(exc))
            return

        client = AkshareClient()

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

        st.session_state["backtest_result"] = result
        st.session_state["backtest_assets"] = assets
        st.session_state["backtest_prices"] = price_frame
        st.session_state["fetched_rows"] = fetched_rows

    result = st.session_state.get("backtest_result")
    assets = st.session_state.get("backtest_assets")
    price_frame = st.session_state.get("backtest_prices")

    if result is not None and assets is not None and price_frame is not None:
        fetched_rows = int(st.session_state.get("fetched_rows", 0))
        st.success(f"回测完成，本次写入/更新 {fetched_rows} 条价格记录。")
        render_metrics(result.metrics)

        st.subheader("净值曲线")
        asset_options = [f"{asset.code} {asset.name or asset.code}" for asset in assets]
        selected_asset_series = st.multiselect(
            "附加显示基金曲线",
            options=asset_options,
            default=asset_options,
            help="组合净值默认始终显示，下面可以单独开关每只基金的归一化曲线。",
        )
        render_nav_chart(result.nav, price_frame, assets, selected_asset_series)
        rebalance_count = int(result.nav["rebalanced"].sum())
        st.caption(f"红点表示触发再平衡的交易日，共 {rebalance_count} 次。")

        with st.expander("查看净值数据"):
            st.dataframe(result.nav, use_container_width=True)

        with st.expander("查看每日权重"):
            st.dataframe(result.weights, use_container_width=True)


if __name__ == "__main__":
    main()
