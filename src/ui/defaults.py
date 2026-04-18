from __future__ import annotations

from datetime import date

from src.data.models import AssetConfig

DEFAULT_DB_PATH = "data/portfolio.db"
DEFAULT_INITIAL_CAPITAL = 1.0
DEFAULT_START_DATE = date(2018, 1, 1)
DEFAULT_END_DATE = date.today()

DEFAULT_ASSETS = [
    AssetConfig(code="110020", name="易方达沪深300ETF联接A", asset_type="fund", weight=0.25),
    AssetConfig(code="166016", name="中欧纯债债券(LOF)C", asset_type="fund", weight=0.25),
    AssetConfig(code="000307", name="易方达黄金ETF联接A", asset_type="fund", weight=0.25),
    AssetConfig(code="710502", name="富安达现金通货币B", asset_type="money_fund", weight=0.25),
]


def default_asset_rows() -> list[dict[str, object]]:
    return [
        {
            "code": asset.code,
            "name": asset.name,
            "asset_type": asset.asset_type,
            "weight": asset.weight,
        }
        for asset in DEFAULT_ASSETS
    ]
