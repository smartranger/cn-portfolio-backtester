from __future__ import annotations

from dataclasses import replace

from .models import AssetConfig, AssetType

SUPPORTED_ASSET_TYPES: tuple[AssetType, ...] = ("fund", "etf", "money_fund")

DEFAULT_ASSET_MAPPING: dict[str, dict[str, str]] = {
    "110020": {"name": "沪深300ETF联接A", "asset_type": "fund"},
    "511090": {"name": "30年国债ETF", "asset_type": "etf"},
    "159934": {"name": "黄金ETF", "asset_type": "etf"},
    "710502": {"name": "货币基金", "asset_type": "money_fund"},
}

ETF_PREFIXES = ("159", "510", "511", "512", "513", "515", "516", "517", "518", "588")


def infer_asset_type(code: str, explicit_type: str | None = None) -> AssetType:
    normalized_code = str(code).strip()

    if explicit_type in SUPPORTED_ASSET_TYPES:
        return explicit_type

    if normalized_code in DEFAULT_ASSET_MAPPING:
        return DEFAULT_ASSET_MAPPING[normalized_code]["asset_type"]  # type: ignore[return-value]

    if normalized_code.startswith(ETF_PREFIXES):
        return "etf"

    return "fund"


def get_default_name(code: str) -> str:
    return DEFAULT_ASSET_MAPPING.get(str(code).strip(), {}).get("name", "")


def enrich_asset_config(asset: AssetConfig) -> AssetConfig:
    normalized_code = asset.normalized_code()
    asset_type = infer_asset_type(normalized_code, asset.asset_type)
    name = asset.normalized_name() or get_default_name(normalized_code)

    return replace(asset, code=normalized_code, asset_type=asset_type, name=name)
