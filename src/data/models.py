from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

AssetType = Literal["fund", "etf", "money_fund"]


@dataclass(slots=True)
class AssetConfig:
    code: str
    weight: float
    asset_type: Optional[AssetType] = None
    name: str = ""

    def normalized_code(self) -> str:
        return str(self.code).strip()

    def normalized_name(self) -> str:
        return self.name.strip()
