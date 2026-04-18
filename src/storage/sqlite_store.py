from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.data.models import AssetConfig


class SQLiteStore:
    def __init__(self, db_path: str = "data/portfolio.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def initialize(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS asset_prices (
                    date TEXT NOT NULL,
                    code TEXT NOT NULL,
                    price REAL NOT NULL,
                    asset_type TEXT,
                    source TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (date, code)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_configs (
                    name TEXT PRIMARY KEY,
                    start_date TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_config_assets (
                    config_name TEXT NOT NULL,
                    asset_order INTEGER NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT,
                    asset_type TEXT,
                    weight REAL NOT NULL,
                    PRIMARY KEY (config_name, asset_order),
                    FOREIGN KEY (config_name) REFERENCES portfolio_configs(name) ON DELETE CASCADE
                )
                """
            )
            conn.commit()

    def upsert_prices(
        self,
        prices: pd.DataFrame,
        asset_type: str | None = None,
        source: str = "akshare",
    ) -> int:
        if prices.empty:
            return 0

        prepared = prices.copy()
        prepared["date"] = pd.to_datetime(prepared["date"]).dt.strftime("%Y-%m-%d")
        prepared["code"] = prepared["code"].astype(str).str.strip()
        prepared["price"] = pd.to_numeric(prepared["price"], errors="coerce")
        prepared = prepared.dropna(subset=["date", "code", "price"])

        if prepared.empty:
            return 0

        rows = [
            (
                row.date,
                row.code,
                float(row.price),
                asset_type,
                source,
            )
            for row in prepared.itertuples(index=False)
        ]

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO asset_prices (date, code, price, asset_type, source)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date, code) DO UPDATE SET
                    price = excluded.price,
                    asset_type = COALESCE(excluded.asset_type, asset_prices.asset_type),
                    source = excluded.source,
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
            conn.commit()

        return len(rows)

    def load_prices(
        self,
        codes: Iterable[str],
        start_date: str | pd.Timestamp,
        end_date: str | pd.Timestamp,
    ) -> pd.DataFrame:
        normalized_codes = [str(code).strip() for code in codes if str(code).strip()]
        if not normalized_codes:
            return pd.DataFrame(columns=["date", "code", "price"])

        placeholders = ", ".join(["?"] * len(normalized_codes))
        params = normalized_codes + [
            pd.Timestamp(start_date).strftime("%Y-%m-%d"),
            pd.Timestamp(end_date).strftime("%Y-%m-%d"),
        ]

        query = f"""
            SELECT date, code, price
            FROM asset_prices
            WHERE code IN ({placeholders})
              AND date BETWEEN ? AND ?
            ORDER BY date, code
        """

        with sqlite3.connect(self.db_path) as conn:
            frame = pd.read_sql_query(query, conn, params=params)

        if frame.empty:
            return frame

        frame["date"] = pd.to_datetime(frame["date"])
        frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
        return frame.dropna(subset=["date", "price"]).reset_index(drop=True)

    def save_portfolio_config(
        self,
        name: str,
        start_date: str | pd.Timestamp,
        assets: list[AssetConfig],
    ) -> None:
        normalized_name = str(name).strip()
        if not normalized_name:
            raise ValueError("配置名称不能为空")

        start_date_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO portfolio_configs (name, start_date)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    start_date = excluded.start_date,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (normalized_name, start_date_str),
            )
            conn.execute(
                "DELETE FROM portfolio_config_assets WHERE config_name = ?",
                (normalized_name,),
            )
            conn.executemany(
                """
                INSERT INTO portfolio_config_assets (
                    config_name, asset_order, code, name, asset_type, weight
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        normalized_name,
                        index,
                        asset.code,
                        asset.name,
                        asset.asset_type,
                        float(asset.weight),
                    )
                    for index, asset in enumerate(assets)
                ],
            )
            conn.commit()

    def list_portfolio_configs(self) -> pd.DataFrame:
        query = """
            SELECT
                c.name,
                c.start_date,
                COUNT(a.code) AS asset_count,
                c.created_at,
                c.updated_at
            FROM portfolio_configs c
            LEFT JOIN portfolio_config_assets a
              ON c.name = a.config_name
            GROUP BY c.name, c.start_date, c.created_at, c.updated_at
            ORDER BY c.updated_at DESC, c.name ASC
        """
        with sqlite3.connect(self.db_path) as conn:
            frame = pd.read_sql_query(query, conn)
        return frame

    def load_portfolio_config(self, name: str) -> tuple[pd.Timestamp, list[AssetConfig]]:
        normalized_name = str(name).strip()
        if not normalized_name:
            raise ValueError("配置名称不能为空")

        with sqlite3.connect(self.db_path) as conn:
            config_row = conn.execute(
                """
                SELECT name, start_date
                FROM portfolio_configs
                WHERE name = ?
                """,
                (normalized_name,),
            ).fetchone()

            asset_rows = conn.execute(
                """
                SELECT code, name, asset_type, weight
                FROM portfolio_config_assets
                WHERE config_name = ?
                ORDER BY asset_order
                """,
                (normalized_name,),
            ).fetchall()

        if config_row is None:
            raise ValueError(f"未找到配置：{normalized_name}")

        assets = [
            AssetConfig(
                code=row[0],
                name=row[1] or "",
                asset_type=row[2],
                weight=float(row[3]),
            )
            for row in asset_rows
        ]
        return pd.Timestamp(config_row[1]), assets

    def delete_portfolio_config(self, name: str) -> None:
        normalized_name = str(name).strip()
        if not normalized_name:
            raise ValueError("配置名称不能为空")

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM portfolio_config_assets WHERE config_name = ?",
                (normalized_name,),
            )
            conn.execute(
                "DELETE FROM portfolio_configs WHERE name = ?",
                (normalized_name,),
            )
            conn.commit()
