from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


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
