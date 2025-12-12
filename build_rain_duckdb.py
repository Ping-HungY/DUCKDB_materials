#!/usr/bin/env python3
"""Load rainfall station CSV data into a DuckDB database."""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple

import duckdb

# 專案根目錄固定在腳本所在位置，資料來源可透過環境變數覆寫
PROJECT_DIR = Path(__file__).resolve().parent
Data_ROOT = PROJECT_DIR #自行修改路徑
DATA_ROOT = Path(os.environ.get("RAIN_DATA_ROOT", Data_ROOT))
DATA_DIR = DATA_ROOT / "data"
SRC_CSV = DATA_DIR / "Rain_1998-2017.csv"
META_TXT = DATA_DIR / "Meta_data.txt"
DB_PATH = PROJECT_DIR / "rainfall.duckdb"


def parse_metadata() -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """從說明檔案讀出欄位意義與特殊值說明。"""
    if not META_TXT.exists():
        return [], []

    field_rows: List[Tuple[str, str]] = []
    special_rows: List[Tuple[str, str]] = []
    section = None

    with META_TXT.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line.startswith("*"):  # Meta_data.txt 以 * 帶出條目
                continue
            content = line[1:].strip()
            if not content:
                continue
            if content.startswith("欄位標題說明"):
                section = "fields"
                continue
            if content.startswith("特殊值"):
                section = "specials"
                continue
            if section == "fields":
                parts = content.split(maxsplit=1)
                if len(parts) == 2 and parts[0].isupper() and parts[0][:2].isalpha():
                    field_rows.append((parts[0], parts[1]))
            elif section == "specials":
                if ":" in content:
                    key, desc = content.split(":", 1)
                    special_rows.append((key.strip(), desc.strip()))

    return field_rows, special_rows


def main() -> None:
    # 先確定原始 CSV 存在，避免建立半成品資料庫。
    if not SRC_CSV.exists():
        raise SystemExit(f"找不到資料檔案: {SRC_CSV}")

    conn = duckdb.connect(str(DB_PATH))
    conn.execute("PRAGMA enable_progress_bar=0")  # 關掉內建進度條，避免批次輸出干擾

    # 每次重建時先刪除舊表，確保資料與 schema 乾淨一致
    conn.execute("DROP TABLE IF EXISTS rainfall_hourly")
    conn.execute(
        """
        CREATE TABLE rainfall_hourly AS
        WITH src AS (
            SELECT
                stno AS station_no,
                yyyymmddhh AS stamp_text,
                NULLIF(CAST(PP01 AS DOUBLE), -9996) AS rainfall_mm
            FROM read_csv_auto(
                ?,
                header=True,
                sample_size=-1,
                types={
                    'stno': 'VARCHAR',
                    'yyyymmddhh': 'VARCHAR',
                    'PP01': 'DOUBLE'
                }
            )
        )
        SELECT
            station_no,
            CASE
                WHEN RIGHT(stamp_text, 2) = '24' THEN
                    strptime(LEFT(stamp_text, 8), '%Y%m%d') + INTERVAL 1 DAY
                ELSE
                    strptime(stamp_text, '%Y%m%d%H')
            END AS obs_ts,
            rainfall_mm
        FROM src
        """,
        [str(SRC_CSV)],
    )

    # 事後再查詢筆數與時間範圍，方便 log 與驗證
    row_count = conn.execute("SELECT COUNT(*) FROM rainfall_hourly").fetchone()[0]
    min_ts, max_ts = conn.execute(
        "SELECT MIN(obs_ts), MAX(obs_ts) FROM rainfall_hourly"
    ).fetchone()

    # 將 metadata 文字檔同步進 DuckDB，方便後續直接 join 說明
    field_rows, special_rows = parse_metadata()
    if field_rows:
        conn.execute("DROP TABLE IF EXISTS metadata_fields")
        conn.execute(
            "CREATE TABLE metadata_fields (code TEXT PRIMARY KEY, description TEXT)"
        )
        conn.executemany("INSERT INTO metadata_fields VALUES (?, ?)", field_rows)
    if special_rows:
        conn.execute("DROP TABLE IF EXISTS metadata_special_values")
        conn.execute(
            "CREATE TABLE metadata_special_values (code TEXT PRIMARY KEY, meaning TEXT)"
        )
        conn.executemany("INSERT INTO metadata_special_values VALUES (?, ?)", special_rows)

    print(
        f"已建立 {DB_PATH.name}, 表格 rainfall_hourly: {row_count:,} 筆 "
        f"（{min_ts} 至 {max_ts}）"
    )

    conn.close()


if __name__ == "__main__":
    main()
