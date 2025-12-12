# Rainfall DuckDB Toolkit

使用 DuckDB 搭配 Python 快速整理中央氣象局雨量站資料。專案提供建置腳本與 Notebook，協助將 `data/Rain_1998-2017.csv` 轉成結構化資料集，並進一步做品質檢核與統計分析。

## 使用 uv 建置環境

1. 安裝 [uv](https://docs.astral.sh/uv/getting-started/installation/)（macOS/Linux 可用官方 `curl` 指令，Windows 可用 `pip install uv` 或 MSI）。
2. 於專案根目錄執行同步並產生虛擬環境：
   ```bash
   cd Duck_DB
   uv python install 3.11          # 只需執行一次，確保本機有對應版本
   uv sync                         # 依據 pyproject.toml / uv.lock 建置 .venv
   source .venv/bin/activate       # Windows PowerShell 改用 .venv\\Scripts\\Activate.ps1
   ```
3. 驗證依賴是否齊全，可執行 `uv run python - <<'PY'` 測試匯入 DuckDB / pandas 等套件。

後續若要執行腳本或 Notebook，建議維持虛擬環境啟用狀態，並使用 `uv run …` 以沿用鎖定的套件版本。

## 程式與 Notebook 說明

### `build_rain_duckdb.py`

```bash
uv run python build_rain_duckdb.py
```

- 從 `data/Rain_1998-2017.csv` 讀取 1998–2017 的逐小時雨量，處理 `yyyymmddhh = 24` 的跨日情境並將缺值 (`-9996`) 轉為 `NULL`。
- 建立/覆寫 `rainfall.duckdb`，並產生 `rainfall_hourly` 表格（`station_no`、`obs_ts`、`rainfall_mm`）。
- 解析 `data/Meta_data.txt`，把欄位說明與特殊值對照寫入 `metadata_fields` 與 `metadata_special_values`，方便後續 SQL join。
- 完成後會輸出筆數與時間範圍摘要，可快速確認資料是否整批載入。
- 若 CSV 更新，只需重新執行一次即可刷新 DuckDB。

### `rainfall_analysis.ipynb`

```bash
uv run jupyter lab rainfall_analysis.ipynb   # 或使用 jupyter notebook
```

Notebook 包含下列模組化步驟，並為主要程式碼加上註解，方便追蹤：

- **資料連線與查核**：檢查資料表清單、抽樣 `rainfall_hourly`、查看 `metadata_*` 內容，確保建置結果正確。
- **DuckDB vs. pandas 對照**：用 SQL 與 pandas 分別計算測站統計、逐月雨量、全站最大 10 筆觀測，驗證兩端邏輯相符。
- **資料覆蓋率評估**：計算各測站應有的 175,320 筆紀錄與實際記錄比例，並將結果寫入 `station_coverage_summary`。
- **逐年極端雨量**：針對 1–72 小時多種 durations 進行滑動視窗累積，輸出 `annual_maxima` 與寬表 `annual_maxima_wide`，再匯出 CSV/Parquet。
- **品質檢核與視覺化**：檢查累積雨量是否隨 duration 單調遞增，並繪製指定測站的年度極值趨勢線。

Notebook 會直接使用 `rainfall.duckdb`，若找不到檔案請先執行前述建置腳本。輸出檔案預設存於 `data/` 目錄，可作為後續分析或分享。
