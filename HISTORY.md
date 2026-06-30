# History

All notable changes to this project are documented here.

---

## 2025-05-05 — Merge origin/main with local changes
- **Goal:** Sync remote origin/main (Feb 25) with local commits (May 2).
- **Root cause:** Divergence between GitHub remote and local development environment (PythonAnywhere + VPS).
- **Implementation detail:** `git merge origin/main` resolved conflicts manually.
- **Related files:** — (merge commit)

---

## 2025-05-04 — Upgrade yfinance to 0.2.58; use `current_symbols_V4` stored procedure
- **Goal:** Fix Yahoo Finance download issues; switch symbol source to DB.
- **Root cause:** yfinance API changes broke downloads; static CSV lists became unmaintainable.
- **Implementation detail:**
  - Bump `yfinance==0.2.58` in `requirements.txt`.
  - `load_symbols("master_db_list")` now calls `CALL GlobalMarketData.current_symbols_V4` instead of reading a CSV.
  - Updated `eoddata_handler.py` and `optchain_fetch.py` to use the new symbol source.
- **Related files:** `requirements.txt`, `Ops/dataUtil.py`, `Ops/eoddata_handler.py`, `Ops/optchain_fetch.py`

---

## 2025-02-26 — Modify shell scripts for new package versions
- **Goal:** Point `optchain-PM.sh` and `eoddata_ext.sh` to the correct virtualenv (`myFinData.v2`).
- **Root cause:** New venv created after package upgrades; old venv path became stale.
- **Implementation detail:** Changed shebang/python path in shell wrappers from `myFinData` to `myFinData.v2`.
- **Related files:** `Ops/eoddata_ext.sh`, `Ops/optchain-PM.sh`

---

## 2025-02-25 — Add symbol/exchange mapping file
- **Goal:** Introduce `stock_exchange.csv` as a lookup table for ticker → exchange.
- **Root cause:** yfinance multi-index columns lost exchange info; needed a mapping for DB storage.
- **Implementation detail:** `load_symbols_dict()` reads the CSV and returns `{symbol: exchange}`.
- **Related files:** `Product_List/stock_exchange.csv`

---

## 2025-02-25 — EOD closing price refactor
- **Goal:** Reliable daily OHLC fetch via yfinance with per-symbol caching.
- **Root cause:** Previous EOD pipeline was fragile (depended on `eoddata` client library).
- **Implementation detail:**
  - New `common_fetch_eod()` in `eoddata_handler.py` fetches per symbol, caches to `{vendor}/{sym}_{start}_{end}.csv`, then batch-inserts to MySQL.
  - Supports both `yfinance` and `tiingo` backends via `VENDOR` env var.
- **Related files:** `Ops/eoddata_handler.py`, `Ops/eoddata_ext_fetch.py`, `Ops/eoddata_fetch.py`

---

## 2025-01-30 — Fix yfinance disconnect / timeouts
- **Goal:** Handle yfinance transient failures gracefully.
- **Root cause:** yfinance `download()` would hang or raise on certain symbols.
- **Implementation detail:** Added retry logic with configurable delay in `yf_download()`; switched to symbol-by-symbol download instead of bulk.
- **Related files:** `Ops/eoddata_ext_fetch.py`, `Ops/optchain_fetch.py`

---

## 2024-05-28 — Options chain shell scripts use master DB list
- **Goal:** `optchain-AM.sh` and `optchain-PM.sh` should process the full tracked universe.
- **Root cause:** Scripts were hardcoded to CSV lists; new symbols weren't picked up automatically.
- **Implementation detail:** Added `-m` / `--master` flag in `optchain_fetch.py` that reads symbols from `GlobalMarketData.current_symbols_V4`.
- **Related files:** `Ops/optchain_fetch.py`, `Ops/optchain-AM.sh`, `Ops/optchain-PM.sh`

---

## 2024-05-27 — Load symbols from database; per-symbol DB upload
- **Goal:** Move symbol management into MySQL; store options per symbol to avoid huge transactions.
- **Root cause:** Uploading all symbols in one `pd.concat` → `to_sql()` caused timeouts on large data.
- **Implementation detail:**
  - New `load_symbols("master_db_list")` calls a stored procedure.
  - Each symbol's option chain is uploaded individually inside its loop.
- **Related files:** `Ops/dataUtil.py`, `Ops/optchain_fetch.py`

---

## 2024-05-27 — Upgrade yfinance to 0.2.37
- **Goal:** Restore option-chain download functionality.
- **Root cause:** yfinance ≤0.2.31 had a bug in `option_chain()` that caused empty DataFrames.
- **Implementation detail:** Bumped version in `requirements.txt`; no code changes needed.
- **Related files:** `requirements.txt`

---

## 2024-04-24 — Reorganize symbol list generation
- **Goal:** Centralize symbol-list loading in `dataUtil.py`.
- **Root cause:** Each script had its own CSV-reading logic, making maintenance error-prone.
- **Implementation detail:** `get_Symbollist()` / `load_symbols()` now serve all callers; `PROD_LIST_DIR` env var points to `Product_List/`.
- **Related files:** `Ops/dataUtil.py`, `Ops/eoddata_fetch.py`, `Ops/optchain_fetch.py`

---

## 2024-02-29 — IB TWS API integration for options snapshots
- **Goal:** Stream real-time options quotes from Interactive Brokers.
- **Root cause:** Needed real-time bid/ask/IV data not available from yfinance.
- **Implementation detail:**
  - New `IBClient` class wrapping `ibapi` (`EWrapper`/`EClient`).
  - `opt_snapshot.py` reads trade signals from DB, requests market data, and stores snapshots.
  - `ibAPIClient.py` handles tick-by-tick price/size/option-computation events.
- **Related files:** `Ops/ibAPIClient.py`, `Ops/opt_snapshot.py`, `Ops/ibapi/`

---

## 2023-09-18 — Options features with volume analytics
- **Goal:** Compute put/call ratios, max-OI strikes, and max-volume strikes per date.
- **Root cause:** Raw option-chain data is too granular; needed derived features for signal generation.
- **Implementation detail:**
  - `genOptionsFeaturesV2()` groups by `(Date, OptionType)`, finds strikes with highest OI/volume, and computes Put/Call ratio.
  - Output stored to `option_features` table.
- **Related files:** `Ops/options_features.py`

---

## 2023-09-04 — C++ DDS client for low-latency data
- **Goal:** Build a native C++ client for direct data-feed access.
- **Root cause:** Python was too slow for real-time market-data ingestion.
- **Implementation detail:** Added `cplusplus/` directory with `DDSClient`, `UNQueue`, `mmapStorage`, `Ticker`, `MarketImage` compiled via `Makefile`.
- **Related files:** `Ops/Makefile`, `Ops/cplusplus/*`

---

## 2023-08-28 — Options chain collection pipeline
- **Goal:** Download full option chains (calls + puts) for all tracked symbols via yfinance.
- **Root cause:** Manual options data collection was infeasible.
- **Implementation detail:**
  - `option_chains()` iterates all expirations, concatenates calls/puts, enriches with underlying price.
  - `ProcessOptions()` filters by OI quartile, picks top-OI strikes, and uploads to DB.
- **Related files:** `Ops/optchain_fetch.py`

---

## 2023-06-05 — Kafka integration for US stock snapshots
- **Goal:** Publish real-time stock data to Kafka topics.
- **Root cause:** Needed a streaming data layer for downstream consumers.
- **Implementation detail:**
  - `US-Kafka-prod.py` reads from DB and publishes to Kafka.
  - `kafka.yml` defines Zookeeper + Kafka (bitnami images) via Docker Compose.
- **Related files:** `Ops/US-Kafka-prod.py`, `Ops/kafka.yml`

---

## 2023-05-09 — GCP migration preparation
- **Goal:** Make the pipeline deployable on Google Cloud Platform.
- **Root cause:** Migration from bare-metal VPS to GCP for better reliability.
- **Implementation detail:** Environment-agnostic config loading; Docker image published to Docker Hub.
- **Related files:** `Dockerfile-dev9.1.tf2-py3`

---

## 2023-05-02 — Docker image for stock prediction
- **Goal:** Create a reproducible environment for daily LSTM prediction jobs.
- **Root cause:** TensorFlow + CUDA dependencies were hard to maintain across machines.
- **Implementation detail:**
  - `Dockerfile-dev9.1.tf2-py3` based on `nvcr.io/nvidia/tensorflow:21.12-tf2-py3`.
  - Installs PyTorch, scikit-learn, arch (GARCH), Keras-Tuner, TA-Lib.
  - `docker_jobs.sh` runs the container with host-volume mounts.
- **Related files:** `Dockerfile-dev9.1.tf2-py3`, `Ops/docker_jobs.sh`

---

## 2023-04-XX — Daily stock prediction pipeline
- **Goal:** LSTM-based daily price-direction prediction with MySQL output.
- **Root cause:** Manual analysis didn't scale; needed automated daily forecasts.
- **Implementation detail:**
  - `daily_gap_model.py` — prepares sliding-window training data from `AdjClose` differences.
  - `Apply_gmodel.py` — loads saved `.h5` models, predicts, stores to `GMDailyOutputs`.
  - Web output generated for `web_stock_list` symbols.
- **Related files:** `Ops/daily_gap_model.py`, `Ops/Apply_gmodel.py`

---

## 2022-XX-XX — US Rates download
- **Goal:** Scrape daily US Treasury / interest rates from the Federal Reserve.
- **Root cause:** Rates data required for volatility models and option pricing.
- **Implementation detail:**
  - `eod_usrate.py` scrapes `https://www.federalreserve.gov/releases/h15/`, parses HTML table, stores to `USRates` table.
- **Related files:** `Ops/eod_usrate.py`

---

## 2022-XX-XX — Volatility cones
- **Goal:** Compute and plot Yang-Zhang volatility cones for tracked symbols.
- **Root cause:** Needed historical vol ranges for options analysis.
- **Implementation detail:**
  - `Volatility.py` implements Yang-Zhang estimator.
  - `eod_volcones.py` computes min/max/median/quantile vol over 30/60/90/120-day windows and saves PDFs.
- **Related files:** `Ops/Volatility.py`, `Ops/eod_volcones.py`

---

## 2021 — Project inception
- **Goal:** Collect daily OHLC stock data and store in MySQL.
- **Root cause:** No centralized, queryable store for financial data analysis.
- **Implementation detail:**
  - Initial `eoddata_fetch.py` using `eoddata-client` library for NYSE/AMEX/NASDAQ.
  - `histdailyprice6` table with primary key `(Date, Symbol, Exchange)`.
  - Virtualenv-based deployment on PythonAnywhere and a bare-metal VPS.
- **Related files:** `Ops/eoddata_fetch.py`, `Ops/eoddata_histdailyprice.sql`
