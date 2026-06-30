# Technical Design

## Architecture

### System Overview

myFinData is a modular financial data pipeline composed of independent Python scripts, each responsible for a specific data domain. There is no central orchestrator — scripts are invoked individually by cron (or manually) and communicate exclusively through the shared MySQL database.

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Data Ingestion  │────▶│  Storage Layer   │────▶│  Data Consumers   │
│                  │     │                  │     │                  │
│ yfinance        │     │  MySQL 8.0      │     │ LSTM Prediction  │
│ IB TWS API      │     │  (DigitalOcean) │     │ Volatility Cones │
│ Tiingo API      │     │  CSV Cache      │     │ Web Dashboard    │
│ Fed Reserve     │     │  (local files)  │     │ Kafka Producer   │
│ (scrape)        │     │                  │     │ Options Features │
└─────────────────┘     └──────────────────┘     └──────────────────┘
```

### Design Principles

1. **No framework lock-in** — Each Python script is self-contained; no shared state beyond the DB.
2. **Fail-safe by default** — Only weekday execution; retries on transient failures; check-only (`-c`) mode.
3. **Idempotent writes** — Primary keys prevent duplicate rows; `append` mode in `to_sql()`.
4. **Caching** — CSV files on disk avoid re-downloading the same data.
5. **Configurable backends** — `VENDOR` env var (`yfinance` | `tiingo` | `eoddata`) lets you swap data providers.

---

## Technologies

### Languages

| Language | Usage |
|----------|-------|
| **Python 3.8+** | Primary pipeline language (all data ingestion, processing, DB interaction) |
| **C++20** | Low-latency DDS client (`cplusplus/` — experimental, not in production use) |
| **Bash** | Shell wrappers for cron invocation, Docker orchestration |
| **SQL (MySQL)** | Stored procedures, schema definitions, analytical queries |

### Python Packages (Core)

| Package | Version | Purpose |
|---------|---------|---------|
| `yfinance` | ≥0.2.58 | Yahoo Finance data (OHLC, option chains) |
| `pandas` | 1.5.3 | Data manipulation (DataFrame, concat, groupby) |
| `numpy` | 1.26.4 | Numerical arrays, diff, log returns |
| `SQLAlchemy` | 1.4.46 | ORM-less DB engine (`create_engine`, `to_sql`, `read_sql`) |
| `PyMySQL` | 1.0.2 | MySQL Python driver |
| `python-dotenv` | 0.21.1 | `.env` file loading |
| `python-dateutil` | 2.8.2 | Date range generation (`rrule`) |
| `pytz` | — | Timezone conversion (→ US/Eastern) |
| `sshtunnel` | 0.4.0 | SSH tunnel for restricted DB access |
| `matplotlib` | 3.7.0 | Volatility cone plotting |
| `statsmodels` | 0.13.5 | Time-series decomposition (STL) |
| `plotly` | 5.13.0 | Interactive option-strike charts |
| `beautifulsoup4` | — | HTML scraping (Federal Reserve rates) |
| `requests` | — | HTTP requests |

### Machine Learning Packages

| Package | Version | Purpose |
|---------|---------|---------|
| `tensorflow` | 2.7.0 (Docker) | LSTM model definition + training |
| `keras` | 2.7.0 | High-level neural network API |
| `scikit-learn` | latest | `mean_squared_error` metric |
| `arch` | 5.0.1 | GARCH volatility models (not actively used) |

### Infrastructure

| Component | Technology | Detail |
|-----------|------------|--------|
| **Database** | MySQL 8.0 | DigitalOcean Managed DB, SFO3, 1 GB RAM / 1 vCPU |
| **Scheduling** | Cron (PythonAnywhere) | Jobs run Mon–Fri, US/Eastern |
| **Containerization** | Docker | NVIDIA TensorFlow base image for GPU prediction |
| **Streaming** | Apache Kafka | Bitnami Kafka 3.4.0 via Docker Compose |
| **Real-time feeds** | Interactive Brokers TWS | IB API (v10.19+) via TWS Gateway |

---

## Database Schema

### Entity-Relationship Overview

There are **no foreign keys** — tables are linked by convention on `(Date, Symbol)`.

### Table: `histdailyprice6`

```sql
CREATE TABLE `histdailyprice6` (
  `Date` date NOT NULL,
  `Symbol` varchar(45) NOT NULL,
  `Exchange` varchar(45) NOT NULL,
  `Close` float DEFAULT NULL,
  `Open` float DEFAULT NULL,
  `High` float DEFAULT NULL,
  `Low` float DEFAULT NULL,
  `Volume` bigint DEFAULT NULL,
  `AdjClose` float DEFAULT NULL,
  PRIMARY KEY (`Date`,`Symbol`,`Exchange`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

- **Primary key:** `(Date, Symbol, Exchange)` — prevents duplicates on re-fetch.
- **Index strategy:** PK covers the most common query pattern `WHERE Symbol = ? AND Date BETWEEN ? AND ?`.
- **Why `histdailyprice6`?** Historical naming — v6 reflects the 6th schema iteration. Some env files point to `histdailyprice7`.

### Table: `OptionChains`

Created dynamically by `df.to_sql()` — no explicit DDL in the repo. The column layout is determined by the `nColumns` list in `optchain_fetch.py`:

```python
nColumns = [
    'Date', 'Section', 'UnderlyingSymbol', 'strike', 'Expiration',
    'OptionType', 'contractSymbol', 'lastTradeDate', 'lastPrice',
    'bid', 'ask', 'change', 'percentChange', 'volume', 'openInterest',
    'impliedVolatility', 'inTheMoney', 'contractSize', 'currency',
    'UnderlyingPrice'
]
```

- **Partitioning:** By `(Date, Section)` — each run inserts ~20K–100K rows.
- **inTheMoney:** Stored as `TINYINT(1)` (pandas bool → MySQL bool).

### Table: `option_features`

Derived from `OptionChains` via `options_features.py`. Columns:

```
Date, Symbol, OptionType, volume, last, MaxOI, MaxOIStrike,
MaxOIExpire, MaxOIImpVol, MaxVol, MaxVolStrike, MaxVolExpire,
MaxVolImpVol, PutCallratio
```

- Each row represents one `(Date, Symbol, OptionType)` combination (two rows per symbol per date).

### Table: `USRates`

Created by `eod_usrate.py` — stores daily Treasury yields and interbank rates scraped from the Federal Reserve H.15 release. Column names are normalized from the HTML table (e.g., `TBond_10_year`, `Federal_funds`, `Bank_prime_loan`).

### Table: `GMDailyOutputs`

Prediction outputs from `Apply_gmodel.py`:

```
Date, Symbol, Exchange, YTest, Ypred, Ypred-L, predClose, logRet
```

### Table: `options_snapshot`

Real-time snapshots from IB TWS API:

```
Symbol, PnC, Strike, Expiration, lastPrice, bid, ask, volume,
openInterest, impliedVolatility, undPrice, PClose, timestamp
```

---

## Module Architecture

### `Ops/dataUtil.py` — Shared utilities

The **central utility module** used by almost every script. Functions:

| Function | Purpose |
|----------|---------|
| `get_DBengine()` | Returns (and caches) a SQLAlchemy engine |
| `setDBSSH()` | Configures SSH tunnel → local port forwarding |
| `StoreEOD()` | `df.to_sql()` wrapper with append mode |
| `load_df()` | Load OHLC data from DB (with optional CSV caching) |
| `load_symbols(listname)` | Returns symbol list; supports `master_db_list` (stored proc) or CSV files |
| `get_Max_date()` | Query `MAX(Date)` from a table |
| `get_Max_Options_date()` | Query `MAX(Date)` + `Section` for options chains |
| `nowbyTZ(tzName)` | Current time in specified timezone (e.g., `US/Eastern`) |
| `ExecSQL()` | Execute arbitrary SQL (used for truncate, drops) |

### `Ops/SQLDB.py` — Object-oriented DB wrapper

A class-based version of `dataUtil.py` that reads config from a specific `.env` file at construction. Used by `Apply_gmodel.py` (prediction pipeline) where two separate DB connections are needed.

### `Ops/optchain_fetch.py` — Options chain collection

**Pipeline:**
1. `load_symbols(listname)` → list of tickers
2. For each ticker:
   - `option_chains(ticker)` → calls `yfinance.Ticker(ticker).option_chain(expiration)` for all expirations
   - Concatenates calls + puts, enriches with `UnderlyingSymbol`, `UnderlyingPrice`, `Expiration`
   - `filter_opt_chain()` → removes low-price options, filters by OI > 25th percentile
   - `pickTopOI()` → returns top 5 OI OTM puts and calls
3. Uploads all chains to `OptionChains` table (per-symbol `to_sql()`)

**Key decisions:**
- Bool columns cast to `int` before `pd.concat()` to avoid FutureWarning.
- `inTheMoney` is cast back to `bool` after all processing.
- AM/PM sections: AM runs before 1 PM EST (pre-close), PM after 1 PM (post-close).

### `Ops/eoddata_handler.py` — Daily OHLC fetch

**Pipeline:**
1. Reads symbols from master list or CSV
2. Per symbol: checks last DB date → fetches new data from yfinance → caches to CSV → batch-inserts to `histdailyprice6`
3. Handles multi-index column flattening and exchange mapping

### `Ops/options_features.py` — Options-derived signals

- Calls `GlobalMarketData.get_option_features` stored procedure
- `genOptionsFeaturesV2()` groups by `(Date, OptionType)` and computes:
  - Max-OI strike and its IV/expiration
  - Max-volume strike and its IV/expiration
  - Put/Call volume ratio
- Stores results to `option_features` table

### `Ops/Apply_gmodel.py` — LSTM prediction

- Loads trained `.h5` models from `Ops/model/{symbol}.h5`
- For each symbol:
  - Fetches 21-day sliding window of `AdjClose` differences
  - Predicts next-day price change via `model.predict()`
  - Computes `predClose` (predicted close price) and `logRet` (log return)
- Uploads results to `GMDailyOutputs` table
- Generates web-display CSV with UP/DOWN signals

### `Ops/eod_volcones.py` — Volatility cones

- Uses Yang-Zhang volatility estimator (`Volatility.py`)
- Computes min/max/median/25th/75th percentile vol over 30/60/90/120-day windows
- Generates PDF plots per symbol

---

## Key Design Decisions

### Why CSV caching alongside MySQL?

Each fetch script writes symbol-level CSV files (e.g., `yfinance/AAPL_2025-01-01_2025-06-30.csv`). These serve as:
1. **Offline cache** — re-running doesn't hit the API again.
2. **Debug medium** — easy to inspect raw data without querying the DB.
3. **Backup** — if DB is unavailable, data can be re-inserted from CSVs.

### Why per-symbol `to_sql()` instead of batch?

Early versions concatenated all symbols into one giant DataFrame and called `to_sql()` once. This caused timeouts and memory issues. Per-symbol inserts are slower but reliable.

### Why two DB utility modules (`dataUtil.py` + `SQLDB.py`)?

Historical evolution: `dataUtil.py` was written first as a module-level singleton. `SQLDB.py` was added later when the prediction pipeline needed simultaneous connections to two databases (market data + predictions). The class-based approach allowed per-instance config.

### Why both `eoddata_fetch.py` and `eoddata_ext_fetch.py`?

- `eoddata_fetch.py` was the original, using the `eoddata-client` library (now deprecated).
- `eoddata_ext_fetch.py` is the extended version supporting `yfinance` and `tiingo` backends.
- `eoddata_handler.py` is the latest simplified version for yfinance-only daily OHLC.

### Why pandas 1.5.3?

Pandas 2.x introduced breaking changes to datetime handling and `concat` behavior. The project pins 1.5.3 for stability. yfinance ≥0.2.54 is compatible with this version.

---

## Data Flow Diagrams

### Daily OHLC Flow

```
yfinance / Tiingo API
       │
       ▼
common_fetch_eod(symbol_list)
       │
       ├── per symbol: check Max(Date) in DB
       │   ├── if up-to-date → skip
       │   └── if not → yf.download(sym, last_date, today)
       │       ├── flatten multi-index columns
       │       ├── map Exchange from stock_exchange.csv
       │       ├── save to {vendor}/{sym}_{start}_{end}.csv (cache)
       │       └── append to DataFrame list
       │
       ▼
  pd.concat() → StoreEOD() → MySQL histdailyprice6
```

### Options Chain Flow

```
yfinance Ticker API
       │
       ▼
option_chains(ticker)
       │
       ├── for each expiration:
       │   ├── asset.option_chain(expiration) → (calls, puts)
       │   ├── cast bool columns to int
       │   ├── pd.concat([calls, puts])
       │   └── add 'Expiration' column
       │
       ▼
filter_opt_chain() → remove low-price, filter by OI quartile
       │
       ▼
pickTopOI() → top 5 OTM puts + calls by OI
       │
       ▼
StoreEOD() → MySQL OptionChains (per symbol)
```

---

## Security

### Credentials

- **Database password** is stored in plaintext in `Prod_config/Stk_eodfetch.env` (committed to repo — known issue).
- **Tiingo API keys** in `Ops/.env`.
- **IB TWS Gateway** IP and port in environment.

### Network

- DigitalOcean MySQL allows connections only from whitelisted IPs.
- SSH tunnel (`setDBSSH()`) provides an alternate access path from PythonAnywhere.

---

## Known Technical Debt

| Issue | Impact | Mitigation |
|-------|--------|------------|
| Credentials committed to git | Security risk | Switch to secret manager or `.env` entries in `.gitignore` |
| `eoddata-client` deprecated | Legacy code | Fully migrated to `yfinance`/`tiingo` |
| No formal testing | Regression risk | Add `pytest` suite for data-util functions |
| Hardcoded `VOL_CON_DIR` path | PythonAnywhere-specific | Make configurable via env var |
| Duplicate DB utility modules | Maintenance burden | Refactor `dataUtil.py` into class, deprecate `SQLDB.py` |
| No migration system for schema | Schema drift | Add Alembic or Flyway migrations |
| `Stk_eodfetch.env` in git | Credential leak risk | Add to `.gitignore`, use sample env for reference |
| `pd.concat` without axis param | Implicit default (axis=0) | Explicitly pass `axis=0` in all concat calls |
