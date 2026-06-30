# Operations

## Overview

| Attribute | Value |
|-----------|-------|
| **Project** | myFinData — financial data pipeline |
| **Primary language** | Python 3.8+ |
| **Data sources** | Yahoo Finance (yfinance), Interactive Brokers (IB TWS), Tiingo, US Federal Reserve |
| **Database** | MySQL 8.0 (DigitalOcean Managed DB) |
| **Deployment targets** | On-prem VPS (SFO3) — all cron jobs run here |
| **Notifications** | Python logging → `./Ops/logging/*.log`; cron stdout/stderr → `/tmp/*.txt` |

---

## Installation

### Prerequisites

- Python 3.8+ with `pip`, `virtualenv`
- MySQL 8.0 client libraries
- (Optional) Docker for prediction jobs (NVIDIA/CUDA base)

### Local Setup

```bash
# 1. Clone the repository
git clone https://github.com/thomas-choi/myFinData.git
cd myFinData

# 2. Create a virtual environment
virtualenv venv -p python3.8
source venv/bin/activate

# 3. Install Python dependencies
pip install -r requirements.txt
```

### Requirements File

The project uses three requirement files:

| File | Purpose |
|------|---------|
| `requirements.txt` | Core pipeline (pandas, yfinance, SQLAlchemy, PyMySQL, dotenv) |
| `requirement_kafka.txt` | Kafka producer dependencies |
| `requirements_nb.txt` | Jupyter notebook dependencies |

### Environment Configuration

Copy the sample env file and fill in credentials:

```bash
cp Prod_config/Stk_eodfetch_sample.env Prod_config/Stk_eodfetch.env
```

Key environment variables:

| Variable | Description |
|----------|-------------|
| `DBHOST` | MySQL host (DigitalOcean DB cluster) |
| `DBPORT` | MySQL port (default 25060 for DO) |
| `DBUSER` | MySQL username |
| `DBPWD` | MySQL password |
| `DBMKTDATA` | Database name (default: `GlobalMarketData`) |
| `TBLDLYPRICE` | Daily price table name (default: `histdailyprice6` or `histdailyprice7`) |
| `TBLOPTCHAIN` | Options chain table name (default: `OptionChains`) |
| `VENDOR` | Data vendor: `yfinance` (default) or `tiingo` |
| `PROD_LIST_DIR` | Path to directory containing symbol-list CSVs |
| `TIIKEY1` / `TIIKEY2` | Tiingo API keys (two for rate-limit rotation) |

---

## Cloud Infrastructure

### 2. On-prem VPS (SFO3) — Primary Deployment

- **Purpose:** All cron jobs, IB TWS Gateway, Kafka, data processing.
- **IP:** `47.106.136.162`
- **User:** `thomas`
- **Project root:** `/home/thomas/projects/myFinData`
- **IB TWS Gateway port:** `7496` (paper) / `7497` (live)
- **Virtualenvs:**
  - `~/env/myFinData` — legacy EOD fetch
  - `~/env/myFinData.v1` — options chain (fallback)
  - `~/env/myFinData.v2` — options chain PM, extended EOD fetch (primary)

---

## Scheduled Jobs (Cron)

All times are **US/Eastern** (local VPS timezone). Only two cron jobs are active:

| Time (ET) | Crontab Entry | Script | Description |
|-----------|---------------|--------|-------------|
| **9:10 PM** (21:10) | `10 21 * * 1-5` | `eoddata_ext.sh` | Daily OHLC fetch using master symbol list from DB; logs to `/tmp/eod_extlog.txt` |
| **9:40 PM** (21:40) | `40 21 * * 1-5` | `optchain-PM.sh` | Post-market options chain snapshot (PM section); logs to `/tmp/optPMlog.txt` |

### Active Cron Tab

```cron
10 21 * * 1-5 /home/thomas/projects/myFinData/Ops/eoddata_ext.sh > /tmp/eod_extlog.txt 2>&1
40 21 * * 1-5 /home/thomas/projects/myFinData/Ops/optchain-PM.sh > /tmp/optPMlog.txt 2>&1
```

Other scripts (`eod_usrate.sh`, `options_features.sh`, `eod_volcones.sh`, `optchain-AM.sh`, `apply_gmodel.sh`, Docker prediction) exist in the repository but are **not currently scheduled** — they must be invoked manually or re-enabled via `crontab -e` if needed.

### Shell Scripts

Each shell script sets `projDIR` to its own directory, `cd`s there, and invokes the Python module from the appropriate virtualenv:

```bash
#!/bin/bash
projDIR=`dirname "$0"`
cd $projDIR
mycmd="$projDIR/optchain_fetch.py"
$HOME/env/myFinData.v2/bin/python $mycmd -S PM -U -m
```

Available flags (consistent across scripts):

| Flag | Long Form | Description |
|------|-----------|-------------|
| `-D` | `--Date` | Override date (YYYY-MM-DD) |
| `-S` | `--Sect` | Section `AM` / `PM` |
| `-c` | `--check` | Check-only mode (dry run) |
| `-f` | `--force` | Force run even on weekends |
| `-t` | `--test` | Use `test_list.csv` (single symbol) |
| `-U` | `--Upload` | Upload results to DB |
| `-m` | `--master` | Use master symbol list from DB |
| `-b` | `--batchsize` | Batch size for bulk operations |

---

## Database

### Vendor

**DigitalOcean Managed MySQL 8.0** (SFO3 region).

### Database: `GlobalMarketData`

### Tables

#### `histdailyprice6` (or `histdailyprice7`)
Daily OHLC prices.

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

#### `OptionChains`
Full option chain data for all tracked symbols.

| Column | Type | Description |
|--------|------|-------------|
| `Date` | date | Market date |
| `Section` | varchar(10) | `AM` or `PM` snapshot |
| `UnderlyingSymbol` | varchar(10) | Ticker |
| `strike` | float | Option strike price |
| `Expiration` | date | Option expiration |
| `OptionType` | varchar(4) | `call` or `put` |
| `contractSymbol` | varchar(50) | Yahoo-finance contract ID |
| `lastPrice` | float | Last traded price |
| `bid` | float | Current bid |
| `ask` | float | Current ask |
| `change` | float | Price change |
| `percentChange` | float | % change |
| `volume` | int | Trading volume |
| `openInterest` | int | Open interest |
| `impliedVolatility` | float | Implied volatility |
| `inTheMoney` | bool | ITM flag |
| `contractSize` | int | Contract multiplier (100) |
| `currency` | varchar(10) | Denomination |
| `UnderlyingPrice` | float | Spot price at snapshot |

#### `option_features`
Derived features per `(Date, Symbol, OptionType)`.

| Column | Description |
|--------|-------------|
| `MaxOI` | Max open interest value |
| `MaxOIStrike` | Strike with max OI |
| `MaxOIExpire` | Expiration of max-OI contract |
| `MaxOIImpVol` | IV of max-OI contract |
| `MaxVol` | Max volume value |
| `MaxVolStrike` | Strike with max volume |
| `MaxVolExpire` | Expiration of max-volume contract |
| `MaxVolImpVol` | IV of max-volume contract |
| `PutCallratio` | Put volume / Call volume |

#### `USRates`
Daily US Treasury and interest rates (scraped from Federal Reserve H.15).

#### `GMDailyOutputs`
LSTM prediction outputs per symbol per date.

#### `DailyPerformance_p1`
Daily prediction performance metrics.

#### `options_snapshot`
Real-time options snapshots from Interactive Brokers.

### Symbol Lists

Symbols are sourced from two mechanisms:

1. **DB stored procedure** — `GlobalMarketData.current_symbols_V4` (used when `--master` flag is passed).
2. **CSV files** in `Product_List/`:

| File | Contents |
|------|----------|
| `stock_list.csv` | ~130 US common stocks |
| `etf_list.csv` | ~20 ETFs |
| `crypto_list.csv` | BTC-USD, ETH-USD |
| `us-cn_stock_list.csv` | ~50 US-listed Chinese ADRs |
| `test_list.csv` | Single test symbol |
| `web_stock_list.csv` | Subset displayed on web frontend |
| `stock_exchange.csv` | Ticker→Exchange mapping |

### Stored Procedures

- `GlobalMarketData.current_symbols_V4` — returns the full active symbol universe.

---

## Python Virtual Environments

The repository contains multiple venvs (not committed to git):

| Directory | Python | Purpose |
|-----------|--------|---------|
| `venv/` | 3.8 | General development |
| `venv-1/` | 3.8 | Staging/fallback |
| `venv-1nb/` | 3.8 | Notebook kernel |
| `venv-nb/` | 3.8 | Notebook kernel |
| `venv.old/` | 3.8 | Legacy (deprecated) |

---

## Config Files

| File | Purpose | Status |
|------|---------|--------|
| `Prod_config/Stk_eodfetch.env` | Production credentials | **Committed (contains real passwords)** |
| `Prod_config/Stk_eodfetch_sample.env` | Template (blanks) | Safe to share |
| `Ops/.env` | Local override for handler scripts | Contains Tiingo keys |

---

## Troubleshooting

### Common Issues

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| `FutureWarning: concatenating bool-dtype...` | pandas bool/numeric dtype mismatch | Cast bool columns to int before concat (see `optchain_fetch.py` fix) |
| Options chain empty | yfinance bug or no options listed | Check `yfinance` version ≥0.2.37; verify ticker has options |
| DB connection timeout | DO firewall or stale SSH tunnel | Verify `DBHOST` IP whitelist; restart tunnel via `setDBSSH()` |
| "Jobs must be ran from Monday to Friday" | Weekend run without `-f` | Add `-f` flag for testing on weekends |
| yfinance download hangs | Rate-limit or symbol no longer valid | Add retry logic; check symbol still trades |

### Logging

All scripts log to `./Ops/logging/` with date-stamped filenames:

```
Ops/logging/
├── optchain_2025-06-30.log
├── eoddata_2025-06-30.log
├── eod_volcones2025-06-30.log
├── opt_features_2025-06-30.log
└── apply_gmodel_2025-06-30.log
```

Set `DEBUG=debug` in the `.env` file (or `myDebug=Debug` in the prod env) to enable verbose logging.

---

## Data Flow Summary

```
                    ┌──────────────────────────────┐
                    │    Data Sources               │
                    │  yfinance  │  IB TWS  │ Tiingo│
                    └─────┬──────┴────┬─────┴──┬───┘
                          │           │        │
                    ┌─────▼───────────▼────────▼───┐
                    │     Fetch Scripts            │
                    │  eoddata_handler.py          │
                    │  optchain_fetch.py           │
                    │  opt_snapshot.py             │
                    │  eod_usrate.py               │
                    └─────┬───────────────────────┘
                          │
                    ┌─────▼───────────────────────┐
                    │    MySQL (DigitalOcean)      │
                    │  GlobalMarketData            │
                    │  ├─ histdailyprice6         │
                    │  ├─ OptionChains            │
                    │  ├─ option_features         │
                    │  ├─ USRates                 │
                    │  └─ GMDailyOutputs          │
                    └─────┬───────────────────────┘
                          │
                    ┌─────▼───────────────────────┐
                    │    Downstream Consumers      │
                    │  │ LSTM Prediction (Docker) │
                    │  │ Volatility Cones (PDFs)  │
                    │  │ Web Dashboard (Django)   │
                    │  │ Kafka Streams            │
                    └─────────────────────────────┘
```
