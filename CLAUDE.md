# CLAUDE.md

## Mandatory Rules for All Changes

When modifying this repository, you **must** follow these rules:

### 1. Update HISTORY.md

Every change **must** be logged in `HISTORY.md`. The entry should include:

- **Date** in `YYYY-MM-DD` format
- **Goal** — what problem was solved or feature added
- **Root cause** — why the change was needed
- **Implementation detail** — what was done and how
- **Related files** — list of all modified files

### 2. Update OPERATIONS.md

If the change affects any of the following, update `OPERATIONS.md`:

- New or modified **cron jobs / scheduled tasks**
- New or modified **environment variables**
- Changes to **virtualenv** or **Docker** setup
- Changes to **cloud infrastructure** or **deployment targets**
- New or modified **shell scripts**
- Changes to **command-line flags** or **invocation methods**

### 3. Update TECHNICAL-DESIGN.md

If the change affects any of the following, update `TECHNICAL-DESIGN.md`:

- New or modified **database tables** or **schema** changes
- New **Python packages** or **technology additions**
- Changes to **architectural patterns** or **data flow**
- New **modules** or **significant refactors**
- Changes to **key design decisions**

### 4. Code Quality Standards

- **FutureWarning / DeprecationWarning:** Must be fixed, not suppressed. If you see one, fix it.
- **Error handling:** Functions that call external APIs or DBs must have `try/except` with logging.
- **Logging:** All scripts must use Python `logging` module, not `print()`.
- **Docstrings:** Public functions should have docstrings explaining purpose and parameters.
- **No hardcoded paths** — use `environ.get()` or join relative to `PROD_LIST_DIR` / `DATADIR`.
- **Timezone awareness** — all date comparisons must use `US/Eastern` timezone via `nowbyTZ()`.

### 5. Working with DataFrames

- **Always explicitly pass `axis=`** to `pd.concat()` and similar functions. `axis=0` for row stacking.
- **Cast bool columns to `int`** before `pd.concat()` to avoid FutureWarning.
- **Handle multi-index columns** from yfinance after v0.2.54: check `sDF.columns.nlevels > 1` and flatten.
- **Check for empty DataFrames** before calls to `.iloc[0]` or `.head()`.

### 6. Working with Databases

- Use `StoreEOD()` with `if_exists='append'` to insert rows.
- Use `get_Max_date()` / `get_Max_Options_date()` to check what data already exists before inserting.
- Symbol lists: prefer `master_db_list` (from stored procedure) over CSV files for production.
- Always close SSH tunnels when done (`setDBSSH()` → `sTunnel.close()`).

### 7. Working with yfinance

| Version | Notes |
|---------|-------|
| ≥0.2.58 | Current (2025). `yf.download()` returns MultiIndex columns by default; use `auto_adjust=False`. |
| ≥0.2.37 | Required for option chains to work. |
| <0.2.54 | Pandas 1.x compatible. |

- After `yf.download()`, always reset index and flatten MultiIndex columns if present.
- `option_chain(expiration)` may return `inTheMoney` as bool in one DataFrame and numeric in another — cast before concat.

### 8. Project Structure

```
myFinData/
├── Ops/              # All pipeline scripts
│   ├── dataUtil.py      # Shared utility module
│   ├── SQLDB.py         # OO DB wrapper (legacy)
│   ├── optchain_fetch.py     # Options chain collection
│   ├── eoddata_handler.py    # Daily OHLC (yfinance)
│   ├── eoddata_ext_fetch.py  # Daily OHLC (multi-vendor)
│   ├── eoddata_fetch.py      # Daily OHLC (legacy eoddata)
│   ├── options_features.py   # Options-derived features
│   ├── Apply_gmodel.py       # LSTM prediction
│   ├── daily_gap_model.py    # Model training helpers
│   ├── eod_volcones.py       # Volatility cones PDFs
│   ├── eod_usrate.py         # US Treasury rates scrape
│   ├── opt_snapshot.py       # IB TWS real-time snapshots
│   ├── ibAPIClient.py        # IB API client wrapper
│   ├── Volatility.py         # Yang-Zhang estimator
│   ├── DDSClient.py          # Data Distribution client
│   ├── US-Kafka-prod.py      # Kafka producer
│   ├── eod_optStrikes.py     # Options strike charting
│   ├── *.sh                  # Cron shell wrappers
│   ├── Makefile              # C++ build
│   └── kafka.yml             # Kafka Docker Compose
├── Prod_config/       # Environment config files
├── Product_List/      # Symbol CSV files
├── requirements.txt   # Python dependencies
└── Dockerfile-*       # Docker images
```

### 9. Before Committing

- [ ] HISTORY.md updated with the change
- [ ] OPERATIONS.md updated if ops/setup changed
- [ ] TECHNICAL-DESIGN.md updated if architecture/schema changed
- [ ] No `FutureWarning` or `DeprecationWarning` in logs
- [ ] Env vars documented in OPERATIONS.md
- [ ] Any new script has a corresponding `.sh` shell wrapper if meant for cron
