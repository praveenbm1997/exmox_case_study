# Exmox Pipeline

## Folder Structure

```
pipeline/
├── 01_bronze_to_silver.ipynb   # Current: Bronze ingestion + Silver transform (DQ gated)
├── 02_silver_to_gold.ipynb     # Current: Gold aggregation with lookback window
├── transforms.py               # Standalone transform functions (importable by pytest)
├── databricks.yml              # Declarative Automation Bundle (CI/CD)
├── SOLUTION.md                 # Full solution documentation
├── README.md                   # This file
├── tests/
│   └── test_transforms.py      # 17 pytest tests for transforms.py
└── legacy/                     # Old single-layer notebooks (move these here)
    ├── 01_bronze_ingestion.ipynb
    ├── 02_silver_transform.ipynb
    ├── 03_gold_aggregation.ipynb
    ├── 04_backfill.ipynb
    └── 05_governance.ipynb
```

## Current Pipeline (v2)

Two notebooks with a shared config module (`00_common` in `../Exploration_notebooks/`):

1. **`01_bronze_to_silver`** — Auto Loader ingestion, silver transformations with DQ gates
   (12 error + 5 warning rules), flags anomalies (late, pre-install, orphan offer), logs rejects
2. **`02_silver_to_gold`** — Daily country/platform aggregation with `LOOKBACK_DAYS = 6` for late data

### Key Files

| File | Purpose |
|---|---|
| `transforms.py` | Pure transform functions (DataFrame in/out). Imported by notebook and pytest |
| `tests/test_transforms.py` | 17 pytest tests: offers, installs, user_profile, events |
| `databricks.yml` | DAB bundle: pytest gate → bronze-to-silver → silver-to-gold + manual backfill |

### Shared Module

`00_common` (in `../Exploration_notebooks/`) provides: `Config`, `tbl()`, `Rule`/`Ctx`/`run_rules()`,
`run_tests()`/`df()`, `Layout`/`writer()`/`optimize()`, `configure_spark()`.

## Legacy Pipeline (v1)

Old single-layer notebooks using `exmox.silver.silver_*` table names (with prefix):
`01_bronze_ingestion`, `02_silver_transform`, `03_gold_aggregation`, `04_backfill`, `05_governance`.

Move these to `legacy/` to keep the folder clean.

## How to Run

### Via DAB
```bash
databricks bundle deploy --target dev
databricks bundle run exmox_pipeline
```

### Via Notebook
1. Run `01_bronze_to_silver` (widgets: full_rebuild, layout, dq_scope, run_ingest, run_tests, optimize)
2. Run `02_silver_to_gold` (widget: full_rebuild)

### Tests
```bash
pytest tests/test_transforms.py -v
```
