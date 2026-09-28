# Exmox Data Engineering Task — Databricks

This repository contains two implementations of a bronze-to-gold ETL pipeline for a rewarded-offer platform, built on Databricks with Unity Catalog.

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- Four CSV files: `installs.csv`, `events.csv`, `offers.csv`, `user_profile.csv`

## Repository Structure

```
exmox_case_study/
├── SOLUTION.md                          # Full write-up: findings, architecture, deeper questions
├── README.md                            # This file
├── pipeline/                            # Notebook-based pipeline (original)
│   ├── 01_bronze_ingestion.py           # Auto Loader + S3 upsert into Delta tables
│   ├── 02_silver_transform.py           # Watermark MERGE, dedup, DQ rules, rejects
│   ├── 03_gold_aggregation.py           # Daily country/platform aggregation with MERGE
│   ├── 04_backfill.py                    # Incremental (watermark) or full (DELETE+INSERT)
│   ├── 05_governance.py                 # UC tags, comments, access grants, constraints
│   ├── transforms.py                     # Shared transform functions (importable by pytest)
│   ├── databricks.yml                   # DAB bundle (experimental)
│   └── tests/
│       └── test_transforms.py.py        # 17 pytest tests for transforms.py
├── sdp_pipeline/                        # Spark Declarative Pipeline (AI-migrated)
│   ├── bronze/
│   │   └── bronze_ingestion.py           # 4 streaming tables (Auto Loader + S3 batch)
│   ├── silver/
│   │   ├── silver_offers.py              # MV: dedup, DQ rules
│   │   ├── silver_installs.py            # MV: dedup, platform derived, DQ rules
│   │   ├── silver_user_profile.py        # MV: dedup, DQ rules
│   │   ├── silver_events.py              # MV: dedup, anomaly flags, DQ rules
│   │   ├── silver_rejects.py             # MV: rejected rows from all sources
│   │   ├── silver_dq_metrics.py          # MV: DQ rule evaluation metrics
│   │   └── silver_dq_issues.py           # MV: aggregated reject counts
│   ├── gold/
│   │   └── gold_daily_country_platform.py  # MV: daily country/platform funnel aggregation
│   └── governance.sql                    # UC tags, PII tags, comments, grants
└── exploration/                          # EDA notebooks
    ├── 00_common.py                      # Shared config and DQ rule runner
    ├── setup.py                           # Catalog, schema, and volume creation
    ├── data_exploration.py                # Profiles all four raw sources
    ├── bronze_ingestion.py               # Early bronze prototype
    ├── silver_transform.py               # Early silver prototype
    └── gold_aggregation.py                # Early gold prototype
```

## Setup

### 1. Clone the repository

In your Databricks workspace, clone this repo into a Git folder:

```
Git URL: https://github.com/praveenbm1997/exmox_case_study
Branch:   main
```

### 2. Run the setup notebook

Open and run `exploration/setup` — this creates the catalog, schemas, and volumes for you:

- Catalog: `exmox`
- Schemas: `exmox.bronze`, `exmox.silver`, `exmox.gold`
- Volumes: `exmox.bronze.landing`, `exmox.bronze._checkpoints`

### 3. Review the shared config

Open `exploration/00_common` — this defines all paths, table names, DQ rules, and Spark settings used across the pipeline. Key defaults:

- Landing path: `/Volumes/exmox/bronze/landing`
- Checkpoints: `/Volumes/exmox/bronze/_checkpoints`
- Lookback days: 6
- Job cutoff: 00:15 UTC
- Funnel steps: `app_open`, `offer_view`, `offer_start`, `goal_reached`, `reward_paid`

No changes needed unless your volume path differs.

### 4. Upload the CSV files

Place the four CSV files into the landing volume subfolders so Auto Loader can pick them up:

```
/Volumes/exmox/bronze/landing/events/events.csv
/Volumes/exmox/bronze/landing/installs/installs.csv
/Volumes/exmox/bronze/landing/offers/offers.csv
/Volumes/exmox/bronze/landing/user_profile/user_profile.csv
```

Upload via the Databricks UI (Catalog > exmox > bronze > landing volume > Upload) or via CLI:

```bash
databricks fs cp events.csv /Volumes/exmox/bronze/landing/events/
databricks fs cp installs.csv /Volumes/exmox/bronze/landing/installs/
databricks fs cp offers.csv /Volumes/exmox/bronze/landing/offers/
databricks fs cp user_profile.csv /Volumes/exmox/bronze/landing/user_profile/
```

### 5. Run the pipeline

Now choose either option below — notebook pipeline or SDP pipeline.

## Option A: Run the Notebook Pipeline

### Run the notebooks in order

1. Open `pipeline/01_bronze_ingestion` — loads CSVs from the volume into `exmox.bronze.*` Delta tables via Auto Loader
2. Open `pipeline/02_silver_transform` — cleans, deduplicates, applies DQ rules, logs rejects into `exmox.silver.*` tables
3. Open `pipeline/03_gold_aggregation` — aggregates into `exmox.gold.gold_daily_country_platform`
4. (Optional) Run `pipeline/05_governance` — applies UC tags, column comments, access grants, and constraints

### Or run via Lakeflow Job

- "Exmox Bronze to Silver to Gold Pipeline" — initial version
- "Exmox Pipeline main" — refined version with daily schedule at 00:15 UTC

### Run the tests

```bash
pytest pipeline/tests/test_transforms.py.py -v
```

Expected: 17 tests, all passing.

## Option B: Run the SDP Pipeline

### 1. Import the pipeline

In your Databricks workspace, go to **Pipelines** and create a new Spark Declarative Pipeline with:

- **Name**: Exmox ETL Pipeline
- **Catalog**: exmox
- **Target schema**: default
- **Serverless**: enabled
- **Photon**: enabled

Add all Python files from `sdp_pipeline/bronze/`, `sdp_pipeline/silver/`, and `sdp_pipeline/gold/` as pipeline code files. Add `governance.sql` as a non-globbed file at the pipeline root.

### 2. Run the pipeline

Click **Start** to run a pipeline update. The pipeline automatically resolves dependencies: bronze to silver to gold.

All tables are published to:
- `exmox.bronze.bronze_events`, `exmox.bronze.bronze_installs`, `exmox.bronze.bronze_offers`, `exmox.bronze.bronze_user_profile`
- `exmox.silver.silver_events`, `exmox.silver.silver_installs`, `exmox.silver.silver_offers`, `exmox.silver.silver_user_profile`, `exmox.silver.silver_rejects`, `exmox.silver.silver_dq_metrics`, `exmox.silver.silver_dq_issues`
- `exmox.gold.gold_daily_country_platform`

### 3. Apply governance

After the pipeline creates all tables, run `sdp_pipeline/governance.sql` in a SQL editor to apply UC table tags, PII tags, and column comments.

### Migrating from notebook pipeline to SDP

If you already ran the notebook pipeline, drop the existing tables first so the SDP pipeline can create its own:

```sql
DROP TABLE IF EXISTS exmox.bronze.bronze_events;
DROP TABLE IF EXISTS exmox.bronze.bronze_installs;
DROP TABLE IF EXISTS exmox.bronze.bronze_offers;
DROP TABLE IF EXISTS exmox.bronze.bronze_user_profile;
DROP TABLE IF EXISTS exmox.silver.silver_events;
DROP TABLE IF EXISTS exmox.silver.silver_installs;
DROP TABLE IF EXISTS exmox.silver.silver_offers;
DROP TABLE IF EXISTS exmox.silver.silver_user_profile;
DROP TABLE IF EXISTS exmox.silver.silver_rejects;
DROP TABLE IF EXISTS exmox.silver.silver_dq_metrics;
DROP TABLE IF EXISTS exmox.silver.silver_dq_issues;
DROP TABLE IF EXISTS exmox.gold.gold_daily_country_platform;
```

## Verify the Results

After running either pipeline, verify with these checks:

### Row counts

```sql
SELECT 'silver_offers' AS table, COUNT(*) AS rows FROM exmox.silver.silver_offers
UNION ALL SELECT 'silver_installs', COUNT(*) FROM exmox.silver.silver_installs
UNION ALL SELECT 'silver_user_profile', COUNT(*) FROM exmox.silver.silver_user_profile
UNION ALL SELECT 'silver_events', COUNT(*) FROM exmox.silver.silver_events
UNION ALL SELECT 'silver_rejects', COUNT(*) FROM exmox.silver.silver_rejects
UNION ALL SELECT 'gold_daily_country_platform', COUNT(*) FROM exmox.gold.gold_daily_country_platform;
```

Expected:

| Table | Rows |
|---|---|
| silver_offers | 180 |
| silver_installs | 40,000 |
| silver_user_profile | 40,000 |
| silver_events | 423,186 |
| silver_rejects | 14,311 |
| gold_daily_country_platform | 1,001 |

### Gold totals match silver

```sql
-- Events total should match
SELECT SUM(events_total) AS gold_events FROM exmox.gold.gold_daily_country_platform;
SELECT COUNT(*) AS silver_events FROM exmox.silver.silver_events;
-- Both should return 423,186

-- Installs total should match
SELECT SUM(installs) AS gold_installs FROM exmox.gold.gold_daily_country_platform;
SELECT COUNT(*) AS silver_installs FROM exmox.silver.silver_installs;
-- Both should return 40,000
```

### Platform derivation

```sql
-- All iPhone models should be ios, all others android
SELECT device_model, platform FROM exmox.silver.silver_installs
WHERE device_model LIKE '%iphone%' AND platform != 'ios';
-- Should return 0 rows
```

### DQ rules

```sql
-- Check DQ metrics (SDP pipeline)
SELECT * FROM exmox.silver.silver_dq_metrics ORDER BY rule_name;

-- Check reject counts by reason
SELECT * FROM exmox.silver.silver_dq_issues ORDER BY source, reject_reason;
```

### Idempotency

Run the pipeline a second time and verify row counts do not change. Both implementations are idempotent by design (MERGE by key in notebook, MV refresh in SDP).

## Data Sources

| File | Grain | Key columns |
|---|---|---|
| events.csv | one row per event | event_id (PK), user_id, event_ts, ingest_ts |
| installs.csv | one row per user | user_id (PK), install_ts, country, platform, device_model |
| offers.csv | one row per offer | offer_id (PK), offer_category, payout_eur |
| user_profile.csv | one row per user | user_id (PK), events_lifetime, revenue_30d_eur, is_payer |

## Dashboard

An AI/BI dashboard "Exmox Funnel and Platform Analytics" is available in the workspace, visualising:
- Funnel chart (Installs to Reward Paid)
- Platform distribution
- Country usage map
- Conversion rate trend over time
- Silver layer data quality metrics

## Contact

For questions about this submission, contact: praveen.b.madhava@gmail.com