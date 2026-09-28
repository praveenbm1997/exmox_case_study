# SOLUTION.md — Exmox Data Engineering Task

## Data Exploration Findings

Source: `exploration/data_exploration` notebook (ID: 2060532591859774).
Profiles all four raw sources and records every finding mapped to the pipeline rule that handles it.

### Data Overview

| Source | Rows | Files | Columns |
|---|---|---|---|
| events | 435,907 | 1 | user_id, event_ts, event_name, offer_id, event_id, ingest_ts |
| installs | 40,000 | 1 | user_id, install_ts, country, platform, media_source, device_model, campaign_id |
| offers | 180 | 1 | offer_id, offer_category, payout_type, payout_eur |
| user_profile | 40,000 | 1 | user_id, events_lifetime, last_seen_ts, revenue_30d_eur, is_payer |

### Duplicates

- **12,721 duplicate event_ids** (12,721 extra rows, 2.92% of events). All are pure retries with identical payloads (0 conflicts). Each has exactly 2 copies with a 90-second ingest spread. Handled by first-arrival-wins dedup in silver_events.
- **0 duplicate install users** — 40,000 rows = 40,000 distinct user_ids.
- **0 duplicate offer_ids** — 180 rows = 180 distinct offer_ids.

### Orphan Offers

- **6 orphan offer IDs** appear in events but are not in the offer catalog: `of_0180` through `of_0185`.
- These generated **13,270 orphan events** across app_open, offer_view, offer_start, and goal_reached — but **0 reward_paid** events.
- Flagged via `is_orphan_offer` in silver_events.

### Pre-Install Events

- **3,003 events from 2,856 users** occurred before the user's install timestamp.
- Breakdown by media source: meta (924), organic (776), tiktok (602), unity (417), applovin (284).
- Median ~46 hours before install; max ~72 hours.
- Flagged via `is_pre_install` in silver_events.

### Orphan Installs

- **2,305 installed users with no events** — installed but never generated any activity.
- **0 events without a corresponding install** — every event user has an install record.

### Funnel Anomalies

- **9,812 user-offer pairs** with reward_paid but no preceding goal_reached.
- **24,496 user-offer pairs** with goal_reached but no preceding offer_start.
- **0 double payout pairs** — no duplicate reward_paid per user-offer pair.

### User Profile Discrepancies

- 32,715 users where events_lifetime matches actual event count.
- 4,980 users where profile events_lifetime is higher than actual events.
- 2,305 users with no events (matches orphan installs count).
- is_payer: 11,410 True, 28,590 False.

### Data Quality Issues

- **1,590 untrimmed country values** in installs (e.g., "br ", "de ") — all normalize to valid ISO-2 after trim+upper.
- 16 raw country spellings map to 8 valid ISO-2 codes: BR, DE, FR, GB, IN, PL, TR, US.
- 0 invalid countries after normalizing.
- 9,887 null campaign_id values — all from organic installs (expected; `nullif(trim, '')` maps to NULL).
- 0 nulls in any other column across all sources.
- 0 unparseable timestamps in events or installs.
- 0 unknown event names — all 5 already canonical.
- 0 negative or zero payouts — range: 0.45 to 9.99 EUR.
- 6 device models appearing on both android and ios (pixel7, redmi9, s22, iphone15, a51, iphone13). Device_model is retained but never used to derive platform.

### Arrival Lag

- p50 lag: 4.97 hours; p99 lag: 72.13 hours; max lag: 5.58 days (within 6-day lookback window).
- 97.49% of events arrive same day; 1.48% next day; 0.996% at 3 days.
- 0 rows with negative lag (no ingest before event).

## Environment

Databricks workspace with Unity Catalog, serverless compute (CPU). Catalog `exmox` with schemas `bronze`, `silver`, `gold`.

## Three Implementations — Notebook Jobs & SDP Pipeline

This project evolved through 3 implementations, all producing identical gold-table results. The pipeline was initially created with notebooks and orchestrated by a Lakeflow Job, then refined into a second notebook-based job, and finally the same bronze-to-gold flow was migrated to a Spark Declarative Pipeline (SDP) using Assistant AI.

### 1. Notebook-Based Pipeline — Initial Job (Built First)

The initial implementation used three Databricks notebooks orchestrated by a Lakeflow Job:
- `01_bronze_ingestion.ipynb` — Auto Loader + S3 upsert into Delta tables
- `02_silver_transform.ipynb` — Watermark-based incremental MERGE, dedup, DQ rules, rejects
- `03_gold_aggregation.ipynb` — Daily country/platform aggregation with MERGE

Job: "Exmox Bronze → Silver to Gold Pipeline", daily trigger. Tagged "With notebooks".

### 2. Notebook-Based Pipeline — Refined Job

After the initial job was validated, a second notebook-based job was created with the same three notebooks, with per-task timeouts and a fixed daily cron schedule. It added alerts, tests, column-level data validations, and governance.

Job: "Exmox Pipeline main", scheduled daily at 00:15 UTC. Tagged "With notebooks: main". Git source: `https://github.com/praveenbm1997/exmox_case_study` (branch: main).

Both notebook jobs orchestrate the identical three notebooks from `/exmox/notebooks/pipeline/` and produce the same results.

Supporting notebooks: `04_backfill.ipynb` (incremental/full recompute), `05_governance.ipynb` (UC tags, constraints, access grants).

Shared transformation logic extracted to `transforms.py` with pytest tests in `tests/test_transforms.py`.

### 3. Spark Declarative Pipeline (Built with AI)

After the notebook pipeline was validated and working, the same bronze-to-gold flow was migrated to a Spark Declarative Pipeline using Assistant AI. The AI assistant:

- Read all three notebooks and the shared `transforms.py` to understand the full transformation logic
- Generated SDP dataset definitions using `dp.create_streaming_table()` + `@dp.append_flow()` for bronze (Auto Loader from volume + one-time S3 batch), materialized views (silver, gold), and DQ expectations
- Replaced manual watermark tracking with SDP's automatic incremental refresh on serverless compute
- Replaced manual MERGE/upsert logic with streaming table append flows and materialized view semantics
- Replaced manual DQ rule evaluation with `@dp.expect_or_drop` / `@dp.expect_or_fail` decorators
- Added S3 source support: one-time batch append flows reading from `s3://exmox/{dataset}/{dataset}.csv` alongside volume Auto Loader flows
- Fixed DQ expectations: `payout_eur > 0` (was `>= 0`) in silver_offers, added missing `ingest_ts_not_null` to silver_events
- Added DQ metrics (`silver_dq_metrics`) and DQ issues (`silver_dq_issues`) materialized views for queryable DQ monitoring
- Created governance SQL script (`governance.sql` at pipeline root) for UC table tags, column PII tags, comments, and access grants
- Created SQL alert "SDP DQ Failures -- Silver Pipeline" (ID: 3181887776925658) for DQ violation monitoring
- Dry run passed and ran successfully after all changes

Pipeline: "Exmox ETL Pipeline" (ID: fadd1463-bb24-42ca-bcce-6c86bf8dc6b8), serverless, photon enabled, catalog `exmox`.

**File structure** (`transformations/`):
```
transformations/
├── bronze/
│   └── bronze_ingestion.py            # 4 streaming tables (volume Auto Loader + S3 batch append flows)
├── silver/
│   ├── silver_offers.py               # MV: dedup by offer_id (latest wins), DQ: offer_id_not_null, payout_eur > 0
│   ├── silver_installs.py            # MV: dedup by user_id (first install_ts wins), platform derived, 4 DQ rules
│   ├── silver_user_profile.py        # MV: dedup by user_id (latest wins), DQ: user_id_not_null
│   ├── silver_events.py              # MV: dedup by event_id (first arrival wins), flags anomalies, 5 DQ rules + 3 warn
│   ├── silver_rejects.py             # MV: rejected rows from all bronze sources
│   ├── silver_dq_metrics.py          # MV: DQ rule evaluation metrics (one row per rule per refresh)
│   └── silver_dq_issues.py           # MV: aggregated reject counts by source and reason
├── gold/
│   └── gold_daily_country_platform.py  # MV: daily country/platform funnel aggregation, 5 DQ rules
└── governance.sql                     # (placeholder -- actual file at pipeline root, outside glob)
```

**Governance SQL** (`governance.sql` at pipeline root, outside `transformations/` glob):
- UC table tags (layer, domain, quality, retention) for all SDP tables including DQ tables
- Column PII tags for sensitive columns (user_id, device_model, event_id, revenue_30d_eur, reject_key, raw_record)
- Table comments for all tables
- Access grants (commented out -- run separately in SQL editor with appropriate permissions)
- Run this AFTER the pipeline creates tables; it is not part of the SDP pipeline code

**DQ Alert**: "SDP DQ Failures -- Silver Pipeline" (ID: 3181887776925658), fires when any DQ violation is found in silver tables (null keys, invalid values, bad platform/country). Scheduled daily at 08:00 UTC.

### Key Differences

| Aspect | Notebook Pipeline | SDP Pipeline |
|---|---|---|
| Orchestration | Lakeflow Job (3 sequential notebook tasks) | Single pipeline update (automatic dependency resolution) |
| Bronze | Auto Loader + S3 upsert via foreachBatch | `dp.create_streaming_table()` + append flows (volume Auto Loader continuous + S3 one-time batch) |
| Silver | Manual watermark tracking + MERGE/DELETE+INSERT | Materialized views with automatic incremental refresh |
| Gold | Manual MERGE on composite key | Materialized view (automatic refresh) |
| DQ | Manual rule evaluation logged to tables | `@dp.expect_or_drop` / `@dp.expect_or_fail` decorators + `silver_dq_metrics` / `silver_dq_issues` MVs |
| Rejects | Computed in notebook, written to table | Separate materialized view |
| Governance | `05_governance.ipynb` (UC tags, comments, grants) | `governance.sql` script at pipeline root (run after pipeline) |
| DQ Alert | SQL alert on `dq_metrics` / `dq_issues` (ID: 2972677863944610) | SQL alert on silver tables (ID: 3181887776925658) |
| Scheduling | Job schedule (daily 00:15 UTC) | Pipeline schedule (daily 00:30 UTC, pending manual approval) |
| Code location | `pipeline/` (notebooks) | `transformations/` (Python files) |

Both implementations produce the same gold table (`exmox.gold.gold_daily_country_platform`) with identical schema and data quality guarantees.

## Folder Structure

```
repository root
├── pipeline/                        # Notebook-based pipeline (original)
│   ├── 01_bronze_ingestion.ipynb   # Bronze: Auto Loader + S3 upsert into Delta tables
│   ├── 02_silver_transform.ipynb    # Silver: clean, dedup, DQ, rejects, watermark MERGE
│   ├── 03_gold_aggregation.ipynb    # Gold: daily country/platform aggregation with MERGE
│   ├── 04_backfill.ipynb            # Backfill: incremental (watermark) or full (DELETE+INSERT)
│   ├── 05_governance.ipynb           # Governance: UC tags, comments, access grants, constraints
│   ├── SOLUTION.md                  # Detailed solution doc (notebook pipeline focus)
│   ├── transforms.py                # Standalone transform functions (importable by pytest)
│   ├── tests/
│   │   └── test_transforms.py       # 17 pytest tests for transforms.py
│   └── databricks.yml               # DAB bundle (experimental)
├── sdp_pipeline/                    # SDP pipeline (AI-migrated)
│   ├── bronze/
│   │   └── bronze_ingestion.py       # 4 streaming tables (volume Auto Loader + S3 batch append flows)
│   ├── silver/
│   │   ├── silver_offers.py          # MV: dedup, DQ: offer_id_not_null, payout_eur > 0
│   │   ├── silver_installs.py        # MV: dedup, platform derived, 4 DQ rules
│   │   ├── silver_user_profile.py    # MV: dedup, DQ: user_id_not_null
│   │   ├── silver_events.py          # MV: dedup, flags, 5 DQ rules + 3 warn
│   │   ├── silver_rejects.py         # MV: rejected rows from all bronze sources
│   │   ├── silver_dq_metrics.py      # MV: DQ rule evaluation metrics per refresh
│   │   └── silver_dq_issues.py       # MV: aggregated reject counts by source/reason
│   ├── gold/
│   │   └── gold_daily_country_platform.py  # MV: daily country/platform funnel aggregation, 5 DQ rules
│   └── governance.sql               # UC tags, PII tags, comments, grants (run after pipeline)
├── exploration/                      # EDA notebooks
├── v2_not_worked/                    # Earlier v2 attempt (not in use)
├── SOLUTION.md                      # This file
└── README.md
```

## How to Run

### Notebook Pipeline
1. **Bronze**: Run notebook `01_bronze_ingestion` — loads CSVs from S3/Volume into Delta tables via Auto Loader and S3 upsert
2. **Silver**: Run notebook `02_silver_transform` — cleans, deduplicates, DQ rules, logs rejects, watermark-based incremental MERGE
3. **Gold**: Run notebook `03_gold_aggregation` — aggregates to daily country/platform metrics with MERGE on composite key
4. **Or via Job**: Run either notebook job:
   - "Exmox Bronze → Silver → Gold Pipeline" (ID: 518342916204664) — initial version, trigger paused
   - "Exmox Pipeline main" (ID: 786513622011200) — refined version with Git source, daily at 00:15 UTC

### SDP Pipeline
1. Open pipeline "Exmox ETL Pipeline" (ID: fadd1463-bb24-42ca-bcce-6c86bf8dc6b8) in the Databricks pipeline editor
2. Click **Start** to run a pipeline update
3. The pipeline automatically resolves dependencies: bronze -> silver -> gold
4. All tables are published to `exmox.bronze.*`, `exmox.silver.*`, `exmox.gold.*`
5. After the pipeline creates tables, run `governance.sql` (at pipeline root) to apply UC table tags, PII tags, comments, and access grants
6. DQ monitoring: SQL alert "SDP DQ Failures -- Silver Pipeline" (ID: 3181887776925658) checks for DQ violations daily at 08:00 UTC
7. Pipeline tags (`pipeline_type: sdp`, `project: exmox`, `layer: bronze-silver-gold`) and daily schedule (00:30 UTC) require manual configuration in pipeline settings

**Note**: If migrating from the notebook pipeline to SDP, drop existing tables first (the SDP pipeline creates its own):
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

### Tests (Notebook Pipeline)
```bash
pytest tests/test_transforms.py -v   # 17 tests, all passing
```

## Architecture — Layering Rationale

Three layers chosen for:
1. **Bronze (raw)**: Exact copy of source data with metadata. No transformation. Enables reprocessing without re-reading source files.
2. **Silver (validated)**: Cleaned, deduplicated, type-cast. DQ rules enforced via Delta constraints and runtime checks (notebook) or `@dp.expect*` decorators (SDP). Rejects logged separately.
3. **Gold (aggregated)**: Business-ready metrics. Daily granularity by country and platform.

**DQ Monitoring (SDP)**: `silver_dq_metrics` and `silver_dq_issues` materialized views track DQ rule results and reject counts per refresh. SQL alert (ID: 3181887776925658) fires on violations.

### Tables

| Layer | Table | Grain | Key | Dedup Strategy |
|---|---|---|---|---|
| Bronze | bronze_installs | one row per install | user_id | — |
| Bronze | bronze_events | one row per event | event_id | — |
| Bronze | bronze_offers | one row per offer | offer_id | — |
| Bronze | bronze_user_profile | one row per user | user_id | — |
| Silver | silver_installs | one row per user | user_id | DELETE+INSERT (full recompute, first install wins) |
| Silver | silver_events | one row per event | event_id | First arrival wins (earliest ingest_ts) |
| Silver | silver_offers | one row per offer | offer_id | Latest version wins (max _ingested_at) |
| Silver | silver_user_profile | one row per user | user_id | Latest version wins (max _ingested_at) |
| Silver | silver_rejects | one row per rejected record | - | Full recompute each run |
| Silver | silver_dq_metrics | one row per rule per refresh | table_name, rule_name | Full recompute each refresh |
| Silver | silver_dq_issues | one row per source/reason per refresh | source, reject_reason | Full recompute each refresh |
| Gold | gold_daily_country_platform | one row per date/country/platform | date, country, platform | MERGE on composite key |

## Data Quality

### DQ Rules (16 total: 12 error + 4 warning, all passing)

| Table | Rule | Enforcement |
|---|---|---|
| silver_offers | offer_id unique | Delta PK + runtime check |
| silver_offers | offer_id not null | Delta NOT NULL constraint |
| silver_offers | payout_eur > 0 | Delta CHECK constraint + runtime filter |
| silver_installs | user_id unique | Delta PK + runtime check |
| silver_installs | user_id not null | Delta NOT NULL constraint |
| silver_installs | install_ts not null | Delta NOT NULL constraint |
| silver_installs | platform in (android, ios) | Delta CHECK constraint + runtime filter |
| silver_installs | country ISO-2 | Delta CHECK constraint + runtime filter |
| silver_events | event_id unique | Delta PK + runtime check |
| silver_events | no null keys/ts | Delta NOT NULL constraints |
| silver_events | event_name known | Delta CHECK constraint + runtime filter |
| silver_user_profile | user_id unique | Delta PK + runtime check |
| silver_user_profile | user_id not null | Delta NOT NULL constraint |

**Warning-severity rules (flagged, not dropped):**

| Table | Rule | Severity |
|---|---|---|
| silver_events | late arrivals (is_late) | warn |
| silver_events | pre-install events | warn |
| silver_events | orphan offer_id | warn |
| silver_installs | users with no events | warn |

### Rejects
- 14,311 rejected rows captured in `silver_rejects` with reject_reason and raw_record JSON
- Reasons: null_id_or_bad_payout (offers), bad_ts_or_country (installs), null_key_bad_ts_or_unknown_event (events), duplicate_event_id (events)

## How to Verify

- All 16 DQ rules pass
- 17 pytest tests pass: `pytest tests/test_transforms.py -v`
- Row counts: silver_offers: 180, silver_installs: 40,000, silver_user_profile: 40,000, silver_events: 423,186, silver_rejects: 14,311, gold_daily_country_platform: 1,001
- Platform derivation: all iPhone models → ios; all other devices → android
- Idempotent: running twice produces the same result
- Gold total: `SELECT SUM(events_total) FROM exmox.gold.gold_daily_country_platform` should equal `SELECT COUNT(*) FROM exmox.silver.silver_events`
- Gold installs: `SELECT SUM(installs) FROM exmox.gold.gold_daily_country_platform` should equal `SELECT COUNT(*) FROM exmox.silver.silver_installs`

## Deeper Questions

### The Daily Job

*Reasoning, not recalling.*

Suppose the job runs at 00:15 over the previous calendar day. The key question: what defines "the previous calendar day"?

If we filter by `event_date` (from `event_ts`), we capture events that *occurred* yesterday — including those not yet ingested (missed) and excluding events from earlier days ingested yesterday.

If we filter by `ingest_ts`, we capture events that *landed* yesterday — including late arrivals from earlier days and missing events from yesterday that arrive later.

**What the job gets wrong**: If running at 00:15 processing by event_date = yesterday, it misses events generated yesterday but ingested after 00:15. If processing by ingest_ts, it may include events from earlier days and miss yesterday's late arrivals.

**How to find out**: Compare `COUNT(*) FROM silver_events WHERE event_date = yesterday` against `COUNT(*) FROM bronze_events WHERE date(ingest_ts) = yesterday`. If bronze count is higher, events arrive after the job runs.

**What the data shows** (from exploration):
- p50 lag: 4.97 hours; p99 lag: 72.13 hours; max lag: 5.58 days
- 97.49% of events arrive same day; 1.48% next day; 0.996% at 3 days
- 2,032 events at 23:00+ were ingested after midnight — these are the midnight crossover cases that `JOB_CUTOFF_MIN = 15` is designed to catch
- Conclusion: filtering by event_date = yesterday works for 97.49% of events, but 2.51% arrive late. The `LOOKBACK_DAYS = 6` parameter in gold handles this by recomputing the last 6 days each run, catching late arrivals within the lookback window.

**What to build instead**:
- Process by `ingest_ts` window (00:00–00:14 today covers all of yesterday plus a 14-minute buffer)
- Use file-arrival triggers rather than a fixed schedule
- For gold: the `LOOKBACK_DAYS = 6` with `is_settled` already recomputes the last 6 days each run — this is the mitigation already in place
- In the SDP pipeline, materialized views with automatic incremental refresh process only changed data, so the fixed-schedule problem is less acute — the MV picks up new rows whenever the pipeline refreshes

### Your Platform Choices
**Partitioning**: No explicit partition columns on any table. Tables are small (180–423K rows), so partitioning adds overhead without benefit. The notebook pipeline uses dynamic partition count via `auto_partition_count()`: 1 partition per 50K rows. Shuffle partitions also scale dynamically (1 per 50K rows). The SDP pipeline relies on serverless auto-scaling and does not set explicit partition counts.

How I would know if wrong: symptom would be small-file problems or partition pruning not working. Would check via `DESCRIBE DETAIL` and query profiles. At this data volume, the cost of being wrong is negligible.

**Clustering**: The SDP pipeline uses liquid clustering via `cluster_by` on key columns:
- Bronze: `cluster_by=["event_id"]`, `["user_id"]`, `["offer_id"]` — optimises dedup and downstream joins
- Silver: same key columns as bronze — optimises MERGE and join performance
- Gold: `cluster_by=["date", "country", "platform"]` — optimises the composite key queries that dashboards use

The notebook pipeline does not use liquid clustering but runs `OPTIMIZE` after each pipeline run for compaction. At this scale, OPTIMIZE is sufficient. Liquid clustering in the SDP pipeline is a forward-looking choice — it becomes meaningful when the data grows.

How I would know if wrong: query profiles showing full scans where range pruning should occur. `DESCRIBE DETAIL` on the Delta table would show clustering metadata.

**Ingestion pattern**: Both pipelines use Auto Loader from UC volumes (`/Volumes/exmox/bronze/landing/{dataset}/`) with CSV format, `header=true`, `inferColumnTypes=false` (all columns as STRING — type casting happens in silver).

The notebook pipeline additionally syncs S3 CSV files (`s3://exmox/{dataset}/{dataset}.csv`) into `exmox.bronze.s3_*` tables, then merges both sources into bronze tables via `foreachBatch` with manual `upsert_to_bronze()` (MERGE by key). This gives bronze upsert semantics — one row per key, updated on re-ingestion.

The SDP pipeline uses `dp.create_streaming_table()` + `@dp.append_flow()` for each source: a continuous Auto Loader flow from volumes and a one-time batch flow (`once=True`) from S3. Bronze is append-only — duplicates are handled in the silver layer via `row_number()` dedup. This is a cleaner separation: bronze is truly raw, silver is curated.

Where the ingestion pattern stops being right:
- Data volume exceeds ~10M rows per table — MERGE becomes expensive (notebook); streaming table append is fine but dedup state grows (SDP)
- Streaming latency requirements emerge — would switch to continuous-trigger SDP or Structured Streaming with watermarking
- Schema changes frequently — would need `cloudFiles.schemaEvolutionMode` and schema hints
- 50+ sources — would need a registry-driven ingestion config rather than per-table code

**What to govern centrally in UC**:
- Table and column tags (PII classification, layer, domain) — done in `05_governance.ipynb` (notebook) and `governance.sql` (SDP)
- Constraints (PK, FK, CHECK, NOT NULL) — done in `02_silver_transform.ipynb` (notebook); `@dp.expect*` decorators in SDP
- Access grants (role-based: bronze = engineers only, silver = engineers + analysts, gold = all roles read-only) — done in governance notebook/script
- Catalog/schema creation and naming — standardised as `exmox.bronze`, `exmox.silver`, `exmox.gold`
- Retention policies — tagged but not enforced (would need Delta table properties for time travel and vacuum scheduling)

### How It Fails

**Silent wrong numbers** (no error, wrong result):
1. **Late-arriving data**: Event generated at 23:50 but ingested at 00:20 — a job at 00:15 processing yesterday misses it. Gold undercounts. No error. Mitigated by `LOOKBACK_DAYS = 6` recomputing the last 6 days, but events arriving >6 days late are permanently missed.
2. **Inner join drops**: Gold uses INNER JOIN between events and installs. If an event exists for a user_id not in silver_installs (install was rejected), that event is silently dropped from gold. 0 such cases in this dataset (all event users have installs), but the failure mode exists.
3. **Watermark drift** (notebook pipeline): If bronze ingestion fails silently (no new `_ingested_at`), the watermark doesn't advance. Next silver run finds "no new data" — even though there should be data. No error, just stale silver tables.
4. **Platform edge cases**: `device_model` containing "iphone" as a substring of a non-iPhone device would be classified as ios. 6 device models appear on both platforms in the raw data. We derive platform from `contains("iphone")` — correct for this data, but fragile for new device names.
5. **Dedup by first arrival**: If the same `event_id` arrives with different data on re-ingestion, the first version wins and the update is silently dropped. Correct for this dataset (all duplicates are identical retries), but a risk if source systems resend corrections.
6. **Gold recompute misses deletes** (notebook pipeline): Gold uses MERGE (upsert). If a silver row is deleted (e.g., by a backfill that removes bad data), the gold aggregate is not recomputed — stale row persists. The SDP pipeline's materialized view handles this better — a full refresh recomputes from current silver state.

**Platform-hidden failures**:
- Delta MERGE with `whenMatchedUpdateAll` can silently overwrite columns if `schema.autoMerge` is enabled (disabled in both pipelines).
- OPTIMIZE can change file layout without notice, affecting query plans.
- SDP materialized view incremental refresh falls back to full recompute when the cost model determines it's cheaper — this is transparent but means the "incremental" guarantee is best-effort, not deterministic.
- `@dp.expect_or_drop` silently removes rows that fail constraints — the rows are gone from the target table with only metrics logged. If the metrics aren't monitored, the data loss is invisible.

### Scale

*The same shape of data now arrives from 50 sources instead of 4 files, continuously rather than as a dump.*

**What survives** (50 sources, continuous):
- Watermark-based incremental processing (notebook) / MV automatic incremental refresh (SDP) — scales with data volume, only processes changes
- DQ rules — simple aggregations, scale linearly
- Governance tags and constraints — UC-native, scale with the catalog
- Gold aggregation pattern — `groupBy` + join, scales with Spark
- Rejects logging — independent of main data flow
- Liquid clustering on key columns (SDP) remains effective at scale
- Auto Loader is designed for continuous ingestion from cloud storage and handles file discovery at scale

**What changes**:
- Ingestion: Auto Loader handles continuous arrival, but would switch from triggered (availableNow) to continuous-trigger SDP pipeline
- Partitioning: Would partition silver_events by `event_date`, silver_installs by `install_date` — enables partition pruning for date-filtered queries
- Clustering: Liquid clustering becomes essential — would add `cluster_by` on frequently filtered columns (user_id, event_date) at scale
- Compute: Move from serverless interactive to job clusters with autoscaling for predictable cost
- Monitoring: Add structured streaming metrics (throughput, latency, backlog) and alert on lag beyond threshold
- Gold: Switch from full recompute to incremental aggregation with daily partitions; consider REPLACE WHERE flow for rolling window recomputation
- DQ: Move from per-table rules to a rules engine or Great Expectations for manageability across 50 sources
- Schema: Would need `cloudFiles.schemaHints` and `schemaEvolutionMode` to handle source-specific schema variations

**What gets thrown away**:
- `auto_partition_count` formula (1 partition per 50K rows) — replaced by explicit partitioning on date columns
- Full recompute of rejects — replaced by incremental reject logging (append-only with dedup)
- Single-notebook-per-layer architecture — split into separate tasks per source for parallelism and isolation
- Manual DQ rule list — replaced by a config-driven rules engine
- Per-table hardcoded volume paths — replaced by a source registry mapping source name to path, schema, and key
- `foreachBatch` with manual MERGE (notebook pipeline) — replaced by SDP streaming tables with append flows and Auto CDC for upsert semantics

## How I Worked with AI

After data exploration was complete and I understood the data and entity relationships between the datasets, I listed the tasks that needed to be done.

**Phase 1 — Notebook Pipeline (built first)**:
- Asked AI for bronze ingestion notebook with Auto Loader and S3 support
- Asked AI for silver transformation with dedup, platform derivation, DQ checks
- Asked AI for gold aggregation by date/country/platform
- Asked AI for DQ alert with email notification, backfill notebook, governance notebook
- Asked AI for pytest suite (17 tests) for transformation functions
- Created initial job "Exmox Bronze → Silver to Gold Pipeline" to orchestrate the three notebooks
- Refined into second job "Exmox Pipeline main" with Git source integration and daily cron schedule

**Phase 2 — SDP Migration (built with AI)**:
- Asked Databricks Assistant to read the existing notebooks and migrate to Spark Declarative Pipelines
- AI read all three notebooks, the shared transforms.py, and the pipeline structure
- AI generated `dp.create_streaming_table()` + `@dp.append_flow()` for bronze (volume Auto Loader + S3 batch), materialized views for silver and gold
- AI added DQ expectations using `@dp.expect_or_drop` / `@dp.expect_or_fail` decorators
- AI added S3 source support as one-time batch append flows reading from `s3://exmox/{dataset}/{dataset}.csv`
- AI fixed DQ expectations: `payout_eur > 0` (was `>= 0`), added missing `ingest_ts_not_null`
- AI created `silver_dq_metrics` and `silver_dq_issues` materialized views for queryable DQ monitoring
- AI created `governance.sql` for UC table tags, PII tags, comments, and access grants (outside glob to avoid SDP parser errors)
- AI created SQL alert for DQ failure monitoring (ID: 3181887776925658)
- AI updated the Git repo with the new SDP pipeline files
- Dry run passed successfully after all changes

**Documentation and evaluation of the Task**:
- After building the notebook and SDP pipelines, I asked the AI assistant to create this SOLUTION.md file by providing the task list. It provided details of the notebooks, jobs, and pipeline.
- I also asked the AI assistant to implement the parts I had overlooked.

**What AI was confidently wrong about (Phase 1)**:
- For capturing and creating data quality checks in the pipeline, the AI would ignore them, always focusing on building the related tables. I had to repeatedly prompt it to include the data quality tables.
- Platform derivation: AI applied the fix to the code but didn't realize existing silver rows wouldn't be reprocessed by MERGE. Caught when user reported incorrect platform values.
- _ingested_at: AI only added metadata columns to the Auto Loader path, not the S3 path. Caught when silver processing failed.

**What AI got right (Phase 2)**:
- Correctly translated notebook logic to SDP dataset definitions
- Properly used `dp.create_streaming_table()` + append flows for bronze (volume Auto Loader continuous + S3 one-time batch)
- Properly used materialized views for silver (with automatic incremental refresh)
- Properly used `expect_or_drop` for DQ filtering and separate rejects MV
- Correctly placed `governance.sql` outside the `transformations/` glob to avoid SDP parser errors (caught and fixed during dry run)
- Added DQ metrics and issues MVs for queryable DQ monitoring
- Dry run passed after fixing governance.sql placement; full update passed after dropping conflicting tables
