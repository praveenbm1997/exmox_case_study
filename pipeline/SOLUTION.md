# SOLUTION.md — Exmox Data Engineering Task

## Environment

Databricks workspace with Unity Catalog, serverless compute (CPU). Catalog `exmox` with schemas `bronze`, `silver`, `gold`.

## Folder Structure

```
pipeline/
├── 01_bronze_ingestion.ipynb   # Bronze: Auto Loader + S3 upsert into Delta tables
├── 02_silver_transform.ipynb    # Silver: clean, dedup, DQ, rejects, watermark MERGE
├── 03_gold_aggregation.ipynb    # Gold: daily country/platform aggregation with MERGE
├── 04_backfill.ipynb            # Backfill: incremental (watermark) or full (DELETE+INSERT)
├── 05_governance.ipynb           # Governance: UC tags, comments, access grants, constraints
├── SOLUTION.md                  # This file
├── README.md                    # Folder structure and run instructions
├── transforms.py                # Standalone transform functions (importable by pytest)
├── tests/
│   └── test_transforms.py       # 17 pytest tests for transforms.py
├── test_full_pipeline.ipynb     # E2E test: full pipeline with test_ prefixed tables
├── databricks.yml               # DAB bundle (experimental, references v2 notebooks)
└── legacy/                      # v2 notebooks (not in use)
    ├── 01_bronze_to_silver.ipynb
    └── 02_silver_to_gold.ipynb
```

Shared config module `00_common` lives in `../Exploration_notebooks/00_common`.

## How to Run

### Pipeline Execution
1. **Bronze**: Run notebook `01_bronze_ingestion` — loads CSVs from S3/Volume into Delta tables via Auto Loader and S3 upsert
2. **Silver**: Run notebook `02_silver_transform` — cleans, deduplicates, DQ rules, logs rejects, watermark-based incremental MERGE
3. **Gold**: Run notebook `03_gold_aggregation` — aggregates to daily country/platform metrics with MERGE on composite key
4. **Governance**: Run notebook `05_governance` — applies UC tags, comments, access grants, and constraints
5. **Or via Job**: Run "Exmox Bronze → Silver → Gold Pipeline" job (ID: 518342916204664)

### Backfill
Run notebook `04_backfill` with `FULL_BACKFILL=true` (DELETE+INSERT all silver tables, then recompute gold). Default mode is incremental (watermark + MERGE).

### Tests
```bash
pytest tests/test_transforms.py -v   # 17 tests, all passing
```

### DQ Monitoring
Alert "DQ Rule Failures — Silver Pipeline" (ID: 2972677863944610) checks `dq_metrics` and `dq_issues`, emails results to praveen.b.madhava@gmail.com

## How to Verify

- All 16 DQ rules pass (12 error + 4 warning; check `exmox.silver.dq_metrics` where `passed = true`)
- 17 pytest tests pass: `pytest tests/test_transforms.py -v`
- Row counts: silver_offers: 180, silver_installs: 40,000, silver_user_profile: 40,000, silver_events: 423,186, silver_rejects: 14,311, gold_daily_country_platform: 1,001
- Platform derivation: all iPhone models (iphone13, iphone15) → ios; all other devices → android
- Idempotent: running twice produces the same result (MERGE by key, watermarks prevent reprocessing)
- Single-day processing: watermark filtering ensures only new bronze rows are processed
- Gold total: `SELECT SUM(events_total) FROM exmox.gold.gold_daily_country_platform` should equal `SELECT COUNT(*) FROM exmox.silver.silver_events`
- Gold installs: `SELECT SUM(installs) FROM exmox.gold.gold_daily_country_platform` should equal `SELECT COUNT(*) FROM exmox.silver.silver_installs`

## Architecture — Layering Rationale

Three layers chosen for:
1. **Bronze (raw)**: Exact copy of source data with metadata. No transformation. Enables reprocessing without re-reading source files. Retains all original values for audit.
2. **Silver (validated)**: Cleaned, deduplicated, type-cast. DQ rules enforced via Delta constraints and runtime checks. Rejects logged separately. Trusted layer for downstream consumers.
3. **Gold (aggregated)**: Business-ready metrics. Daily granularity by country and platform.

### Pipeline Diagram

```
                          SOURCES
                    S3 Bucket  /  UC Volume
                              │
                              ▼
                    ┌─────────────────────┐
                    │   01_bronze_ingestion │
                    │   Auto Loader + S3    │
                    │   availableNow        │
                    └────────┬────────────┘
                             │
               ┌─────────────┼─────────────┐
               ▼             ▼             ▼
         bronze_installs  bronze_events  bronze_offers  bronze_user_profile
         (40,000 rows)   (435,907)     (180)           (40,000)
               │             │             │               │
               └─────────────┴─────────────┴───────────────┘
                             │
                             ▼
                    ┌──────────────────────────┐
                    │   02_silver_transform      │
                    │                            │
                    │  ┌─ Constraints (PK, NOT   │
                    │  │  NULL, CHECK)           │
                    │  ├─ Clean + dedup         │
                    │  ├─ Platform derivation   │
                    │  │  (device_model → ios)   │
                    │  ├─ Flag anomalies:        │
                    │  │  is_late, is_pre_install│
                    │  │  is_orphan_offer        │
                    │  ├─ DQ Rules (16)         │
                    │  │  12 error + 4 warn     │
                    │  ├─ Rejects → silver_rejects│
                    │  ├─ Watermark tracking     │
                    │  │  (silver_watermark)     │
                    │  └─ DQ metrics + issues   │
                    └────────┬─────────────────┘
                             │
            ┌────────────────┼────────────────────┐
            ▼                ▼                    ▼
     silver_installs   silver_events        silver_offers   silver_user_profile
     (40,000)         (423,186)            (180)           (40,000)
     DELETE+INSERT     MERGE insert-only     MERGE           MERGE
     (full recompute)  (watermark)          (watermark)     (watermark)
            │                │                    │
            └────────────────┼────────────────────┘
                             │
                             ▼
                    ┌──────────────────────────┐
                    │  03_gold_aggregation       │
                    │                            │
                    │  Join events + installs   │
                    │  + offers (broadcast)     │
                    │  GroupBy date × country   │
                    │  × platform              │
                    │  Full outer join          │
                    │  (installs ∪ events)      │
                    │  MERGE on composite key   │
                    │  DQ check (nulls, negs)   │
                    │  OPTIMIZE                 │
                    └────────┬─────────────────┘
                             │
                             ▼
                   gold_daily_country_platform
                   (1,001 rows)
                   date × country × platform


    ┌──────────────────────────────────────────────────────────┐
    │                    CROSS-CUTTING                           │
    │                                                            │
    │  04_backfill            05_governance         DQ Alert      │
    │  Incremental (watermark) UC tags (PII, layer) (ID: 2972…)  │
    │  Full (DELETE+INSERT)   Column comments      Checks        │
    │  Recompute gold          Access grants        dq_metrics + │
    │                          Constraints (PK,FK)  dq_issues   │
    │                                                → email    │
    └──────────────────────────────────────────────────────────┘

    Job: "Exmox Bronze → Silver → Gold Pipeline" (ID: 518342916204664)
    DAB:  databricks.yml (experimental, references v2 notebooks)
    Tests: 17 pytest tests (test_transforms.py) + Delta constraints
```

### Tables

| Layer | Table | Grain | Key | Dedup Strategy |
|---|---|---|---|---|
| Bronze | bronze_installs | one row per install | user_id | — |
| Bronze | bronze_events | one row per event | event_id | — |
| Bronze | bronze_offers | one row per offer | offer_id | — |
| Bronze | bronze_user_profile | one row per user | user_id | — |
| Silver | silver_installs | one row per user | user_id | DELETE+INSERT (full recompute, first install wins) |
| Silver | silver_events | one row per event | event_id | First arrival wins (earliest ingest_ts); watermark + MERGE insert-only |
| Silver | silver_offers | one row per offer | offer_id | Latest version wins (max _ingested_at); watermark + MERGE |
| Silver | silver_user_profile | one row per user | user_id | Latest version wins (max _ingested_at); watermark + MERGE |
| Silver | silver_rejects | one row per rejected record | — | Full recompute each run |
| Silver | silver_watermark | one row per source table | table_name | Append/MERGE |
| Silver | dq_metrics | one row per rule per run | run_id, rule | Append-only |
| Silver | dq_issues | one row per issue per run | run_id, source_table | Append-only |
| Gold | gold_daily_country_platform | one row per date/country/platform | date, country, platform | MERGE on composite key |

### Idempotency
- Bronze: Auto Loader tracks consumed files; re-run ingests nothing new
- Silver: MERGE on primary key with watermark (incremental) or DELETE+INSERT (full backfill)
- Gold: MERGE on composite key (date, country, platform)
- Watermarks: Track max `_ingested_at` per bronze table in `silver_watermark`, updated after each run

## Data Quality Findings

### 1. Platform derivation was unreliable in bronze
- **Problem**: The `platform` column in bronze data was inconsistent — some iPhone devices had `platform = "android"`. This propagated to silver, producing incorrect platform attribution.
- **Fix**: Derive `platform` from `device_model` in the silver layer: `contains("iphone") → ios, else android`. Applied to both silver_installs and silver_rejects.
- **Impact**: All silver tables recomputed via DELETE+INSERT. Verified: all iPhone models → ios; all other devices → android.

### 2. _ingested_at missing on S3 path
- **Problem**: Bronze ingestion only added `_ingested_at` to the Auto Loader (volume) path, not the S3 upsert path. Tables recreated from S3 had no `_ingested_at`, breaking watermark-based incremental processing.
- **Fix**: Added `_ingested_at` and metadata columns to the S3 upsert in `load_bronze`. Backfilled existing bronze tables.

### 3. Watermark-based MERGE didn't reprocess existing rows
- **Problem**: After fixing platform derivation in code, the MERGE found "no new data" because the watermark was already past the bronze data. Existing silver rows kept old, incorrect platform values.
- **Fix**: Changed silver_installs to DELETE+INSERT (full recompute). Created backfill notebook (`04_backfill`) with dual mode: incremental (watermark + MERGE) by default, full backfill (DELETE+INSERT) on demand.

### 4. DQ Rules (16 total: 12 error + 4 warning, all passing)

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

**Warning-severity rules (flagged, not dropped — do not fail the pipeline):**

| Table | Rule | Severity |
|---|---|---|
| silver_events | late arrivals (is_late) | warn |
| silver_events | pre-install events | warn |
| silver_events | orphan offer_id | warn |
| silver_installs | users with no events | warn |

### 5. Rejects
- 14,311 rejected rows captured in `silver_rejects` with reject_reason and raw_record JSON
- Reasons: null_id_or_bad_payout (offers), bad_ts_platform_or_country (installs), null_key_bad_ts_or_unknown_event (events), duplicate_event_id (events)

### 6. EDA Findings (Systematic Exploration)

Ran systematic EDA on all bronze tables. Findings beyond the platform bug:

**bronze_installs (40,000 rows)**:
- `campaign_id`: 9,887 nulls (24.7%) — significant installs without campaign attribution
- `_source_file` and `_source`: 100% null — metadata columns not populated (S3 path issue, fixed for new ingests)
- PLATFORM vs DEVICE_MODEL mismatches: 18,830 (47%) — the platform bug, fixed in silver by deriving platform from device_model
- Non-ISO country codes: 1,590 (4%) — rejected in silver via runtime DQ check
- No duplicate user_ids

**bronze_events (435,907 rows)**:
- Duplicate event_ids: 12,721 — rejected in silver (first arrival wins)
- All event_names valid (no unknown events)
- Date ranges: event_ts 2026-03-29 to 2026-05-27, ingest_ts 2026-04-01 to 2026-05-27
- Events start 3 days before installs (2026-03-29 vs 2026-04-01) — pre-install events flagged with `is_pre_install`

**bronze_offers (180 rows)**:
- No duplicates, no null payout_eur, no bad payouts — clean table

**bronze_user_profile (40,000 rows)**:
- No duplicates
- 11,410 payers (28.5%), 28,590 non-payers

**Cross-table consistency**:
- Events with user_id NOT in installs: 0 — all event users have installs (good)
- Events with offer_id NOT in offers: 13,672 — significant orphan offers. Flagged with `is_orphan_offer` in silver_events but NOT rejected. Excluded from gold reward cost (INNER JOIN drops them for priced payouts; counted in `rewards_unpriced`).

**Key new finding**: 13,672 events reference offer_ids that don't exist in the offers table. These are kept in silver_events with the `is_orphan_offer` flag. In gold, they are excluded from `reward_cost_eur` (priced payouts) but counted in `rewards_unpriced`. This is a data supply issue — the offers table may be incomplete or events reference offers from a different time window.

## Governance

### Unity Catalog Tags
- **Table-level**: `layer` (bronze/silver/gold), `domain` (installs/events/offers/user_profile/rejects/dq/aggregates), `quality` (raw/validated/quarantined/aggregated), `retention` (90-days/1-year/2-years)
- **Column-level**: `pii = true` for user_id, device_model, event_id, revenue_30d_eur, reject_key, raw_record

### Constraints (platform-enforced)
- Primary keys on all silver tables (offer_id, user_id, event_id)
- Foreign keys on silver_events (user_id → silver_installs, offer_id → silver_offers)
- NOT NULL on all key columns
- CHECK constraints: platform IN ('android','ios'), country LENGTH = 2, event_name IN known steps, payout_eur >= 0

### Access Control
- Bronze: data_engineers only (raw data, no analyst access)
- Silver: data_engineers (read+write), data_analysts (read-only)
- Gold: all roles (read-only)
- DQ tables: data_engineers + data_quality team

### DQ Monitoring
- 16 DQ rules evaluated after each silver run, logged to `dq_metrics` and `dq_issues`
- Alert fires when any rule fails, emails distinct issues and counts

## Data Dictionary

### Bronze Tables

| Table | Column | Type | Description |
|---|---|---|---|
| bronze_installs | user_id | string | Unique user identifier (PK, PII) |
| bronze_installs | install_ts | string | Install timestamp |
| bronze_installs | country | string | ISO-2 country code |
| bronze_installs | platform | string | Platform (unreliable in bronze, derived in silver) |
| bronze_installs | media_source | string | Acquisition media source |
| bronze_installs | device_model | string | Device model (PII) |
| bronze_installs | campaign_id | string | Campaign ID (24.7% null) |
| bronze_installs | _ingested_at | timestamp | When row was ingested |
| bronze_events | event_id | string | Unique event identifier (PK, PII) |
| bronze_events | user_id | string | User identifier (PII) |
| bronze_events | event_ts | string | When event occurred |
| bronze_events | event_name | string | Event type (app_open, offer_view, offer_start, goal_reached, reward_paid) |
| bronze_events | offer_id | string | Offer reference (nullable for app_open) |
| bronze_events | ingest_ts | string | When event was ingested |
| bronze_offers | offer_id | string | Unique offer identifier (PK) |
| bronze_offers | offer_category | string | Offer category |
| bronze_offers | payout_type | string | Payout type |
| bronze_offers | payout_eur | string | Payout in EUR |
| bronze_user_profile | user_id | string | Unique user identifier (PK, PII) |
| bronze_user_profile | events_lifetime | string | Total events count |
| bronze_user_profile | last_seen_ts | string | Last seen timestamp |
| bronze_user_profile | revenue_30d_eur | string | 30-day revenue in EUR (PII) |
| bronze_user_profile | is_payer | string | Whether user is a payer |

### Silver Tables

| Table | Column | Type | Description |
|---|---|---|---|
| silver_installs | user_id | string | Unique user identifier (PK, PII) |
| silver_installs | install_ts | timestamp | Install timestamp |
| silver_installs | country | string | ISO-2 country code |
| silver_installs | platform | string | Platform derived from device_model |
| silver_installs | device_model | string | Device model (PII) |
| silver_installs | install_date | date | Date of install (derived) |
| silver_events | event_id | string | Unique event identifier (PK, PII) |
| silver_events | user_id | string | User identifier (PII) |
| silver_events | event_ts | timestamp | When event occurred |
| silver_events | event_name | string | Event type |
| silver_events | offer_id | string | Offer reference (nullable) |
| silver_events | is_orphan_offer | boolean | True if offer_id not in silver_offers |
| silver_events | is_pre_install | boolean | True if event_ts < install_ts |
| silver_events | is_late | boolean | True if ingested >15min after midnight next day |
| silver_events | lag_hours | double | Hours between event_ts and ingest_ts |
| silver_events | event_date | date | Date of event (derived from event_ts) |
| silver_events | dq_flags | array<string> | Compact array of flag names: late, pre_install, orphan_offer |
| silver_offers | offer_id | string | Unique offer identifier (PK) |
| silver_offers | payout_eur | decimal(18,2) | Payout in EUR |
| silver_user_profile | user_id | string | Unique user identifier (PK, PII) |
| silver_user_profile | revenue_30d_eur | decimal(18,2) | 30-day revenue in EUR (PII) |
| silver_user_profile | is_payer | boolean | Whether user is a payer |
| silver_rejects | source | string | Which bronze table rejected this row |
| silver_rejects | reject_key | string | Key value of rejected record (PII) |
| silver_rejects | reject_reason | string | Why the row was rejected |
| silver_rejects | raw_record | string | JSON of the original row (PII) |
| silver_watermark | table_name | string | Source table name |
| silver_watermark | max_ingested_at | timestamp | Max _ingested_at processed |
| silver_watermark | updated_at | timestamp | When the watermark was updated |
| dq_metrics | run_id | string | Unique run identifier |
| dq_metrics | run_ts | timestamp | When the rule was evaluated |
| dq_metrics | layer | string | DQ layer (silver) |
| dq_metrics | rule | string | DQ rule name |
| dq_metrics | severity | string | error or warning |
| dq_metrics | passed | boolean | Whether the rule passed |
| dq_metrics | violations | double | Number of violations |
| dq_metrics | error | string | Error message if rule failed |
| dq_issues | run_id | string | Unique run identifier |
| dq_issues | run_ts | timestamp | When the issue was logged |
| dq_issues | source_table | string | Source bronze table |
| dq_issues | issue | string | Issue type (reject_reason) |
| dq_issues | issue_count | bigint | Number of occurrences |

### Gold Table

| Table | Column | Type | Description |
|---|---|---|---|
| gold_daily_country_platform | date | date | Aggregation date |
| gold_daily_country_platform | country | string | ISO-2 country code |
| gold_daily_country_platform | platform | string | android or ios |
| gold_daily_country_platform | installs | bigint | Number of installs |
| gold_daily_country_platform | events_total | bigint | Total events |
| gold_daily_country_platform | app_open_users | bigint | Unique users with app_open |
| gold_daily_country_platform | offer_view_users | bigint | Unique users with offer_view |
| gold_daily_country_platform | offer_start_users | bigint | Unique users with offer_start |
| gold_daily_country_platform | goal_reached_users | bigint | Unique users with goal_reached |
| gold_daily_country_platform | reward_paid_users | bigint | Unique users with reward_paid |
| gold_daily_country_platform | reward_payouts | bigint | Count of reward_paid events with priced offers |
| gold_daily_country_platform | rewards_unpriced | bigint | Count of reward_paid events on orphan offers (no payout) |
| gold_daily_country_platform | late_events | bigint | Count of events flagged is_late |
| gold_daily_country_platform | events_pre_install | bigint | Count of events flagged is_pre_install |
| gold_daily_country_platform | reward_cost_eur | decimal(18,2) | Total reward payouts in EUR (from offers) |
| gold_daily_country_platform | is_settled | boolean | True if date is older than LOOKBACK_DAYS |

## Tests

### Platform-Enforced Tests (Delta Constraints)
- NOT NULL on user_id, offer_id, event_id, install_ts, platform, country — fails the MERGE if violated
- CHECK (platform IN ('android','ios')) — rejects writes with invalid platform
- CHECK (country LENGTH = 2) — rejects non-ISO country codes
- Primary key uniqueness — duplicates cause MERGE to update rather than insert

### Runtime Tests (DQ Rules)
- 16 rules evaluated after each silver run (12 error + 4 warning)
- Error rules fail the pipeline; warning rules are logged only
- DQ metrics logged to `dq_metrics` for audit trail; alert fires on failure
- Issues from rejects table logged to `dq_issues`
- Warning rules surface: late arrivals (111,674), pre-install events (3,003), orphan offers (13,270), users with no events (2,305)

### Tests That Would Fail If Logic Were Wrong
- Platform: `SELECT device_model, platform FROM silver_installs WHERE device_model LIKE '%iphone%' AND platform != 'ios'` — should return 0 rows
- Dedup: `SELECT user_id, count(*) FROM silver_installs GROUP BY user_id HAVING count > 1` — should return 0 rows
- Gold total: `SELECT SUM(events_total) FROM gold_daily_country_platform` should equal `SELECT COUNT(*) FROM silver_events`
- Gold installs: `SELECT SUM(installs) FROM gold_daily_country_platform` should equal `SELECT COUNT(*) FROM silver_installs`

### Pytest Unit Tests (17 tests, all passing)

Standalone pytest suite in `tests/test_transforms.py` tests the transformation functions extracted to `transforms.py`:

| Test | What it verifies |
|---|---|
| test_offers_latest_snapshot_wins_and_bad_payout_rejected | Latest _ingested_at wins; unparseable/negative payouts rejected |
| test_offers_null_id_rejected | Null offer_id is rejected, not silently dropped |
| test_offers_empty_table | Empty input produces empty good and empty rejects |
| test_country_normalised_and_bad_rejected | ' de ' → 'DE'; 'Germany' rejected (not ISO-2) |
| test_first_install_wins | Earliest install_ts wins when same user_id appears twice |
| test_bad_platform_rejected | Platform not in (android, ios) is rejected |
| test_user_profile_latest_row_and_payer_parsing | Latest _ingested_at wins; 'True'/'yes'/'1'/'t' → True |
| test_user_profile_non_payer | 'no' → is_payer False |
| test_dedup_first_arrival_wins | Same event_id: earliest ingest_ts wins; rest → rejects |
| test_late_flag_uses_next_day_0015 | is_late True only when ingest_ts > 00:15 next day |
| test_orphan_and_pre_install_are_flagged_not_dropped | Unknown offer_id and pre-install ts are flagged, not rejected |
| test_unknown_event_name_rejected | Event names not in STEPS are rejected |
| test_malformed_timestamp_and_null_key_rejected | Bad timestamp → NULL → rejected; null user_id → rejected |
| test_event_with_known_offer_not_flagged | Valid offer_id → is_orphan_offer = False |
| test_null_offer_id_not_orphan | app_open with no offer_id → is_orphan_offer = False |
| test_lag_hours_computed | lag_hours = (ingest_ts - event_ts) in hours |
| test_empty_events | Empty events DataFrame → empty good + empty rejects |

Run with: `pytest tests/test_transforms.py -v`

## CI/CD — Job & DAB Bundle

**Job**: "Exmox Bronze → Silver → Gold Pipeline" (ID: 518342916204664) runs the pipeline notebooks in sequence with email-on-failure notification.

**DAB bundle** (experimental, `databricks.yml`): Defines `exmox_pipeline` and `exmox_backfill` jobs with a pytest gate. References v2 notebooks (`01_bronze_to_silver`, `02_silver_to_gold`). Not currently the primary pipeline.

Deploy and run (experimental):
```bash
databricks bundle deploy --target dev
databricks bundle run exmox_pipeline --target dev
```

## Deeper Questions

### The Daily Job

*Reasoning, not recalling.*

Suppose the job runs at 00:15 over the previous calendar day. The key question: what defines "the previous calendar day"?

If we filter by `event_date` (from `event_ts`), we capture events that *occurred* yesterday — including those not yet ingested (missed) and excluding events from earlier days ingested yesterday.

If we filter by `ingest_ts`, we capture events that *landed* yesterday — including late arrivals from earlier days and missing events from yesterday that arrive later.

The silver_events table has `is_late` and `lag_hours` columns. The `JOB_CUTOFF_MIN = 15` parameter defines late events as those ingested more than 15 minutes after midnight the next day.

**What the job gets wrong**: If running at 00:15 processing by event_date = yesterday, it misses events generated yesterday but ingested after 00:15. If processing by ingest_ts, it may include events from earlier days and miss yesterday's late arrivals.

**How to find out**: Compare `COUNT(*) FROM silver_events WHERE event_date = yesterday` against `COUNT(*) FROM bronze_events WHERE date(ingest_ts) = yesterday`. If bronze count is higher, events arrive after the job runs.

**What to build instead**:
- Process by `ingest_ts` window (00:00–00:14 today covers all of yesterday plus a 14-minute buffer)
- Use file-arrival triggers rather than a fixed schedule
- For gold: recompute the last N days each run (the `LOOKBACK_DAYS = 6` parameter with `is_settled` already does this)

**Actual analysis (from data)**:
- Median lag between event_ts and ingest_ts: 4.97 hours
- 72.6% of events ingested same-day, 26% next-day, 1.3% with 2+ day delay
- Max lag: 133.86 hours (5.6 days)
- Daily job simulation for 2026-05-27: 5,773 events with that event_date, ALL ingested by 00:15 next day (0 missed)
- BUT: 2,280 events ingested on 2026-05-27 had earlier event dates — would be wrongly included if filtering by ingest_ts
- Conclusion: filtering by event_date = yesterday works for this dataset (all events ingested by midnight+15min), but 26% of events consistently arrive next-day. The `LOOKBACK_DAYS = 6` parameter in gold handles late arrivals by recomputing the last 6 days each run.
- Risk: if ingestion delays increase beyond 1 day, the daily job would miss events. The 1.3% with 2+ day delay shows this already happens for some events.
- 2,032 events at 23:00+ were ingested after midnight — these are the midnight crossover cases that the `JOB_CUTOFF_MIN = 15` parameter is designed to catch.

### Platform Choices

**Partitioning/Clustering**: No explicit partition columns. Tables are small (180-423K rows), so partitioning adds overhead without benefit. Partition count is now **dynamic** via `auto_partition_count()`: 1 partition per 50K rows, clamped to [1, 32]. Shuffle partitions also scale dynamically (1 per 50K rows, clamped to [8, 200]). OPTIMIZE runs after each pipeline run.

If wrong about partitioning: symptom would be small-file problems or partition pruning not working. Would check via `DESCRIBE DETAIL` and query profiles.

**Ingestion pattern**: Auto Loader for volume/S3, MERGE with watermark for silver. Stops being right when:
- Data volume exceeds ~10M rows per table (MERGE becomes expensive)
- Streaming latency requirements emerge (would switch to Structured Streaming)
- Schema changes frequently (would need schema evolution strategy)

**What to govern centrally in UC**:
- Table and column tags (PII classification, layer, domain) — done in `05_governance`
- Constraints (PK, FK, CHECK, NOT NULL) — done in `02_silver_transform`
- Access grants (role-based) — done in `05_governance`
- Catalog/schema creation and naming — should be standardized
- Retention policies — tagged but not enforced (would need Delta table properties)

### How It Fails

**Silent wrong numbers** (no error, wrong result):
1. **Late-arriving data**: Event generated at 23:50 but ingested at 00:20 — a job at 00:15 processing yesterday misses it. Gold undercounts. No error.
2. **Inner join drops**: Gold uses INNER JOIN between events and installs. If an event exists for a user_id not in silver_installs (install was rejected), that event is silently dropped from gold.
3. **Watermark drift**: If bronze ingestion fails silently (no new _ingested_at), watermark doesn't advance, next silver run finds "no new data" — even though there should be data.
4. **Platform edge cases**: `device_model` containing "iphone" as a substring of a non-iPhone device would be classified as ios. No validation on device_model format.
5. **Dedup by first arrival**: If the same event_id arrives with different data on re-ingestion, the first version wins and the update is silently dropped.
6. **Gold recompute misses deletes**: Gold uses MERGE (upsert). If a silver row is deleted, the gold aggregate is not recomputed — stale row persists.

**Platform-hidden failures**:
- Delta MERGE with `whenMatchedUpdateAll` can silently overwrite columns if `schema.autoMerge` is enabled (disabled here — the right choice).
- OPTIMIZE can change file layout without notice, affecting query plans.

### Scale

**What survives** (50 sources, continuous):
- Watermark-based incremental processing — scales with data volume
- DQ rules — simple aggregations, scale linearly
- Governance tags and constraints — UC-native, scale with the catalog
- Gold aggregation pattern — groupBy + join, scales with Spark
- Rejects logging — independent of main data flow

**What changes**:
- Ingestion: Auto Loader handles continuous arrival; would switch from batch to streaming trigger
- Partitioning: Would partition silver_events by event_date, silver_installs by install_date
- Clustering: Liquid Clustering on frequently filtered columns (user_id, event_date)
- Compute: Move from serverless interactive to job clusters with autoscaling
- Monitoring: Add structured streaming metrics (throughput, latency, backlog)
- Gold: Switch from full recompute to incremental aggregation with daily partitions

**What gets thrown away**:
- `auto_partition_count` formula (1 partition per 50K rows) — would need proper partitioning
- Full recompute of rejects — would use incremental reject logging
- Single-notebook architecture — would split into separate tasks per source
- Manual DQ rule list — would use a rules engine or Great Expectations

## How I Worked with AI

**Tool**: Databricks Genie Code (built-in AI assistant in Databricks notebooks)

**What I asked for**:
- Bronze ingestion notebook with Auto Loader and S3 support
- Silver transformation with dedup, platform derivation, DQ checks
- Gold aggregation by date/country/platform
- DQ alert with email notification
- Backfill notebook with incremental/full recompute modes
- Governance notebook with UC tags and comments
- Pytest suite (17 tests) for transformation functions
- CI/CD job with daily schedule

**What I kept**:
- Overall pipeline architecture (bronze/silver/gold layers)
- Watermark-based incremental processing pattern
- DQ rules and constraints
- Platform derivation from device_model
- DELETE+INSERT for full backfill mode

**What I rejected and why**:
- AI initially used MERGE-only for silver_installs. Correct for incremental but couldn't reprocess existing rows when platform fix was applied. Caught when user reported "iPhone still not assigned to ios" — watermark was already past data, so MERGE found "no new data". Fix: Changed to DELETE+INSERT for full recompute.
- AI initially used `mode("overwrite")` for silver_rejects in backfill. Auto-approval flagged this as destructive. Fix: Changed to DELETE + INSERT.
- AI created multiple DQ alert attempts without a schedule. Consolidated into a single alert with correct query, notification template, and daily schedule.

**Where AI was confidently wrong**:
- Platform derivation: AI applied the fix to the code but didn't realize existing silver rows wouldn't be reprocessed by MERGE. The fix was in the code but not in the data. Caught by user's query showing incorrect platform values.
- _ingested_at: AI only added metadata columns to the Auto Loader path, not the S3 path. The two paths had different schemas. Caught when silver processing failed.

## What's Not Done
- [ ] **Scale simulation**: Haven't tested with larger datasets 
- [ ] **Data retention enforcement**: Tags applied but not enforced. Need Delta table properties for time travel and vacuum scheduling.
- [ ] **Column-level access control**: PII tags applied but no column masks or row-level filters implemented. Investigated but no real personal data is visible in the dataset — `user_id`, `device_model`, `event_id` are synthetic identifiers, not actual PII. Column masks would add complexity without benefit for this data.
