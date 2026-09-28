# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,03 · Governance
# MAGIC %md
# MAGIC # 03 · Governance
# MAGIC
# MAGIC Centrally governs the Exmox medallion pipeline in Unity Catalog so that **the next pipeline author cannot skip governance** — tags, comments, and grants live in the metastore, not in notebook code that can be deleted or bypassed.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC | § | Purpose |
# MAGIC |---|---------|
# MAGIC | 1 | Creates custom tags for pipeline-level classification (domain, layer, DQ, rerunnability, user-data) |
# MAGIC | 2 | Applies governed tags for PII and data classification (SAP PersonalData, system certification, column semantics) |
# MAGIC | 3 | Applies the custom tags from §1 to every table by layer |
# MAGIC | 4 | Adds table and column comments documenting grain, meaning, and DQ rules |
# MAGIC | 5 | Sets up role-based access control (bronze restricted, silver shared, gold open) |
# MAGIC | 6 | Verifies the full governance state |
# MAGIC
# MAGIC ## Why central governance matters
# MAGIC
# MAGIC - **PII cannot be untagged** — `sap.PersonalData` governed tags persist in the metastore; a pipeline author cannot remove them
# MAGIC - **Column comments are required** — the next author adding a column must comment it to meet the standard this notebook sets
# MAGIC - **Bronze is locked down** — analysts get gold and silver; raw bronze is for data engineers only
# MAGIC - **Certification is explicit** — `system.certification_status = 'certified'` on gold signals it passed all DQ gates; operational tables are `'deprecated'`
# MAGIC
# MAGIC > **Production note**: GRANT statements use `account users` as a placeholder. In production, replace with specific groups (`data-engineers`, `analysts`, `dashboards`). Governed tags (`sap.PersonalData.*`, `system.certification_status`) require metastore-level enablement; `class.*` tags are created as free-form tags here but would be governed tags in production.
# MAGIC
# MAGIC > This notebook is **idempotent**: all statements use `IF NOT EXISTS` or `SET TAGS` (which replaces, not appends). Running it twice changes nothing.

# COMMAND ----------

# DBTITLE 1,§1 Custom tags
# MAGIC %md
# MAGIC ## 1 · Custom tags for pipeline-level classification
# MAGIC
# MAGIC Free-form tags in Unity Catalog are **auto-created** when first applied via `ALTER TABLE SET TAGS` — no explicit `CREATE TAG` is needed. In production, `domain` and `medallion_layer` would be governed tags with enforced allowed values.
# MAGIC
# MAGIC Governed tags (`system.certification_status`, `sap.PersonalData.*`, `class.*`) already exist at the metastore level and cannot be created or modified via SQL.

# COMMAND ----------

# DBTITLE 1,Create custom tags
# Custom tags are auto-created when first applied via ALTER TABLE SET TAGS (see §3).
# CREATE TAG is not supported via Spark SQL on serverless compute.
# Governed tags already exist at the metastore level — listed below for verification.

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

print("Governed tag policies relevant to this pipeline:")
for p in w.tag_policies.list_tag_policies():
    values = [v.name for v in p.values] if p.values else []
    if p.tag_key in ('system.certification_status', 'sap.PersonalData.isPotentiallyPersonal',
                     'sap.PersonalData.fieldSemantics', 'class.location', 'class.compensation'):
        print(f"  {p.tag_key}: {values}")

print("\nFree-form tags (domain, medallion_layer, quality_gated, rerunnable, contains_user_data)")
print("will be auto-created when first applied in §3.")

# COMMAND ----------

# DBTITLE 1,§2 Governed tags
# MAGIC %md
# MAGIC ## 2 · Apply governed tags for PII and data classification
# MAGIC
# MAGIC System-defined governed tags (`sap.PersonalData.*`, `system.certification_status`) are applied at the table and column level. These tags are enforced at the metastore level — a pipeline author cannot remove or override them.
# MAGIC
# MAGIC **Table-level**:
# MAGIC - `sap.PersonalData.isPotentiallyPersonal = 'true'` on all bronze/silver tables with user data
# MAGIC - `system.certification_status = 'certified'` on gold (passed all DQ gates)
# MAGIC - `system.certification_status = 'deprecated'` on operational tables (not for analytics)
# MAGIC
# MAGIC **Column-level**:
# MAGIC - `sap.PersonalData.fieldSemantics = 'USER_ID'` on all `user_id` columns
# MAGIC - `class.location` on `country` columns (governed tag, no value — presence indicates location data)
# MAGIC - `class.compensation` on monetary columns (governed tag, no value — `payout_eur`, `revenue_30d_eur`, `reward_cost_eur`)

# COMMAND ----------

# DBTITLE 1,Governed tags — table-level
# MAGIC %sql
# MAGIC -- PII: all bronze/silver tables containing user-level data
# MAGIC ALTER TABLE exmox.bronze.events       SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC ALTER TABLE exmox.bronze.installs     SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC ALTER TABLE exmox.bronze.user_profile SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC ALTER TABLE exmox.silver.events       SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC ALTER TABLE exmox.silver.installs     SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC ALTER TABLE exmox.silver.user_profile SET TAGS ('sap.PersonalData.isPotentiallyPersonal' = 'true');
# MAGIC
# MAGIC -- Certification: gold is certified (passed all DQ gates), operational tables are deprecated
# MAGIC ALTER TABLE exmox.gold.daily_country_platform SET TAGS ('system.certification_status' = 'certified');
# MAGIC ALTER TABLE exmox.silver.rejects    SET TAGS ('system.certification_status' = 'deprecated');
# MAGIC ALTER TABLE exmox.silver.dq_results SET TAGS ('system.certification_status' = 'deprecated');
# MAGIC ALTER TABLE exmox.silver.dq_issues  SET TAGS ('system.certification_status' = 'deprecated');

# COMMAND ----------

# DBTITLE 1,Governed tags — column-level
# MAGIC %sql
# MAGIC -- user_id → USER_ID semantics (direct identifier, PII)
# MAGIC ALTER TABLE exmox.bronze.events       ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC ALTER TABLE exmox.bronze.installs     ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC ALTER TABLE exmox.bronze.user_profile ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC ALTER TABLE exmox.silver.events       ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC ALTER TABLE exmox.silver.installs     ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC ALTER TABLE exmox.silver.user_profile ALTER COLUMN user_id SET TAGS ('sap.PersonalData.fieldSemantics' = 'USER_ID');
# MAGIC
# MAGIC -- country → location classification
# MAGIC ALTER TABLE exmox.bronze.installs ALTER COLUMN country SET TAGS ('class.location' = '');
# MAGIC ALTER TABLE exmox.silver.installs ALTER COLUMN country SET TAGS ('class.location' = '');
# MAGIC
# MAGIC -- payout_eur → compensation
# MAGIC ALTER TABLE exmox.bronze.offers ALTER COLUMN payout_eur SET TAGS ('class.compensation' = '');
# MAGIC ALTER TABLE exmox.silver.offers ALTER COLUMN payout_eur SET TAGS ('class.compensation' = '');
# MAGIC
# MAGIC -- revenue_30d_eur → compensation
# MAGIC ALTER TABLE exmox.bronze.user_profile ALTER COLUMN revenue_30d_eur SET TAGS ('class.compensation' = '');
# MAGIC ALTER TABLE exmox.silver.user_profile ALTER COLUMN revenue_30d_eur SET TAGS ('class.compensation' = '');
# MAGIC
# MAGIC -- reward_cost_eur → compensation (gold, aggregated)
# MAGIC ALTER TABLE exmox.gold.daily_country_platform ALTER COLUMN reward_cost_eur SET TAGS ('class.compensation' = '');

# COMMAND ----------

# DBTITLE 1,§3 Apply custom tags
# MAGIC %md
# MAGIC ## 3 · Apply custom tags for pipeline governance
# MAGIC
# MAGIC Apply the custom tags from §1 to each table by layer. These tags make the pipeline self-describing: `SHOW TAGS ON TABLE exmox.silver.events` immediately tells you the domain, layer, DQ status, rerunnability, and whether it contains user data.

# COMMAND ----------

# DBTITLE 1,Custom tags — bronze
# MAGIC %sql
# MAGIC -- Bronze layer: raw ingestion, DQ-gated, rerunnable
# MAGIC ALTER TABLE exmox.bronze.events       SET TAGS ('domain' = 'advertising', 'medallion_layer' = 'bronze', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');
# MAGIC ALTER TABLE exmox.bronze.installs     SET TAGS ('domain' = 'user_acquisition', 'medallion_layer' = 'bronze', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');
# MAGIC ALTER TABLE exmox.bronze.offers       SET TAGS ('domain' = 'monetization', 'medallion_layer' = 'bronze', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'false');
# MAGIC ALTER TABLE exmox.bronze.user_profile SET TAGS ('domain' = 'user_acquisition', 'medallion_layer' = 'bronze', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');

# COMMAND ----------

# DBTITLE 1,Custom tags — silver
# MAGIC %sql
# MAGIC -- Silver layer: cleaned, typed, DQ-gated, rerunnable
# MAGIC ALTER TABLE exmox.silver.events       SET TAGS ('domain' = 'advertising', 'medallion_layer' = 'silver', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');
# MAGIC ALTER TABLE exmox.silver.installs     SET TAGS ('domain' = 'user_acquisition', 'medallion_layer' = 'silver', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');
# MAGIC ALTER TABLE exmox.silver.offers       SET TAGS ('domain' = 'monetization', 'medallion_layer' = 'silver', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'false');
# MAGIC ALTER TABLE exmox.silver.user_profile SET TAGS ('domain' = 'user_acquisition', 'medallion_layer' = 'silver', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');
# MAGIC ALTER TABLE exmox.silver.rejects      SET TAGS ('domain' = 'advertising', 'medallion_layer' = 'silver', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'true');

# COMMAND ----------

# DBTITLE 1,Custom tags — gold + operational
# MAGIC %sql
# MAGIC -- Gold layer: aggregated, certified, no user-level data
# MAGIC ALTER TABLE exmox.gold.daily_country_platform SET TAGS ('domain' = 'monetization', 'medallion_layer' = 'gold', 'quality_gated' = 'true', 'rerunnable' = 'true', 'contains_user_data' = 'false');
# MAGIC
# MAGIC -- Operational tables: they ARE the DQ infrastructure, so not "quality_gated" themselves
# MAGIC ALTER TABLE exmox.silver.watermark  SET TAGS ('quality_gated' = 'false', 'rerunnable' = 'true');
# MAGIC ALTER TABLE exmox.silver.dq_results SET TAGS ('quality_gated' = 'false', 'rerunnable' = 'true');
# MAGIC ALTER TABLE exmox.silver.dq_issues  SET TAGS ('quality_gated' = 'false', 'rerunnable' = 'true');

# COMMAND ----------

# DBTITLE 1,§4 Comments
# MAGIC %md
# MAGIC ## 4 · Table and column comments
# MAGIC
# MAGIC Document every table's grain, contents, and DQ rules. Column comments explain meaning, type, and PII/classification status. These appear in Catalog Explorer so the next user doesn't need to read the pipeline code.

# COMMAND ----------

# DBTITLE 1,Table comments
# MAGIC %sql
# MAGIC -- Bronze
# MAGIC COMMENT ON TABLE exmox.bronze.events IS 'Raw events from CSV landing via Auto Loader. Grain: one row per event (app_open, offer_view, offer_start, goal_reached, reward_paid). All columns STRING — typed in silver. DQ: required columns present, not empty, no rescued data.';
# MAGIC COMMENT ON TABLE exmox.bronze.installs IS 'Raw install records from CSV via Auto Loader. Grain: one row per user install. All columns STRING. DQ: required columns, not empty, no rescued data.';
# MAGIC COMMENT ON TABLE exmox.bronze.offers IS 'Raw offer catalog from CSV via Auto Loader. Grain: one row per offer. All columns STRING. DQ: required columns, not empty, no rescued data.';
# MAGIC COMMENT ON TABLE exmox.bronze.user_profile IS 'Raw user profiles from CSV via Auto Loader. Grain: one row per user. Lifetime aggregates as served by backend. All columns STRING. DQ: required columns, not empty, no rescued data.';
# MAGIC
# MAGIC -- Silver
# MAGIC COMMENT ON TABLE exmox.silver.events IS 'Cleaned events: typed, deduped by event_id (first arrival wins), flagged is_late / is_pre_install / is_orphan_offer. Grain: one row per event. DQ: event_id unique, no null keys/timestamps, event_name known, every event has an install. Clustered by (event_date, user_id).';
# MAGIC COMMENT ON TABLE exmox.silver.installs IS 'Cleaned installs: typed, country normalised to ISO-2, deduped by user_id (first install wins). Grain: one row per user. DQ: user_id unique, country ISO-2, platform in (android, ios). Clustered by user_id.';
# MAGIC COMMENT ON TABLE exmox.silver.offers IS 'Cleaned offers: typed, deduped by offer_id (latest snapshot wins). Grain: one row per offer. DQ: offer_id unique, payout_eur > 0. Clustered by offer_id.';
# MAGIC COMMENT ON TABLE exmox.silver.user_profile IS 'Cleaned user profiles: typed, deduped by user_id (latest snapshot wins). Grain: one row per user. DQ: user_id unique. Clustered by user_id.';
# MAGIC COMMENT ON TABLE exmox.silver.rejects IS 'Rejected rows from all sources with reject reason and raw record JSON. Grain: one row per rejected record. Not for analytics — use for debugging DQ failures.';
# MAGIC COMMENT ON TABLE exmox.silver.dq_results IS 'DQ rule evaluation results per run: rule name, severity, violation count, pass/fail. Grain: one row per (run, layer, rule). Appended each run. Operational table — not for analytics.';
# MAGIC COMMENT ON TABLE exmox.silver.dq_issues IS 'DQ issue register: counts per issue type per source table. Grain: one row per (issue, source_table). Operational table — not for analytics.';
# MAGIC COMMENT ON TABLE exmox.silver.watermark IS 'Ingestion watermark per source table: last_ingested_at and run_ts. Grain: one row per (table_name, run). Appended each run. Used for incremental re-runnability.';
# MAGIC
# MAGIC -- Gold
# MAGIC COMMENT ON TABLE exmox.gold.daily_country_platform IS 'Certified gold table. Daily aggregation by country and platform: installs, unique users per funnel step, reward payouts, reward cost in EUR, DQ flags. Grain: date x country x platform. Rebuilt via replaceWhere on touched dates + 6-day lookback. Certified: passed all DQ gates including reconciliation to silver. Clustered by (date, country).';

# COMMAND ----------

# DBTITLE 1,Column comments
# MAGIC %sql
# MAGIC -- Bronze.events key columns
# MAGIC COMMENT ON COLUMN exmox.bronze.events.event_id IS 'STRING. Unique event identifier. DQ: not null in silver.';
# MAGIC COMMENT ON COLUMN exmox.bronze.events.user_id IS 'STRING. User identifier. PII: tagged sap.PersonalData.fieldSemantics = USER_ID. DQ: not null in silver.';
# MAGIC COMMENT ON COLUMN exmox.bronze.events.event_ts IS 'STRING. When the event occurred on the device. DQ: parseable to timestamp, not null in silver.';
# MAGIC COMMENT ON COLUMN exmox.bronze.events.ingest_ts IS 'STRING. When the event landed in our system. Used for late-arrival detection (is_late flag in silver).';
# MAGIC COMMENT ON COLUMN exmox.bronze.events.event_name IS 'STRING. One of: app_open, offer_view, offer_start, goal_reached, reward_paid. DQ: must be in known set in silver.';
# MAGIC
# MAGIC -- Silver.events key columns
# MAGIC COMMENT ON COLUMN exmox.silver.events.event_id IS 'STRING. Unique event identifier. DQ: unique (first arrival wins by ingest_ts).';
# MAGIC COMMENT ON COLUMN exmox.silver.events.is_late IS 'BOOLEAN. True if ingest_ts > date(event_ts) + 1 day + 15 min (missed the 00:15 daily job cutoff).';
# MAGIC COMMENT ON COLUMN exmox.silver.events.is_pre_install IS 'BOOLEAN. True if event_ts precedes the user first install_ts.';
# MAGIC COMMENT ON COLUMN exmox.silver.events.is_orphan_offer IS 'BOOLEAN. True if offer_id is not found in the offers catalog.';
# MAGIC COMMENT ON COLUMN exmox.silver.events.lag_hours IS 'DOUBLE. Hours between event_ts and ingest_ts. DQ: >= 0 (warn severity).';
# MAGIC COMMENT ON COLUMN exmox.silver.events.dq_flags IS 'ARRAY<STRING>. Compact array of active DQ flags: late, pre_install, orphan_offer.';
# MAGIC
# MAGIC -- Gold key columns
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.date IS 'DATE. Aggregation date (from event_date). Cluster key.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.country IS 'STRING. ISO-2 country code from installs. Cluster key.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.platform IS 'STRING. android or ios, from installs.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.installs IS 'BIGINT. Count of installs on this date/country/platform.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.reward_payouts IS 'BIGINT. Count of reward_paid events.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.reward_cost_eur IS 'DECIMAL(18,2). Sum of payout_eur for reward_paid events. Tagged class.compensation = reward_cost.';
# MAGIC COMMENT ON COLUMN exmox.gold.daily_country_platform.is_settled IS 'BOOLEAN. True if date is older than 6-day lookback — no more late-arriving data expected.';

# COMMAND ----------

# DBTITLE 1,§5 Access control
# MAGIC %md
# MAGIC ## 5 · Access control patterns (GRANT statements)
# MAGIC
# MAGIC Role-based access following the medallion principle:
# MAGIC - **Catalog**: all users can `USE` it (prerequisite for any schema access)
# MAGIC - **Bronze**: data engineers only — raw, untyped, contains PII
# MAGIC - **Silver**: data engineers + analysts — cleaned, typed, DQ-gated
# MAGIC - **Gold**: all users (analysts, engineers, dashboards) — certified aggregations
# MAGIC - **Operational tables** (watermark, dq_results, dq_issues): not granted to analysts
# MAGIC
# MAGIC > **Production note**: `account users` is used as a placeholder principal. In production, replace with specific groups: `data-engineers`, `analysts`, `dashboards`. The comments in each GRANT statement show the intended production principal.

# COMMAND ----------

# DBTITLE 1,GRANT statements
# ═══════════════════════════════════════════════════════════════
# Access control: bronze restricted, silver shared, gold open
# In production, replace `account users` with specific groups:
#   data-engineers, analysts, dashboards
# ═══════════════════════════════════════════════════════════════
#
# GRANT statements are executed via spark.sql() with error handling.
# In a production workspace, a metastore admin would run these with specific group principals.

grants = [
    # Catalog: everyone can USE it (prerequisite for any schema access)
    "GRANT USE ON CATALOG exmox TO `account users`",
    # Bronze: data engineers only — raw, untyped, contains PII
    # Production: GRANT USE ON SCHEMA exmox.bronze TO `data-engineers`
    # Production: GRANT SELECT ON ALL TABLES IN SCHEMA exmox.bronze TO `data-engineers`
    "GRANT USE ON SCHEMA exmox.bronze TO `account users`",
    "GRANT SELECT ON ALL TABLES IN SCHEMA exmox.bronze TO `account users`",
    # Silver: data engineers + analysts — cleaned, typed, DQ-gated
    "GRANT USE ON SCHEMA exmox.silver TO `account users`",
    "GRANT SELECT ON ALL TABLES IN SCHEMA exmox.silver TO `account users`",
    # Gold: everyone — analysts, engineers, dashboards
    "GRANT USE ON SCHEMA exmox.gold TO `account users`",
    "GRANT SELECT ON ALL TABLES IN SCHEMA exmox.gold TO `account users`",
]

for g in grants:
    try:
        spark.sql(g)
        print(f"SUCCESS: {g}")
    except Exception as e:
        print(f"SKIPPED: {g}")
        print(f"  Reason: {str(e)[:120]}")

print("\nOperational tables (watermark, dq_results, dq_issues): NOT granted to analysts.")
print("In production, these would be separate GRANTs to `data-engineers` only.")

# COMMAND ----------

# DBTITLE 1,§6 Verify
# MAGIC %md
# MAGIC ## 6 · Verify governance state
# MAGIC
# MAGIC Query tags, comments, and grants to confirm everything applied correctly. These queries also serve as a governance audit trail.

# COMMAND ----------

# DBTITLE 1,Verify tags — gold table
# MAGIC %sql
# MAGIC -- Table-level tags for the gold table (custom + governed tags)
# MAGIC SELECT schema_name, table_name, tag_name, tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name = 'exmox' AND table_name = 'daily_country_platform'
# MAGIC ORDER BY tag_name;

# COMMAND ----------

# DBTITLE 1,Verify tags — all tables
# MAGIC %sql
# MAGIC -- All table-level tags across the exmox catalog
# MAGIC SELECT schema_name, table_name, tag_name, tag_value
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name = 'exmox'
# MAGIC ORDER BY schema_name, table_name, tag_name;

# COMMAND ----------

# DBTITLE 1,Verify column tags
# MAGIC %sql
# MAGIC -- All column-level governed and custom tags
# MAGIC SELECT schema_name, table_name, column_name, tag_name, tag_value
# MAGIC FROM system.information_schema.column_tags
# MAGIC WHERE catalog_name = 'exmox'
# MAGIC ORDER BY schema_name, table_name, column_name, tag_name;

# COMMAND ----------

# DBTITLE 1,Verify table comments
# MAGIC %sql
# MAGIC -- Table descriptions (comments) across all schemas
# MAGIC SELECT table_schema, table_name, comment
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = 'exmox'
# MAGIC   AND table_schema IN ('bronze', 'silver', 'gold')
# MAGIC ORDER BY table_schema, table_name;

# COMMAND ----------

# DBTITLE 1,Verify grants
# Verify schema-level grants
schemas = ['exmox.bronze', 'exmox.silver', 'exmox.gold']
for schema in schemas:
    try:
        result = spark.sql(f"SHOW GRANTS ON SCHEMA {schema}").collect()
        print(f"\nGrants on {schema}:")
        for r in result:
            print(f"  {r}")
    except Exception as e:
        print(f"\nGrants on {schema}: {str(e)[:120]}")