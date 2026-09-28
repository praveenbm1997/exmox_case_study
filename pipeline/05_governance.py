# Databricks notebook source
# DBTITLE 1,Governance — Title
# MAGIC %md
# MAGIC # Governance — Unity Catalog Tags, Comments & Access Control
# MAGIC
# MAGIC This notebook applies governance policies to all pipeline tables:
# MAGIC - **Table tags**: layer, domain, quality, retention
# MAGIC - **Column tags**: PII classification for sensitive columns
# MAGIC - **Table/column comments**: documentation for discovery
# MAGIC - **Access grants**: role-based access control
# MAGIC - **Constraints**: PK, FK, CHECK, NOT NULL (already applied in 02_silver_transform)

# COMMAND ----------

# DBTITLE 1,Table Tags — Layer, Domain, Quality
# ── Table-level tags: layer, domain, quality, retention ────────────────────
_tables = {
    "exmox.bronze.bronze_installs": {"layer": "bronze", "domain": "installs", "quality": "raw", "retention": "90-days"},
    "exmox.bronze.bronze_events": {"layer": "bronze", "domain": "events", "quality": "raw", "retention": "90-days"},
    "exmox.bronze.bronze_offers": {"layer": "bronze", "domain": "offers", "quality": "raw", "retention": "90-days"},
    "exmox.bronze.bronze_user_profile": {"layer": "bronze", "domain": "user_profile", "quality": "raw", "retention": "90-days"},
    "exmox.silver.silver_installs": {"layer": "silver", "domain": "installs", "quality": "validated", "retention": "1-year"},
    "exmox.silver.silver_events": {"layer": "silver", "domain": "events", "quality": "validated", "retention": "1-year"},
    "exmox.silver.silver_offers": {"layer": "silver", "domain": "offers", "quality": "validated", "retention": "1-year"},
    "exmox.silver.silver_user_profile": {"layer": "silver", "domain": "user_profile", "quality": "validated", "retention": "1-year"},
    "exmox.silver.silver_rejects": {"layer": "silver", "domain": "rejects", "quality": "quarantined", "retention": "1-year"},
    "exmox.silver.dq_metrics": {"layer": "silver", "domain": "dq", "quality": "metadata", "retention": "1-year"},
    "exmox.silver.dq_issues": {"layer": "silver", "domain": "dq", "quality": "metadata", "retention": "1-year"},
    "exmox.gold.gold_daily_country_platform": {"layer": "gold", "domain": "aggregates", "quality": "aggregated", "retention": "2-years"},
}
for _tbl, _tags in _tables.items():
    _tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in _tags.items()])
    try:
        spark.sql(f"ALTER TABLE {_tbl} SET TAGS ({_tag_str})")
        print(f"  ✓ {_tbl}: {', '.join([f'{k}={v}' for k, v in _tags.items()])}", flush=True)
    except Exception as e:
        print(f"  ⚠ {_tbl}: {e}", flush=True)
print("── Table tags applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Column Tags — PII Classification
# ── Column-level tags: PII classification ───────────────────────────────────
_col_tags = [
    ("exmox.bronze.bronze_installs", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.bronze.bronze_installs", "device_model", {"pii": "true", "pii_type": "device_info"}),
    ("exmox.bronze.bronze_events", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.bronze.bronze_events", "event_id", {"pii": "true", "pii_type": "event_identifier"}),
    ("exmox.bronze.bronze_user_profile", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.bronze.bronze_user_profile", "revenue_30d_eur", {"pii": "true", "pii_type": "financial_data"}),
    ("exmox.silver.silver_installs", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.silver.silver_installs", "device_model", {"pii": "true", "pii_type": "device_info"}),
    ("exmox.silver.silver_events", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.silver.silver_events", "event_id", {"pii": "true", "pii_type": "event_identifier"}),
    ("exmox.silver.silver_user_profile", "user_id", {"pii": "true", "pii_type": "user_identifier"}),
    ("exmox.silver.silver_user_profile", "revenue_30d_eur", {"pii": "true", "pii_type": "financial_data"}),
    ("exmox.silver.silver_rejects", "reject_key", {"pii": "true", "pii_type": "identifier"}),
    ("exmox.silver.silver_rejects", "raw_record", {"pii": "true", "pii_type": "raw_pii_payload"}),
]
for _tbl, _col, _tags in _col_tags:
    _tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in _tags.items()])
    try:
        spark.sql(f"ALTER TABLE {_tbl} ALTER COLUMN {_col} SET TAGS ({_tag_str})")
        print(f"  ✓ {_tbl}.{_col}: {', '.join([f'{k}={v}' for k, v in _tags.items()])}", flush=True)
    except Exception as e:
        print(f"  ⚠ {_tbl}.{_col}: {e}", flush=True)
print("── Column PII tags applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Table Comments
# ── Table and column comments ──────────────────────────────────────────────
_table_comments = {
    "exmox.bronze.bronze_installs": "Raw install data from S3/Volume. One row per install with acquisition attributes.",
    "exmox.bronze.bronze_events": "Raw event data from S3/Volume. One row per event (app_open, offer_view, offer_start, goal_reached, reward_paid).",
    "exmox.bronze.bronze_offers": "Raw offer data from S3/Volume. One row per offer with category, payout type, and payout in EUR.",
    "exmox.bronze.bronze_user_profile": "Raw user profile data from S3/Volume. One row per user with lifetime aggregates.",
    "exmox.silver.silver_installs": "Cleaned, deduplicated install data. Platform derived from device_model. DQ-validated.",
    "exmox.silver.silver_events": "Cleaned, deduplicated event data. First-arrival wins. Enriched with orphan/pre-install/late flags.",
    "exmox.silver.silver_offers": "Cleaned, deduplicated offer data. Latest version wins. DQ-validated.",
    "exmox.silver.silver_user_profile": "Cleaned, deduplicated user profile data. Latest version wins. DQ-validated.",
    "exmox.silver.silver_rejects": "Rejected rows from all bronze sources with reject_reason and raw_record JSON.",
    "exmox.silver.dq_metrics": "Data quality rule evaluation results. One row per rule per run.",
    "exmox.silver.dq_issues": "Aggregated issue counts from silver_rejects. One row per issue type per run.",
    "exmox.gold.gold_daily_country_platform": "Daily aggregates by country and platform. Installs, unique users per event type, reward payouts and cost in EUR.",
}
for _tbl, _comment in _table_comments.items():
    _c = _comment.replace("'", "\\'")
    try:
        spark.sql(f"COMMENT ON TABLE {_tbl} IS '{_c}'")
        print(f"  ✓ {_tbl}: comment set", flush=True)
    except Exception as e:
        print(f"  ⚠ {_tbl}: {e}", flush=True)
print("── Table comments applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Access Control — Role-Based Grants
# ── Access control: role-based grants ──────────────────────────────────────
# Bronze: write = data_engineers, read = data_engineers (raw, no analyst access)
# Silver: write = data_engineers, read = data_engineers + data_analysts
# Gold: write = data_engineers, read = all roles
# DQ: read = data_engineers + data_quality
_grants = [
    ("exmox.bronze.bronze_installs", "MODIFY", "data_engineers"),
    ("exmox.bronze.bronze_installs", "SELECT", "data_engineers"),
    ("exmox.bronze.bronze_events", "MODIFY", "data_engineers"),
    ("exmox.bronze.bronze_events", "SELECT", "data_engineers"),
    ("exmox.bronze.bronze_offers", "MODIFY", "data_engineers"),
    ("exmox.bronze.bronze_offers", "SELECT", "data_engineers"),
    ("exmox.bronze.bronze_user_profile", "MODIFY", "data_engineers"),
    ("exmox.bronze.bronze_user_profile", "SELECT", "data_engineers"),
    ("exmox.silver.silver_installs", "MODIFY", "data_engineers"),
    ("exmox.silver.silver_installs", "SELECT", "data_engineers"),
    ("exmox.silver.silver_installs", "SELECT", "data_analysts"),
    ("exmox.silver.silver_events", "MODIFY", "data_engineers"),
    ("exmox.silver.silver_events", "SELECT", "data_engineers"),
    ("exmox.silver.silver_events", "SELECT", "data_analysts"),
    ("exmox.silver.silver_offers", "MODIFY", "data_engineers"),
    ("exmox.silver.silver_offers", "SELECT", "data_engineers"),
    ("exmox.silver.silver_offers", "SELECT", "data_analysts"),
    ("exmox.silver.silver_user_profile", "MODIFY", "data_engineers"),
    ("exmox.silver.silver_user_profile", "SELECT", "data_engineers"),
    ("exmox.silver.silver_user_profile", "SELECT", "data_analysts"),
    ("exmox.silver.dq_metrics", "SELECT", "data_engineers"),
    ("exmox.silver.dq_metrics", "SELECT", "data_quality"),
    ("exmox.silver.dq_issues", "SELECT", "data_engineers"),
    ("exmox.silver.dq_issues", "SELECT", "data_quality"),
    ("exmox.gold.gold_daily_country_platform", "MODIFY", "data_engineers"),
    ("exmox.gold.gold_daily_country_platform", "SELECT", "data_engineers"),
    ("exmox.gold.gold_daily_country_platform", "SELECT", "data_analysts"),
    ("exmox.gold.gold_daily_country_platform", "SELECT", "data_quality"),
]
for _tbl, _priv, _role in _grants:
    try:
        spark.sql(f"GRANT {_priv} ON TABLE {_tbl} TO `{_role}`")
        print(f"  ✓ GRANT {_priv} ON {_tbl} TO `{_role}`", flush=True)
    except Exception as e:
        print(f"  ⚠ GRANT {_priv} ON {_tbl} TO `{_role}`: {str(e).splitlines()[0][:120]}", flush=True)
print("── Access grants applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Verify — Tags, Comments & Constraints
# ── Verify governance: show all tags, comments, and constraints ──────────────
print("=== TABLE TAGS ===", flush=True)
spark.sql("""
    SELECT table_schema, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    WHERE table_catalog = 'exmox'
    ORDER BY table_schema, table_name, tag_name
""").display()

print("\n=== COLUMN TAGS (PII) ===", flush=True)
spark.sql("""
    SELECT table_schema, table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE table_catalog = 'exmox' AND tag_name = 'pii' AND tag_value = 'true'
    ORDER BY table_schema, table_name, column_name
""").display()

print("\n=== TABLE COMMENTS ===", flush=True)
spark.sql("""
    SELECT table_schema, table_name, comment
    FROM system.information_schema.tables
    WHERE table_catalog = 'exmox' AND comment IS NOT NULL
    ORDER BY table_schema, table_name
""").display()

print("\n=== CONSTRAINTS ===", flush=True)
spark.sql("""
    SELECT table_schema, table_name, constraint_type, constraint_name
    FROM system.information_schema.table_constraints
    WHERE table_catalog = 'exmox'
    ORDER BY table_schema, table_name, constraint_type
""").display()
print("\n── Governance verification complete ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Verify — Tags & PII Classification
# ── Verify governance: show all tags and comments ─────────────────────────
print("=== TABLE TAGS ===", flush=True)
spark.sql("""
    SELECT schema_name, table_name, tag_name, tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name = 'exmox'
    ORDER BY schema_name, table_name, tag_name
""").display()

print("\n=== COLUMN TAGS (PII) ===", flush=True)
spark.sql("""
    SELECT schema_name, table_name, column_name, tag_name, tag_value
    FROM system.information_schema.column_tags
    WHERE catalog_name = 'exmox' AND tag_name = 'pii' AND tag_value = 'true'
    ORDER BY schema_name, table_name, column_name
""").display()

print("\n── Governance verification complete ──", flush=True)