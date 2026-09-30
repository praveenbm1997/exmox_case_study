# Databricks notebook source
# DBTITLE 1,Silver — Column Constraints (idempotent)
# ── Silver column constraints: NOT NULL + CHECK rules (idempotent) ──────────
def _not_null(tbl, col):
    try: spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET NOT NULL")
    except: pass

def _check(tbl, name, expr):
    try: spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} CHECK ({expr})")
    except Exception as e:
        if "already exists" not in str(e).lower(): print(f"  ⚠ CHECK {name}: {e}")

if spark.catalog.tableExists("exmox.silver.nb_offers"):
    _not_null("exmox.silver.nb_offers", "offer_id")
    _not_null("exmox.silver.nb_offers", "payout_eur")
    _check(   "exmox.silver.nb_offers", "chk_offers_payout_eur",  "payout_eur >= 0")

if spark.catalog.tableExists("exmox.silver.nb_installs"):
    _not_null("exmox.silver.nb_installs", "user_id")
    _not_null("exmox.silver.nb_installs", "install_ts")
    _not_null("exmox.silver.nb_installs", "install_date")
    _not_null("exmox.silver.nb_installs", "platform")
    _not_null("exmox.silver.nb_installs", "country")
    _check(   "exmox.silver.nb_installs", "chk_installs_platform", "platform IN ('android','ios')")
    _check(   "exmox.silver.nb_installs", "chk_installs_country",  "LENGTH(country) = 2")

if spark.catalog.tableExists("exmox.silver.nb_user_profile"):
    _not_null("exmox.silver.nb_user_profile", "user_id")

if spark.catalog.tableExists("exmox.silver.nb_events"):
    _not_null("exmox.silver.nb_events", "event_id")
    _not_null("exmox.silver.nb_events", "user_id")
    _not_null("exmox.silver.nb_events", "event_ts")
    _not_null("exmox.silver.nb_events", "event_date")
    _check(   "exmox.silver.nb_events", "chk_events_name",
              "event_name IN ('app_open','offer_view','offer_start','goal_reached','reward_paid')")

# ── Primary keys (informational — used by query optimizer) ──────────────
def _pk(tbl, name, cols):
    try: spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} PRIMARY KEY ({cols})")
    except Exception as e:
        if "already exists" not in str(e).lower(): print(f"  ⚠ PK {name}: {e}")

def _fk(tbl, name, col, ref_tbl, ref_col):
    try: spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} FOREIGN KEY ({col}) REFERENCES {ref_tbl} ({ref_col})")
    except Exception as e:
        if "already exists" not in str(e).lower(): print(f"  ⚠ FK {name}: {e}")

if spark.catalog.tableExists("exmox.silver.nb_offers"):
    _pk("exmox.silver.nb_offers",       "pk_silver_offers",       "offer_id")
if spark.catalog.tableExists("exmox.silver.nb_installs"):
    _pk("exmox.silver.nb_installs",     "pk_silver_installs",     "user_id")
if spark.catalog.tableExists("exmox.silver.nb_user_profile"):
    _pk("exmox.silver.nb_user_profile", "pk_silver_user_profile", "user_id")
if spark.catalog.tableExists("exmox.silver.nb_events"):
    _pk("exmox.silver.nb_events",       "pk_silver_events",       "event_id")
    _fk("exmox.silver.nb_events", "fk_events_user_id",  "user_id",  "exmox.silver.nb_installs", "user_id")
    _fk("exmox.silver.nb_events", "fk_events_offer_id", "offer_id", "exmox.silver.nb_offers",   "offer_id")

print("── Silver constraints applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Performance Config — Optimized Writes, Broadcast, Low-Shuffle MERGE
# ── Spark performance tuning (serverless-safe, AQE already managed) ──────────
_conf = {
    "spark.databricks.delta.optimizeWrite.enabled":          "true",
    "spark.databricks.delta.autoCompact.enabled":            "auto",
    "spark.databricks.delta.schema.autoMerge.enabled":       "false",
    "spark.sql.autoBroadcastJoinThreshold":                  str(100 * 1024 * 1024),
    "spark.sql.shuffle.partitions":                          "32",
    "spark.databricks.delta.merge.enableLowShuffle.enabled": "true",
}
_skipped = []
for _k, _v in _conf.items():
    try: spark.conf.set(_k, _v)
    except Exception: _skipped.append(_k.split(".")[-1])
if _skipped:
    print(f"  ↷ serverless-managed: {', '.join(_skipped)}", flush=True)
print("── Perf config applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Imports, Constants & Functions
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MIN = 15

spark.sql("CREATE TABLE IF NOT EXISTS exmox.silver.nb_watermark (table_name STRING, max_ingested_at TIMESTAMP, updated_at TIMESTAMP)")

def get_watermark(bronze_table):
    """Get latest processed timestamp for a bronze table from the watermark table."""
    try:
        rows = spark.sql(f"SELECT max_ingested_at FROM exmox.silver.nb_watermark WHERE table_name = '{bronze_table}'").collect()
        return rows[0]["max_ingested_at"] if rows else None
    except:
        return None

def update_watermark(bronze_table):
    max_ts = spark.read.table(bronze_table).agg(F.max("_ingested_at").cast("timestamp").alias("max_ts")).collect()[0]["max_ts"]
    spark.sql(f"MERGE INTO exmox.silver.nb_watermark AS t USING (SELECT '{bronze_table}' AS table_name, '{max_ts}' AS max_ingested_at, current_timestamp() AS updated_at) AS s ON t.table_name = s.table_name WHEN MATCHED THEN UPDATE SET max_ingested_at = s.max_ingested_at, updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT *")

def merge_silver(df, silver_table, key_col, update_condition=None, insert_only=False):
    """MERGE into silver Delta table by primary key.
    update_condition: only update matching rows satisfying this SQL condition.
    insert_only: skip update entirely, only insert new rows."""
    if not spark.catalog.tableExists(silver_table):
        df.write.format("delta").saveAsTable(silver_table)
        return
    target = DeltaTable.forName(spark, silver_table)
    merge_builder = target.alias("t").merge(df.alias("s"), f"t.{key_col} = s.{key_col}")
    if not insert_only:
        if update_condition:
            merge_builder = merge_builder.whenMatchedUpdateAll(condition=update_condition)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()
    merge_builder.whenNotMatchedInsertAll().execute()

def validate_before_merge(df, table_name, key_col):
    """Abort MERGE if key column has nulls — last guard before every Delta write."""
    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(F.col(key_col).isNull().cast("long")).alias("null_keys")
    ).collect()[0]
    if int(stats["null_keys"]) > 0:
        raise ValueError(
            f"DQ FAIL [{table_name}]: {int(stats['null_keys']):,}/{int(stats['total']):,} "
            f"null '{key_col}' after cleaning — aborting MERGE."
        )
    print(f"    ✓ DQ [{table_name}]: {int(stats['total']):,} rows, {key_col} not null", flush=True)

print("── Silver functions ready ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Silver — Offers
# ── Silver Offers — dedup by offer_id (latest wins) ───────────────────────
# Watermark fetched fresh here so this cell is safe to re-run independently.
wm = get_watermark("exmox.bronze.nb_offers")
print(f"  watermark [offers]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.nb_offers")
if wm:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm))
o = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("offer_id", F.trim("offer_id")).withColumn("offer_category", F.lower(F.trim("offer_category")))
    .withColumn("payout_type", F.lower(F.trim("payout_type"))).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2))))
bad = o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0))
_bad_cnt = bad.count()
if _bad_cnt: print(f"    ⚠️  DQ [bronze_offers → silver]: {_bad_cnt:,} rows rejected (null id/payout or payout<0)", flush=True)
silver_offers = (o.subtract(bad)
    .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn")
    .withColumn("_loaded_at", F.current_timestamp())
    .coalesce(1))
try:
    if not silver_offers.isEmpty():
        validate_before_merge(silver_offers, "exmox.silver.nb_offers", "offer_id")
        merge_silver(silver_offers, "exmox.silver.nb_offers", "offer_id")
        print("✓ silver_offers (MERGE)", flush=True)
    else:
        print("[INFO] silver_offers: No new data found — skipping MERGE.", flush=True)
    update_watermark("exmox.bronze.nb_offers")
except Exception as _e:
    print(f"[ERROR] silver_offers failed: {_e}", flush=True)
    raise

# COMMAND ----------

# DBTITLE 1,Silver — Installs
# ── Silver Installs — full recompute via DELETE+INSERT (dedup by user_id, first install wins)
# Full recompute from ALL bronze data ensures platform is always derived from device_model.
raw = spark.read.table("exmox.bronze.nb_installs")
i = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
    .withColumn("country", F.upper(F.trim("country"))).withColumn("device_model", F.trim("device_model"))
    .withColumn("platform", F.when(F.lower(F.col("device_model")).rlike("iphone|ipad|ipod"), F.lit("ios")).otherwise(F.lit("android")))
    .withColumn("media_source", F.lower(F.trim("media_source")))
    .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit(""))))
bad = i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$"))
_bad_cnt = bad.count()
if _bad_cnt: print(f"    ⚠️  DQ [bronze_installs → silver]: {_bad_cnt:,} rows rejected (null user/ts or bad platform/country)", flush=True)
silver_installs = (i.subtract(bad)
    .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn")
    .withColumn("install_date", F.to_date("install_ts")).withColumn("_loaded_at", F.current_timestamp())
    .repartition(4))
try:
    if not silver_installs.isEmpty():
        validate_before_merge(silver_installs, "exmox.silver.nb_installs", "user_id")
        _cnt = silver_installs.count()
        if spark.catalog.tableExists("exmox.silver.nb_installs"):
            spark.sql("DELETE FROM exmox.silver.nb_installs")
        silver_installs.write.format("delta").mode("append").saveAsTable("exmox.silver.nb_installs")
        print(f"✓ silver_installs (DELETE+INSERT: {_cnt:,} rows)", flush=True)
    else:
        print("[INFO] silver_installs: No new data found — skipping write.", flush=True)
    update_watermark("exmox.bronze.nb_installs")
except Exception as _e:
    print(f"[ERROR] silver_installs failed: {_e}", flush=True)
    raise

# COMMAND ----------

# DBTITLE 1,Silver — User Profile
# ── Silver User Profile — dedup by user_id (latest wins) ─────────────────
wm = get_watermark("exmox.bronze.nb_user_profile")
print(f"  watermark [user_profile]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.nb_user_profile")
if wm:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm))
silver_user_profile = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
    .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts"))).withColumn("last_seen_date", F.to_date("last_seen_ts"))
    .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2))).withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
    .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn").withColumn("_loaded_at", F.current_timestamp())
    .repartition(4))
try:
    if not silver_user_profile.isEmpty():
        validate_before_merge(silver_user_profile, "exmox.silver.nb_user_profile", "user_id")
        merge_silver(silver_user_profile, "exmox.silver.nb_user_profile", "user_id")
        print("✓ silver_user_profile (MERGE)", flush=True)
    else:
        print("[INFO] silver_user_profile: No new data found — skipping MERGE.", flush=True)
    update_watermark("exmox.bronze.nb_user_profile")
except Exception as _e:
    print(f"[ERROR] silver_user_profile failed: {_e}", flush=True)
    raise

# COMMAND ----------

# DBTITLE 1,Silver — Events
# ── Silver Events — dedup by event_id (first arrival, insert-only) ──────────
# Depends on silver_offers + silver_installs already being up to date.
wm = get_watermark("exmox.bronze.nb_events")
print(f"  watermark [events]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.nb_events")
if wm:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm))
e = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("event_id", F.trim("event_id")).withColumn("user_id", F.trim("user_id"))
    .withColumn("event_name", F.lower(F.trim("event_name"))).withColumn("offer_id", F.nullif(F.trim("offer_id"), F.lit("")))
    .withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))).withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts"))))
good = (e.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("ingest_ts").isNotNull() & F.col("event_name").isin(*STEPS))
    .withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at")))
    .filter("_rn = 1").drop("_rn")
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0)
    .withColumn("is_late", F.col("ingest_ts") > (F.to_timestamp(F.date_add(F.to_date("event_ts"), 1)) + F.expr(f"INTERVAL {JOB_CUTOFF_MIN} MINUTES"))))
offers = spark.read.table("exmox.silver.nb_offers")
good = (good.join(F.broadcast(offers.select("offer_id", F.lit(True).alias("_known"))), "offer_id", "left")
    .withColumn("is_orphan_offer", F.col("offer_id").isNotNull() & F.col("_known").isNull()).drop("_known"))
installs = spark.read.table("exmox.silver.nb_installs")
good = (good.join(F.broadcast(installs.select("user_id", "install_ts")), "user_id", "left")
    .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False))).drop("install_ts"))
good = good.withColumn("dq_flags", F.array_compact(F.array(
    F.when(F.col("is_late"), F.lit("late")),
    F.when(F.col("is_pre_install"), F.lit("pre_install")),
    F.when(F.col("is_orphan_offer"), F.lit("orphan_offer")))))
silver_events = good.withColumn("_loaded_at", F.current_timestamp()).repartition(8)
try:
    if not silver_events.isEmpty():
        validate_before_merge(silver_events, "exmox.silver.nb_events", "event_id")
        # First arrival wins — insert only, never update existing events
        merge_silver(silver_events, "exmox.silver.nb_events", "event_id", insert_only=True)
        print("✓ silver_events (MERGE)", flush=True)
    else:
        print("[INFO] silver_events: No new data found — skipping MERGE.", flush=True)
    update_watermark("exmox.bronze.nb_events")
except Exception as _e:
    print(f"[ERROR] silver_events failed: {_e}", flush=True)
    raise

# COMMAND ----------

# DBTITLE 1,Silver — Rejects
# ── Silver Rejects — recompute from ALL bronze data (full overwrite) ─────────
# Safe to re-run independently — always a full recompute, no watermark dependency.
def _str(df): return df.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in df.columns])
def _reject_rows(df, source, key_col, reason): return df.select(F.lit(source).alias("source"), F.col(key_col).cast("string").alias("reject_key"), F.lit(reason).alias("reject_reason"), F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"), F.col("_ingested_at"))

raw_offers = spark.read.table("exmox.bronze.nb_offers")
o = _str(raw_offers).withColumn("offer_id", F.trim("offer_id")).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2)))
rj_offers = _reject_rows(o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0)), "offers", "offer_id", "null_id_or_bad_payout")

raw_installs = spark.read.table("exmox.bronze.nb_installs")
i = _str(raw_installs).withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts"))).withColumn("country", F.upper(F.trim("country"))).withColumn("device_model", F.trim("device_model")).withColumn("platform", F.when(F.lower(F.trim("device_model")).rlike("iphone|ipad|ipod"), F.lit("ios")).otherwise(F.lit("android")))
rj_installs = _reject_rows(i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("country").rlike("^[A-Z]{2}$")), "installs", "user_id", "bad_ts_or_country")

raw_events = spark.read.table("exmox.bronze.nb_events")
e = _str(raw_events).withColumn("event_id", F.trim("event_id")).withColumn("user_id", F.trim("user_id")).withColumn("event_name", F.lower(F.trim("event_name"))).withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))).withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts")))
rj_events = _reject_rows(e.filter(F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull() | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*STEPS)), "events", "event_id", "null_key_bad_ts_or_unknown_event")
e_good = e.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("ingest_ts").isNotNull() & F.col("event_name").isin(*STEPS))
e_dups = e_good.withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))).filter("_rn > 1").drop("_rn")
rj_dups = _reject_rows(e_dups, "events", "event_id", "duplicate_event_id")

try:
    (rj_offers.unionByName(rj_installs).unionByName(rj_events).unionByName(rj_dups)
     .coalesce(1)
     .write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable("exmox.silver.nb_rejects"))
    print("✓ silver_rejects", flush=True)
    print("=== SILVER COMPLETE ===", flush=True)
except Exception as _e:
    print(f"[ERROR] silver_rejects failed: {_e}", flush=True)
    raise

# COMMAND ----------

# DBTITLE 1,DQ Metrics & Issues
# ── DQ Metrics & Issues — evaluate rules and log to dedicated tables ───────
_run_id = spark.sql("SELECT date_format(current_timestamp(), 'yyyyMMdd_HHmmss')").collect()[0][0]

spark.sql("""
CREATE TABLE IF NOT EXISTS exmox.silver.dq_metrics (
  run_id STRING, run_ts TIMESTAMP, layer STRING, rule STRING,
  severity STRING, passed BOOLEAN, violations DOUBLE, error STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS exmox.silver.dq_issues (
  run_id STRING, run_ts TIMESTAMP, source_table STRING, issue STRING, issue_count BIGINT
) USING DELTA
""")

_dq_rules = [
    ("nb_offers",  "offer_id unique",          "error", lambda df: df.groupBy("offer_id").count().filter("count > 1").count()),
    ("nb_offers",  "offer_id not null",       "error", lambda df: df.filter("offer_id IS NULL").count()),
    ("nb_offers",  "payout_eur > 0",           "error", lambda df: df.filter("payout_eur IS NULL OR payout_eur <= 0").count()),
    ("nb_installs","user_id unique",           "error", lambda df: df.groupBy("user_id").count().filter("count > 1").count()),
    ("nb_installs","user_id not null",         "error", lambda df: df.filter("user_id IS NULL").count()),
    ("nb_installs","install_ts not null",     "error", lambda df: df.filter("install_ts IS NULL").count()),
    ("nb_installs","platform in (android, ios)","error", lambda df: df.filter("platform NOT IN ('android', 'ios')").count()),
    ("nb_installs","country ISO-2",            "error", lambda df: df.filter("NOT country RLIKE '^[A-Z]{2}$'").count()),
    ("nb_events",  "event_id unique",          "error", lambda df: df.groupBy("event_id").count().filter("count > 1").count()),
    ("nb_events",  "no null keys/ts",          "error", lambda df: df.filter("event_id IS NULL OR user_id IS NULL OR event_ts IS NULL").count()),
    ("nb_events",  "event_name known",         "error", lambda df: df.filter(f"event_name NOT IN ({', '.join([repr(s) for s in STEPS])})").count()),
    ("nb_user_profile","user_id unique",      "error", lambda df: df.groupBy("user_id").count().filter("count > 1").count()),
    ("nb_user_profile","user_id not null",    "error", lambda df: df.filter("user_id IS NULL").count()),
    # ── Warning-severity observational rules (do not fail the pipeline) ──
    ("nb_events",  "late arrivals (is_late)",        "warning", lambda df: df.filter(F.col("is_late") == True).count()),
    ("nb_events",  "pre-install events",             "warning", lambda df: df.filter(F.col("is_pre_install") == True).count()),
    ("nb_events",  "orphan offer_id",                "warning", lambda df: df.filter(F.col("is_orphan_offer") == True).count()),
    ("nb_installs","users with no events",           "warning", lambda df: df.join(spark.read.table("exmox.silver.nb_events").select("user_id").distinct(), "user_id", "left_anti").count()),
]

_results = []
for _tbl, _rule, _sev, _fn in _dq_rules:
    try:
        _df = spark.read.table(f"exmox.silver.{_tbl}")
        _v = _fn(_df)
        _ok = _v == 0
        _err = None
    except Exception as e:
        _v, _ok, _err = -1, False, f"{type(e).__name__}: {str(e).strip().splitlines()[0][:200]}"
    print(f"{'PASS' if _ok else _sev.upper():5}  {_tbl}: {_rule}" + ("" if _ok else f"  -> {_err or _v}"), flush=True)
    _results.append(("silver", f"{_tbl}: {_rule}", _sev, _ok, float(_v), _err))

# Log metrics
_dq_metrics = spark.createDataFrame(_results, "layer STRING, rule STRING, severity STRING, passed BOOLEAN, violations DOUBLE, error STRING")
_dq_metrics = (_dq_metrics
    .withColumn("run_id", F.lit(_run_id))
    .withColumn("run_ts", F.current_timestamp())
    .select("run_id", "run_ts", "layer", "rule", "severity", "passed", "violations", "error"))
_dq_metrics.write.mode("append").saveAsTable("exmox.silver.dq_metrics")

# Log issues from rejects table
try:
    _issues = (spark.read.table("exmox.silver.nb_rejects")
        .groupBy("source", "reject_reason").count()
        .withColumnRenamed("source", "source_table")
        .withColumnRenamed("reject_reason", "issue")
        .withColumnRenamed("count", "issue_count")
        .withColumn("run_id", F.lit(_run_id))
        .withColumn("run_ts", F.current_timestamp())
        .select("run_id", "run_ts", "source_table", "issue", "issue_count"))
    _issues.write.mode("append").saveAsTable("exmox.silver.dq_issues")
except Exception:
    pass

# Fail if error-severity rules failed
_failed = [r[1] for r in _results if r[2] == "error" and not r[3]]
if _failed:
    raise AssertionError(f"DQ: {len(_failed)} rule(s) failed: {_failed}")
print("── DQ metrics & issues logged ──", flush=True)

# COMMAND ----------

# DBTITLE 1,EDA — Bronze Data Quality Exploration
# ── EDA: Bronze Tables — Nulls, Duplicates, Distributions ──────────────────
from pyspark.sql import functions as F

for _tbl in ["nb_installs", "nb_events", "nb_offers", "nb_user_profile"]:
    _full = f"exmox.bronze.{_tbl}"
    _df = spark.read.table(_full)
    _total = _df.count()
    print(f"\n{'─' * 60}")
    print(f"TABLE: {_full} ({_total:,} rows)")
    print(f"{'─' * 60}")

    # Null counts
    _null_cols = []
    for _c in _df.columns:
        _n = _df.filter(F.col(_c).isNull()).count()
        if _n > 0:
            _null_cols.append(f"    {_c}: {_n:,} ({100*_n/_total:.1f}%)")
    if _null_cols:
        print("  NULLS:")
        print("\n".join(_null_cols))

    # Empty strings
    _empty_cols = []
    for _c, _t in _df.dtypes:
        if _t == "string" and not _c.startswith("_"):
            _n = _df.filter(F.col(_c) == "").count()
            if _n > 0:
                _empty_cols.append(f"    {_c}: {_n:,} ({100*_n/_total:.1f}%)")
    if _empty_cols:
        print("  EMPTY STRINGS:")
        print("\n".join(_empty_cols))

    if _tbl == "nb_installs":
        _dups = _df.groupBy("user_id").count().filter("count > 1").count()
        print(f"  DUPLICATE user_ids: {_dups}")
        print("  PLATFORM distribution:")
        for r in _df.groupBy("platform").count().orderBy("count", ascending=False).collect():
            print(f"    {r['platform']}: {r['count']:,}")
        print("  DEVICE_MODEL distribution:")
        for r in _df.groupBy("device_model").count().orderBy("count", ascending=False).collect():
            print(f"    {r['device_model']}: {r['count']:,}")
        _mismatch = _df.filter((F.lower(F.col("device_model")).rlike("iphone|ipad|ipod") & (F.col("platform") != "ios")) | (~F.lower(F.col("device_model")).rlike("iphone|ipad|ipod") & (F.col("platform") == "ios"))).count()
        print(f"  PLATFORM vs DEVICE_MODEL mismatches: {_mismatch}")
        _bad_country = _df.filter(~F.col("country").rlike("^[A-Z]{2}$")).count()
        print(f"  Non-ISO country codes: {_bad_country}")

    elif _tbl == "nb_events":
        _dups = _df.groupBy("event_id").count().filter("count > 1").count()
        print(f"  DUPLICATE event_ids: {_dups}")
        print("  EVENT_NAME distribution:")
        for r in _df.groupBy("event_name").count().orderBy("count", ascending=False).collect():
            print(f"    {r['event_name']}: {r['count']:,}")
        _unknown = _df.filter(~F.col("event_name").isin("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")).count()
        print(f"  Unknown event_names: {_unknown}")

    elif _tbl == "nb_offers":
        _dups = _df.groupBy("offer_id").count().filter("count > 1").count()
        print(f"  DUPLICATE offer_ids: {_dups}")
        _bad_payout = _df.filter((F.col("payout_eur").isNull()) | (F.col("payout_eur").cast("double") <= 0)).count()
        print(f"  Null or <=0 payout_eur: {_bad_payout}")

    elif _tbl == "nb_user_profile":
        _dups = _df.groupBy("user_id").count().filter("count > 1").count()
        print(f"  DUPLICATE user_ids: {_dups}")
        print("  IS_PAYER distribution:")
        for r in _df.groupBy("is_payer").count().orderBy("count", ascending=False).collect():
            print(f"    {r['is_payer']}: {r['count']:,}")

# Cross-table consistency: events with user_id not in installs
_events = spark.read.table("exmox.bronze.nb_events")
_installs = spark.read.table("exmox.bronze.nb_installs")
_orphan_users = _events.join(_installs.select("user_id"), "user_id", "left_anti").count()
print(f"\n{'─' * 60}")
print(f"CROSS-TABLE: Events with user_id NOT in installs: {_orphan_users:,}")

# Events with offer_id not in offers
_offers = spark.read.table("exmox.bronze.nb_offers")
_orphan_offers = _events.filter(F.col("offer_id").isNotNull()).join(_offers.select("offer_id"), "offer_id", "left_anti").count()
print(f"CROSS-TABLE: Events with offer_id NOT in offers: {_orphan_offers:,}")

# Date ranges
for _tbl, _ts in [("nb_installs", "install_ts"), ("nb_events", "event_ts"), ("nb_events", "ingest_ts")]:
    _df = spark.read.table(f"exmox.bronze.{_tbl}")
    _min = _df.agg(F.min(_ts)).collect()[0][0]
    _max = _df.agg(F.max(_ts)).collect()[0][0]
    print(f"DATE RANGE {_tbl}.{_ts}: {_min} → {_max}")

print("\n=== EDA COMPLETE ===")

# COMMAND ----------

# DBTITLE 1,EDA — ingest_ts Late Arrival Analysis
# ── ingest_ts vs event_ts analysis: late-arriving data patterns ────────────────
_e = spark.read.table("exmox.bronze.nb_events")

# Cast timestamps
_e = _e.withColumn("event_ts", F.to_timestamp("event_ts")).withColumn("ingest_ts", F.to_timestamp("ingest_ts"))

# Lag in hours
_e = _e.withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0)

print("=== INGEST_TS vs EVENT_TS ANALYSIS ===")
print(f"Total events: {_e.count():,}")

# Lag distribution
print("\nLAG DISTRIBUTION (hours):")
_lag_stats = _e.agg(
    F.min("lag_hours").alias("min"),
    F.expr("percentile_approx(lag_hours, 0.25)").alias("p25"),
    F.expr("percentile_approx(lag_hours, 0.50)").alias("median"),
    F.expr("percentile_approx(lag_hours, 0.75)").alias("p75"),
    F.expr("percentile_approx(lag_hours, 0.95)").alias("p95"),
    F.max("lag_hours").alias("max"),
).collect()[0]
print(f"  min: {_lag_stats['min']:.2f}h, p25: {_lag_stats['p25']:.2f}h, median: {_lag_stats['median']:.2f}h, p75: {_lag_stats['p75']:.2f}h, p95: {_lag_stats['p95']:.2f}h, max: {_lag_stats['max']:.2f}h")

# Negative lag (ingest before event — clock skew)
_neg_lag = _e.filter(F.col("lag_hours") < 0).count()
print(f"\n  Negative lag (ingest before event): {_neg_lag:,}")

# Same-day ingestion
_same_day = _e.filter(F.to_date("ingest_ts") == F.to_date("event_ts")).count()
print(f"  Same-day ingestion: {_same_day:,} ({100*_same_day/_e.count():.1f}%)")

# Next-day ingestion
_next_day = _e.filter(F.to_date("ingest_ts") == F.date_add(F.to_date("event_ts"), 1)).count()
print(f"  Next-day ingestion: {_next_day:,} ({100*_next_day/_e.count():.1f}%)")

# 2+ day delay
_two_plus = _e.filter(F.to_date("ingest_ts") > F.date_add(F.to_date("event_ts"), 1)).count()
print(f"  2+ day delay: {_two_plus:,} ({100*_two_plus/_e.count():.1f}%)")

# Midnight crossover: events with event_ts before midnight but ingest_ts after
_midnight = _e.filter(
    (F.hour("event_ts") >= 23) & (F.hour("ingest_ts") < 1) & (F.to_date("ingest_ts") == F.date_add(F.to_date("event_ts"), 1))
).count()
print(f"  Events at 23:00+ ingested after midnight: {_midnight:,}")

# Daily job simulation: if we process by event_date = yesterday, how many events would be missed?
# Simulate for the last day in the data
_max_event_date = _e.agg(F.max(F.to_date("event_ts"))).collect()[0][0]
_yesterday_events = _e.filter(F.to_date("event_ts") == F.lit(_max_event_date))
_yesterday_total = _yesterday_events.count()
_yesterday_ingested_by_0015 = _yesterday_events.filter(F.col("ingest_ts") <= F.lit(_max_event_date).cast("timestamp") + F.expr("INTERVAL 1 DAY + 15 MINUTES")).count()
_yesterday_missed = _yesterday_total - _yesterday_ingested_by_0015
print(f"\nDAILY JOB SIMULATION (for {_max_event_date}):")
print(f"  Total events with event_date = {_max_event_date}: {_yesterday_total:,}")
print(f"  Ingested by 00:15 next day: {_yesterday_ingested_by_0015:,}")
print(f"  MISSED by 00:15 job: {_yesterday_missed:,} ({100*_yesterday_missed/_yesterday_total:.1f}%)")

# Also check: events ingested on last day but with event_ts from earlier days
_ingested_last_day = _e.filter(F.to_date("ingest_ts") == F.lit(_max_event_date))
_earlier_event_date = _ingested_last_day.filter(F.to_date("event_ts") < F.lit(_max_event_date)).count()
print(f"  Events ingested on {_max_event_date} but with earlier event_date: {_earlier_event_date:,}")
print(f"  (These would be WRONGLY INCLUDED if job filters by ingest_ts = yesterday)")

print("\n=== INGEST_TS ANALYSIS COMPLETE ===")

# COMMAND ----------

# DBTITLE 1,Post-Write OPTIMIZE — Liquid Clustering + Compaction
# ── OPTIMIZE silver tables after each run ──────────────────────────────────
print("  → OPTIMIZE silver ...", flush=True)
for _t in [
    "exmox.silver.nb_events",
    "exmox.silver.nb_installs",
    "exmox.silver.nb_offers",
    "exmox.silver.nb_user_profile",
]:
    _r = spark.sql(f"OPTIMIZE {_t}").collect()[0]["metrics"]
    print(f"    ✓ {_t}: +{_r['numFilesAdded']} / -{_r['numFilesRemoved']} files", flush=True)
print("── Silver OPTIMIZE complete ──", flush=True)