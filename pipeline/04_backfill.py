# Databricks notebook source
# DBTITLE 1,Silver → Gold Backfill — Title
# MAGIC %md
# MAGIC # Silver → Gold Backfill (Incremental / Full Recompute)
# MAGIC
# MAGIC This notebook backfills **silver and gold** tables from bronze data.
# MAGIC
# MAGIC **Two modes** (controlled by the `FULL_BACKFILL` widget parameter):
# MAGIC - `false` (default): **Incremental** — watermark-based filtering + MERGE (only processes new bronze rows)
# MAGIC - `true`: **Full Backfill** — DELETE+INSERT all silver tables, then recompute gold from scratch
# MAGIC
# MAGIC Both modes apply the same transformation logic as 02_silver_transform, log rejects and DQ metrics, recompute gold, and update watermarks.

# COMMAND ----------

# DBTITLE 1,Imports, Config & Functions
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MIN = 15
LOOKBACK_DAYS = 6

# ── Backfill mode parameter ────────────────────────────────────────────────
dbutils.widgets.text("FULL_BACKFILL", "false", "Full Backfill (true/false)")
FULL_BACKFILL = dbutils.widgets.get("FULL_BACKFILL").lower() == "true"
print(f"  Mode: {'FULL BACKFILL (DELETE+INSERT)' if FULL_BACKFILL else 'INCREMENTAL (watermark + MERGE)'}", flush=True)

# ── Spark performance tuning (serverless-safe) ─────────────────────────────
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

# ── Watermark & merge helpers ───────────────────────────────────────────────
spark.sql("CREATE TABLE IF NOT EXISTS exmox.silver.silver_watermark (table_name STRING, max_ingested_at TIMESTAMP, updated_at TIMESTAMP)")

def get_watermark(bronze_table):
    try:
        rows = spark.sql(f"SELECT max_ingested_at FROM exmox.silver.silver_watermark WHERE table_name = '{bronze_table}'").collect()
        return rows[0]["max_ingested_at"] if rows else None
    except:
        return None

def update_watermark(bronze_table):
    max_ts = spark.read.table(bronze_table).agg(F.max("_ingested_at").cast("timestamp").alias("max_ts")).collect()[0]["max_ts"]
    spark.sql(f"MERGE INTO exmox.silver.silver_watermark AS t USING (SELECT '{bronze_table}' AS table_name, '{max_ts}' AS max_ingested_at, current_timestamp() AS updated_at) AS s ON t.table_name = s.table_name WHEN MATCHED THEN UPDATE SET max_ingested_at = s.max_ingested_at, updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT *")

def validate_before_merge(df, table_name, key_col):
    stats = df.agg(F.count("*").alias("total"), F.sum(F.col(key_col).isNull().cast("long")).alias("null_keys")).collect()[0]
    if int(stats["null_keys"]) > 0:
        raise ValueError(f"DQ FAIL [{table_name}]: {int(stats['null_keys']):,}/{int(stats['total']):,} null '{key_col}' — aborting.")
    print(f"    ✓ DQ [{table_name}]: {int(stats['total']):,} rows, {key_col} not null", flush=True)

def merge_silver(df, silver_table, key_col, update_condition=None, insert_only=False):
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

def delete_insert(df, silver_table, key_col):
    validate_before_merge(df, silver_table, key_col)
    _cnt = df.count()
    spark.sql(f"DELETE FROM {silver_table}")
    df.write.format("delta").mode("append").saveAsTable(silver_table)
    print(f"✓ {silver_table} (DELETE+INSERT: {_cnt:,} rows)", flush=True)

print("── Backfill functions ready ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Backfill — Silver Offers
# ── Silver Offers — incremental (MERGE) or full backfill (DELETE+INSERT) ────
wm = get_watermark("exmox.bronze.bronze_offers") if not FULL_BACKFILL else None
print(f"  watermark [offers]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.bronze_offers")
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
if not silver_offers.isEmpty():
    if FULL_BACKFILL:
        delete_insert(silver_offers, "exmox.silver.silver_offers", "offer_id")
    else:
        validate_before_merge(silver_offers, "exmox.silver.silver_offers", "offer_id")
        merge_silver(silver_offers, "exmox.silver.silver_offers", "offer_id")
        print("✓ silver_offers (MERGE)", flush=True)
else:
    print("✓ silver_offers (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_offers")

# COMMAND ----------

# DBTITLE 1,Backfill — Silver Installs
# ── Silver Installs — incremental (MERGE) or full backfill (DELETE+INSERT) ───
wm = get_watermark("exmox.bronze.bronze_installs") if not FULL_BACKFILL else None
print(f"  watermark [installs]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.bronze_installs")
if wm:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm))
i = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
    .withColumn("country", F.upper(F.trim("country"))).withColumn("device_model", F.trim("device_model"))
    .withColumn("platform", F.when(F.lower(F.col("device_model")).contains("iphone"), F.lit("ios")).otherwise(F.lit("android")))
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
if not silver_installs.isEmpty():
    if FULL_BACKFILL:
        delete_insert(silver_installs, "exmox.silver.silver_installs", "user_id")
    else:
        validate_before_merge(silver_installs, "exmox.silver.silver_installs", "user_id")
        merge_silver(silver_installs, "exmox.silver.silver_installs", "user_id", update_condition="s.install_ts < t.install_ts")
        print("✓ silver_installs (MERGE)", flush=True)
else:
    print("✓ silver_installs (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_installs")

# COMMAND ----------

# DBTITLE 1,Backfill — Silver User Profile
# ── Silver User Profile — incremental (MERGE) or full backfill (DELETE+INSERT) ─
wm = get_watermark("exmox.bronze.bronze_user_profile") if not FULL_BACKFILL else None
print(f"  watermark [user_profile]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.bronze_user_profile")
if wm:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm))
silver_user_profile = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
    .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts"))).withColumn("last_seen_date", F.to_date("last_seen_ts"))
    .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2))).withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
    .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn").withColumn("_loaded_at", F.current_timestamp())
    .repartition(4))
if not silver_user_profile.isEmpty():
    if FULL_BACKFILL:
        delete_insert(silver_user_profile, "exmox.silver.silver_user_profile", "user_id")
    else:
        validate_before_merge(silver_user_profile, "exmox.silver.silver_user_profile", "user_id")
        merge_silver(silver_user_profile, "exmox.silver.silver_user_profile", "user_id")
        print("✓ silver_user_profile (MERGE)", flush=True)
else:
    print("✓ silver_user_profile (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_user_profile")

# COMMAND ----------

# DBTITLE 1,Backfill — Silver Events
# ── Silver Events — incremental (MERGE) or full backfill (DELETE+INSERT) ───
# Depends on silver_offers + silver_installs already being up to date.
wm = get_watermark("exmox.bronze.bronze_events") if not FULL_BACKFILL else None
print(f"  watermark [events]: {wm}", flush=True)

raw = spark.read.table("exmox.bronze.bronze_events")
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
offers = spark.read.table("exmox.silver.silver_offers")
good = (good.join(F.broadcast(offers.select("offer_id", F.lit(True).alias("_known"))), "offer_id", "left")
    .withColumn("is_orphan_offer", F.col("offer_id").isNotNull() & F.col("_known").isNull()).drop("_known"))
installs = spark.read.table("exmox.silver.silver_installs")
good = (good.join(F.broadcast(installs.select("user_id", "install_ts")), "user_id", "left")
    .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False))).drop("install_ts"))
good = good.withColumn("dq_flags", F.array_compact(F.array(
    F.when(F.col("is_late"), F.lit("late")),
    F.when(F.col("is_pre_install"), F.lit("pre_install")),
    F.when(F.col("is_orphan_offer"), F.lit("orphan_offer")))))
silver_events = good.withColumn("_loaded_at", F.current_timestamp()).repartition(8)
if not silver_events.isEmpty():
    if FULL_BACKFILL:
        delete_insert(silver_events, "exmox.silver.silver_events", "event_id")
    else:
        validate_before_merge(silver_events, "exmox.silver.silver_events", "event_id")
        merge_silver(silver_events, "exmox.silver.silver_events", "event_id", insert_only=True)
        print("✓ silver_events (MERGE)", flush=True)
else:
    print("✓ silver_events (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_events")

# COMMAND ----------

# DBTITLE 1,Backfill — Silver Rejects
# ── Silver Rejects — full recompute from ALL bronze data (always full overwrite) ─
def _str(df): return df.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in df.columns])
def _reject_rows(df, source, key_col, reason): return df.select(F.lit(source).alias("source"), F.col(key_col).cast("string").alias("reject_key"), F.lit(reason).alias("reject_reason"), F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"), F.col("_ingested_at"))

raw_offers = spark.read.table("exmox.bronze.bronze_offers")
o = _str(raw_offers).withColumn("offer_id", F.trim("offer_id")).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2)))
rj_offers = _reject_rows(o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0)), "offers", "offer_id", "null_id_or_bad_payout")

raw_installs = spark.read.table("exmox.bronze.bronze_installs")
i = _str(raw_installs).withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts"))).withColumn("country", F.upper(F.trim("country"))).withColumn("device_model", F.trim("device_model")).withColumn("platform", F.when(F.lower(F.trim("device_model")).contains("iphone"), F.lit("ios")).otherwise(F.lit("android")))
rj_installs = _reject_rows(i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("country").rlike("^[A-Z]{2}$")), "installs", "user_id", "bad_ts_or_country")

raw_events = spark.read.table("exmox.bronze.bronze_events")
e = _str(raw_events).withColumn("event_id", F.trim("event_id")).withColumn("user_id", F.trim("user_id")).withColumn("event_name", F.lower(F.trim("event_name"))).withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))).withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts")))
rj_events = _reject_rows(e.filter(F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull() | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*STEPS)), "events", "event_id", "null_key_bad_ts_or_unknown_event")
e_good = e.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("ingest_ts").isNotNull() & F.col("event_name").isin(*STEPS))
e_dups = e_good.withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))).filter("_rn > 1").drop("_rn")
rj_dups = _reject_rows(e_dups, "events", "event_id", "duplicate_event_id")

_rejects_df = rj_offers.unionByName(rj_installs).unionByName(rj_events).unionByName(rj_dups).coalesce(1)
if spark.catalog.tableExists("exmox.silver.silver_rejects"):
    spark.sql("DELETE FROM exmox.silver.silver_rejects")
_rejects_df.write.format("delta").mode("append").saveAsTable("exmox.silver.silver_rejects")
print(f"✓ silver_rejects ({_rejects_df.count():,} rows)", flush=True)

# COMMAND ----------

# DBTITLE 1,Backfill — DQ Metrics & Issues
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
    ("silver_offers",  "offer_id unique",          "error", lambda df: df.groupBy("offer_id").count().filter("count > 1").count()),
    ("silver_offers",  "offer_id not null",       "error", lambda df: df.filter("offer_id IS NULL").count()),
    ("silver_offers",  "payout_eur > 0",           "error", lambda df: df.filter("payout_eur IS NULL OR payout_eur <= 0").count()),
    ("silver_installs","user_id unique",           "error", lambda df: df.groupBy("user_id").count().filter("count > 1").count()),
    ("silver_installs","user_id not null",         "error", lambda df: df.filter("user_id IS NULL").count()),
    ("silver_installs","install_ts not null",     "error", lambda df: df.filter("install_ts IS NULL").count()),
    ("silver_installs","platform in (android, ios)","error", lambda df: df.filter("platform NOT IN ('android', 'ios')").count()),
    ("silver_installs","country ISO-2",            "error", lambda df: df.filter("NOT country RLIKE '^[A-Z]{2}$'").count()),
    ("silver_events",  "event_id unique",          "error", lambda df: df.groupBy("event_id").count().filter("count > 1").count()),
    ("silver_events",  "no null keys/ts",          "error", lambda df: df.filter("event_id IS NULL OR user_id IS NULL OR event_ts IS NULL").count()),
    ("silver_events",  "event_name known",         "error", lambda df: df.filter(f"event_name NOT IN ({', '.join([repr(s) for s in STEPS])})").count()),
    ("silver_user_profile","user_id unique",      "error", lambda df: df.groupBy("user_id").count().filter("count > 1").count()),
    ("silver_user_profile","user_id not null",    "error", lambda df: df.filter("user_id IS NULL").count()),
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

_dq_metrics = spark.createDataFrame(_results, "layer STRING, rule STRING, severity STRING, passed BOOLEAN, violations DOUBLE, error STRING")
_dq_metrics = (_dq_metrics
    .withColumn("run_id", F.lit(_run_id))
    .withColumn("run_ts", F.current_timestamp())
    .select("run_id", "run_ts", "layer", "rule", "severity", "passed", "violations", "error"))
_dq_metrics.write.mode("append").saveAsTable("exmox.silver.dq_metrics")

try:
    _issues = (spark.read.table("exmox.silver.silver_rejects")
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

_failed = [r[1] for r in _results if r[2] == "error" and not r[3]]
if _failed:
    raise AssertionError(f"DQ: {len(_failed)} rule(s) failed: {_failed}")
print("── DQ metrics & issues logged ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Backfill — Gold Aggregation
# ── Gold Aggregation — Daily Country/Platform (always full recompute from silver) ─
print("=== GOLD LAYER ===", flush=True)
events = spark.read.table("exmox.silver.silver_events")
installs = spark.read.table("exmox.silver.silver_installs")
offers = spark.read.table("exmox.silver.silver_offers")

enriched = (events.join(F.broadcast(installs.select("user_id", "country", "platform")), "user_id", "inner")
    .join(F.broadcast(offers.select("offer_id", "payout_eur")), "offer_id", "left")
    .withColumn("is_reward", F.col("event_name") == "reward_paid"))

def _users_at(step): return F.countDistinct(F.when(F.col("event_name") == step, F.col("user_id"))).alias(f"{step}_users")

ev_m = enriched.groupBy(F.col("event_date").alias("date"), "country", "platform").agg(
    F.count("*").alias("events_total"),
    *[_users_at(s) for s in STEPS],
    F.sum(F.col("is_reward").cast("int")).alias("reward_payouts"),
    F.sum(F.when(F.col("is_reward"), F.col("payout_eur"))).cast(DecimalType(18, 2)).alias("reward_cost_eur"),
    F.sum((F.col("is_reward") & F.col("payout_eur").isNull()).cast("int")).alias("rewards_unpriced"),
    F.sum(F.col("is_late").cast("int")).alias("late_events"),
    F.sum(F.col("is_pre_install").cast("int")).alias("events_pre_install"),
)

in_m = installs.groupBy(F.col("install_date").alias("date"), "country", "platform").agg(F.count("*").alias("installs"))

gold_counts = ["installs", "events_total", *[f"{s}_users" for s in STEPS], "reward_payouts", "rewards_unpriced", "late_events", "events_pre_install"]

gold = (in_m.join(ev_m, ["date", "country", "platform"], "full_outer")
    .fillna(0, subset=gold_counts).fillna({"reward_cost_eur": 0})
    .withColumn("reward_cost_eur", F.col("reward_cost_eur").cast(DecimalType(18, 2)))
    .select("date", "country", "platform", "installs", "events_total", *[f"{s}_users" for s in STEPS], "reward_payouts", "reward_cost_eur", "rewards_unpriced", "late_events", "events_pre_install")
    .withColumn("is_settled", F.col("date") <= F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .withColumn("_loaded_at", F.current_timestamp()))

# DQ check before write
dq_stats = gold.agg(
    F.count("*").alias("total"),
    F.sum(F.col("date").isNull().cast("long")).alias("null_dates"),
    F.sum(F.col("country").isNull().cast("long")).alias("null_countries"),
    F.sum(F.col("platform").isNull().cast("long")).alias("null_platforms"),
    F.sum((F.col("installs") < 0).cast("long")).alias("neg_installs"),
    F.sum((F.col("events_total") < 0).cast("long")).alias("neg_events"),
).collect()[0]
issues = [
    f"{int(dq_stats[k]):,} {k.replace('null_','null ').replace('neg_','negative ')}"
    for k in ("null_dates", "null_countries", "null_platforms", "neg_installs", "neg_events")
    if int(dq_stats[k]) > 0
]
if issues:
    raise ValueError("DQ FAIL [gold_daily_country_platform]: " + "; ".join(issues))
print(f"  ✓ DQ [gold]: {int(dq_stats['total']):,} rows, composite key non-null, counts ≥ 0", flush=True)

gold_table = "exmox.gold.gold_daily_country_platform"
gold = gold.coalesce(1)
if not spark.catalog.tableExists(gold_table):
    gold.write.format("delta").saveAsTable(gold_table)
else:
    target = DeltaTable.forName(spark, gold_table)
    (target.alias("t")
     .merge(gold.alias("s"), "t.date = s.date AND t.country = s.country AND t.platform = s.platform")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
print("✓ gold_daily_country_platform (MERGE)", flush=True)
print("=== GOLD COMPLETE ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Backfill — OPTIMIZE & Summary
# ── OPTIMIZE silver + gold tables after backfill ──────────────────────────
print("  → OPTIMIZE silver + gold ...", flush=True)
for _t in [
    "exmox.silver.silver_events",
    "exmox.silver.silver_installs",
    "exmox.silver.silver_offers",
    "exmox.silver.silver_user_profile",
    "exmox.gold.gold_daily_country_platform",
]:
    _r = spark.sql(f"OPTIMIZE {_t}").collect()[0]["metrics"]
    print(f"    ✓ {_t}: +{_r['numFilesAdded']} / -{_r['numFilesRemoved']} files", flush=True)
print("── OPTIMIZE complete ──", flush=True)

# ── Summary ────────────────────────────────────────────────────────────────
print("\n=== BACKFILL COMPLETE ===", flush=True)
for _t in ["silver_offers", "silver_installs", "silver_user_profile", "silver_events", "silver_rejects"]:
    _c = spark.read.table(f"exmox.silver.{_t}").count()
    print(f"  silver.{_t:25s}: {_c:,} rows", flush=True)
_c = spark.read.table("exmox.gold.gold_daily_country_platform").count()
print(f"  gold.gold_daily_country_platform: {_c:,} rows", flush=True)