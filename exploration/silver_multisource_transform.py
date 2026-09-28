# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Silver transform
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MIN = 15

spark.sql("CREATE TABLE IF NOT EXISTS exmox.silver.silver_watermark (table_name STRING, max_ingested_at TIMESTAMP, updated_at TIMESTAMP)")

def get_watermark(bronze_table):
    """Get latest processed timestamp for a bronze table from the watermark table."""
    try:
        rows = spark.sql(f"SELECT max_ingested_at FROM exmox.silver.silver_watermark WHERE table_name = '{bronze_table}'").collect()
        return rows[0]["max_ingested_at"] if rows else None
    except:
        return None

def update_watermark(bronze_table):
    max_ts = spark.read.table(bronze_table).agg(F.max("_ingested_at").cast("timestamp").alias("max_ts")).collect()[0]["max_ts"]
    spark.sql(f"MERGE INTO exmox.silver.silver_watermark AS t USING (SELECT '{bronze_table}' AS table_name, '{max_ts}' AS max_ingested_at, current_timestamp() AS updated_at) AS s ON t.table_name = s.table_name WHEN MATCHED THEN UPDATE SET max_ingested_at = s.max_ingested_at, updated_at = s.updated_at WHEN NOT MATCHED THEN INSERT *")

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

print("=== SILVER LAYER (Multi-Source Incremental + Watermark + MERGE) ===", flush=True)

# Get watermarks for incremental filtering
wm_offers = get_watermark("exmox.bronze.bronze_offers")
wm_installs = get_watermark("exmox.bronze.bronze_installs")
wm_user_profile = get_watermark("exmox.bronze.bronze_user_profile")
wm_events = get_watermark("exmox.bronze.bronze_events")
print(f"  watermarks: offers={wm_offers}, installs={wm_installs}, user_profile={wm_user_profile}, events={wm_events}", flush=True)

# 1. SILVER OFFERS — dedup by offer_id (latest wins), MERGE
raw = spark.read.table("exmox.bronze.bronze_offers")
if wm_offers:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm_offers))
o = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("offer_id", F.trim("offer_id")).withColumn("offer_category", F.lower(F.trim("offer_category")))
    .withColumn("payout_type", F.lower(F.trim("payout_type"))).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2))))
bad = o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0))
silver_offers = (o.subtract(bad)
    .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn")
    .withColumn("_loaded_at", F.current_timestamp())
    .coalesce(1))
if not silver_offers.isEmpty():
    merge_silver(silver_offers, "exmox.silver.silver_offers", "offer_id")
    print("✓ silver_offers (MERGE)", flush=True)
else:
    print("✓ silver_offers (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_offers")

# 2. SILVER INSTALLS — dedup by user_id (first install wins), MERGE
raw = spark.read.table("exmox.bronze.bronze_installs")
if wm_installs:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm_installs))
i = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
    .withColumn("country", F.upper(F.trim("country"))).withColumn("platform", F.lower(F.trim("platform")))
    .withColumn("media_source", F.lower(F.trim("media_source"))).withColumn("device_model", F.trim("device_model"))
    .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit(""))))
bad = i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$"))
silver_installs = (i.subtract(bad)
    .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn")
    .withColumn("install_date", F.to_date("install_ts")).withColumn("_loaded_at", F.current_timestamp())
    .repartition(4))
if not silver_installs.isEmpty():
    # First install wins — only update if new record has earlier install_ts
    merge_silver(silver_installs, "exmox.silver.silver_installs", "user_id", update_condition="s.install_ts < t.install_ts")
    print("✓ silver_installs (MERGE)", flush=True)
else:
    print("✓ silver_installs (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_installs")

# 3. SILVER USER PROFILE — dedup by user_id (latest wins), MERGE
raw = spark.read.table("exmox.bronze.bronze_user_profile")
if wm_user_profile:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm_user_profile))
silver_user_profile = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
    .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts"))).withColumn("last_seen_date", F.to_date("last_seen_ts"))
    .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2))).withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
    .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
    .filter("_rn = 1").drop("_rn").withColumn("_loaded_at", F.current_timestamp())
    .repartition(4))
if not silver_user_profile.isEmpty():
    merge_silver(silver_user_profile, "exmox.silver.silver_user_profile", "user_id")
    print("✓ silver_user_profile (MERGE)", flush=True)
else:
    print("✓ silver_user_profile (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_user_profile")

# 4. SILVER EVENTS — dedup by event_id (first arrival wins), MERGE (insert-only)
raw = spark.read.table("exmox.bronze.bronze_events")
if wm_events:
    raw = raw.filter(F.col("_ingested_at").cast("timestamp") > F.lit(wm_events))
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
    # First arrival wins — insert only, never update existing events
    merge_silver(silver_events, "exmox.silver.silver_events", "event_id", insert_only=True)
    print("✓ silver_events (MERGE)", flush=True)
else:
    print("✓ silver_events (no new data)", flush=True)
update_watermark("exmox.bronze.bronze_events")

# 5. SILVER REJECTS — recompute from ALL bronze data (overwrite)
def _str(df): return df.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in df.columns])
def _reject_rows(df, source, key_col, reason): return df.select(F.lit(source).alias("source"), F.col(key_col).cast("string").alias("reject_key"), F.lit(reason).alias("reject_reason"), F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"), F.col("_ingested_at"))
raw_offers = spark.read.table("exmox.bronze.bronze_offers")
o = _str(raw_offers).withColumn("offer_id", F.trim("offer_id")).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2)))
rj_offers = _reject_rows(o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0)), "offers", "offer_id", "null_id_or_bad_payout")
raw_installs = spark.read.table("exmox.bronze.bronze_installs")
i = _str(raw_installs).withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts"))).withColumn("country", F.upper(F.trim("country"))).withColumn("platform", F.lower(F.trim("platform")))
rj_installs = _reject_rows(i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$")), "installs", "user_id", "bad_ts_platform_or_country")
raw_events = spark.read.table("exmox.bronze.bronze_events")
e = _str(raw_events).withColumn("event_id", F.trim("event_id")).withColumn("user_id", F.trim("user_id")).withColumn("event_name", F.lower(F.trim("event_name"))).withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))).withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts")))
rj_events = _reject_rows(e.filter(F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull() | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*STEPS)), "events", "event_id", "null_key_bad_ts_or_unknown_event")
e_good = e.filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("ingest_ts").isNotNull() & F.col("event_name").isin(*STEPS))
e_dups = e_good.withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))).filter("_rn > 1").drop("_rn")
rj_dups = _reject_rows(e_dups, "events", "event_id", "duplicate_event_id")
(rj_offers.unionByName(rj_installs).unionByName(rj_events).unionByName(rj_dups)
 .coalesce(1)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("exmox.silver.silver_rejects"))
print("✓ silver_rejects", flush=True)
print("=== SILVER COMPLETE ===", flush=True)