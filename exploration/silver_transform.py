# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MIN = 15

print("=== SILVER LAYER ===", flush=True)

# 1. SILVER OFFERS — dedup by offer_id (latest wins), coalesce(1) for tiny table
raw = spark.read.table("exmox.bronze.bronze_offers")
o = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("offer_id", F.trim("offer_id")).withColumn("offer_category", F.lower(F.trim("offer_category")))
    .withColumn("payout_type", F.lower(F.trim("payout_type"))).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2))))
bad = o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0))
(o.subtract(bad)
 .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
 .filter("_rn = 1").drop("_rn")
 .withColumn("_loaded_at", F.current_timestamp())
 .coalesce(1)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("exmox.silver.silver_offers"))
print("✓ silver_offers", flush=True)

# 2. SILVER INSTALLS — dedup by user_id (first install wins)
raw = spark.read.table("exmox.bronze.bronze_installs")
i = (raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
    .withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
    .withColumn("country", F.upper(F.trim("country"))).withColumn("platform", F.lower(F.trim("platform")))
    .withColumn("media_source", F.lower(F.trim("media_source"))).withColumn("device_model", F.trim("device_model"))
    .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit(""))))
bad = i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$"))
(i.subtract(bad)
 .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
 .filter("_rn = 1").drop("_rn")
 .withColumn("install_date", F.to_date("install_ts")).withColumn("_loaded_at", F.current_timestamp())
 .repartition(4)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("exmox.silver.silver_installs"))
print("✓ silver_installs", flush=True)

# 3. SILVER USER PROFILE — dedup by user_id (latest wins)
raw = spark.read.table("exmox.bronze.bronze_user_profile")
(raw.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in raw.columns])
 .withColumn("user_id", F.trim("user_id")).withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
 .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts"))).withColumn("last_seen_date", F.to_date("last_seen_ts"))
 .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2))).withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
 .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
 .filter("_rn = 1").drop("_rn").withColumn("_loaded_at", F.current_timestamp())
 .repartition(4)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("exmox.silver.silver_user_profile"))
print("✓ silver_user_profile", flush=True)

# 4. SILVER EVENTS — dedup by event_id (first arrival wins)
# Broadcast joins for small dimension tables (offers + installs)
raw = spark.read.table("exmox.bronze.bronze_events")
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
# Broadcast installs too (small — ~40K rows)
installs = spark.read.table("exmox.silver.silver_installs")
good = (good.join(F.broadcast(installs.select("user_id", "install_ts")), "user_id", "left")
    .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False))).drop("install_ts"))
good = good.withColumn("dq_flags", F.array_compact(F.array(
    F.when(F.col("is_late"), F.lit("late")),
    F.when(F.col("is_pre_install"), F.lit("pre_install")),
    F.when(F.col("is_orphan_offer"), F.lit("orphan_offer")))))
(good.withColumn("_loaded_at", F.current_timestamp())
 .repartition(8)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("exmox.silver.silver_events"))
print("✓ silver_events", flush=True)

# 5. SILVER REJECTS
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

# COMMAND ----------

