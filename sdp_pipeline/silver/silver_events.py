"""Silver events: deduplicated, type-cast, flagged event data.

Reads from bronze_events, deduplicates by event_id (first ingest_ts wins),
casts types, and flags anomalies:
  is_late: ingested after 00:15 the next day
  is_pre_install: event_ts < install_ts
  is_orphan_offer: offer_id not in offers catalog
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window as W

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MIN = 15


@dp.materialized_view(
    name="exmox.silver.silver_events",
    comment="Cleaned events. First ingest_ts wins per event_id. Flags: is_late, is_pre_install, is_orphan_offer.",
    cluster_by=["event_id"],
)
@dp.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dp.expect_or_drop("user_id_not_null", "user_id IS NOT NULL")
@dp.expect_or_drop("event_ts_not_null", "event_ts IS NOT NULL")
@dp.expect("event_name_valid", "event_name IN ('app_open','offer_view','offer_start','goal_reached','reward_paid')")
def silver_events():
    raw = spark.read.table("exmox.bronze.bronze_events")
    e = (
        raw.select([
            F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
            for c in raw.columns
        ])
        .withColumn("event_id", F.trim("event_id"))
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("event_name", F.lower(F.trim("event_name")))
        .withColumn("offer_id", F.nullif(F.trim("offer_id"), F.lit("")))
        .withColumn("event_ts", F.to_timestamp(F.trim("event_ts")))
        .withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts")))
    )
    good = (
        e.filter(
            F.col("event_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("event_ts").isNotNull()
            & F.col("ingest_ts").isNotNull()
            & F.col("event_name").isin(*STEPS)
        )
        .withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at")))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0)
        .withColumn(
            "is_late",
            F.col("ingest_ts") > (F.to_timestamp(F.date_add(F.to_date("event_ts"), 1)) + F.expr(f"INTERVAL {JOB_CUTOFF_MIN} MINUTES")),
        )
    )
    # Flag orphan offers (offer_id not in catalog)
    offers = spark.read.table("exmox.silver.silver_offers")
    good = (
        good.join(F.broadcast(offers.select("offer_id", F.lit(True).alias("_known"))), "offer_id", "left")
        .withColumn("is_orphan_offer", F.col("offer_id").isNotNull() & F.col("_known").isNull())
        .drop("_known")
    )
    # Flag pre-install events (event_ts < install_ts)
    installs = spark.read.table("exmox.silver.silver_installs")
    good = (
        good.join(F.broadcast(installs.select("user_id", "install_ts")), "user_id", "left")
        .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False)))
        .drop("install_ts")
    )
    return good.withColumn("_loaded_at", F.current_timestamp())
