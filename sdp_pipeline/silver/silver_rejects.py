"""Silver rejects: rejected rows from all bronze sources with reasons.

Recomputes rejections from all bronze tables each refresh.
Captures: null keys, bad timestamps, unknown event names, duplicates.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")


def _str(df):
    """Cast every non-audit column to STRING for consistent reject records."""
    return df.select([
        F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
        for c in df.columns
    ])


def _reject_rows(df, source, key_col, reason):
    """Build a rejects DataFrame with source, key, reason, raw JSON, _ingested_at."""
    return df.select(
        F.lit(source).alias("source"),
        F.col(key_col).cast("string").alias("reject_key"),
        F.lit(reason).alias("reject_reason"),
        F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"),
        F.col("_ingested_at"),
    )


@dp.materialized_view(
    name="exmox.silver.silver_rejects",
    comment="Rejected rows from all bronze sources with reason and raw record JSON.",
)
def silver_rejects():
    # Offers rejects: null id or bad payout
    raw_offers = spark.read.table("exmox.bronze.bronze_offers")
    o = _str(raw_offers).withColumn("offer_id", F.trim("offer_id")).withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2)))
    rj_offers = _reject_rows(
        o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0)),
        "offers", "offer_id", "null_id_or_bad_payout",
    )

    # Installs rejects: null user/ts or bad country
    raw_installs = spark.read.table("exmox.bronze.bronze_installs")
    i = _str(raw_installs).withColumn("user_id", F.trim("user_id")).withColumn("install_ts", F.to_timestamp(F.trim("install_ts"))).withColumn("country", F.upper(F.trim("country")))
    rj_installs = _reject_rows(
        i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull() | ~F.col("country").rlike("^[A-Z]{2}$")),
        "installs", "user_id", "bad_ts_or_country",
    )

    # Events rejects: null keys, bad ts, unknown event name
    raw_events = spark.read.table("exmox.bronze.bronze_events")
    e = _str(raw_events).withColumn("event_id", F.trim("event_id")).withColumn("user_id", F.trim("user_id")).withColumn("event_name", F.lower(F.trim("event_name"))).withColumn("event_ts", F.to_timestamp(F.trim("event_ts"))).withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts")))
    rj_events = _reject_rows(
        e.filter(F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull() | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*STEPS)),
        "events", "event_id", "null_key_bad_ts_or_unknown_event",
    )

    # Event duplicates: second+ arrival of same event_id
    e_good = e.filter(
        F.col("event_id").isNotNull() & F.col("user_id").isNotNull()
        & F.col("event_ts").isNotNull() & F.col("ingest_ts").isNotNull()
        & F.col("event_name").isin(*STEPS)
    )
    e_dups = e_good.withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))).filter("_rn > 1").drop("_rn")
    rj_dups = _reject_rows(e_dups, "events", "event_id", "duplicate_event_id")

    return rj_offers.unionByName(rj_installs).unionByName(rj_events).unionByName(rj_dups)