"""Pure transformation functions for the Exmox silver layer.

Extracted from 01_bronze_to_silver notebook so they can be imported by both the
notebook (via %run-pull or sys.path) and pytest tests.

Every function is DataFrame-in, DataFrame-out — no I/O, no globals.
Bad rows go to a rejects DataFrame with a reason; event anomalies are flagged,
not dropped.
"""

from pyspark.sql import DataFrame, functions as F, Window as W
from pyspark.sql.types import DecimalType

# ── Constants (mirror 00_common.Config defaults) ─────────────────────────────
STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
JOB_CUTOFF_MINUTES = 15


def _str(df: DataFrame) -> DataFrame:
    """Force every non-audit column to STRING so the casts below are the only typing that happens."""
    return df.select([
        F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
        for c in df.columns
    ])


def reject_rows(df: DataFrame, source: str, key_col: str, reason: str) -> DataFrame:
    """Build a rejects DataFrame with source, key, reason, raw JSON, and _ingested_at."""
    return df.select(
        F.lit(source).alias("source"),
        F.col(key_col).cast("string").alias("reject_key"),
        F.lit(reason).alias("reject_reason"),
        F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"),
        F.col("_ingested_at"),
    )


def clean_offers(raw: DataFrame):
    """Dedup by offer_id (latest _ingested_at wins). Reject null id or bad payout."""
    o = (_str(raw)
         .withColumn("offer_id", F.trim("offer_id"))
         .withColumn("offer_category", F.lower(F.trim("offer_category")))
         .withColumn("payout_type", F.lower(F.trim("payout_type")))
         .withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2))))
    bad_rows = o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0))
    good = (o.subtract(bad_rows)
            .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn"))
    return good, reject_rows(bad_rows, "offers", "offer_id", "null_id_or_bad_payout")


def clean_installs(raw: DataFrame):
    """Dedup by user_id (first install_ts wins). Reject null id, bad ts, bad platform/country."""
    i = (_str(raw)
         .withColumn("user_id", F.trim("user_id"))
         .withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
         .withColumn("country", F.upper(F.trim("country")))
         .withColumn("platform", F.lower(F.trim("platform")))
         .withColumn("media_source", F.lower(F.trim("media_source")))
         .withColumn("device_model", F.trim("device_model"))
         .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit(""))))
    bad_rows = i.filter(
        F.col("user_id").isNull() | F.col("install_ts").isNull()
        | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$")
    )
    good = (i.subtract(bad_rows)
            .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn")
            .withColumn("install_date", F.to_date("install_ts")))
    return good, reject_rows(bad_rows, "installs", "user_id", "bad_ts_platform_or_country")


def clean_user_profile(raw: DataFrame) -> DataFrame:
    """Dedup by user_id (latest _ingested_at wins). Reference only; gold never reads it."""
    return (_str(raw)
            .withColumn("user_id", F.trim("user_id"))
            .withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
            .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts")))
            .withColumn("last_seen_date", F.to_date("last_seen_ts"))
            .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2)))
            .withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
            .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn"))


def clean_events(raw: DataFrame, offers: DataFrame, installs: DataFrame,
                 steps: tuple = STEPS, job_cutoff_minutes: int = JOB_CUTOFF_MINUTES):
    """Typed, deduped (first arrival wins on event_id), flagged.

    Returns (good_events, rejects). Event anomalies are flagged, not dropped:
      - is_late: ingested after 00:15 the next day
      - is_pre_install: event_ts < install_ts
      - is_orphan_offer: offer_id not in the offers catalog
    """
    e = (_str(raw)
         .withColumn("event_id", F.trim("event_id"))
         .withColumn("user_id", F.trim("user_id"))
         .withColumn("event_name", F.lower(F.trim("event_name")))
         .withColumn("offer_id", F.nullif(F.trim("offer_id"), F.lit("")))
         .withColumn("event_ts", F.to_timestamp(F.trim("event_ts")))
         .withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts"))))
    bad_rows = e.filter(
        F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull()
        | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*steps)
    )
    tagged = (e.subtract(bad_rows)
              .withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))))
    dups = tagged.filter("_rn > 1").drop("_rn")
    cutoff = F.to_timestamp(F.date_add(F.to_date("event_ts"), 1)) + F.expr(f"INTERVAL {job_cutoff_minutes} MINUTES")
    good = (tagged.filter("_rn = 1").drop("_rn")
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0)
            .withColumn("is_late", F.col("ingest_ts") > cutoff)
            .join(F.broadcast(offers.select("offer_id", F.lit(True).alias("_known"))), "offer_id", "left")
            .withColumn("is_orphan_offer", F.col("offer_id").isNotNull() & F.col("_known").isNull()).drop("_known")
            .join(installs.select("user_id", "install_ts"), "user_id", "left")
            .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False))).drop("install_ts")
            .withColumn("dq_flags", F.array_compact(F.array(
                F.when(F.col("is_late"), F.lit("late")),
                F.when(F.col("is_pre_install"), F.lit("pre_install")),
                F.when(F.col("is_orphan_offer"), F.lit("orphan_offer"))))))
    rejects = reject_rows(bad_rows, "events", "event_id", "null_key_bad_ts_or_unknown_event") \
        .unionByName(reject_rows(dups, "events", "event_id", "duplicate_event_id"))
    return good, rejects
