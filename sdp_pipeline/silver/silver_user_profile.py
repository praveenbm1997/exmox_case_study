"""Silver user profile: deduplicated, type-cast user profile data.

Reads from bronze_user_profile, deduplicates by user_id (latest _ingested_at wins),
casts types. Reference only; gold layer never reads it.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType


@dp.materialized_view(
    name="exmox.silver.silver_user_profile",
    comment="Cleaned user profiles. Latest _ingested_at wins per user_id. Reference only.",
    cluster_by=["user_id"],
)
@dp.expect_or_drop("user_id_not_null", "user_id IS NOT NULL")
def silver_user_profile():
    raw = spark.read.table("exmox.bronze.bronze_user_profile")
    return (
        raw.select([
            F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
            for c in raw.columns
        ])
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
        .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts")))
        .withColumn("last_seen_date", F.to_date("last_seen_ts"))
        .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2)))
        .withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
        .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_loaded_at", F.current_timestamp())
    )