"""Silver installs: deduplicated, type-cast install data with derived platform.

Reads from bronze_installs, deduplicates by user_id (first install_ts wins),
derives platform from device_model (iphone -> ios, else android),
and drops rows with null user_id, null install_ts, or bad country format.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window as W


@dp.materialized_view(
    name="exmox.silver.silver_installs",
    comment="Cleaned installs. First install_ts wins per user_id. Platform derived from device_model.",
    cluster_by=["user_id"],
)
@dp.expect_or_drop("user_id_not_null", "user_id IS NOT NULL")
@dp.expect_or_drop("install_ts_not_null", "install_ts IS NOT NULL")
@dp.expect_or_drop("country_valid", "country IS NOT NULL AND LENGTH(country) = 2")
@dp.expect_or_drop("platform_valid", "platform IN ('android', 'ios')")
def silver_installs():
    raw = spark.read.table("exmox.bronze.bronze_installs")
    return (
        raw.select([
            F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
            for c in raw.columns
        ])
        .withColumn("user_id", F.trim("user_id"))
        .withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("device_model", F.trim("device_model"))
        .withColumn("platform", F.when(F.lower(F.col("device_model")).contains("iphone"), F.lit("ios")).otherwise(F.lit("android")))
        .withColumn("media_source", F.lower(F.trim("media_source")))
        .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit("")))
        .withColumn("install_date", F.to_date("install_ts"))
        .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_loaded_at", F.current_timestamp())
    )