"""Silver offers: deduplicated, type-cast offer dimension data.

Reads from bronze_offers, deduplicates by offer_id (latest _ingested_at wins),
casts types, and drops rows with null offer_id or bad payout.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window as W
from pyspark.sql.types import DecimalType


@dp.materialized_view(
    name="exmox.silver.silver_offers",
    comment="Cleaned and deduplicated offers. Latest _ingested_at wins per offer_id.",
    cluster_by=["offer_id"],
)
@dp.expect_or_drop("offer_id_not_null", "offer_id IS NOT NULL")
@dp.expect_or_drop("payout_eur_valid", "payout_eur IS NOT NULL AND payout_eur >= 0")
def silver_offers():
    raw = spark.read.table("exmox.bronze.bronze_offers")
    return (
        raw.select([
            F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c)
            for c in raw.columns
        ])
        .withColumn("offer_id", F.trim("offer_id"))
        .withColumn("offer_category", F.lower(F.trim("offer_category")))
        .withColumn("payout_type", F.lower(F.trim("payout_type")))
        .withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2)))
        .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
        .filter("_rn = 1")
        .drop("_rn")
        .withColumn("_loaded_at", F.current_timestamp())
    )
