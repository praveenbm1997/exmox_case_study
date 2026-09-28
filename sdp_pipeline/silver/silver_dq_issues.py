"""Silver DQ issues: aggregated reject counts per source and reason.

Reads from silver_rejects and aggregates by (source, reject_reason).
Recomputed on each pipeline refresh.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="exmox.silver.silver_dq_issues",
    comment="Aggregated issue counts from silver_rejects. One row per source/reason per refresh.",
)
def silver_dq_issues():
    rejects = spark.read.table("exmox.silver.silver_rejects")
    return (
        rejects.groupBy("source", "reject_reason")
        .agg(
            F.count("*").alias("issue_count"),
            F.countDistinct("reject_key").alias("distinct_keys"),
            F.max("_ingested_at").alias("latest_ingested_at"),
        )
        .withColumn("refresh_ts", F.current_timestamp())
        .orderBy(F.desc("issue_count"))
    )