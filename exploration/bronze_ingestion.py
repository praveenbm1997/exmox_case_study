# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

def load_bronze(dataset_name, volume_path, s3_table, bronze_table, parts=4):
    """Load from volume CSV + S3 managed table → bronze Delta table."""
    print(f"  → {dataset_name}: reading volume CSV ...", flush=True)
    volume = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(volume_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp().cast("string"))
        .withColumn("_source", F.lit("volume"))
    )
    print(f"  → {dataset_name}: reading S3 table ...", flush=True)
    s3 = spark.read.table(s3_table)
    s3 = s3.select([F.col(c).cast("string").alias(c) for c in s3.columns])
    print(f"  → {dataset_name}: union + write (parts={parts}) ...", flush=True)
    (volume.unionByName(s3, allowMissingColumns=True)
     .repartition(parts)
     .write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(bronze_table))
    print(f"  ✓ {dataset_name} done", flush=True)

print("=== BRONZE LAYER ===", flush=True)
load_bronze("events", "/Volumes/exmox/bronze/landing/events/", "exmox.bronze.s3_events", "exmox.bronze.bronze_events", parts=8)
load_bronze("installs", "/Volumes/exmox/bronze/landing/installs/", "exmox.bronze.s3_installs", "exmox.bronze.bronze_installs", parts=4)
load_bronze("offers", "/Volumes/exmox/bronze/landing/offers/", "exmox.bronze.s3_offers", "exmox.bronze.bronze_offers", parts=1)
load_bronze("user_profile", "/Volumes/exmox/bronze/landing/user_profile/", "exmox.bronze.s3_user_profile", "exmox.bronze.bronze_user_profile", parts=4)
print("=== BRONZE COMPLETE ===", flush=True)

# COMMAND ----------

