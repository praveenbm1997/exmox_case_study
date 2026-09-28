# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CHECKPOINT_BASE = "/Volumes/exmox/bronze/_checkpoints"

def sync_s3_table(dataset_name, s3_table):
    """Read CSV from S3 and create/refresh the managed Delta table in exmox.bronze."""
    s3_path = f"s3://exmox/{dataset_name}/{dataset_name}.csv"
    print(f"  -> syncing S3: {s3_path} → {s3_table} ...", flush=True)
    (spark.read.format("csv")
     .option("header", "true")
     .option("inferSchema", "false")
     .load(s3_path)
     .write.mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(s3_table))
    print(f"  ✓ {s3_table} synced", flush=True)

def upsert_to_bronze(df, bronze_table, key_col):
    if not spark.catalog.tableExists(bronze_table):
        df.write.format("delta").saveAsTable(bronze_table)
        return
    target = DeltaTable.forName(spark, bronze_table)
    # Only map columns present in both source and target.
    # whenNotMatchedInsertAll() fails when target has extra cols (e.g. _rescued_data)
    # so we use explicit whenNotMatchedInsert(values=...) instead.
    target_col_set = set(target.toDF().columns)
    insert_vals = {c: F.col(f"s.{c}") for c in df.columns if c in target_col_set}
    (target.alias("t")
     .merge(df.alias("s"), f"t.{key_col} = s.{key_col}")
     .whenNotMatchedInsert(values=insert_vals)
     .execute())

def load_bronze(dataset_name, volume_path, s3_table, bronze_table, key_col, parts=4):
    checkpoint_path = f"{CHECKPOINT_BASE}/{dataset_name}"
    print(f"  -> {dataset_name}: Auto Loader from {volume_path} ...", flush=True)
    volume_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(volume_path)
        .withColumns({
            "_source_file": F.col("_metadata.file_path"),
            "_ingested_at": F.current_timestamp().cast("string"),
            "_source": F.lit("volume"),
        })
    )
    def process_volume_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        cnt = batch_df.count()
        print(f"    volume batch {batch_id}: {cnt} new rows", flush=True)
        upsert_to_bronze(batch_df, bronze_table, key_col)
    (volume_stream.writeStream
     .foreachBatch(process_volume_batch)
     .trigger(availableNow=True)
     .option("checkpointLocation", f"{checkpoint_path}/volume")
     .start()
     .awaitTermination())
    print(f"  -> {dataset_name}: MERGE from {s3_table} ...", flush=True)
    s3 = spark.read.table(s3_table)
    cols = s3.columns
    s3 = s3.select([F.col(c).cast("string").alias(c) for c in cols])
    upsert_to_bronze(s3, bronze_table, key_col)
    count = spark.read.table(bronze_table).count()
    print(f"  ✓ {dataset_name}: {count:,} rows in {bronze_table}", flush=True)

print("=== BRONZE LAYER (Multi-Source: Auto Loader + S3 + Checkpoint + Watermark) ===", flush=True)
for ds, tbl in [
    ("events",       "exmox.bronze.s3_events"),
    ("installs",     "exmox.bronze.s3_installs"),
    ("offers",       "exmox.bronze.s3_offers"),
    ("user_profile", "exmox.bronze.s3_user_profile"),
]:
    sync_s3_table(ds, tbl)
load_bronze("events",       "/Volumes/exmox/bronze/landing/events/",       "exmox.bronze.s3_events",       "exmox.bronze.bronze_events",       "event_id", parts=8)
load_bronze("installs",     "/Volumes/exmox/bronze/landing/installs/",     "exmox.bronze.s3_installs",     "exmox.bronze.bronze_installs",     "user_id",  parts=4)
load_bronze("offers",       "/Volumes/exmox/bronze/landing/offers/",       "exmox.bronze.s3_offers",       "exmox.bronze.bronze_offers",       "offer_id", parts=1)
load_bronze("user_profile", "/Volumes/exmox/bronze/landing/user_profile/", "exmox.bronze.s3_user_profile", "exmox.bronze.bronze_user_profile", "user_id",  parts=4)
print("=== BRONZE COMPLETE ===", flush=True)

# COMMAND ----------

