# Databricks notebook source
# DBTITLE 1,Bronze — Column Constraints (idempotent)
# ── Bronze column constraints: NOT NULL on merge keys (idempotent) ──────────
def _not_null(tbl, col):
    try:
        spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET NOT NULL")
        print(f"  ✓ NOT NULL: {tbl}.{col}")
    except Exception as e:
        if "not null" not in str(e).lower() and "already" not in str(e).lower():
            print(f"  ⚠ NOT NULL {tbl}.{col}: {e}")

if spark.catalog.tableExists("exmox.bronze.bronze_events"):       _not_null("exmox.bronze.bronze_events",       "event_id")
if spark.catalog.tableExists("exmox.bronze.bronze_installs"):     _not_null("exmox.bronze.bronze_installs",     "user_id")
if spark.catalog.tableExists("exmox.bronze.bronze_offers"):       _not_null("exmox.bronze.bronze_offers",       "offer_id")
if spark.catalog.tableExists("exmox.bronze.bronze_user_profile"): _not_null("exmox.bronze.bronze_user_profile", "user_id")
print("── Bronze constraints applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Performance Config — Optimized Writes, Broadcast, Low-Shuffle MERGE
# ── Spark performance tuning (serverless-safe, AQE already managed) ──────────
_conf = {
    # Delta write quality
    "spark.databricks.delta.optimizeWrite.enabled":          "true",  # right-sized Parquet files
    "spark.databricks.delta.autoCompact.enabled":            "auto",  # compact when file count warrants it
    "spark.databricks.delta.schema.autoMerge.enabled":       "false", # no silent schema drift
    # Join performance
    "spark.sql.autoBroadcastJoinThreshold":                  str(100 * 1024 * 1024),  # 100 MB broadcast threshold
    # Shuffle
    "spark.sql.shuffle.partitions":                          "32",    # tuned for ≤500 K-row batches
    # MERGE performance
    "spark.databricks.delta.merge.enableLowShuffle.enabled": "true",  # low-shuffle MERGE algorithm
}
_skipped = []
for _k, _v in _conf.items():
    try: spark.conf.set(_k, _v)
    except Exception: _skipped.append(_k.split(".")[-1])
if _skipped:
    print(f"  ↷ serverless-managed (already optimal): {', '.join(_skipped)}", flush=True)
print("── Perf config applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Imports, Constants & Functions
from pyspark.sql import functions as F
from delta.tables import DeltaTable

CHECKPOINT_BASE = "/Volumes/exmox/bronze/_checkpoints"

def sync_s3_table(dataset_name, s3_table):
    """Read CSV from S3 and create/refresh the managed Delta table in exmox.bronze."""
    s3_path = f"s3://exmox/{dataset_name}/{dataset_name}.csv"
    print(f"  -> syncing S3: {s3_path} \u2192 {s3_table} ...", flush=True)
    (spark.read.format("csv")
     .option("header", "true")
     .option("inferSchema", "false")
     .load(s3_path)
     .write.mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(s3_table))
    print(f"  \u2713 {s3_table} synced", flush=True)

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

def validate_key_col(df, label, key_col):
    """Fail fast if the merge key has nulls — call before every upsert_to_bronze."""
    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(F.col(key_col).isNull().cast("long")).alias("nulls")
    ).collect()[0]
    if int(stats["nulls"]) > 0:
        raise ValueError(
            f"DQ FAIL [{label}]: '{key_col}' has {int(stats['nulls']):,}/"
            f"{int(stats['total']):,} null values — aborting write."
        )
    print(f"    ✓ DQ [{label}]: {key_col} not null ({int(stats['total']):,} rows)", flush=True)

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
        validate_key_col(batch_df, bronze_table, key_col)
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
    s3 = s3.withColumns({
        "_source_file": F.lit("s3"),
        "_ingested_at": F.current_timestamp().cast("string"),
        "_source": F.lit("s3"),
    })
    if spark.catalog.tableExists(bronze_table):
        _existing_cols = set(spark.read.table(bronze_table).columns)
        for _mc in ["_ingested_at", "_source_file", "_source"]:
            if _mc not in _existing_cols:
                spark.sql(f"ALTER TABLE {bronze_table} ADD COLUMN IF NOT EXISTS {_mc} STRING")
        if "_ingested_at" not in _existing_cols:
            spark.sql(f"UPDATE {bronze_table} SET _ingested_at = cast(current_timestamp() as string) WHERE _ingested_at IS NULL")
    validate_key_col(s3, s3_table, key_col)
    upsert_to_bronze(s3, bronze_table, key_col)
    count = spark.read.table(bronze_table).count()
    print(f"  \u2713 {dataset_name}: {count:,} rows in {bronze_table}", flush=True)

print("── Bronze functions ready ──", flush=True)

# COMMAND ----------

# DBTITLE 1,S3 Sync — All Datasets
# ── Sync all four S3 CSVs into staging Delta tables (overwrite, idempotent) ───
print("=== BRONZE: S3 Sync ===", flush=True)
for ds, tbl in [
    ("events",       "exmox.bronze.s3_events"),
    ("installs",     "exmox.bronze.s3_installs"),
    ("offers",       "exmox.bronze.s3_offers"),
    ("user_profile", "exmox.bronze.s3_user_profile"),
]:
    sync_s3_table(ds, tbl)
print("=== S3 Sync complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Bronze — Events
# ── Bronze Events: Auto Loader (volume) + S3 MERGE ──────────────────────
print("=== BRONZE: Events ===", flush=True)
load_bronze("events", "/Volumes/exmox/bronze/landing/events/",
            "exmox.bronze.s3_events", "exmox.bronze.bronze_events", "event_id", parts=8)
print("=== Bronze Events complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Bronze — Installs
# ── Bronze Installs: Auto Loader (volume) + S3 MERGE ─────────────────────
print("=== BRONZE: Installs ===", flush=True)
load_bronze("installs", "/Volumes/exmox/bronze/landing/installs/",
            "exmox.bronze.s3_installs", "exmox.bronze.bronze_installs", "user_id", parts=4)
print("=== Bronze Installs complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Bronze — Offers
# ── Bronze Offers: Auto Loader (volume) + S3 MERGE ───────────────────────
print("=== BRONZE: Offers ===", flush=True)
load_bronze("offers", "/Volumes/exmox/bronze/landing/offers/",
            "exmox.bronze.s3_offers", "exmox.bronze.bronze_offers", "offer_id", parts=1)
print("=== Bronze Offers complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Bronze — User Profile
# ── Bronze User Profile: Auto Loader (volume) + S3 MERGE ─────────────────
print("=== BRONZE: User Profile ===", flush=True)
load_bronze("user_profile", "/Volumes/exmox/bronze/landing/user_profile/",
            "exmox.bronze.s3_user_profile", "exmox.bronze.bronze_user_profile", "user_id", parts=4)
print("=== Bronze User Profile complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Post-Write OPTIMIZE — Liquid Clustering + Compaction
# ── OPTIMIZE bronze tables after each run (applies liquid clustering + compacts small files) ──
print("  → OPTIMIZE bronze ...", flush=True)
for _t in [
    "exmox.bronze.bronze_events",
    "exmox.bronze.bronze_installs",
    "exmox.bronze.bronze_offers",
    "exmox.bronze.bronze_user_profile",
]:
    _r = spark.sql(f"OPTIMIZE {_t}").collect()[0]["metrics"]
    print(f"    ✓ {_t}: +{_r['numFilesAdded']} / -{_r['numFilesRemoved']} files", flush=True)
print("── Bronze OPTIMIZE complete ──", flush=True)