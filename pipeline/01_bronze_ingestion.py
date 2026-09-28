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
# -- Spark performance tuning (serverless-safe, AQE already managed) ----------
# Shuffle partitions scale with total row count across all bronze tables.
# Formula: 1 partition per 50K rows, clamped to [8, 200].
_total_rows = 0
for _t in ["exmox.bronze.bronze_events", "exmox.bronze.bronze_installs",
           "exmox.bronze.bronze_offers", "exmox.bronze.bronze_user_profile"]:
    if spark.catalog.tableExists(_t):
        _total_rows += spark.read.table(_t).count()
_shuffle_parts = max(8, min(200, _total_rows // 50000)) if _total_rows > 0 else 32

_conf = {
    # Delta write quality
    "spark.databricks.delta.optimizeWrite.enabled":          "true",  # right-sized Parquet files
    "spark.databricks.delta.autoCompact.enabled":            "auto",  # compact when file count warrants it
    "spark.databricks.delta.schema.autoMerge.enabled":       "false", # no silent schema drift
    # Join performance
    "spark.sql.autoBroadcastJoinThreshold":                  str(100 * 1024 * 1024),  # 100 MB broadcast threshold
    # Shuffle (dynamic based on data size)
    "spark.sql.shuffle.partitions":                          str(_shuffle_parts),
    # MERGE performance
    "spark.databricks.delta.merge.enableLowShuffle.enabled": "true",  # low-shuffle MERGE algorithm
}
print(f"  shuffle.partitions = {_shuffle_parts} (for {_total_rows:,} total bronze rows)", flush=True)
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

def auto_partition_count(row_count, target_rows_per_partition=50000):
    """Calculate optimal partition count based on row count.
    Small tables (<=50K rows) get 1 partition; large tables get up to 32."""
    if row_count <= 0:
        return 1
    return max(1, min(32, row_count // target_rows_per_partition))

def load_bronze(dataset_name, volume_path, s3_table, bronze_table, key_col, parts="auto"):
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
    # Dynamic repartitioning based on data size
    _s3_count = s3.count()
    _parts = auto_partition_count(_s3_count) if parts == "auto" else parts
    s3 = s3.coalesce(1) if _parts == 1 else s3.repartition(_parts)
    print(f"    {dataset_name} (S3): {_s3_count:,} rows -> {_parts} partition(s)", flush=True)
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
    _bronze_parts = auto_partition_count(count)
    print(f"    {dataset_name} (bronze): {count:,} rows -> {_bronze_parts} optimal partition(s)", flush=True)
    print(f"  \u2713 {dataset_name}: {count:,} rows in {bronze_table}", flush=True)

print("── Bronze functions ready ──", flush=True)

# COMMAND ----------

# DBTITLE 1,S3 Sync — All Datasets
# -- S3 sync: driven by DATASETS config (defined in next cell) ----------
# This cell is defined after the DATASETS list, but runs first because
# cells execute in order. We define a local S3 map here for the sync step.
_S3_DATASETS = [
    ("events",       "exmox.bronze.s3_events"),
    ("installs",     "exmox.bronze.s3_installs"),
    ("offers",       "exmox.bronze.s3_offers"),
    ("user_profile", "exmox.bronze.s3_user_profile"),
]
print("=== BRONZE: S3 Sync ===", flush=True)
for ds, tbl in _S3_DATASETS:
    sync_s3_table(ds, tbl)
print("=== S3 Sync complete ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Bronze — All Datasets (Auto-Partitioned)
# -- Bronze ingestion: Auto Loader (volume) + S3 MERGE for all datasets ----------
# Config-driven loop: each dataset is processed with auto-calculated partitions.
# No hardcoded parts values -- partitioning is dynamic based on row count.

DATASETS = [
    {"name": "events",       "key": "event_id", "volume": "/Volumes/exmox/bronze/landing/events/",       "s3": "exmox.bronze.s3_events"},
    {"name": "installs",     "key": "user_id",   "volume": "/Volumes/exmox/bronze/landing/installs/",     "s3": "exmox.bronze.s3_installs"},
    {"name": "offers",       "key": "offer_id",  "volume": "/Volumes/exmox/bronze/landing/offers/",       "s3": "exmox.bronze.s3_offers"},
    {"name": "user_profile", "key": "user_id",   "volume": "/Volumes/exmox/bronze/landing/user_profile/", "s3": "exmox.bronze.s3_user_profile"},
]

for ds in DATASETS:
    print(f"=== BRONZE: {ds['name'].upper()} ===", flush=True)
    load_bronze(
        dataset_name=ds["name"],
        volume_path=ds["volume"],
        s3_table=ds["s3"],
        bronze_table=f"exmox.bronze.bronze_{ds['name']}",
        key_col=ds["key"],
        parts="auto",
    )
    print(f"=== Bronze {ds['name']} complete ===\n", flush=True)

print("=== ALL BRONZE INGESTION COMPLETE ===")

# COMMAND ----------

# DBTITLE 1,Consolidated — see cell above
# This cell has been consolidated into the "Bronze — All Datasets (Auto-Partitioned)" cell above.
# All four datasets (events, installs, offers, user_profile) are now processed in a single
# config-driven loop with dynamic partitioning based on row count.

# COMMAND ----------

# DBTITLE 1,Post-Write OPTIMIZE — Liquid Clustering + Compaction
# -- OPTIMIZE bronze tables after each run (dynamic list) --
print("  -> OPTIMIZE bronze ...", flush=True)
for ds in DATASETS:
    _t = f"exmox.bronze.bronze_{ds['name']}"
    if spark.catalog.tableExists(_t):
        _r = spark.sql(f"OPTIMIZE {_t}").collect()[0]["metrics"]
        print(f"    +{_r['numFilesAdded']} / -{_r['numFilesRemoved']} files  {_t}", flush=True)
print("-- Bronze OPTIMIZE complete --", flush=True)