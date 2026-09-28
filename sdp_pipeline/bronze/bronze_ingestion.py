"""Bronze layer: Auto Loader (volume) + S3 batch sources into streaming tables.

Ingests CSV files from two sources for each dataset:
  - Volume: /Volumes/exmox/bronze/landing/{dataset}/ (Auto Loader, continuous)
  - S3:     s3://exmox/{dataset}/{dataset}.csv (batch, one-time backfill)

Datasets: events (key: event_id), installs (key: user_id),
          offers (key: offer_id), user_profile (key: user_id)

All columns are inferred as STRING (matching the legacy notebook behaviour);
type casting happens in the silver layer. Metadata columns track provenance
(_source_file, _ingested_at, _source). The silver layer deduplicates by key
so duplicate rows from multiple sources or re-runs are handled there.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# -- Shared helpers ----------------------------------------------------------

def _autoloader_csv(volume_path: str):
    """Auto Loader config for volume CSV sources (continuous streaming)."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(volume_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("volume"))
    )


def _s3_csv(s3_path: str):
    """Batch read from S3 CSV (one-time per pipeline update)."""
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .load(s3_path)
        .withColumn("_source_file", F.lit("s3"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("s3"))
    )


# -- Streaming table targets (volume + S3 append flows) ---------------------

DATASETS = [
    {"name": "events",       "key": "event_id", "volume": "/Volumes/exmox/bronze/landing/events/",       "s3": "s3://exmox/events/events.csv"},
    {"name": "installs",     "key": "user_id",   "volume": "/Volumes/exmox/bronze/landing/installs/",     "s3": "s3://exmox/installs/installs.csv"},
    {"name": "offers",       "key": "offer_id",  "volume": "/Volumes/exmox/bronze/landing/offers/",       "s3": "s3://exmox/offers/offers.csv"},
    {"name": "user_profile", "key": "user_id",   "volume": "/Volumes/exmox/bronze/landing/user_profile/", "s3": "s3://exmox/user_profile/user_profile.csv"},
]

for ds in DATASETS:
    dp.create_streaming_table(
        name=f"exmox.bronze.bronze_{ds['name']}",
        comment=f"Raw {ds['name']} from Auto Loader (volume) + S3 batch. Key: {ds['key']}.",
        cluster_by=[ds["key"]],
    )


# -- Volume append flows (continuous, Auto Loader) --------------------------

@dp.append_flow(target="exmox.bronze.bronze_events", name="events_volume_flow")
def events_volume_flow():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/events/")


@dp.append_flow(target="exmox.bronze.bronze_installs", name="installs_volume_flow")
def installs_volume_flow():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/installs/")


@dp.append_flow(target="exmox.bronze.bronze_offers", name="offers_volume_flow")
def offers_volume_flow():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/offers/")


@dp.append_flow(target="exmox.bronze.bronze_user_profile", name="user_profile_volume_flow")
def user_profile_volume_flow():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/user_profile/")


# -- S3 append flows (one-time batch backfill) -------------------------------
# S3 data is loaded once as a historical backfill. If the S3 CSV files are
# updated, do a full refresh to re-ingest them.

@dp.append_flow(target="exmox.bronze.bronze_events", name="events_s3_flow", once=True)
def events_s3_flow():
    return _s3_csv("s3://exmox/events/events.csv")


@dp.append_flow(target="exmox.bronze.bronze_installs", name="installs_s3_flow", once=True)
def installs_s3_flow():
    return _s3_csv("s3://exmox/installs/installs.csv")


@dp.append_flow(target="exmox.bronze.bronze_offers", name="offers_s3_flow", once=True)
def offers_s3_flow():
    return _s3_csv("s3://exmox/offers/offers.csv")


@dp.append_flow(target="exmox.bronze.bronze_user_profile", name="user_profile_s3_flow", once=True)
def user_profile_s3_flow():
    return _s3_csv("s3://exmox/user_profile/user_profile.csv")
