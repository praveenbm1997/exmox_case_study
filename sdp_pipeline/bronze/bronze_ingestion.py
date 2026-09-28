"""Bronze layer: Auto Loader streaming tables from volume landing zones.

Ingests CSV files from /Volumes/exmox/bronze/landing/{dataset}/ for:
  events (key: event_id), installs (key: user_id),
  offers (key: offer_id), user_profile (key: user_id)

All columns are inferred as STRING (matching the legacy notebook behaviour);
type casting happens in the silver layer. Metadata columns track provenance.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _autoloader_csv(volume_path: str):
    """Shared Auto Loader config for all bronze CSV sources."""
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


@dp.table(
    name="exmox.bronze.bronze_events",
    comment="Raw events from Auto Loader (CSV, all STRING). Key: event_id.",
    cluster_by=["event_id"],
)
def bronze_events():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/events/")


@dp.table(
    name="exmox.bronze.bronze_installs",
    comment="Raw installs from Auto Loader (CSV, all STRING). Key: user_id.",
    cluster_by=["user_id"],
)
def bronze_installs():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/installs/")


@dp.table(
    name="exmox.bronze.bronze_offers",
    comment="Raw offers from Auto Loader (CSV, all STRING). Key: offer_id.",
    cluster_by=["offer_id"],
)
def bronze_offers():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/offers/")


@dp.table(
    name="exmox.bronze.bronze_user_profile",
    comment="Raw user profiles from Auto Loader (CSV, all STRING). Key: user_id.",
    cluster_by=["user_id"],
)
def bronze_user_profile():
    return _autoloader_csv("/Volumes/exmox/bronze/landing/user_profile/")
