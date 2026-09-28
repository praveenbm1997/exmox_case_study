# Databricks notebook source
"""Pytest suite for the Exmox silver-layer transformations.

Runs with a local SparkSession (no Databricks runtime required):
    pytest tests/test_transforms.py -v

Or from within a Databricks job/notebook:
    %pip install pytest
    import pytest, sys; sys.exit(pytest.main(['-v', 'tests/test_transforms.py']))
"""

import sys
import os
from datetime import datetime, date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType

# Make the pipeline directory importable when running from tests/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transforms import (
    clean_offers, clean_installs, clean_user_profile, clean_events,
    reject_rows, STEPS, JOB_CUTOFF_MINUTES,
)


# ── Fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    """SparkSession for unit tests — uses existing session on Databricks, local otherwise."""
    try:
        # On Databricks serverless: returns the existing Spark Connect session
        session = SparkSession.builder.appName("exmox_transforms_tests").getOrCreate()
    except Exception:
        # On a local machine: create a local session
        session = (SparkSession.builder
                   .master("local[1]")
                   .appName("exmox_transforms_tests")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.ansi.enabled", "false")
                   .getOrCreate())
    try:
        session.conf.set("spark.sql.ansi.enabled", "false")
    except Exception:
        pass  # serverless may reject this
    yield session
    # Don't stop the session — it's shared on Databricks


ING = datetime(2026, 5, 1, 13, 0, 0)

# Schema strings
EV = "event_id string, user_id string, event_ts string, event_name string, offer_id string, ingest_ts string, _ingested_at timestamp"
IN = "user_id string, install_ts string, country string, platform string, media_source string, device_model string, campaign_id string, _ingested_at timestamp"
OF = "offer_id string, offer_category string, payout_type string, payout_eur string, _ingested_at timestamp"
UP = "user_id string, events_lifetime string, last_seen_ts string, revenue_30d_eur string, is_payer string, _ingested_at timestamp"


def _dims(spark, installs=(("u1", "2026-05-01 00:00:00", "DE", "ios", "organic", "x", None, ING),), offers=()):
    """Build small offers + installs DataFrames for event tests."""
    return clean_offers(spark.createDataFrame(offers, OF))[0], \
           clean_installs(spark.createDataFrame(installs, IN))[0]


# ── Offers tests ────────────────────────────────────────────────────────────

def test_offers_latest_snapshot_wins_and_bad_payout_rejected(spark):
    """Later _ingested_at for the same offer_id overwrites; unparseable/negative payouts rejected."""
    good, rej = clean_offers(spark.createDataFrame([
        ("of_1", "rpg", "cpe", "1.00", ING),
        ("of_1", "RPG ", "CPE", "1.50", datetime(2026, 5, 2, 13, 0, 0)),
        ("of_2", "rpg", "cpe", "abc", ING),          # unparseable
        ("of_3", "rpg", "cpe", "-1", ING),             # negative
    ], OF))
    rows = {r["offer_id"]: r for r in good.collect()}
    assert set(rows) == {"of_1"}
    assert rows["of_1"]["payout_eur"] == Decimal("1.50")
    assert rows["of_1"]["offer_category"] == "rpg"
    assert {r["reject_key"] for r in rej.collect()} == {"of_2", "of_3"}


def test_offers_null_id_rejected(spark):
    """Null offer_id is rejected, not silently dropped."""
    good, rej = clean_offers(spark.createDataFrame([
        (None, "rpg", "cpe", "1.00", ING),
    ], OF))
    assert good.count() == 0
    assert rej.count() == 1


def test_offers_empty_table(spark):
    """Empty input produces empty good and empty rejects."""
    good, rej = clean_offers(spark.createDataFrame([], OF))
    assert good.count() == 0
    assert rej.count() == 0


# ── Installs tests ──────────────────────────────────────────────────────────

def test_country_normalised_and_bad_rejected(spark):
    """' de ' -> 'DE'; 'Germany' rejected (not ISO-2)."""
    good, rej = clean_installs(spark.createDataFrame([
        ("u1", "2026-05-01 00:00:00", " de ", "iOS", "organic", "x", "", ING),
        ("u2", "2026-05-01 00:00:00", "Germany", "ios", "organic", "x", None, ING),
    ], IN))
    r = good.first()
    assert good.count() == 1
    assert r["country"] == "DE"
    assert r["platform"] == "ios"
    assert r["campaign_id"] is None  # empty string -> NULL
    assert rej.count() == 1


def test_first_install_wins(spark):
    """When the same user_id appears twice, the earliest install_ts is kept."""
    good, _ = clean_installs(spark.createDataFrame([
        ("u1", "2026-05-03 00:00:00", "DE", "ios", "organic", "x", None, ING),
        ("u1", "2026-05-01 00:00:00", "FR", "ios", "organic", "x", None, ING),
    ], IN))
    r = good.collect()
    assert len(r) == 1
    assert r[0]["country"] == "FR"
    assert r[0]["install_date"] == date(2026, 5, 1)


def test_bad_platform_rejected(spark):
    """Platform not in (android, ios) is rejected."""
    good, rej = clean_installs(spark.createDataFrame([
        ("u1", "2026-05-01 00:00:00", "DE", "windows", "organic", "x", None, ING),
    ], IN))
    assert good.count() == 0
    assert rej.count() == 1


# ── User profile tests ──────────────────────────────────────────────────────

def test_user_profile_latest_row_and_payer_parsing(spark):
    """Latest _ingested_at wins; 'True'/'yes'/'1'/'t' -> True, everything else -> False."""
    raw = spark.createDataFrame([
        ("u1", "5", "2026-05-01 10:00:00", "1.00", "no", ING),
        ("u1", "7", "2026-05-02 10:00:00", "2.00", "True", datetime(2026, 5, 2, 13, 0, 0)),
    ], UP)
    r = clean_user_profile(raw).collect()
    assert len(r) == 1
    assert r[0]["events_lifetime"] == 7
    assert r[0]["is_payer"] is True


def test_user_profile_non_payer(spark):
    """'no' -> is_payer False."""
    r = clean_user_profile(spark.createDataFrame([
        ("u1", "3", "2026-05-01 10:00:00", "0.00", "no", ING),
    ], UP)).first()
    assert r["is_payer"] is False


# ── Events tests ────────────────────────────────────────────────────────────

def test_dedup_first_arrival_wins(spark):
    """Same event_id with different ingest_ts: earliest ingest_ts wins; rest go to rejects."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:00:00", ING),
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:01:30", ING),
    ], EV)
    good, rej = clean_events(raw, *_dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)]))
    assert good.count() == 1
    assert good.first()["ingest_ts"] == datetime(2026, 5, 1, 13, 0, 0)
    assert rej.filter("reject_reason = 'duplicate_event_id'").count() == 1


def test_late_flag_uses_next_day_0015(spark):
    """is_late is True only when ingest_ts > 00:15 the next day, not just because lag > 15 min."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:10:00", ING),   # 20 min lag, on time
        ("e2", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:20:00", ING),  # 30 min lag, late
    ], EV)
    good, _ = clean_events(raw, *_dims(spark))
    flags = {r["event_id"]: r["is_late"] for r in good.collect()}
    assert flags == {"e1": False, "e2": True}


def test_orphan_and_pre_install_are_flagged_not_dropped(spark):
    """Events with unknown offer_id or pre-install ts are kept with flags, not rejected."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-04-30 10:00:00", "offer_view", "of_9", "2026-04-30 11:00:00", ING),
    ], EV)
    good, _ = clean_events(raw, *_dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)]))
    r = good.first()
    assert good.count() == 1
    assert r["is_orphan_offer"] is True
    assert r["is_pre_install"] is True
    assert set(r["dq_flags"]) == {"pre_install", "orphan_offer"}


def test_unknown_event_name_rejected(spark):
    """Event names not in STEPS are rejected."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 10:00:00", "purchase", None, "2026-05-01 11:00:00", ING),
    ], EV)
    good, rej = clean_events(raw, *_dims(spark))
    assert good.count() == 0
    assert rej.count() == 1


def test_malformed_timestamp_and_null_key_rejected(spark):
    """Bad timestamp casts to NULL (ANSI off); null user_id rejected. Both go to rejects."""
    raw = spark.createDataFrame([
        ("e1", "u1", "yesterday", "app_open", None, "2026-05-01 11:00:00", ING),
        ("e2", None, "2026-05-01 10:00:00", "app_open", None, "2026-05-01 11:00:00", ING),
    ], EV)
    good, rej = clean_events(raw, *_dims(spark))
    assert good.count() == 0
    assert rej.count() == 2


def test_event_with_known_offer_not_flagged(spark):
    """Event with a valid offer_id should have is_orphan_offer = False."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 12:00:00", "offer_view", "of_1", "2026-05-01 13:00:00", ING),
    ], EV)
    good, _ = clean_events(raw, *_dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)]))
    r = good.first()
    assert r["is_orphan_offer"] is False
    assert r["is_pre_install"] is False
    assert r["is_late"] is False


def test_null_offer_id_not_orphan(spark):
    """app_open events with no offer_id should have is_orphan_offer = False (null is not orphan)."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", None, "2026-05-01 13:00:00", ING),
    ], EV)
    good, _ = clean_events(raw, *_dims(spark))
    r = good.first()
    assert r["is_orphan_offer"] is False


def test_lag_hours_computed(spark):
    """lag_hours is the difference between ingest_ts and event_ts in hours."""
    raw = spark.createDataFrame([
        ("e1", "u1", "2026-05-01 10:00:00", "app_open", None, "2026-05-01 15:00:00", ING),
    ], EV)
    good, _ = clean_events(raw, *_dims(spark))
    r = good.first()
    assert r["lag_hours"] == pytest.approx(5.0, abs=0.01)


def test_empty_events(spark):
    """Empty events DataFrame produces empty good and empty rejects."""
    raw = spark.createDataFrame([], EV)
    good, rej = clean_events(raw, *_dims(spark))
    assert good.count() == 0
    assert rej.count() == 0
