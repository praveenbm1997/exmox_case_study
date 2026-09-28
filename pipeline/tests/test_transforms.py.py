# Databricks notebook source
"""Pytest tests for transformation logic.

Run locally or on Databricks:
  pytest -v --tb=short tests/test_transforms.py

Each test encodes one business rule from the legacy 02_silver_transform notebook.
Tests use hand-built rows — no Spark tables required.
"""

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType

# ── Spark session fixture (works on Databricks + local) ────────────────────
@pytest.fixture(scope="session")
def spark():
    try:
        # On Databricks, a SparkSession already exists
        spark = SparkSession.builder.getOrCreate()
        yield spark
    except Exception:
        # Local fallback
        spark = (SparkSession.builder
                 .master("local[2]")
                 .appName("legacy_transforms_tests")
                 .config("spark.sql.shuffle.partitions", "2")
                 .getOrCreate())
        yield spark
        spark.stop()


# ── Import the transforms module ────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from transforms import (
    clean_offers, clean_installs, clean_events, clean_user_profile, aggregate_gold,
)


# ── Helpers ────────────────────────────────────────────────────────────────
ING = datetime(2026, 5, 1, 13, 0, 0)

EV = "event_id string, user_id string, event_ts string, event_name string, offer_id string, ingest_ts string, _ingested_at timestamp"
IN = "user_id string, install_ts string, country string, device_model string, media_source string, campaign_id string, _ingested_at timestamp"
OF = "offer_id string, offer_category string, payout_type string, payout_eur string, _ingested_at timestamp"
UP = "user_id string, events_lifetime string, last_seen_ts string, revenue_30d_eur string, is_payer string, _ingested_at timestamp"


def df(spark, schema, rows):
    return spark.createDataFrame(rows, schema)


def _dims(spark, installs=(("u1", "2026-05-01 00:00:00", "DE", "iPhone 15", "organic", None, ING),), offers=()):
    clean_o, _ = clean_offers(df(spark, OF, list(offers)))
    clean_i, _ = clean_installs(df(spark, IN, list(installs)))
    return clean_o, clean_i


# ── Offers tests ───────────────────────────────────────────────────────────

def test_offers_latest_snapshot_wins(spark):
    raw = df(spark, OF, [
        ("of_1", "rpg", "cpe", "1.00", ING),
        ("of_1", "rpg", "cpe", "2.50", datetime(2026, 5, 2, 10, 0, 0)),  # newer
    ])
    good, rej = clean_offers(raw)
    assert good.count() == 1
    assert good.first()["payout_eur"] == DecimalType(18, 2)() and float(good.first()["payout_eur"]) == 2.50


def test_offers_bad_payout_rejected(spark):
    raw = df(spark, OF, [
        ("of_1", "rpg", "cpe", "1.00", ING),
        ("of_2", "rpg", "cpe", "-1.00", ING),   # negative payout
        (None, "rpg", "cpe", "1.00", ING),         # null offer_id
    ])
    good, rej = clean_offers(raw)
    assert good.count() == 1
    assert rej.count() == 2


# ── Installs tests ─────────────────────────────────────────────────────────

def test_installs_platform_derived_from_device_model(spark):
    raw = df(spark, IN, [
        ("u1", "2026-05-01 00:00:00", "DE", "iPhone 15 Pro", "organic", None, ING),
        ("u2", "2026-05-01 00:00:00", "FR", "Samsung Galaxy S24", "organic", None, ING),
    ])
    good, rej = clean_installs(raw)
    platforms = {r["user_id"]: r["platform"] for r in good.collect()}
    assert platforms == {"u1": "ios", "u2": "android"}


def test_installs_first_install_wins(spark):
    raw = df(spark, IN, [
        ("u1", "2026-05-03 00:00:00", "DE", "iPhone 15", "organic", None, ING),
        ("u1", "2026-05-01 00:00:00", "FR", "iPhone 15", "organic", None, ING),  # earlier
    ])
    good, rej = clean_installs(raw)
    assert good.count() == 1
    assert good.first()["country"] == "FR"


def test_installs_bad_country_rejected(spark):
    raw = df(spark, IN, [
        ("u1", "2026-05-01 00:00:00", "DE", "iPhone 15", "organic", None, ING),
        ("u2", "2026-05-01 00:00:00", "Germany", "iPhone 15", "organic", None, ING),  # not ISO-2
    ])
    good, rej = clean_installs(raw)
    assert good.count() == 1
    assert rej.count() == 1


# ── Events tests ───────────────────────────────────────────────────────────

def test_event_dedup_first_arrival_wins(spark):
    raw = df(spark, EV, [
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:00:00", ING),
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:01:30", ING),
    ])
    offers, installs = _dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)])
    good, rej = clean_events(raw, offers, installs)
    assert good.count() == 1
    assert rej.filter("reject_reason = 'duplicate_event_id'").count() == 1


def test_event_late_flag_uses_next_day_0015(spark):
    raw = df(spark, EV, [
        ("e1", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:10:00", ING),   # 20 min, on time
        ("e2", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:20:00", ING),   # 30 min, late
    ])
    offers, installs = _dims(spark)
    good, rej = clean_events(raw, offers, installs)
    flags = {r["event_id"]: r["is_late"] for r in good.collect()}
    assert flags == {"e1": False, "e2": True}


def test_pre_install_flagged_not_dropped(spark):
    raw = df(spark, EV, [
        ("e1", "u1", "2026-04-30 20:00:00", "app_open", None, "2026-05-01 00:05:00", ING),  # before install
    ])
    offers, installs = _dims(spark, installs=[("u1", "2026-05-01 00:00:00", "DE", "iPhone 15", "organic", None, ING)])
    good, rej = clean_events(raw, offers, installs)
    assert good.count() == 1
    assert good.first()["is_pre_install"] is True


def test_orphan_offer_flagged_not_dropped(spark):
    raw = df(spark, EV, [
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_999", "2026-05-01 13:00:00", ING),
    ])
    offers, installs = _dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)])  # of_999 not in catalog
    good, rej = clean_events(raw, offers, installs)
    assert good.count() == 1
    assert good.first()["is_orphan_offer"] is True


def test_unknown_event_name_rejected(spark):
    raw = df(spark, EV, [
        ("e1", "u1", "2026-05-01 12:00:00", "unknown_step", None, "2026-05-01 13:00:00", ING),
    ])
    offers, installs = _dims(spark)
    good, rej = clean_events(raw, offers, installs)
    assert good.count() == 0
    assert rej.count() == 1


# ── User profile tests ─────────────────────────────────────────────────────

def test_user_profile_latest_row_wins(spark):
    raw = df(spark, UP, [
        ("u1", "10", "2026-05-01 10:00:00", "5.00", "true", ING),
        ("u1", "25", "2026-05-10 10:00:00", "15.00", "false", datetime(2026, 5, 10, 10, 0, 0)),
    ])
    good = clean_user_profile(raw)
    assert good.count() == 1
    r = good.first()
    assert r["events_lifetime"] == 25
    assert r["is_payer"] is False


def test_user_profile_payer_parsing(spark):
    raw = df(spark, UP, [
        ("u1", "10", "2026-05-01 10:00:00", "5.00", "Yes", ING),
        ("u2", "10", "2026-05-01 10:00:00", "0.00", "0", ING),
    ])
    good = clean_user_profile(raw)
    payers = {r["user_id"]: r["is_payer"] for r in good.collect()}
    assert payers == {"u1": True, "u2": False}


# ── Gold aggregation tests ────────────────────────────────────────────────

def test_gold_aggregation_counts_events_and_installs(spark):
    """Verify gold produces a row per date/country/platform with correct counts."""
    offers, installs = _dims(spark, offers=[("of_1", "rpg", "cpe", "1.00", ING)])
    events_raw = df(spark, EV, [
        ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:00:00", ING),
        ("e2", "u1", "2026-05-01 13:00:00", "reward_paid", "of_1", "2026-05-01 13:30:00", ING),
    ])
    events, _ = clean_events(events_raw, offers, installs)
    gold = aggregate_gold(events, installs, offers)
    assert gold.count() == 1
    r = gold.first()
    assert r["date"].strftime("%Y-%m-%d") == "2026-05-01"
    assert r["country"] == "DE"
    assert r["platform"] == "ios"
    assert r["installs"] == 1
    assert r["events_total"] == 2
    assert r["app_open_users"] == 1
    assert r["reward_payouts"] == 1
    assert float(r["reward_cost_eur"]) == 1.00
