# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze → Silver
# MAGIC
# MAGIC | # | Step | Stops the run? |
# MAGIC |---|---|---|
# MAGIC | 1 | Spark tuning: AQE, auto shuffle partitions, broadcast threshold, optimized writes, ANSI off, UTC | no (serverless-rejected confs are listed) |
# MAGIC | 2 | Silver transformations (pure functions) | – |
# MAGIC | 3 | Unit tests of those transformations on hand-built rows | yes, before any table is touched |
# MAGIC | 4 | Landing → bronze with Auto Loader (`availableNow`) | yes |
# MAGIC | 5 | DQ gate on bronze: column contract, not empty, rescued data | on `error` rules |
# MAGIC | 6 | Bronze → silver: dimensions overwritten, events MERGEd past the watermark | yes |
# MAGIC | 7 | DQ gate on silver (touched window, or full history with `dq_scope=full`) | on `error` rules |
# MAGIC | 8 | DQ observability: `silver.dq_metrics` (append), `silver.dq_issues` (MERGE open/resolved) | yes |
# MAGIC | 9 | `OPTIMIZE silver.events`: window + Z-order when partitioned, incremental when liquid | yes |
# MAGIC | 10 | Hand-off: `touched` dates as a task value for `02_silver_to_gold` | – |
# MAGIC
# MAGIC **Layout** (`layout` widget): `liquid` = `CLUSTER BY (event_date, user_id)`;
# MAGIC `partition` = `PARTITIONED BY (event_date)` + `ZORDER BY (user_id)`. Switching an existing table
# MAGIC needs one run with `full_rebuild=true`.

# COMMAND ----------

# DBTITLE 0,Parameters
dbutils.widgets.dropdown("full_rebuild", "false", ["false", "true"], "Full rebuild (ignore watermark)")
dbutils.widgets.dropdown("layout", "liquid", ["liquid", "partition"], "Table layout")
dbutils.widgets.dropdown("dq_scope", "window", ["window", "full"], "DQ scope")
dbutils.widgets.dropdown("run_ingest", "true", ["true", "false"], "Run Auto Loader first")
dbutils.widgets.dropdown("run_tests", "true", ["true", "false"], "Gate on unit tests")
dbutils.widgets.dropdown("optimize", "true", ["true", "false"], "OPTIMIZE after write")

# COMMAND ----------

# MAGIC %run ../Exploration_notebooks/00_common

# COMMAND ----------

# DBTITLE 0,1 · Spark tuning
FULL = dbutils.widgets.get("full_rebuild") == "true"
LAYOUT = dbutils.widgets.get("layout")
DQ_FULL = dbutils.widgets.get("dq_scope") == "full"
RUN_INGEST = dbutils.widgets.get("run_ingest") == "true"
RUN_TESTS = dbutils.widgets.get("run_tests") == "true"
OPTIMIZE = dbutils.widgets.get("optimize") == "true"
print(f"full_rebuild={FULL} layout={LAYOUT} dq_scope={'full' if DQ_FULL else 'window'}")

display(configure_spark())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Transformations
# MAGIC DataFrame in, DataFrame out, no I/O, so the tests below run them on hand-built rows.
# MAGIC Bad rows are never dropped silently: they go to `silver.rejects` with a reason. Event anomalies are
# MAGIC **flagged, not dropped**: `is_late` (missed the 00:15 job), `is_pre_install`, `is_orphan_offer`.

# COMMAND ----------

# DBTITLE 1,Module import (transforms.py)
# Import transforms from the standalone module so pytest can test them independently.
# The inline definitions below are kept as the runtime source; the module mirrors them.
# To fully decouple: remove the inline defs and call clean_events(raw, offers, installs,
# steps=CFG.steps, job_cutoff_minutes=CFG.job_cutoff_minutes) from the module.
import sys
sys.path.insert(0, "/Workspace/Users/praveen.b.madhava@gmail.com/exmox/notebooks/pipeline")
try:
    from transforms import (
        _str as _mod_str, reject_rows as _mod_reject_rows,
        clean_offers as _mod_clean_offers, clean_installs as _mod_clean_installs,
        clean_user_profile as _mod_clean_user_profile, clean_events as _mod_clean_events,
    )
    print("transforms.py module available for pytest")
except ImportError as _e:
    print(f"transforms.py not importable in this context: {_e}")
    print("Inline definitions will be used (tests run separately)")

# COMMAND ----------

# DBTITLE 0,Silver transformations
def _str(df: DataFrame) -> DataFrame:
    """Force every non-audit column to STRING so the casts below are the only typing that happens."""
    return df.select([F.col(c).cast("string").alias(c) if not c.startswith("_") else F.col(c) for c in df.columns])


def reject_rows(df: DataFrame, source: str, key_col: str, reason: str) -> DataFrame:
    return df.select(
        F.lit(source).alias("source"),
        F.col(key_col).cast("string").alias("reject_key"),
        F.lit(reason).alias("reject_reason"),
        F.to_json(F.struct(*[c for c in df.columns if not c.startswith("_")])).alias("raw_record"),
        F.col("_ingested_at"))


def clean_offers(raw: DataFrame):
    o = (_str(raw)
         .withColumn("offer_id", F.trim("offer_id"))
         .withColumn("offer_category", F.lower(F.trim("offer_category")))
         .withColumn("payout_type", F.lower(F.trim("payout_type")))
         .withColumn("payout_eur", F.trim("payout_eur").cast(DecimalType(18, 2))))
    bad_rows = o.filter(F.col("offer_id").isNull() | F.col("payout_eur").isNull() | (F.col("payout_eur") < 0))
    good = (o.subtract(bad_rows)
            .withColumn("_rn", F.row_number().over(W.partitionBy("offer_id").orderBy(F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn"))                          # latest snapshot wins for a dimension
    return good, reject_rows(bad_rows, "offers", "offer_id", "null_id_or_bad_payout")


def clean_installs(raw: DataFrame):
    i = (_str(raw)
         .withColumn("user_id", F.trim("user_id"))
         .withColumn("install_ts", F.to_timestamp(F.trim("install_ts")))
         .withColumn("country", F.upper(F.trim("country")))          # 16 spellings -> 8 ISO-2 codes
         .withColumn("platform", F.lower(F.trim("platform")))
         .withColumn("media_source", F.lower(F.trim("media_source")))
         .withColumn("device_model", F.trim("device_model"))         # kept, never trusted: models appear on both platforms
         .withColumn("campaign_id", F.nullif(F.trim("campaign_id"), F.lit(""))))   # NULL == organic
    bad_rows = i.filter(F.col("user_id").isNull() | F.col("install_ts").isNull()
                        | ~F.col("platform").isin("android", "ios") | ~F.col("country").rlike("^[A-Z]{2}$"))
    good = (i.subtract(bad_rows)
            .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts", F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn")                           # first install wins
            .withColumn("install_date", F.to_date("install_ts")))
    return good, reject_rows(bad_rows, "installs", "user_id", "bad_ts_platform_or_country")


def clean_user_profile(raw: DataFrame) -> DataFrame:
    """Reference only. Later snapshot than events, own metric definitions; gold never reads it."""
    return (_str(raw)
            .withColumn("user_id", F.trim("user_id"))
            .withColumn("events_lifetime", F.trim("events_lifetime").cast("bigint"))
            .withColumn("last_seen_ts", F.to_timestamp(F.trim("last_seen_ts")))
            .withColumn("last_seen_date", F.to_date("last_seen_ts"))
            .withColumn("revenue_30d_eur", F.trim("revenue_30d_eur").cast(DecimalType(18, 2)))
            .withColumn("is_payer", F.lower(F.trim("is_payer")).isin("true", "1", "t", "yes"))
            .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy(F.desc("_ingested_at"))))
            .filter("_rn = 1").drop("_rn"))


def clean_events(raw: DataFrame, offers: DataFrame, installs: DataFrame):
    """Typed, deduped (first arrival wins on event_id), flagged."""
    e = (_str(raw)
         .withColumn("event_id", F.trim("event_id"))
         .withColumn("user_id", F.trim("user_id"))
         .withColumn("event_name", F.lower(F.trim("event_name")))
         .withColumn("offer_id", F.nullif(F.trim("offer_id"), F.lit("")))
         .withColumn("event_ts", F.to_timestamp(F.trim("event_ts")))
         .withColumn("ingest_ts", F.to_timestamp(F.trim("ingest_ts"))))
    bad_rows = e.filter(F.col("event_id").isNull() | F.col("user_id").isNull() | F.col("event_ts").isNull()
                        | F.col("ingest_ts").isNull() | ~F.col("event_name").isin(*CFG.steps))
    tagged = (e.subtract(bad_rows)
              .withColumn("_rn", F.row_number().over(W.partitionBy("event_id").orderBy("ingest_ts", "_ingested_at"))))
    dups = tagged.filter("_rn > 1").drop("_rn")
    cutoff = F.to_timestamp(F.date_add("event_date", 1)) + F.expr(f"INTERVAL {CFG.job_cutoff_minutes} MINUTES")
    good = (tagged.filter("_rn = 1").drop("_rn")
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0)
            .withColumn("is_late", F.col("ingest_ts") > cutoff)
            # offers catalog is tiny: broadcast, no shuffle of events
            .join(F.broadcast(offers.select("offer_id", F.lit(True).alias("_known"))), "offer_id", "left")
            .withColumn("is_orphan_offer", F.col("offer_id").isNotNull() & F.col("_known").isNull()).drop("_known")
            # installs can be large: left to AQE (broadcast if small at runtime, skew-split if not)
            .join(installs.select("user_id", "install_ts"), "user_id", "left")
            .withColumn("is_pre_install", F.coalesce(F.col("event_ts") < F.col("install_ts"), F.lit(False))).drop("install_ts")
            .withColumn("dq_flags", F.array_compact(F.array(
                F.when(F.col("is_late"), F.lit("late")),
                F.when(F.col("is_pre_install"), F.lit("pre_install")),
                F.when(F.col("is_orphan_offer"), F.lit("orphan_offer"))))))
    rejects = reject_rows(bad_rows, "events", "event_id", "null_key_bad_ts_or_unknown_event") \
        .unionByName(reject_rows(dups, "events", "event_id", "duplicate_event_id"))
    return good, rejects

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Transformation tests
# MAGIC Each test encodes one rule and fails if the rule were removed or inverted.

# COMMAND ----------

# DBTITLE 0,Unit tests (hand-built rows, no tables)
ING = datetime(2026, 5, 1, 13, 0, 0)
EV = "event_id string, user_id string, event_ts string, event_name string, offer_id string, ingest_ts string, _ingested_at timestamp"
IN = "user_id string, install_ts string, country string, platform string, media_source string, device_model string, campaign_id string, _ingested_at timestamp"
OF = "offer_id string, offer_category string, payout_type string, payout_eur string, _ingested_at timestamp"


def _dims(installs=(("u1", "2026-05-01 00:00:00", "DE", "ios", "organic", "x", None, ING),), offers=()):
    return clean_offers(df(OF, list(offers)))[0], clean_installs(df(IN, list(installs)))[0]


def test_dedup_first_arrival_wins():
    raw = df(EV, [("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:00:00", ING),
                  ("e1", "u1", "2026-05-01 12:00:00", "app_open", "of_1", "2026-05-01 13:01:30", ING)])
    good, rej = clean_events(raw, *_dims(offers=[("of_1", "rpg", "cpe", "1.00", ING)]))
    assert good.count() == 1
    assert good.first()["ingest_ts"] == datetime(2026, 5, 1, 13, 0, 0)
    assert rej.filter("reject_reason = 'duplicate_event_id'").count() == 1


def test_late_flag_uses_next_day_0015_not_raw_lag():
    raw = df(EV, [("e1", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:10:00", ING),   # 20 min lag, on time
                  ("e2", "u1", "2026-05-01 23:50:00", "app_open", None, "2026-05-02 00:20:00", ING)])  # 30 min lag, late
    good, _ = clean_events(raw, *_dims())
    assert {r["event_id"]: r["is_late"] for r in good.collect()} == {"e1": False, "e2": True}


def test_country_normalised_and_bad_rejected():
    good, rej = clean_installs(df(IN, [("u1", "2026-05-01 00:00:00", " de ", "iOS", "organic", "x", "", ING),
                                       ("u2", "2026-05-01 00:00:00", "Germany", "ios", "organic", "x", None, ING)]))
    r = good.first()
    assert good.count() == 1 and r["country"] == "DE" and r["platform"] == "ios" and r["campaign_id"] is None
    assert rej.count() == 1


def test_first_install_wins():
    good, _ = clean_installs(df(IN, [("u1", "2026-05-03 00:00:00", "DE", "ios", "organic", "x", None, ING),
                                     ("u1", "2026-05-01 00:00:00", "FR", "ios", "organic", "x", None, ING)]))
    r = good.collect()
    assert len(r) == 1 and r[0]["country"] == "FR" and r[0]["install_date"] == date(2026, 5, 1)


def test_orphan_and_pre_install_are_flagged_not_dropped():
    raw = df(EV, [("e1", "u1", "2026-04-30 10:00:00", "offer_view", "of_9", "2026-04-30 11:00:00", ING)])
    good, _ = clean_events(raw, *_dims(offers=[("of_1", "rpg", "cpe", "1.00", ING)]))
    r = good.first()
    assert good.count() == 1 and r["is_orphan_offer"] and r["is_pre_install"]
    assert set(r["dq_flags"]) == {"pre_install", "orphan_offer"}


def test_unknown_event_name_rejected():
    raw = df(EV, [("e1", "u1", "2026-05-01 10:00:00", "purchase", None, "2026-05-01 11:00:00", ING)])
    good, rej = clean_events(raw, *_dims())
    assert good.count() == 0 and rej.count() == 1


def test_malformed_timestamp_and_null_key_rejected_not_raised():
    """Needs spark.sql.ansi.enabled=false (step 1): under ANSI the cast raises and the run dies."""
    raw = df(EV, [("e1", "u1", "yesterday", "app_open", None, "2026-05-01 11:00:00", ING),
                  ("e2", None, "2026-05-01 10:00:00", "app_open", None, "2026-05-01 11:00:00", ING)])
    good, rej = clean_events(raw, *_dims())
    assert good.count() == 0 and rej.count() == 2


def test_offers_latest_snapshot_wins_and_bad_payout_rejected():
    good, rej = clean_offers(df(OF, [("of_1", "rpg", "cpe", "1.00", ING),
                                     ("of_1", "RPG ", "CPE", "1.50", datetime(2026, 5, 2, 13, 0, 0)),
                                     ("of_2", "rpg", "cpe", "abc", ING),          # unparseable
                                     ("of_3", "rpg", "cpe", "-1", ING)]))         # negative
    rows = {r["offer_id"]: r for r in good.collect()}
    assert set(rows) == {"of_1"} and rows["of_1"]["payout_eur"] == Decimal("1.50") and rows["of_1"]["offer_category"] == "rpg"
    assert {r["reject_key"] for r in rej.collect()} == {"of_2", "of_3"}


def test_user_profile_latest_row_and_payer_parsing():
    raw = df("user_id string, events_lifetime string, last_seen_ts string, revenue_30d_eur string, is_payer string, _ingested_at timestamp",
             [("u1", "5", "2026-05-01 10:00:00", "1.00", "no", ING),
              ("u1", "7", "2026-05-02 10:00:00", "2.00", "True", datetime(2026, 5, 2, 13, 0, 0))])
    r = clean_user_profile(raw).collect()
    assert len(r) == 1 and r[0]["events_lifetime"] == 7 and r[0]["is_payer"] is True


def test_dq_rule_catches_duplicate_event_id():
    ev = df("event_id string, event_date date", [("e1", date(2026, 5, 1))] * 2)
    rule = [Rule("dup", lambda c: dupes(c.t("silver.events"), "event_id"))]
    try:
        run_rules(rule, "test", read={"silver.events": ev}.__getitem__, log=False)
        raise RuntimeError("duplicate not caught")
    except AssertionError:
        pass


if RUN_TESTS:
    run_tests([test_dedup_first_arrival_wins, test_late_flag_uses_next_day_0015_not_raw_lag,
               test_country_normalised_and_bad_rejected, test_first_install_wins,
               test_orphan_and_pre_install_are_flagged_not_dropped, test_unknown_event_name_rejected,
               test_malformed_timestamp_and_null_key_rejected_not_raised,
               test_offers_latest_snapshot_wins_and_bad_payout_rejected,
               test_user_profile_latest_row_and_payer_parsing, test_dq_rule_catches_duplicate_event_id])
else:
    print("tests skipped (run_tests=false)")

# COMMAND ----------

# DBTITLE 1,4 · Landing → Bronze (Batch)
def ingest_source(name: str) -> int:
    """Batch read all CSV files from the landing zone into bronze.<name>.
    Overwrites the table each run — idempotent, no checkpoints, no streaming."""
    table = tbl(f"bronze.{name}")
    batch = (spark.read.format("csv")
             .option("header", "true")
             .option("inferSchema", "false")           # everything STRING; silver owns typing
             .load(f"{CFG.landing}/{name}/")
             .withColumn("_source_file", F.col("_metadata.file_path"))
             .withColumn("_ingested_at", F.current_timestamp()))
    count = batch.count()
    (batch.write.mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(table))
    return count


if RUN_INGEST:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CFG.bronze}")
    print("bronze rows loaded:", {s: ingest_source(s) for s in CFG.sources})
else:
    print("ingest skipped (run_ingest=false)")

# COMMAND ----------

# DBTITLE 0,5 · DQ gate: bronze contract
REQUIRED_COLUMNS = {
    "events": ("event_id", "user_id", "event_ts", "event_name", "offer_id", "ingest_ts"),
    "installs": ("user_id", "install_ts", "country", "platform", "media_source", "device_model", "campaign_id"),
    "offers": ("offer_id", "offer_category", "payout_type", "payout_eur"),
    "user_profile": ("user_id", "events_lifetime", "last_seen_ts", "revenue_30d_eur", "is_payer"),
}


def _bronze_rules(src):
    t = f"bronze.{src}"
    return [
        Rule(f"{t}: required columns present", lambda c: len(set(REQUIRED_COLUMNS[src]) - set(c.t(t).columns))),
        Rule(f"{t}: not empty", lambda c: int(c.t(t).isEmpty())),
        # Auto Loader parks values that do not fit the tracked schema in _rescued_data instead of failing
        Rule(f"{t}: no rescued data",
             lambda c: bad(c.t(t), "_rescued_data IS NOT NULL") if "_rescued_data" in c.t(t).columns else 0, "warn"),
    ]


BRONZE_RULES = [r for s in CFG.sources for r in _bronze_rules(s)]
run_rules(BRONZE_RULES, layer="bronze")

# COMMAND ----------

# DBTITLE 0,6 · Bronze → Silver
def _watermark(name):
    t = tbl("silver.watermark")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t} (table_name STRING, last_ingested_at TIMESTAMP, run_ts TIMESTAMP)")
    return spark.table(t).filter(F.col("table_name") == name).agg(F.max("last_ingested_at")).first()[0]


def _advance(name, ts):
    (spark.createDataFrame([(name, ts)], "table_name STRING, last_ingested_at TIMESTAMP")
        .withColumn("run_ts", F.current_timestamp())
        .write.mode("append").saveAsTable(tbl("silver.watermark")))


def _overwrite(frame, short):
    """Dimensions: whole snapshot, overwrite. Small, so no partitioning."""
    (writer(frame.withColumn("_loaded_at", F.current_timestamp()), short)
        .mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl(short)))


def load_silver(full_rebuild: bool) -> set:
    """Returns the set of event_dates touched, so gold rebuilds only those."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CFG.silver}")
    b = lambda n: spark.table(tbl(f"bronze.{n}"))

    # dimensions
    offers, rj_o = clean_offers(b("offers"))
    installs, rj_i = clean_installs(b("installs"))
    _overwrite(offers, "silver.offers")
    _overwrite(installs, "silver.installs")
    _overwrite(clean_user_profile(b("user_profile")), "silver.user_profile")
    offers, installs = spark.table(tbl("silver.offers")), spark.table(tbl("silver.installs"))

    # events: incremental by _ingested_at. Fix the upper bound BEFORE processing, so rows landing in bronze
    # mid-run are picked up next run instead of being skipped by a watermark computed after the MERGE.
    wm = None if full_rebuild else _watermark("events")
    raw = b("events") if wm is None else b("events").filter(F.col("_ingested_at") > wm)
    hi = raw.agg(F.max("_ingested_at")).first()[0]
    if hi is None:
        print("silver: no new bronze rows since watermark", wm)
        return set()
    raw = raw.filter(F.col("_ingested_at") <= hi)

    events, rj_e = clean_events(raw, offers, installs)
    events = events.withColumn("_loaded_at", F.current_timestamp())
    ev_t = tbl("silver.events")
    if full_rebuild:
        prepare_full_rebuild("silver.events")
    if full_rebuild or not spark.catalog.tableExists(ev_t):
        writer(events, "silver.events").mode("overwrite").option("overwriteSchema", "true").saveAsTable(ev_t)
    else:
        (DeltaTable.forName(spark, ev_t).alias("t")
            .merge(events.alias("s"), "t.event_id = s.event_id")
            .whenNotMatchedInsertAll()                     # a retry seen in a later batch is a no-op
            .execute())

    rejects = rj_o.unionByName(rj_i).unionByName(rj_e)
    rj_t = tbl("silver.rejects")
    if full_rebuild or not spark.catalog.tableExists(rj_t):
        _overwrite(rejects, "silver.rejects")
    else:
        if "_ingested_at" not in spark.table(rj_t).columns:
            spark.sql(f"ALTER TABLE {rj_t} ADD COLUMNS (_ingested_at TIMESTAMP)")
        rejects.withColumn("_loaded_at", F.current_timestamp()).write.mode("append").saveAsTable(rj_t)

    touched = {r[0] for r in events.select("event_date").distinct().collect()}
    _advance("events", hi)
    return touched


warn_layout_drift("silver.events")
touched = load_silver(FULL)
# same window gold will rebuild: touched dates plus the late-arrival lookback. None = whole table
lo, hi = (None, None) if FULL or not touched else (min(touched) - timedelta(days=CFG.lookback_days), max(touched))
print(f"touched {len(touched)} event_date(s):", sorted(map(str, touched)), "| window:", lo, hi)

# COMMAND ----------

# DBTITLE 1,DQ window setup (standalone)
# Derive lo/hi from existing silver.events so the DQ gate can run standalone
from datetime import timedelta as _td
_ev_d = spark.table(tbl("silver.events")).select("event_date").distinct().collect()
_touched = {r[0] for r in _ev_d}
if _touched:
    lo = min(_touched) - _td(days=CFG.lookback_days)
    hi = max(_touched)
else:
    lo, hi = None, None
print(f"DQ window: {lo} to {hi} ({len(_touched)} dates)")

# COMMAND ----------

# DBTITLE 0,7 · DQ gate: silver expectations
_steps = ", ".join(f"'{s}'" for s in CFG.steps)
_ev = lambda c: c.win(c.t("silver.events"), "event_date")
_in = lambda c: c.t("silver.installs")

SILVER_RULES = [
    Rule("silver.events: event_id unique", lambda c: dupes(_ev(c), "event_id")),
    Rule("silver.events: no null keys/ts",
         lambda c: bad(_ev(c), "event_id IS NULL OR user_id IS NULL OR event_ts IS NULL OR ingest_ts IS NULL")),
    Rule("silver.events: event_name known", lambda c: bad(_ev(c), f"event_name NOT IN ({_steps})")),
    Rule("silver.events: event_date = date(event_ts)", lambda c: bad(_ev(c), "event_date <> to_date(event_ts)")),
    Rule("silver.events: every event has an install", lambda c: _ev(c).join(_in(c), "user_id", "left_anti").count()),
    Rule("silver.events: lag_hours >= 0", lambda c: bad(_ev(c), "lag_hours < 0"), "warn"),
    Rule("silver.installs: user_id unique", lambda c: dupes(_in(c), "user_id")),
    Rule("silver.installs: country ISO-2", lambda c: bad(_in(c), "NOT country RLIKE '^[A-Z]{2}$'")),
    Rule("silver.installs: platform enum", lambda c: bad(_in(c), "platform NOT IN ('android', 'ios')")),
    Rule("silver.offers: offer_id unique", lambda c: dupes(c.t("silver.offers"), "offer_id")),
    Rule("silver.offers: payout > 0", lambda c: bad(c.t("silver.offers"), "payout_eur IS NULL OR payout_eur <= 0")),
    Rule("silver.user_profile: user_id unique", lambda c: dupes(c.t("silver.user_profile"), "user_id")),
    # ── Warning-severity observational rules (flagged, not dropped) ──
    Rule("silver.events: late arrivals (is_late)", lambda c: bad(_ev(c), "is_late = true"), "warn"),
    Rule("silver.events: pre-install events", lambda c: bad(_ev(c), "is_pre_install = true"), "warn"),
    Rule("silver.events: orphan offer_id", lambda c: bad(_ev(c), "is_orphan_offer = true"), "warn"),
    Rule("silver.installs: users with no events", lambda c: _in(c).join(_ev(c).select("user_id").distinct(), "user_id", "left_anti").count(), "warn"),
]

dq_lo, dq_hi = (None, None) if DQ_FULL else (lo, hi)
run_rules(SILVER_RULES, layer="silver", lo=dq_lo, hi=dq_hi)

# COMMAND ----------

# DBTITLE 0,8 · DQ observability: metrics (how many) + issue register (which rows)
def dq_metrics() -> None:
    """Appended per run: late-arrival profile, pre-install, orphan offers, double payouts, rejects."""
    ev = spark.table(tbl("silver.events"))
    m = lambda c, n, v, d=None: (c, n, float(v) if v is not None else None, d)
    rows = []
    a = ev.agg(F.avg(F.col("is_late").cast("int")), F.max("lag_hours"), F.expr("percentile(lag_hours, 0.5)"),
               F.expr("percentile(lag_hours, 0.95)"), F.sum((F.col("lag_hours") < 0).cast("int"))).first()
    rows += [m("late_arrival", "share_missed_0015_job", a[0]), m("late_arrival", "max_lag_hours", a[1]),
             m("late_arrival", "p50_lag_hours", a[2]), m("late_arrival", "p95_lag_hours", a[3]),
             m("late_arrival", "negative_lag_rows", a[4])]
    rows += [m("late_arrival", "rows_by_lag_day", r["count"], f"lag_day={r['d']}")
             for r in ev.groupBy(F.floor(F.col("lag_hours") / 24).alias("d")).count().collect()]
    pre = ev.filter("is_pre_install")
    rows += [m("pre_install", "events", pre.count()), m("pre_install", "users", pre.select("user_id").distinct().count())]
    orph = ev.filter("is_orphan_offer")
    rows += [m("orphan_offers", "distinct_offer_ids", orph.select("offer_id").distinct().count()),
             m("orphan_offers", "events", orph.count()),
             m("orphan_offers", "reward_paid_events", orph.filter("event_name = 'reward_paid'").count()),
             m("orphan_offers", "offer_ids", None, ",".join(sorted(r[0] for r in orph.select("offer_id").distinct().collect())))]
    rp = ev.filter("event_name = 'reward_paid'")
    rows += [m("funnel", "double_payout_pairs", rp.groupBy("user_id", "offer_id").count().filter("count > 1").count()),
             m("funnel", "reward_pairs", rp.count())]
    rows += [m("rejects", r["reject_reason"], r["count"], r["source"])
             for r in spark.table(tbl("silver.rejects")).groupBy("source", "reject_reason").count().collect()]
    (spark.createDataFrame(rows, "check STRING, metric STRING, value DOUBLE, detail STRING")
        .withColumn("run_ts", F.current_timestamp())
        .withColumn("run_id", F.date_format("run_ts", "yyyyMMdd_HHmmss"))
        .select("run_id", "run_ts", "check", "metric", "detail", "value")
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tbl("silver.dq_metrics")))


def _issue(frame, name, source, key_col, detail_col=None):
    col = lambda c, t: F.col(c) if c in frame.columns else F.lit(None).cast(t)
    return frame.select(F.lit(name).alias("issue"), F.lit(source).alias("source_table"), F.lit(key_col).alias("key_col"),
                        F.col(key_col).cast("string").alias("key_value"),
                        col("user_id", "string").alias("user_id"), col("offer_id", "string").alias("offer_id"),
                        col("event_date", "date").alias("event_date"),
                        (F.col(detail_col) if detail_col else F.lit(None)).cast("string").alias("detail"))


def dq_issues(lo=None, hi=None) -> None:
    """MERGEd with open/resolved status: an issue that disappears is closed, not deleted."""
    t = tbl("silver.dq_issues")
    # Recreate if schema is from an older version (missing key_value column)
    if spark.catalog.tableExists(t) and "key_value" not in spark.table(t).columns:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
    ev, ins, rej = spark.table(tbl("silver.events")), spark.table(tbl("silver.installs")), spark.table(tbl("silver.rejects"))
    raw_ins = spark.table(tbl("bronze.installs")).select(F.trim("user_id").alias("user_id"), F.col("country").alias("raw_country"))
    parts = [
        _issue(rej.filter("reject_reason = 'duplicate_event_id'").withColumn("event_id", F.col("reject_key"))
               .withColumn("user_id", F.get_json_object("raw_record", "$.user_id"))
               .withColumn("offer_id", F.get_json_object("raw_record", "$.offer_id"))
               .withColumn("event_date", F.to_date(F.get_json_object("raw_record", "$.event_ts"))),
               "duplicate_event_id", "bronze.events", "event_id"),
        _issue(ev.filter("is_late and lag_hours > 24").withColumn("d", F.round("lag_hours", 1)),
               "late_arrival_over_1d", "bronze.events", "event_id", "d"),
        _issue(ev.filter("is_pre_install").join(ins.select("user_id", "install_ts", "media_source"), "user_id")
               .withColumn("d", F.concat_ws("|", "media_source",
                           F.round((F.unix_timestamp("install_ts") - F.unix_timestamp("event_ts")) / 3600, 1))),
               "pre_install", "bronze.events", "event_id", "d"),
        _issue(ev.filter("is_orphan_offer").withColumn("d", F.col("event_name")), "orphan_offer", "bronze.events", "event_id", "d"),
        _issue(ins.join(raw_ins, "user_id").filter("raw_country <> country").withColumn("d", F.col("raw_country")),
               "country_normalised", "bronze.installs", "user_id", "d"),
        _issue(rej.filter("reject_reason <> 'duplicate_event_id'").withColumn("k", F.col("reject_key")),
               "rejected", "bronze", "k", "reject_reason"),
    ]
    cur = reduce(lambda a, b: a.unionByName(b), parts)
    if lo is not None:
        cur = cur.filter(F.col("event_date").isNull() | F.col("event_date").between(lo, hi))
    cur = cur.withColumn("run_id", F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss")).dropDuplicates(["issue", "key_value"])

    if not spark.catalog.tableExists(t):
        (cur.withColumn("first_seen_run", F.col("run_id")).withColumn("last_seen_run", F.col("run_id"))
                   .withColumn("status", F.lit("open"))
            .write.mode("overwrite").saveAsTable(t))
        return
    win = "TRUE" if lo is None else f"(t.event_date IS NULL OR t.event_date BETWEEN '{lo}' AND '{hi}')"
    existing = spark.table(t).columns
    for c, typ in [("first_seen_run", "STRING"), ("last_seen_run", "STRING"), ("status", "STRING")]:
        if c not in existing:
            spark.sql(f"ALTER TABLE {t} ADD COLUMNS ({c} {typ})")
    (DeltaTable.forName(spark, t).alias("t")
        .merge(cur.alias("s"), "t.issue = s.issue AND t.key_value = s.key_value")
        .whenMatchedUpdate(set={"last_seen_run": "s.run_id", "detail": "s.detail", "status": F.lit("open")})
        .whenNotMatchedInsert(values={**{c: f"s.{c}" for c in cur.columns},
                                      "first_seen_run": "s.run_id", "last_seen_run": "s.run_id", "status": F.lit("open")})
        .whenNotMatchedBySourceUpdate(condition=f"t.status = 'open' AND {win}", set={"status": F.lit("resolved")})
        .execute())


dq_metrics()
dq_issues(lo, hi)
display(spark.table(tbl("silver.dq_issues")).filter("status = 'open'")
        .groupBy("issue", "source_table").count().orderBy("issue"))

# COMMAND ----------

# DBTITLE 0,9 · Maintenance: compact what was written
if OPTIMIZE and (touched or FULL):
    optimize("silver.events", "event_date", lo, hi)
else:
    print("optimize skipped")

# COMMAND ----------

# DBTITLE 0,10 · Hand-off to 02_silver_to_gold + run summary
dbutils.jobs.taskValues.set("touched", json.dumps(sorted(map(str, touched))))
display(spark.table(tbl("silver.dq_results"))
        .filter("run_ts >= current_timestamp() - INTERVAL 6 HOURS AND layer IN ('bronze', 'silver')")
        .orderBy(F.desc("run_ts"), "layer", "rule"))