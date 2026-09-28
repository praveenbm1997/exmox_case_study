# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Silver → Gold
# MAGIC
# MAGIC Builds `gold.daily_country_platform` at the grain **date × country × platform**.
# MAGIC
# MAGIC | # | Step | Stops the run? |
# MAGIC |---|---|---|
# MAGIC | 1 | Spark tuning (same settings as 01) | no |
# MAGIC | 2 | Gold transformations (pure functions) | – |
# MAGIC | 3 | Unit tests of those transformations on hand-built silver rows | yes, before gold is touched |
# MAGIC | 4 | DQ gate on inputs: silver tables not empty | yes |
# MAGIC | 5 | Rebuild: `replaceWhere` on touched dates + lookback, or full overwrite | yes |
# MAGIC | 6 | DQ gate on gold: grain, invariants, totals reconciled to silver, table == recompute | on `error` rules |
# MAGIC | 7 | `OPTIMIZE`: window + Z-order when partitioned, incremental when liquid | yes |
# MAGIC | 8 | Hand-off: rebuilt `window` as a task value, preview | – |
# MAGIC
# MAGIC **Touched dates** come from the upstream job task (`upstream` = task key of `01_bronze_to_silver`).
# MAGIC To run by hand or backfill, type ISO dates in `dates`, e.g. `2026-05-01,2026-05-03`: the window is
# MAGIC `[min - lookback, max]`. A date is **settled** once older than the lookback; final reporting filters `is_settled`.
# MAGIC
# MAGIC **Layout**: `liquid` = `CLUSTER BY (date, country)`; `partition` = `PARTITIONED BY (date)` + `ZORDER BY (country, platform)`.

# COMMAND ----------

# DBTITLE 0,Parameters
dbutils.widgets.dropdown("full_rebuild", "false", ["false", "true"], "Full rebuild")
dbutils.widgets.dropdown("layout", "liquid", ["liquid", "partition"], "Table layout")
dbutils.widgets.dropdown("dq_scope", "window", ["window", "full"], "DQ scope")
dbutils.widgets.text("upstream", "bronze_to_silver", "Upstream task key")
dbutils.widgets.text("dates", "", "Override touched dates (comma-separated)")
dbutils.widgets.dropdown("run_tests", "true", ["true", "false"], "Gate on unit tests")
dbutils.widgets.dropdown("optimize", "true", ["true", "false"], "OPTIMIZE after write")

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

# DBTITLE 0,1 · Spark tuning + touched dates
FULL = dbutils.widgets.get("full_rebuild") == "true"
LAYOUT = dbutils.widgets.get("layout")
DQ_FULL = dbutils.widgets.get("dq_scope") == "full"
UP = dbutils.widgets.get("upstream")
DATES = dbutils.widgets.get("dates").strip()
RUN_TESTS = dbutils.widgets.get("run_tests") == "true"
OPTIMIZE = dbutils.widgets.get("optimize") == "true"

if DATES:
    touched, source = {date.fromisoformat(d.strip()) for d in DATES.split(",") if d.strip()}, "widget 'dates'"
else:
    touched = {date.fromisoformat(d) for d in json.loads(
        dbutils.jobs.taskValues.get(UP, "touched", default="[]", debugValue="[]"))}
    source = f"task '{UP}'"
print(f"full_rebuild={FULL} layout={LAYOUT} | touched from {source}:", sorted(map(str, touched)))

display(configure_spark())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Transformations
# MAGIC Country and platform come from the **install**; events do not carry them.
# MAGIC `build_gold_window` does **not** pre-filter installs to the window: an event inside the window from a
# MAGIC user who installed before it still needs that install for country/platform.

# COMMAND ----------

# DBTITLE 0,Gold transformations
GOLD_TABLE = "gold.daily_country_platform"
GOLD_COUNTS = ["installs", "events_total", *[f"{s}_users" for s in CFG.steps],
               "reward_payouts", "rewards_unpriced", "late_events", "events_pre_install"]


def build_gold(events: DataFrame, installs: DataFrame, offers: DataFrame) -> DataFrame:
    enriched = (events
                .join(installs.select("user_id", "country", "platform"), "user_id", "inner")
                .join(F.broadcast(offers.select("offer_id", "payout_eur")), "offer_id", "left")   # catalog is tiny
                .withColumn("is_reward", F.col("event_name") == "reward_paid"))

    def users_at(step):
        return F.countDistinct(F.when(F.col("event_name") == step, F.col("user_id"))).alias(f"{step}_users")

    ev_m = (enriched.groupBy(F.col("event_date").alias("date"), "country", "platform")
            .agg(F.count("*").alias("events_total"),
                 *[users_at(s) for s in CFG.steps],
                 F.sum(F.col("is_reward").cast("int")).alias("reward_payouts"),
                 F.sum(F.when(F.col("is_reward"), F.col("payout_eur"))).cast(DecimalType(18, 2)).alias("reward_cost_eur"),
                 F.sum((F.col("is_reward") & F.col("payout_eur").isNull()).cast("int")).alias("rewards_unpriced"),
                 F.sum(F.col("is_late").cast("int")).alias("late_events"),
                 F.sum(F.col("is_pre_install").cast("int")).alias("events_pre_install")))
    in_m = installs.groupBy(F.col("install_date").alias("date"), "country", "platform").agg(F.count("*").alias("installs"))

    return (in_m.join(ev_m, ["date", "country", "platform"], "full_outer")      # keep install-only and event-only days
            .fillna(0, subset=GOLD_COUNTS).fillna({"reward_cost_eur": 0})
            .withColumn("reward_cost_eur", F.col("reward_cost_eur").cast(DecimalType(18, 2)))
            .select("date", "country", "platform", "installs", "events_total",
                    *[f"{s}_users" for s in CFG.steps],
                    "reward_payouts", "reward_cost_eur", "rewards_unpriced", "late_events", "events_pre_install"))


def build_gold_window(events: DataFrame, installs: DataFrame, offers: DataFrame, lo, hi) -> DataFrame:
    """Gold for dates [lo, hi] only (replaceWhere). Install counts are windowed by the final date filter."""
    ev = events.filter(F.col("event_date").between(lo, hi))
    return build_gold(ev, installs, offers).filter(F.col("date").between(lo, hi))


def finish(frame: DataFrame) -> DataFrame:
    return (frame.withColumn("is_settled", F.col("date") <= F.date_sub(F.current_date(), CFG.lookback_days))
                 .withColumn("_loaded_at", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Transformation tests

# COMMAND ----------

# DBTITLE 0,Unit tests (hand-built silver rows, no tables)
S_EV = "user_id string, offer_id string, event_name string, event_date date, is_late boolean, is_pre_install boolean, is_orphan_offer boolean"
S_IN = "user_id string, country string, platform string, install_date date"
S_OF = "offer_id string, payout_eur decimal(18,2)"
D1, D3 = date(2026, 5, 1), date(2026, 5, 3)


def _sev(*rows):  # (user, offer, name, date[, late, pre, orphan])
    return df(S_EV, [(*r, *(False,) * (7 - len(r))) for r in rows])


def test_cost_and_unpriced():
    ins = df(S_IN, [("u1", "DE", "ios", D1)])
    off = df(S_OF, [("of_1", Decimal("2.50"))])
    g = build_gold(_sev(("u1", "of_1", "reward_paid", D1), ("u1", "of_9", "reward_paid", D1, False, False, True)), ins, off).first()
    assert g["reward_payouts"] == 2 and g["reward_cost_eur"] == Decimal("2.50") and g["rewards_unpriced"] == 1
    assert g["reward_paid_users"] == 1 and g["installs"] == 1


def test_full_outer_keeps_install_only_days():
    g = build_gold(_sev(), df(S_IN, [("u1", "DE", "ios", D3)]), df(S_OF, [])).first()
    assert g["date"] == D3 and g["installs"] == 1 and g["events_total"] == 0 and g["reward_cost_eur"] == 0


def test_grain_unique_and_users_distinct():
    ins = df(S_IN, [("u1", "DE", "ios", D1), ("u2", "DE", "ios", D1), ("u3", "DE", "android", D1)])
    ev = _sev(("u1", None, "app_open", D1), ("u2", None, "app_open", D1), ("u2", None, "app_open", D1), ("u3", None, "app_open", D1))
    rows = build_gold(ev, ins, df(S_OF, [])).collect()
    g = {r["platform"]: r for r in rows}
    assert len(rows) == len(g) == 2
    assert g["ios"]["installs"] == 2 and g["ios"]["events_total"] == 3 and g["ios"]["app_open_users"] == 2


def test_flags_are_summed():
    ins = df(S_IN, [("u1", "DE", "ios", D1)])
    g = build_gold(_sev(("u1", None, "app_open", D1, True, False), ("u1", None, "app_open", D1, True, True),
                       ("u1", None, "app_open", D1)), ins, df(S_OF, [])).first()
    assert g["late_events"] == 2 and g["events_pre_install"] == 1 and g["events_total"] == 3


def test_window_keeps_users_installed_before_window():
    """Regression: pre-filtering installs to the window made the inner join drop these events."""
    ins = df(S_IN, [("u1", "DE", "ios", date(2026, 4, 20)), ("u2", "FR", "ios", D1)])
    ev = _sev(("u1", None, "app_open", D1), ("u1", None, "app_open", date(2026, 4, 21)))
    rows = {r["country"]: r for r in build_gold_window(ev, ins, df(S_OF, []), date(2026, 4, 28), date(2026, 5, 2)).collect()}
    assert {r["date"] for r in rows.values()} == {D1}                             # nothing outside the window
    assert rows["DE"]["events_total"] == 1 and rows["DE"]["installs"] == 0        # event kept, old install not recounted
    assert rows["FR"]["installs"] == 1


def test_is_settled_after_lookback():
    frame = df("date date", [(date.today() - timedelta(days=CFG.lookback_days),), (date.today(),)])
    got = {r["date"]: r["is_settled"] for r in finish(frame).collect()}
    assert got == {date.today() - timedelta(days=CFG.lookback_days): True, date.today(): False}


if RUN_TESTS:
    run_tests([test_cost_and_unpriced, test_full_outer_keeps_install_only_days, test_grain_unique_and_users_distinct,
               test_flags_are_summed, test_window_keeps_users_installed_before_window, test_is_settled_after_lookback])
else:
    print("tests skipped (run_tests=false)")

# COMMAND ----------

# DBTITLE 0,4 · DQ gate: silver inputs
INPUT_RULES = [Rule(f"silver.{n}: not empty", (lambda n: lambda c: int(c.t(f"silver.{n}").isEmpty()))(n))
               for n in ("events", "installs", "offers")]
run_rules(INPUT_RULES, layer="gold_inputs")

# COMMAND ----------

# DBTITLE 0,5 · Silver → Gold
def load_gold(touched: set, full_rebuild: bool):
    """Rebuild only the touched dates plus the lookback window, via replaceWhere. Returns (lo, hi) rebuilt;
    (None, None) = full rebuild or nothing to do."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CFG.gold}")
    s = lambda n: spark.table(tbl(f"silver.{n}"))
    t = tbl(GOLD_TABLE)

    if full_rebuild:
        prepare_full_rebuild(GOLD_TABLE)
    if full_rebuild or not spark.catalog.tableExists(t):
        g = finish(build_gold(s("events"), s("installs"), s("offers")))
        writer(g, GOLD_TABLE).mode("overwrite").option("overwriteSchema", "true").saveAsTable(t)
        return None, None
    if not touched:
        return None, None

    lo, hi = min(touched) - timedelta(days=CFG.lookback_days), max(touched)
    g = finish(build_gold_window(s("events"), s("installs"), s("offers"), lo, hi))
    for c, typ in [("is_settled", "BOOLEAN"), ("_loaded_at", "TIMESTAMP")]:
        if c not in spark.table(t).columns:
            spark.sql(f"ALTER TABLE {t} ADD COLUMNS ({c} {typ})")
    (writer(g, GOLD_TABLE, create=False).mode("overwrite")
        .option("replaceWhere", f"date >= '{lo}' AND date <= '{hi}'")   # same lo/hi as the frame, by construction
        .saveAsTable(t))
    return lo, hi


warn_layout_drift(GOLD_TABLE)
existed = spark.catalog.tableExists(tbl(GOLD_TABLE))
lo, hi = load_gold(touched, FULL)
rebuilt = FULL or not existed or lo is not None
print("gold:", ("full rebuild" if lo is None else f"window {lo} .. {hi}") if rebuilt else "nothing to do")

# COMMAND ----------

# DBTITLE 0,6 · DQ gate: gold expectations + reconciliation to silver
_g = lambda c: c.win(c.t(GOLD_TABLE), "date")
_ev = lambda c: c.win(c.t("silver.events"), "event_date")
_rw = lambda c: _ev(c).filter("event_name = 'reward_paid'")


def _recompute_diff(c):
    """Table content == a fresh recompute from silver (catches a partial/stale replaceWhere)."""
    s = lambda n: c.t(f"silver.{n}")
    fresh = (build_gold(s("events"), s("installs"), s("offers")) if c.lo is None
             else build_gold_window(s("events"), s("installs"), s("offers"), c.lo, c.hi))
    stored = _g(c).drop("is_settled", "_loaded_at")
    return stored.exceptAll(fresh).count() + fresh.exceptAll(stored).count()


GOLD_RULES = [
    Rule("gold: grain unique", lambda c: dupes(_g(c), "date", "country", "platform")),
    Rule("gold: no null grain", lambda c: bad(_g(c), "date IS NULL OR country IS NULL OR platform IS NULL")),
    Rule("gold: no negatives", lambda c: bad(_g(c), " OR ".join(f"{x} < 0" for x in [*GOLD_COUNTS, "reward_cost_eur"]))),
    Rule("gold: unpriced <= payouts", lambda c: bad(_g(c), "rewards_unpriced > reward_payouts")),
    Rule("gold: reward_paid_users <= payouts", lambda c: bad(_g(c), "reward_paid_users > reward_payouts")),
    Rule("gold: flagged events <= events_total",
         lambda c: bad(_g(c), "late_events > events_total OR events_pre_install > events_total")),
    Rule("gold: step users <= events_total",
         lambda c: bad(_g(c), " OR ".join(f"{s}_users > events_total" for s in CFG.steps))),
    # every total recomputed from silver by an independent path
    Rule("gold: installs == silver.installs",
         lambda c: gap(total(_g(c), "installs"), c.win(c.t("silver.installs"), "install_date").count())),
    Rule("gold: events == silver.events", lambda c: gap(total(_g(c), "events_total"), _ev(c).count())),
    Rule("gold: payouts == reward_paid rows", lambda c: gap(total(_g(c), "reward_payouts"), _rw(c).count())),
    Rule("gold: cost == independent join",
         lambda c: gap(total(_g(c), "reward_cost_eur"), total(_rw(c).join(c.t("silver.offers"), "offer_id"), "payout_eur"))),
    Rule("gold: unpriced == rewards on orphans",
         lambda c: gap(total(_g(c), "rewards_unpriced"), _rw(c).filter("is_orphan_offer").count())),
    Rule("gold: late == silver flag", lambda c: gap(total(_g(c), "late_events"), _ev(c).filter("is_late").count())),
    Rule("gold: pre_install == silver flag",
         lambda c: gap(total(_g(c), "events_pre_install"), _ev(c).filter("is_pre_install").count())),
    Rule("gold: table == recompute from silver", _recompute_diff),
]

if rebuilt or DQ_FULL:
    dq_lo, dq_hi = (None, None) if DQ_FULL else (lo, hi)
    run_rules(GOLD_RULES, layer="gold", lo=dq_lo, hi=dq_hi)
else:
    print("no dates rebuilt; gold DQ skipped (set dq_scope=full to audit the whole table)")

# COMMAND ----------

# DBTITLE 0,7 · Maintenance: compact what was written
if OPTIMIZE and rebuilt:
    optimize(GOLD_TABLE, "date", lo, hi)
else:
    print("optimize skipped")

# COMMAND ----------

# DBTITLE 0,8 · Hand-off + preview
dbutils.jobs.taskValues.set("window", json.dumps([str(lo), str(hi)] if lo else []))
g = spark.table(tbl(GOLD_TABLE))
display((g if lo is None else g.filter(F.col("date").between(lo, hi))).orderBy("date", "country", "platform"))

# COMMAND ----------

# DBTITLE 0,Run summary
display(spark.table(tbl("silver.dq_results"))
        .filter("run_ts >= current_timestamp() - INTERVAL 6 HOURS AND layer IN ('gold_inputs', 'gold')")
        .orderBy(F.desc("run_ts"), "layer", "rule"))