# Databricks notebook source
# MAGIC %md
# MAGIC # Data exploration · exmox
# MAGIC
# MAGIC Profiles the four raw sources (`events`, `installs`, `offers`, `user_profile`) and, if they exist, the
# MAGIC pipeline outputs. Every section ends by recording its numbers in a **findings** table (last cell), each
# MAGIC mapped to the pipeline rule that handles it, so the numbers behind the rules in `01_bronze_to_silver` / `02_silver_to_gold`
# MAGIC are reproducible.
# MAGIC
# MAGIC | § | Question |
# MAGIC |---|---|
# MAGIC | 1 | What landed: rows, files, columns |
# MAGIC | 2 | Column profile: nulls, blanks, untrimmed values, cardinality, top values |
# MAGIC | 3 | Events: keys & duplicates, event names, timestamps, arrival lag, late share, funnel, offer ids |
# MAGIC | 4 | Installs: duplicate users, country spellings, platforms, device models, campaigns |
# MAGIC | 5 | Offers: duplicates, payouts, categories |
# MAGIC | 6 | User profile: does it agree with events? |
# MAGIC | 7 | Cross-source: events without installs, events before install, orphan offers |
# MAGIC | 8 | Physical design: volume per day, skew, broadcast size → partition vs cluster, AQE |
# MAGIC | 9 | Pipeline outputs: rejects, DQ results, open issues, gold, table health |
# MAGIC | 10 | Findings summary |
# MAGIC
# MAGIC Read-only: this notebook writes nothing.

# COMMAND ----------

# DBTITLE 0,Parameters
dbutils.widgets.dropdown("source", "landing", ["landing", "bronze"], "Read raw data from")
dbutils.widgets.text("catalog", "exmox", "Catalog")
dbutils.widgets.text("landing", "/Volumes/exmox/bronze/landing", "Landing path")

SOURCE = dbutils.widgets.get("source")
CATALOG = dbutils.widgets.get("catalog")
LANDING = dbutils.widgets.get("landing").rstrip("/")

# COMMAND ----------

# DBTITLE 0,Setup
from functools import reduce
from pyspark.sql import DataFrame, functions as F, Window as W

# same parsing semantics as the pipeline: a bad cast is NULL (and counted here), not an exception
for k, v in {"spark.sql.ansi.enabled": "false", "spark.sql.session.timeZone": "UTC"}.items():
    try:
        spark.conf.set(k, v)
    except Exception as e:
        print(f"could not set {k}: {type(e).__name__}")

STEPS = ["app_open", "offer_view", "offer_start", "goal_reached", "reward_paid"]
JOB_CUTOFF_MIN = 15          # daily job at 00:15
LOOKBACK_DAYS = 6            # pipeline's rebuild window
SOURCES = ["events", "installs", "offers", "user_profile"]

findings = []                # (area, finding, value, handled_by)


def note(area, finding, value, handled_by=""):
    findings.append((area, finding, None if value is None else str(value), handled_by))
    print(f"[{area}] {finding}: {value}")


def ts(c):  return F.to_timestamp(F.trim(F.col(c)))
def dec(c): return F.trim(F.col(c)).cast("decimal(18,2)")
def pct(a, b): return None if not b else round(100.0 * a / b, 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · What landed

# COMMAND ----------

# DBTITLE 0,Load raw sources (all columns STRING)
def read_raw(name: str) -> DataFrame:
    if SOURCE == "bronze":
        return spark.table(f"{CATALOG}.bronze.{name}")
    return (spark.read.option("header", "true").csv(f"{LANDING}/{name}/")      # no inferSchema: everything STRING
            .withColumn("_source_file", F.col("_metadata.file_path")))


raw = {n: read_raw(n) for n in SOURCES}

overview = []
for n, d in raw.items():
    a = d.agg(F.count("*").alias("rows"), F.countDistinct("_source_file").alias("files")).first()
    overview.append((n, a["rows"], a["files"], len([c for c in d.columns if not c.startswith("_")]),
                     ", ".join(c for c in d.columns if not c.startswith("_"))))
display(spark.createDataFrame(overview, "source STRING, rows LONG, files LONG, n_columns INT, columns STRING"))

ROWS = {r[0]: r[1] for r in overview}

# COMMAND ----------

# DBTITLE 0,Rows per source file
display(reduce(lambda a, b: a.unionByName(b), [
    d.groupBy(F.lit(n).alias("source"), F.element_at(F.split("_source_file", "/"), -1).alias("file"))
     .count().withColumnRenamed("count", "rows") for n, d in raw.items()]).orderBy("source", "file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Column profile
# MAGIC `untrimmed` = value differs from its trimmed form; `blank` = empty after trim. Both are why silver trims every column.

# COMMAND ----------

# DBTITLE 0,Profile every column of every source
def profile(name: str, frame: DataFrame, top: int = 5) -> DataFrame:
    cols = [c for c in frame.columns if not c.startswith("_")]
    aggs = []
    for c in cols:
        v = F.trim(F.col(c))
        aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__null"),
                 F.sum((v == "").cast("int")).alias(f"{c}__blank"),
                 F.sum((F.col(c) != v).cast("int")).alias(f"{c}__untrimmed"),
                 F.approx_count_distinct(c).alias(f"{c}__distinct"),
                 F.min(F.length(c)).alias(f"{c}__minlen"),
                 F.max(F.length(c)).alias(f"{c}__maxlen")]
    r = frame.agg(*aggs).first().asDict()
    rows = []
    for c in cols:
        tops = frame.groupBy(c).count().orderBy(F.desc("count")).limit(top).collect()
        rows.append((name, c, r[f"{c}__null"], r[f"{c}__blank"], r[f"{c}__untrimmed"], r[f"{c}__distinct"],
                     r[f"{c}__minlen"], r[f"{c}__maxlen"], " | ".join(f"{t[0]!r} ({t[1]})" for t in tops)))
    return spark.createDataFrame(rows, "source STRING, column STRING, nulls LONG, blank LONG, untrimmed LONG, "
                                       "approx_distinct LONG, min_len INT, max_len INT, top_values STRING")


prof = reduce(lambda a, b: a.unionByName(b), [profile(n, d) for n, d in raw.items()])
display(prof)

untrimmed = prof.filter("untrimmed > 0").select("source", "column").collect()
note("profile", "columns with untrimmed values", ", ".join(f"{r[0]}.{r[1]}" for r in untrimmed) or "none",
     "silver trims every column (_str + trim)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Events

# COMMAND ----------

# DBTITLE 0,Parse events (keep raw next to parsed to count parse failures)
e = raw["events"]
ev = (e.select(F.trim("event_id").alias("event_id"), F.trim("user_id").alias("user_id"),
               F.col("event_name").alias("event_name_raw"), F.lower(F.trim("event_name")).alias("event_name"),
               F.nullif(F.trim("offer_id"), F.lit("")).alias("offer_id"),
               F.col("event_ts").alias("event_ts_raw"), ts("event_ts").alias("event_ts"),
               F.col("ingest_ts").alias("ingest_ts_raw"), ts("ingest_ts").alias("ingest_ts"))
      .withColumn("event_date", F.to_date("event_ts"))
      .withColumn("lag_hours", (F.unix_timestamp("ingest_ts") - F.unix_timestamp("event_ts")) / 3600.0))

ev_stats = ev.agg(F.count("*").alias("rows"),
           F.sum(F.col("event_id").isNull().cast("int")).alias("null_event_id"),
           F.sum(F.col("user_id").isNull().cast("int")).alias("null_user_id"),
           F.sum((F.col("event_ts").isNull() & F.col("event_ts_raw").isNotNull()).cast("int")).alias("unparseable_event_ts"),
           F.sum((F.col("ingest_ts").isNull() & F.col("ingest_ts_raw").isNotNull()).cast("int")).alias("unparseable_ingest_ts"),
           F.countDistinct("event_id").alias("distinct_event_id"),
           F.min("event_ts").alias("min_event_ts"), F.max("event_ts").alias("max_event_ts"),
           F.min("ingest_ts").alias("min_ingest_ts"), F.max("ingest_ts").alias("max_ingest_ts"))
display(ev_stats)
a = ev_stats.first()
note("events", "null event_id / user_id", f"{a['null_event_id']} / {a['null_user_id']}", "rejected: null_key_bad_ts_or_unknown_event")
note("events", "unparseable event_ts / ingest_ts", f"{a['unparseable_event_ts']} / {a['unparseable_ingest_ts']}",
     "rejected; needs ANSI off")
note("events", "event_ts range", f"{a['min_event_ts']} .. {a['max_event_ts']}")

# COMMAND ----------

# DBTITLE 0,Duplicate event_id: retries or conflicts?
dup = (ev.filter("event_id IS NOT NULL").groupBy("event_id")
       .agg(F.count("*").alias("copies"),
            F.countDistinct(F.to_json(F.struct("user_id", "event_name", "event_ts", "offer_id"))).alias("payload_variants"),
            ((F.unix_timestamp(F.max("ingest_ts")) - F.unix_timestamp(F.min("ingest_ts")))).alias("ingest_spread_s"))
       .filter("copies > 1"))
dup_stats = dup.agg(F.count("*").alias("dup_ids"), F.sum(F.col("copies") - 1).alias("extra_rows"),
                    F.sum((F.col("payload_variants") > 1).cast("int")).alias("ids_with_conflicting_payload"),
                    F.expr("percentile(ingest_spread_s, 0.5)").alias("p50_spread_s"), F.max("ingest_spread_s").alias("max_spread_s"))
display(dup_stats)
d = dup_stats.first()
display(dup.orderBy(F.desc("copies")).limit(20))
note("events", "duplicate event_ids (extra rows)", f"{d['dup_ids']} ({d['extra_rows']}, {pct(d['extra_rows'] or 0, ROWS['events'])}%)",
     "dedup: first arrival wins on event_id; MERGE whenNotMatchedInsert")
note("events", "duplicates with a DIFFERENT payload", d["ids_with_conflicting_payload"],
     "0 = pure retries, so first-arrival-wins loses nothing")

# COMMAND ----------

# DBTITLE 0,Event names: spellings and unknowns
names = (ev.groupBy("event_name_raw", "event_name").count()
         .withColumn("known", F.col("event_name").isin(*STEPS)).orderBy(F.desc("count")))
display(names)
unknown = ev.filter(~F.col("event_name").isin(*STEPS) | F.col("event_name").isNull()).count()
note("events", "raw event_name spellings -> canonical", f"{names.count()} -> {names.filter('known').select('event_name').distinct().count()}",
     "lower(trim(event_name))")
note("events", "rows with unknown event_name", unknown, "rejected")

# COMMAND ----------

# DBTITLE 0,Volume per day and hour-of-day (timezone sanity)
display(ev.groupBy("event_date").agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users")).orderBy("event_date"))
display(ev.groupBy(F.hour("event_ts").alias("hour_utc")).count().orderBy("hour_utc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Arrival lag
# MAGIC `lag = ingest_ts − event_ts`. Two different questions:
# MAGIC * **late** = arrived after the 00:15 job of the next day, i.e. missed its day's first build (`is_late`).
# MAGIC * **still changing after N days** = how long a date keeps receiving events. This sets the rebuild
# MAGIC   lookback and when a date becomes `is_settled`.

# COMMAND ----------

# DBTITLE 0,Lag distribution
lag = ev.filter("lag_hours IS NOT NULL")
q = lag.agg(F.percentile_approx("lag_hours", [0.5, 0.9, 0.95, 0.99, 0.999]).alias("q"),
            F.max("lag_hours").alias("max"), F.min("lag_hours").alias("min"),
            F.sum((F.col("lag_hours") < 0).cast("int")).alias("negative")).first()
display(spark.createDataFrame([(float(q["q"][0]), float(q["q"][1]), float(q["q"][2]), float(q["q"][3]), float(q["q"][4]),
                                float(q["max"]), float(q["min"]), q["negative"])],
                              "p50_h DOUBLE, p90_h DOUBLE, p95_h DOUBLE, p99_h DOUBLE, p999_h DOUBLE, max_h DOUBLE, min_h DOUBLE, negative_rows LONG"))
display(lag.groupBy(F.floor(F.col("lag_hours") / 24).alias("lag_day")).count()
        .withColumn("share_pct", F.round(100 * F.col("count") / F.sum("count").over(W.partitionBy()), 3)).orderBy("lag_day"))
note("lag", "max lag (days)", round(q["max"] / 24, 2), f"lookback_days={LOOKBACK_DAYS} must exceed this")
note("lag", "p50 / p99 lag (hours)", f"{round(q['q'][0], 2)} / {round(q['q'][3], 2)}")
note("lag", "rows with negative lag (ingest before event)", q["negative"], "warn rule: lag_hours >= 0")

# COMMAND ----------

# DBTITLE 0,Late share vs job cutoff, and 'still changing' vs lookback
cut = lambda m: F.to_timestamp(F.date_add("event_date", 1)) + F.expr(f"INTERVAL {m} MINUTES")
late = lag.agg(*[F.avg((F.col("ingest_ts") > cut(m)).cast("int")).alias(f"cutoff_{m}m") for m in (0, 15, 60, 120, 360, 720)]).first()
display(spark.createDataFrame([(k, round(100 * v, 3)) for k, v in late.asDict().items()], "job_cutoff STRING, share_late_pct DOUBLE"))

days = lag.withColumn("arrival_days", F.datediff(F.to_date("ingest_ts"), "event_date"))
tail = days.agg(*[F.sum((F.col("arrival_days") > n).cast("int")).alias(str(n)) for n in range(0, 11)]).first()
display(spark.createDataFrame([(int(n), v, pct(v, lag.count())) for n, v in tail.asDict().items()],
                              "lookback_days INT, rows_arriving_later LONG, share_pct DOUBLE"))
note("lag", f"share missing the 00:{JOB_CUTOFF_MIN:02d} job", f"{round(100 * late[f'cutoff_{JOB_CUTOFF_MIN}m'], 3)}%",
     "is_late flag, kept not dropped")
note("lag", f"rows arriving > {LOOKBACK_DAYS} days after their date", tail[str(LOOKBACK_DAYS)],
     "must be 0: otherwise a 'settled' date would still change")

# COMMAND ----------

# DBTITLE 0,Funnel: users per step and ordering
clean = ev.filter(F.col("event_name").isin(*STEPS) & F.col("event_id").isNotNull()).dropDuplicates(["event_id"])
steps = clean.groupBy("event_name").agg(F.countDistinct("user_id").alias("users"), F.count("*").alias("events")).collect()
by = {r["event_name"]: r for r in steps}
rows, prev = [], None
for s in STEPS:
    u = by[s]["users"] if s in by else 0
    rows.append((s, u, by[s]["events"] if s in by else 0, pct(u, prev) if prev else None))
    prev = u
display(spark.createDataFrame(rows, "step STRING, users LONG, events LONG, conversion_from_prev_pct DOUBLE"))

per = (clean.filter("offer_id IS NOT NULL").groupBy("user_id", "offer_id")
       .pivot("event_name", STEPS).count().fillna(0))
seq_stats = per.agg(F.sum(((F.col("reward_paid") > 0) & (F.col("goal_reached") == 0)).cast("int")).alias("paid_without_goal"),
              F.sum(((F.col("goal_reached") > 0) & (F.col("offer_start") == 0)).cast("int")).alias("goal_without_start"),
              F.sum((F.col("reward_paid") > 1).cast("int")).alias("double_payout_pairs"),
              F.count("*").alias("user_offer_pairs"))
display(seq_stats)
seq = seq_stats.first()
note("funnel", "user x offer pairs paid more than once", seq["double_payout_pairs"], "dq_metrics funnel.double_payout_pairs")
note("funnel", "reward_paid without goal_reached (pairs)", seq["paid_without_goal"], "reported, not corrected")

# COMMAND ----------

# DBTITLE 0,offer_id presence by event name
display(ev.groupBy("event_name").agg(F.count("*").alias("rows"),
                                     F.round(100 * F.avg(F.col("offer_id").isNull().cast("int")), 2).alias("offer_id_null_pct"))
        .orderBy("event_name"))

# COMMAND ----------

# DBTITLE 0,Events per user (skew → AQE skew join)
epu = clean.groupBy("user_id").count()
s = epu.agg(F.percentile_approx("count", [0.5, 0.9, 0.99]).alias("q"), F.max("count").alias("max"),
            F.count("*").alias("users"), F.sum("count").alias("events")).first()
top1 = epu.orderBy(F.desc("count")).limit(max(1, s["users"] // 100)).agg(F.sum("count")).first()[0]
display(spark.createDataFrame([(s["q"][0], s["q"][1], s["q"][2], s["max"], pct(top1, s["events"]))],
                              "p50 LONG, p90 LONG, p99 LONG, max LONG, top_1pct_users_share_of_events_pct DOUBLE"))
note("physical", "events per user p50 / max", f"{s['q'][0]} / {s['max']}",
     "spark.sql.adaptive.skewJoin.enabled splits hot users in the events x installs join")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Installs

# COMMAND ----------

# DBTITLE 0,Parse installs
i = raw["installs"]
ins = (i.select(F.trim("user_id").alias("user_id"), F.col("install_ts").alias("install_ts_raw"), ts("install_ts").alias("install_ts"),
                F.col("country").alias("country_raw"), F.upper(F.trim("country")).alias("country"),
                F.col("platform").alias("platform_raw"), F.lower(F.trim("platform")).alias("platform"),
                F.lower(F.trim("media_source")).alias("media_source"), F.trim("device_model").alias("device_model"),
                F.col("campaign_id").alias("campaign_id_raw"))
       .withColumn("install_date", F.to_date("install_ts")))
ins_stats = ins.agg(F.count("*").alias("rows"), F.countDistinct("user_id").alias("distinct_users"),
            F.sum(F.col("user_id").isNull().cast("int")).alias("null_user_id"),
            F.sum((F.col("install_ts").isNull() & F.col("install_ts_raw").isNotNull()).cast("int")).alias("unparseable_install_ts"),
            F.min("install_ts").alias("min_install_ts"), F.max("install_ts").alias("max_install_ts"))
display(ins_stats)
a = ins_stats.first()
note("installs", "rows vs distinct users", f"{a['rows']} vs {a['distinct_users']}", "first install per user wins")

# COMMAND ----------

# DBTITLE 0,Duplicate installs per user
dupi = ins.groupBy("user_id").agg(F.count("*").alias("rows"), F.countDistinct("install_ts").alias("distinct_ts"),
                                  F.countDistinct("country").alias("distinct_country"),
                                  F.countDistinct("platform").alias("distinct_platform")).filter("rows > 1")
display(dupi.orderBy(F.desc("rows")).limit(20))
note("installs", "users with >1 install row", dupi.count(), "row_number over install_ts: earliest wins")

# COMMAND ----------

# DBTITLE 0,Country spellings → ISO-2
cm = (ins.groupBy("country_raw", "country").count()
      .withColumn("valid_iso2", F.col("country").rlike("^[A-Z]{2}$")).orderBy("country", F.desc("count")))
display(cm)
spellings, valid = cm.count(), cm.filter("valid_iso2").select("country").distinct().count()
invalid_rows = cm.filter("NOT valid_iso2 OR country IS NULL").agg(F.sum("count")).first()[0] or 0
note("installs", "raw country spellings -> valid ISO-2 codes", f"{spellings} -> {valid}", "upper(trim(country))")
note("installs", "rows whose country is not ISO-2 after normalising", invalid_rows, "rejected: bad_ts_platform_or_country")

# COMMAND ----------

# DBTITLE 0,Platform, media source, campaign
display(ins.groupBy("platform_raw", "platform").count().withColumn("valid", F.col("platform").isin("android", "ios")).orderBy(F.desc("count")))
display(ins.groupBy("media_source").agg(F.count("*").alias("installs"),
                                        F.sum(F.col("campaign_id_raw").isNull().cast("int")).alias("campaign_null"),
                                        F.sum((F.trim("campaign_id_raw") == "").cast("int")).alias("campaign_blank"))
        .orderBy(F.desc("installs")))
note("installs", "blank campaign_id rows", ins.filter(F.trim("campaign_id_raw") == "").count(), "nullif(trim, '') -> NULL = organic")

# COMMAND ----------

# DBTITLE 0,Device models seen on both platforms
both = (ins.filter(F.col("platform").isin("android", "ios")).groupBy("device_model")
        .agg(F.countDistinct("platform").alias("platforms"), F.count("*").alias("installs")).filter("platforms > 1"))
display(both.orderBy(F.desc("installs")))
note("installs", "device models appearing on both android and ios", both.count(),
     "device_model kept but never used to derive platform")

# COMMAND ----------

# DBTITLE 0,Installs per day
display(ins.groupBy("install_date").count().orderBy("install_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Offers

# COMMAND ----------

# DBTITLE 0,Offer catalog quality
o = raw["offers"]
off = o.select(F.trim("offer_id").alias("offer_id"), F.col("payout_eur").alias("payout_raw"), dec("payout_eur").alias("payout_eur"),
               F.col("offer_category").alias("category_raw"), F.lower(F.trim("offer_category")).alias("offer_category"),
               F.lower(F.trim("payout_type")).alias("payout_type"))
off_stats = off.agg(F.count("*").alias("rows"), F.countDistinct("offer_id").alias("distinct_offers"),
            F.sum(F.col("offer_id").isNull().cast("int")).alias("null_offer_id"),
            F.sum((F.col("payout_eur").isNull() & F.col("payout_raw").isNotNull()).cast("int")).alias("unparseable_payout"),
            F.sum((F.col("payout_eur") < 0).cast("int")).alias("negative_payout"),
            F.sum((F.col("payout_eur") == 0).cast("int")).alias("zero_payout"),
            F.min("payout_eur").alias("min_payout"), F.max("payout_eur").alias("max_payout"))
display(off_stats)
a = off_stats.first()
display(off.groupBy("offer_id").agg(F.count("*").alias("rows"), F.countDistinct("payout_eur").alias("distinct_payouts"))
        .filter("rows > 1").orderBy(F.desc("rows")))
display(off.groupBy("category_raw", "offer_category", "payout_type").count().orderBy("offer_category"))
note("offers", "unparseable / negative / zero payouts",
     f"{a['unparseable_payout']} / {a['negative_payout']} / {a['zero_payout']}",
     "unparseable & negative rejected; zero fails rule 'payout > 0'")
note("offers", "rows vs distinct offer_id", f"{a['rows']} vs {a['distinct_offers']}", "latest snapshot per offer_id wins")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · User profile: does it agree with events?

# COMMAND ----------

# DBTITLE 0,Profile vs events
up = (raw["user_profile"].select(F.trim("user_id").alias("user_id"),
                                 F.trim("events_lifetime").cast("bigint").alias("events_lifetime"),
                                 ts("last_seen_ts").alias("last_seen_ts"),
                                 F.col("is_payer").alias("is_payer_raw"))
      .dropDuplicates(["user_id"]))
actual = clean.groupBy("user_id").agg(F.count("*").alias("events_actual"), F.max("event_ts").alias("last_event_ts"))
cmp = (up.join(actual, "user_id", "full_outer")
       .withColumn("cmp", F.when(F.col("events_lifetime").isNull(), "no profile")
                           .when(F.col("events_actual").isNull(), "no events")
                           .when(F.col("events_lifetime") == F.col("events_actual"), "equal")
                           .when(F.col("events_lifetime") > F.col("events_actual"), "profile higher")
                           .otherwise("profile lower")))
display(cmp.groupBy("cmp").count().orderBy(F.desc("count")))
display(cmp.withColumn("diff_h", (F.unix_timestamp("last_seen_ts") - F.unix_timestamp("last_event_ts")) / 3600)
        .select(F.expr("transform(percentile(diff_h, array(0.5, 0.95)), x -> round(x, 2))")
                .alias("last_seen_minus_last_event_hours_p50_p95")))
display(raw["user_profile"].groupBy("is_payer").count().orderBy(F.desc("count")))
agree = cmp.filter("cmp = 'equal'").count()
note("user_profile", "users whose events_lifetime == events counted", f"{agree} of {cmp.count()}",
     "profile has its own definitions / later snapshot: reference only, gold never reads it")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Cross-source

# COMMAND ----------

# DBTITLE 0,Events without an install, installs without events
first_ins = (ins.filter("user_id IS NOT NULL AND install_ts IS NOT NULL")
             .withColumn("_rn", F.row_number().over(W.partitionBy("user_id").orderBy("install_ts"))).filter("_rn = 1").drop("_rn"))
no_ins = clean.join(first_ins, "user_id", "left_anti")
no_ev = first_ins.join(clean.select("user_id").distinct(), "user_id", "left_anti")
display(spark.createDataFrame([(no_ins.count(), no_ins.select("user_id").distinct().count(), no_ev.count())],
                              "events_without_install LONG, users_without_install LONG, installed_users_without_events LONG"))
note("cross", "events whose user has no install", no_ins.count(),
     "error rule: every event has an install (gold inner-joins installs for country/platform)")

# COMMAND ----------

# DBTITLE 0,Events before the user's install
pre = (clean.join(first_ins.select("user_id", "install_ts", "media_source"), "user_id")
       .filter("event_ts < install_ts")
       .withColumn("hours_before_install", (F.unix_timestamp("install_ts") - F.unix_timestamp("event_ts")) / 3600))
display(pre.groupBy("media_source").agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users"),
                                        F.expr("percentile(hours_before_install, 0.5)").alias("p50_hours_before"),
                                        F.max("hours_before_install").alias("max_hours_before")).orderBy(F.desc("events")))
note("cross", "events before install (users)", f"{pre.count()} ({pre.select('user_id').distinct().count()})",
     "is_pre_install flag, kept; listed in dq_issues with media_source")

# COMMAND ----------

# DBTITLE 0,Orphan offers: offer ids in events that are not in the catalog
catalog = off.filter("offer_id IS NOT NULL").select("offer_id").distinct()
orph = clean.filter("offer_id IS NOT NULL").join(catalog, "offer_id", "left_anti")
display(orph.groupBy("offer_id").pivot("event_name", STEPS).count().fillna(0).orderBy("offer_id"))
orph_paid = orph.filter("event_name = 'reward_paid'").count()
note("cross", "orphan offer ids (events, of which reward_paid)",
     f"{orph.select('offer_id').distinct().count()} ({orph.count()}, {orph_paid})",
     "is_orphan_offer flag; gold counts them in rewards_unpriced (cost unknown, not 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Physical design: partition vs cluster, broadcast, AQE
# MAGIC Databricks guidance: partition only when each partition holds **≥ ~1 GB**; below that, partitioning
# MAGIC creates many small files and liquid clustering is the better layout.

# COMMAND ----------

# DBTITLE 0,Volume per date, bytes per row, broadcast size
def source_bytes(name):
    try:
        if SOURCE == "bronze":
            return spark.sql(f"DESCRIBE DETAIL {CATALOG}.bronze.{name}").first()["sizeInBytes"]
        return sum(f.size for f in dbutils.fs.ls(f"{LANDING}/{name}/"))
    except Exception as ex:
        print(f"size of {name} unavailable: {type(ex).__name__}")
        return None


size = {n: source_bytes(n) for n in SOURCES}
bpr = (size["events"] or 0) / max(ROWS["events"], 1)
per_day = clean.groupBy("event_date").count()
vol = per_day.agg(F.min("count").alias("min"), F.avg("count").alias("avg"), F.max("count").alias("max"), F.count("*").alias("dates")).first()
card = clean.agg(F.countDistinct("event_date").alias("event_date"), F.countDistinct("user_id").alias("user_id")).first()
ins_card = ins.agg(F.countDistinct("country").alias("country"), F.countDistinct("platform").alias("platform")).first()

mb_day = vol["avg"] * bpr / 1024 ** 2
display(spark.createDataFrame([
    ("events: dates", str(vol["dates"])), ("events: rows/day min | avg | max", f"{vol['min']} | {round(vol['avg'])} | {vol['max']}"),
    ("events: bytes/row (source)", f"{round(bpr)}"), ("events: ~MB per date", f"{round(mb_day, 2)}"),
    ("offers: MB (broadcast threshold 32 MB)", f"{round((size['offers'] or 0) / 1024 ** 2, 3)}"),
    ("installs: MB", f"{round((size['installs'] or 0) / 1024 ** 2, 3)}"),
    ("cardinality event_date | user_id", f"{card['event_date']} | {card['user_id']}"),
    ("cardinality country | platform", f"{ins_card['country']} | {ins_card['platform']}"),
], "metric STRING, value STRING"))

rec = "partition" if mb_day >= 1024 else "liquid"
note("physical", "avg MB per event_date", round(mb_day, 2),
     f"recommended layout={rec} ({'>=' if rec == 'partition' else '<'} 1 GB per partition)")
note("physical", "offers catalog MB", round((size["offers"] or 0) / 1024 ** 2, 3), "F.broadcast(offers) in both joins")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 · Pipeline outputs (skipped if not built yet)

# COMMAND ----------

# DBTITLE 0,Rejects, DQ results, open issues
t = lambda s: f"{CATALOG}.{s}"
exists = lambda s: spark.catalog.tableExists(t(s))

if exists("silver.rejects"):
    display(spark.table(t("silver.rejects")).groupBy("source", "reject_reason").count().orderBy("source"))
if exists("silver.dq_results"):
    r = spark.table(t("silver.dq_results"))
    last = r.groupBy("layer").agg(F.max("run_id").alias("run_id"))
    display(r.join(last, ["layer", "run_id"]).orderBy("passed", "layer", "rule"))
if exists("silver.dq_issues"):
    display(spark.table(t("silver.dq_issues")).groupBy("issue", "status").count().orderBy("issue", "status"))
if not exists("silver.events"):
    print("silver not built yet: run 01_bronze_to_silver")

# COMMAND ----------

# DBTITLE 0,Gold: daily trend and funnel by country x platform
G = "gold.daily_country_platform"
if exists(G):
    g = spark.table(t(G))
    display(g.groupBy("date", "is_settled").agg(F.sum("installs").alias("installs"), F.sum("events_total").alias("events"),
                                               F.sum("reward_payouts").alias("payouts"), F.sum("reward_cost_eur").alias("cost_eur"),
                                               F.sum("rewards_unpriced").alias("unpriced")).orderBy("date"))
    display(g.filter("is_settled").groupBy("country", "platform")
            .agg(F.sum("installs").alias("installs"),
                 *[F.sum(f"{s}_users").alias(f"{s}_user_days") for s in STEPS],
                 F.sum("reward_cost_eur").alias("cost_eur"))
            .withColumn("cost_per_install", F.round(F.col("cost_eur") / F.col("installs"), 4))
            .orderBy(F.desc("installs")))
else:
    print("gold not built yet: run 02_silver_to_gold")

# COMMAND ----------

# DBTITLE 0,Table health: files, size, layout
health = []
for s in ["silver.events", "silver.installs", "silver.offers", G]:
    if exists(s):
        d = spark.sql(f"DESCRIBE DETAIL {t(s)}").first().asDict()
        avg_mb = (d["sizeInBytes"] or 0) / max(d["numFiles"] or 1, 1) / 1024 ** 2
        health.append((s, d["numFiles"], round((d["sizeInBytes"] or 0) / 1024 ** 2, 2), round(avg_mb, 2),
                       ", ".join(d.get("partitionColumns") or []), ", ".join(d.get("clusteringColumns") or []),
                       "small files: OPTIMIZE" if (d["numFiles"] or 0) > 10 and avg_mb < 32 else "ok"))
if health:
    display(spark.createDataFrame(health, "table STRING, files LONG, size_mb DOUBLE, avg_file_mb DOUBLE, "
                                          "partitioned_by STRING, clustered_by STRING, verdict STRING"))