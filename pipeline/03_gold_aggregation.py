# Databricks notebook source
# DBTITLE 1,Gold — Column Constraints (idempotent)
# ── Gold column constraints: NOT NULL on composite key + non-negative counts ─
def _not_null(tbl, col):
    try: spark.sql(f"ALTER TABLE {tbl} ALTER COLUMN {col} SET NOT NULL")
    except: pass

def _check(tbl, name, expr):
    try: spark.sql(f"ALTER TABLE {tbl} ADD CONSTRAINT {name} CHECK ({expr})")
    except Exception as e:
        if "already exists" not in str(e).lower(): print(f"  ⚠ CHECK {name}: {e}")

if spark.catalog.tableExists("exmox.gold.gold_daily_country_platform"):
    _not_null("exmox.gold.gold_daily_country_platform", "date")
    _not_null("exmox.gold.gold_daily_country_platform", "country")
    _not_null("exmox.gold.gold_daily_country_platform", "platform")
    _check(   "exmox.gold.gold_daily_country_platform", "chk_gold_installs",     "installs >= 0")
    _check(   "exmox.gold.gold_daily_country_platform", "chk_gold_events_total", "events_total >= 0")

# ── Primary key (informational) ────────────────────────────────────
if spark.catalog.tableExists("exmox.gold.gold_daily_country_platform"):
    try: spark.sql("ALTER TABLE exmox.gold.gold_daily_country_platform ADD CONSTRAINT pk_gold_daily PRIMARY KEY (date, country, platform)")
    except Exception as e:
        if "already exists" not in str(e).lower(): print(f"  ⚠ PK pk_gold_daily: {e}")

print("── Gold constraints applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Performance Config — Optimized Writes, Broadcast, Low-Shuffle MERGE
# ── Spark performance tuning (serverless-safe, AQE already managed) ──────────
_conf = {
    "spark.databricks.delta.optimizeWrite.enabled":          "true",
    "spark.databricks.delta.autoCompact.enabled":            "auto",
    "spark.databricks.delta.schema.autoMerge.enabled":       "false",
    "spark.sql.autoBroadcastJoinThreshold":                  str(100 * 1024 * 1024),
    "spark.sql.shuffle.partitions":                          "32",
    "spark.databricks.delta.merge.enableLowShuffle.enabled": "true",
}
_skipped = []
for _k, _v in _conf.items():
    try: spark.conf.set(_k, _v)
    except Exception: _skipped.append(_k.split(".")[-1])
if _skipped:
    print(f"  ↷ serverless-managed: {', '.join(_skipped)}", flush=True)
print("── Perf config applied ──", flush=True)

# COMMAND ----------

# DBTITLE 1,Gold Aggregation — Daily Country/Platform
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
LOOKBACK_DAYS = 6

print("=== GOLD LAYER (Multi-Source Aggregation) ===", flush=True)
events = spark.read.table("exmox.silver.silver_events")
installs = spark.read.table("exmox.silver.silver_installs")
offers = spark.read.table("exmox.silver.silver_offers")

# Broadcast all small dimension tables — avoids shuffle joins
enriched = (events.join(F.broadcast(installs.select("user_id", "country", "platform")), "user_id", "inner")
    .join(F.broadcast(offers.select("offer_id", "payout_eur")), "offer_id", "left")
    .withColumn("is_reward", F.col("event_name") == "reward_paid"))

def _users_at(step): return F.countDistinct(F.when(F.col("event_name") == step, F.col("user_id"))).alias(f"{step}_users")

ev_m = enriched.groupBy(F.col("event_date").alias("date"), "country", "platform").agg(
    F.count("*").alias("events_total"),
    *[_users_at(s) for s in STEPS],
    F.sum(F.col("is_reward").cast("int")).alias("reward_payouts"),
    F.sum(F.when(F.col("is_reward"), F.col("payout_eur"))).cast(DecimalType(18, 2)).alias("reward_cost_eur"),
    F.sum((F.col("is_reward") & F.col("payout_eur").isNull()).cast("int")).alias("rewards_unpriced"),
    F.sum(F.col("is_late").cast("int")).alias("late_events"),
    F.sum(F.col("is_pre_install").cast("int")).alias("events_pre_install"),
)

in_m = installs.groupBy(F.col("install_date").alias("date"), "country", "platform").agg(F.count("*").alias("installs"))

gold_counts = ["installs", "events_total", *[f"{s}_users" for s in STEPS], "reward_payouts", "rewards_unpriced", "late_events", "events_pre_install"]

gold = (in_m.join(ev_m, ["date", "country", "platform"], "full_outer")
    .fillna(0, subset=gold_counts).fillna({"reward_cost_eur": 0})
    .withColumn("reward_cost_eur", F.col("reward_cost_eur").cast(DecimalType(18, 2)))
    .select("date", "country", "platform", "installs", "events_total", *[f"{s}_users" for s in STEPS], "reward_payouts", "reward_cost_eur", "rewards_unpriced", "late_events", "events_pre_install")
    .withColumn("is_settled", F.col("date") <= F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .withColumn("_loaded_at", F.current_timestamp()))

# ── Data quality check before MERGE ──────────────────────────────────────────
dq_stats = gold.agg(
    F.count("*").alias("total"),
    F.sum(F.col("date").isNull().cast("long")).alias("null_dates"),
    F.sum(F.col("country").isNull().cast("long")).alias("null_countries"),
    F.sum(F.col("platform").isNull().cast("long")).alias("null_platforms"),
    F.sum((F.col("installs") < 0).cast("long")).alias("neg_installs"),
    F.sum((F.col("events_total") < 0).cast("long")).alias("neg_events"),
).collect()[0]
issues = [
    f"{int(dq_stats[k]):,} {k.replace('null_','null ').replace('neg_','negative ')}"
    for k in ("null_dates", "null_countries", "null_platforms", "neg_installs", "neg_events")
    if int(dq_stats[k]) > 0
]
if issues:
    raise ValueError("DQ FAIL [gold_daily_country_platform]: " + "; ".join(issues))
print(f"  ✓ DQ [gold]: {int(dq_stats['total']):,} rows, composite key non-null, counts ≥ 0", flush=True)
# MERGE INTO gold table by (date, country, platform) composite key
gold_table = "exmox.gold.gold_daily_country_platform"
gold = gold.coalesce(1)
if not spark.catalog.tableExists(gold_table):
    gold.write.format("delta").saveAsTable(gold_table)
else:
    target = DeltaTable.forName(spark, gold_table)
    (target.alias("t")
     .merge(gold.alias("s"), "t.date = s.date AND t.country = s.country AND t.platform = s.platform")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
print("\u2713 gold_daily_country_platform (MERGE)", flush=True)
print("=== GOLD COMPLETE ===", flush=True)

# COMMAND ----------

# DBTITLE 1,Post-Write OPTIMIZE — Liquid Clustering + Compaction
# ── OPTIMIZE gold table after each run ───────────────────────────────────────
print("  → OPTIMIZE gold ...", flush=True)
_r = spark.sql("OPTIMIZE exmox.gold.gold_daily_country_platform").collect()[0]["metrics"]
print(f"    ✓ gold_daily_country_platform: +{_r['numFilesAdded']} / -{_r['numFilesRemoved']} files", flush=True)
print("── Gold OPTIMIZE complete ──", flush=True)