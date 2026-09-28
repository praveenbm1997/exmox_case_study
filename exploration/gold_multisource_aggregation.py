# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
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
print("✓ gold_daily_country_platform (MERGE)", flush=True)
print("=== GOLD COMPLETE ===", flush=True)

# COMMAND ----------

