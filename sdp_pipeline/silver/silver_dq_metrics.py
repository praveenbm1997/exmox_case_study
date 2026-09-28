"""Silver DQ metrics: data quality rule evaluation results per refresh.

Computes DQ metrics from all silver tables and the rejects table.
One row per (table, rule) with total_rows, passed_rows, failed_rows, pass_rate.
Recomputed on each pipeline refresh.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

STEPS = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")


def _rule_metrics(df, table_name, rule_name, condition_expr, severity="error"):
    """Compute pass/fail counts for a single DQ rule on a DataFrame."""
    total = df.count()
    failed = df.filter(f"NOT ({condition_expr})").count()
    passed = total - failed
    return df.sparkSession.createDataFrame(
        [(table_name, rule_name, severity, total, passed, failed)],
        ["table_name", "rule_name", "severity", "total_rows", "passed_rows", "failed_rows"],
    )


@dp.materialized_view(
    name="exmox.silver.silver_dq_metrics",
    comment="DQ rule evaluation metrics. One row per rule per refresh.",
)
def silver_dq_metrics():
    # -- silver_offers --
    offers = spark.read.table("exmox.silver.silver_offers")
    m_offer_id_nn = _rule_metrics(offers, "silver_offers", "offer_id_not_null", "offer_id IS NOT NULL")
    m_payout_pos  = _rule_metrics(offers, "silver_offers", "payout_eur_positive", "payout_eur IS NOT NULL AND payout_eur > 0")

    # -- silver_installs --
    installs = spark.read.table("exmox.silver.silver_installs")
    m_inst_uid_nn  = _rule_metrics(installs, "silver_installs", "user_id_not_null", "user_id IS NOT NULL")
    m_inst_ts_nn   = _rule_metrics(installs, "silver_installs", "install_ts_not_null", "install_ts IS NOT NULL")
    m_inst_country = _rule_metrics(installs, "silver_installs", "country_valid", "country IS NOT NULL AND LENGTH(country) = 2")
    m_inst_plat    = _rule_metrics(installs, "silver_installs", "platform_valid", "platform IN ('android', 'ios')")

    # -- silver_events --
    events = spark.read.table("exmox.silver.silver_events")
    m_ev_id_nn    = _rule_metrics(events, "silver_events", "event_id_not_null", "event_id IS NOT NULL")
    m_ev_uid_nn   = _rule_metrics(events, "silver_events", "user_id_not_null", "user_id IS NOT NULL")
    m_ev_ts_nn    = _rule_metrics(events, "silver_events", "event_ts_not_null", "event_ts IS NOT NULL")
    m_ev_its_nn   = _rule_metrics(events, "silver_events", "ingest_ts_not_null", "ingest_ts IS NOT NULL")
    m_ev_name_val = _rule_metrics(events, "silver_events", "event_name_valid",
                                  f"event_name IN ({','.join(repr(s) for s in STEPS)})")
    # Warning-severity rules (flags, not drops)
    m_ev_late    = _rule_metrics(events, "silver_events", "is_late", "is_late = false", severity="warn")
    m_ev_preinst = _rule_metrics(events, "silver_events", "is_pre_install", "is_pre_install = false", severity="warn")
    m_ev_orphan  = _rule_metrics(events, "silver_events", "is_orphan_offer", "is_orphan_offer = false", severity="warn")

    # -- silver_user_profile --
    user_profile = spark.read.table("exmox.silver.silver_user_profile")
    m_up_uid_nn = _rule_metrics(user_profile, "silver_user_profile", "user_id_not_null", "user_id IS NOT NULL")

    # -- gold_daily_country_platform --
    gold = spark.read.table("exmox.gold.gold_daily_country_platform")
    m_g_date  = _rule_metrics(gold, "gold_daily_country_platform", "date_not_null", "date IS NOT NULL")
    m_g_ctry  = _rule_metrics(gold, "gold_daily_country_platform", "country_not_null", "country IS NOT NULL")
    m_g_plat  = _rule_metrics(gold, "gold_daily_country_platform", "platform_not_null", "platform IS NOT NULL")
    m_g_inst  = _rule_metrics(gold, "gold_daily_country_platform", "installs_non_negative", "installs >= 0")
    m_g_evts  = _rule_metrics(gold, "gold_daily_country_platform", "events_total_non_negative", "events_total >= 0")

    # -- Union all metrics --
    all_metrics = (
        m_offer_id_nn
        .unionByName(m_payout_pos)
        .unionByName(m_inst_uid_nn)
        .unionByName(m_inst_ts_nn)
        .unionByName(m_inst_country)
        .unionByName(m_inst_plat)
        .unionByName(m_ev_id_nn)
        .unionByName(m_ev_uid_nn)
        .unionByName(m_ev_ts_nn)
        .unionByName(m_ev_its_nn)
        .unionByName(m_ev_name_val)
        .unionByName(m_ev_late)
        .unionByName(m_ev_preinst)
        .unionByName(m_ev_orphan)
        .unionByName(m_up_uid_nn)
        .unionByName(m_g_date)
        .unionByName(m_g_ctry)
        .unionByName(m_g_plat)
        .unionByName(m_g_inst)
        .unionByName(m_g_evts)
    )

    return (
        all_metrics
        .withColumn("pass_rate", F.when(F.col("total_rows") > 0, F.col("passed_rows") / F.col("total_rows")).otherwise(1.0))
        .withColumn("passed", F.col("failed_rows") == 0)
        .withColumn("refresh_ts", F.current_timestamp())
    )