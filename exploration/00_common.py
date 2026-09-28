# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00 · Common
# MAGIC
# MAGIC Pulled into `01_bronze_to_silver` and `02_silver_to_gold` with `%run ./00_common`.
# MAGIC Defines config, Spark tuning (AQE), table layout (partitioning / Z-order / liquid clustering),
# MAGIC the DQ rule runner and the test runner. It creates no widgets and touches no tables.
# MAGIC
# MAGIC The calling notebook sets `LAYOUT` (`"liquid"` or `"partition"`) from its widget after the `%run`.

# COMMAND ----------

# DBTITLE 0,Imports
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from functools import reduce
from typing import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F, Window as W
from pyspark.sql.types import DecimalType

# COMMAND ----------

# DBTITLE 0,Config
@dataclass(frozen=True)
class Config:
    catalog: str = "exmox"
    landing: str = "/Volumes/exmox/bronze/landing"       # or s3://bucket/exmox/landing via an external location
    checkpoints: str = "/Volumes/exmox/bronze/_checkpoints"
    lookback_days: int = 6                               # > max observed lag (5.6 d)
    job_cutoff_minutes: int = 15                         # daily job runs at 00:15
    steps: tuple = ("app_open", "offer_view", "offer_start", "goal_reached", "reward_paid")
    sources: tuple = ("events", "installs", "offers", "user_profile")
    # runtime tuning
    timezone: str = "UTC"                                # event_date and the 00:15 cutoff are computed in this zone
    advisory_partition_mb: int = 64                      # AQE coalesce / skew-split target size
    broadcast_mb: int = 32                               # offers catalog is always under this

    @property
    def bronze(self): return f"{self.catalog}.bronze"
    @property
    def silver(self): return f"{self.catalog}.silver"
    @property
    def gold(self): return f"{self.catalog}.gold"


CFG = Config()
LAYOUT = "liquid"                                        # overwritten by the calling notebook's widget


def tbl(short: str) -> str:
    """'silver.events' -> 'exmox.silver.events'"""
    return f"{CFG.catalog}.{short}"

# COMMAND ----------

# DBTITLE 0,Spark tuning: AQE, shuffle partitions, broadcast, optimized writes
SPARK_SETTINGS = {
    # correctness: the transforms rely on a bad cast returning NULL (-> reject row), not raising.
    # DBR 17 / Spark 4 / serverless default ANSI to true, which would kill the run on the first bad payout.
    "spark.sql.ansi.enabled": "false",
    "spark.sql.session.timeZone": CFG.timezone,
    # AQE: re-plans at every shuffle boundary using real sizes
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",    # merge tiny shuffle partitions
    "spark.sql.adaptive.skewJoin.enabled": "true",              # split skewed join partitions (heavy users)
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": f"{CFG.advisory_partition_mb}m",
    "spark.sql.autoBroadcastJoinThreshold": f"{CFG.broadcast_mb}m",
    "spark.sql.shuffle.partitions": "auto",                     # Databricks auto-optimized shuffle
    # Delta file sizing
    "spark.databricks.delta.optimizeWrite.enabled": "true",     # ~128 MB files instead of one per task
    "spark.databricks.delta.autoCompact.enabled": "auto",       # compact small files after MERGE / append
}


def configure_spark() -> DataFrame:
    """Apply SPARK_SETTINGS. Serverless manages most of these itself and rejects the set: that is reported,
    not fatal. Returns a DataFrame of what was applied, for display()."""
    out = []
    for k, v in SPARK_SETTINGS.items():
        try:
            spark.conf.set(k, v)
            out.append((k, v, "applied"))
        except Exception as e:
            out.append((k, v, f"skipped ({type(e).__name__})"))
    return spark.createDataFrame(out, "setting STRING, value STRING, status STRING")

# COMMAND ----------

# DBTITLE 0,Table layout: partition + Z-order, or liquid clustering
@dataclass(frozen=True)
class Layout:
    partition_by: tuple = ()      # LAYOUT="partition": Hive-style partitions (low-cardinality date column)
    zorder_by: tuple = ()         # LAYOUT="partition": OPTIMIZE ... ZORDER BY inside each partition
    cluster_by: tuple = ()        # LAYOUT="liquid":    CLUSTER BY (Delta liquid clustering)


# Only the date-grained fact tables get a layout. Dimensions (offers, installs, user_profile) are small
# full-snapshot overwrites: partitioning them would only create small files.
LAYOUTS = {
    "silver.events": Layout(("event_date",), ("user_id",), ("event_date", "user_id")),
    "gold.daily_country_platform": Layout(("date",), ("country", "platform"), ("date", "country")),
    "silver.dq_issues": Layout((), (), ("issue",)),
}


def _wanted(short):
    lay = LAYOUTS.get(short)
    if lay is None:
        return "none", ()
    if LAYOUT == "partition":
        return ("partition", lay.partition_by) if lay.partition_by else ("none", ())
    return ("liquid", lay.cluster_by) if lay.cluster_by else ("none", ())


def current_layout(short):
    d = spark.sql(f"DESCRIBE DETAIL {tbl(short)}").first().asDict()
    if d.get("clusteringColumns"):
        return "liquid", tuple(d["clusteringColumns"])
    if d.get("partitionColumns"):
        return "partition", tuple(d["partitionColumns"])
    return "none", ()


def writer(df: DataFrame, short: str, create: bool = True):
    """df.write with the active layout. create=False for writes into an existing table (replaceWhere):
    its layout is already fixed, only the data is aligned to it."""
    kind, cols = _wanted(short)
    if kind == "partition":
        w = df.repartition(*cols).write           # one shuffle so each date lands in few files, not one per task
        return w.partitionBy(*cols) if create else w
    if kind == "liquid" and create:
        return df.write.clusterBy(*cols)
    return df.write


def prepare_full_rebuild(short: str) -> None:
    """Before a full-rebuild overwrite. Delta cannot switch partitioning <-> liquid clustering in place,
    so a table in the other layout is dropped (full rebuild recreates it from upstream anyway).
    Liquid with different keys is changed in place with ALTER TABLE ... CLUSTER BY."""
    if not spark.catalog.tableExists(tbl(short)):
        return
    (want, wcols), (have, hcols) = _wanted(short), current_layout(short)
    if want == have and wcols == hcols:
        return
    if want == have == "liquid":
        spark.sql(f"ALTER TABLE {tbl(short)} CLUSTER BY ({', '.join(wcols)})")
        print(f"{short}: cluster keys {hcols} -> {wcols}")
    else:
        print(f"{short}: layout {have}{hcols} -> {want}{wcols}; dropping before rebuild")
        spark.sql(f"DROP TABLE {tbl(short)}")


def warn_layout_drift(short: str) -> None:
    if spark.catalog.tableExists(tbl(short)) and current_layout(short) != _wanted(short):
        print(f"NOTE {short}: table is {current_layout(short)}, widget asks for {_wanted(short)}. "
              f"Run once with full_rebuild=true to switch.")


def optimize(short: str, date_col: str = None, lo=None, hi=None) -> None:
    """Compact what this run wrote, according to the table's ACTUAL layout.
    partition: OPTIMIZE only the rebuilt dates, Z-ordered.  liquid: OPTIMIZE is incremental already
    (clusters only unclustered files) and takes neither WHERE nor ZORDER."""
    have, _ = current_layout(short)
    lay = LAYOUTS.get(short, Layout())
    sql = f"OPTIMIZE {tbl(short)}"
    if have == "partition":
        if lo is not None and date_col:
            sql += f" WHERE {date_col} BETWEEN '{lo}' AND '{hi}'"
        if lay.zorder_by:
            sql += f" ZORDER BY ({', '.join(lay.zorder_by)})"
    print(sql)
    spark.sql(sql)

# COMMAND ----------

# DBTITLE 0,DQ rule runner
@dataclass(frozen=True)
class Rule:
    name: str
    fn: Callable                    # Ctx -> number of violations (0 = pass)
    severity: str = "error"         # "error" fails the task, "warn" is logged only


class Ctx:
    """What a rule sees: tables by short name and this run's date window (None = whole table)."""

    def __init__(self, read, lo=None, hi=None):
        self._read, self.lo, self.hi = read, lo, hi

    def t(self, short):
        return self._read(short)

    def win(self, df, col):
        return df if self.lo is None else df.filter(F.col(col).between(self.lo, self.hi))


def dupes(df, *keys): return df.groupBy(*keys).count().filter("count > 1").count()
def bad(df, cond):    return df.filter(cond).count()
def total(df, c):     return df.agg(F.sum(c)).first()[0] or 0
def gap(a, b):        return abs(float(a or 0) - float(b or 0))


def run_rules(rules, layer: str, lo=None, hi=None, read=None, log: bool = True) -> list:
    """Run every rule, log all results to silver.dq_results, THEN raise if an error-severity rule failed,
    so a red run is still queryable. A rule that throws (missing table/column) counts as failed."""
    ctx = Ctx(read or (lambda s: spark.table(tbl(s))), lo, hi)
    results = []
    for r in rules:
        try:
            v, err = float(r.fn(ctx)), None
        except Exception as e:
            v, err = None, f"{type(e).__name__}: {str(e).strip().splitlines()[0][:300]}"
        ok = v == 0
        print(f"{'PASS' if ok else r.severity.upper():5}  {r.name}" + ("" if ok else f"  -> {err or v}"))
        results.append({"layer": layer, "rule": r.name, "severity": r.severity,
                        "violations": v, "passed": ok, "error": err})
    if log:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CFG.silver}")
        (spark.createDataFrame(results, "layer STRING, rule STRING, severity STRING, violations DOUBLE, passed BOOLEAN, error STRING")
            .withColumn("run_ts", F.current_timestamp())
            .withColumn("run_id", F.date_format("run_ts", "yyyyMMdd_HHmmss"))
            .withColumn("window_lo", F.lit(lo).cast("date"))
            .withColumn("window_hi", F.lit(hi).cast("date"))
            .select("run_id", "run_ts", "layer", "window_lo", "window_hi", "rule", "severity", "passed", "violations", "error")
            .write.mode("append").saveAsTable(f"{CFG.silver}.dq_results"))
    failed = [x["rule"] for x in results if x["severity"] == "error" and not x["passed"]]
    if failed:
        raise AssertionError(f"{layer}: {len(failed)} DQ rule(s) failed: {failed}")
    return results

# COMMAND ----------

# DBTITLE 0,Test runner
def run_tests(tests) -> None:
    """Run plain test functions (assert-based, hand-built rows, no tables). Raises if any failed, so the
    notebook stops BEFORE writing anything."""
    failed = []
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
        except Exception as e:
            failed.append(t.__name__)
            print(f"FAIL  {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - len(failed)}/{len(tests)} passed")
    if failed:
        raise AssertionError(f"{len(failed)} transformation test(s) failed: {failed}; no table was touched")


def df(schema: str, rows: list) -> DataFrame:
    return spark.createDataFrame(rows, schema)