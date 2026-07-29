# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7eb57c95-5be1-421d-aee0-8ac6bde14d68",
# META       "default_lakehouse_name": "lh_governance_bronze",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "7eb57c95-5be1-421d-aee0-8ac6bde14d68"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_bronze_capacity_cu_detail
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** The "Fabric Capacity Metrics" app's own semantic model — `CUDetail` table (DAX query via `executeQueries` REST API)
# **Destination:** `lh_governance_bronze` → Delta Table `raw_capacity_cu_detail`
# **Schedule:** Daily
#
# This complements `nb_bronze_capacity_metrics` (`MetricsByItem`, item-level)
# with the **capacity-level** view needed for growth/capacity-planning
# questions ("how much did we grow in 6 months", "do we need a bigger SKU
# next year") — the kind `MetricsByItem` structurally can't answer safely.
#
# **Why a second source instead of just using `MetricsByItem` for this too:**
# `MetricsByItem`'s `sum_CU` is a *rolling 14-day total* — legitimate to
# **chart as a trend** over time, but mathematically wrong to **sum across
# many days** (each operation gets counted in ~14 different snapshots, so a
# multi-month sum wildly overcounts). `CUDetail`, by contrast, has real
# `WindowStartTime`/`WindowEndTime` columns — each row is an immutable,
# already-happened time bucket (looks like hourly, per `StartOfHour`), not a
# rolling window. That makes it **safely summable**: add up a year of these
# rows and you get an actual total, which is what capacity growth/sizing
# analysis needs. Trade-off: it's capacity-level only (no `ItemId`), so it
# answers "is the whole capacity growing" — for "which item is driving that
# growth," it's `MetricsByItem`'s trend that does the pointing.
#
# Because each row is an immutable past time bucket rather than a rolling
# total, this can be captured **properly append-only** (dedup by the time
# bucket key, same shape as `nb_bronze_activity_events`/
# `nb_bronze_refresh_history`) instead of the daily-snapshot-plus-delta
# workaround `nb_bronze_capacity_metrics` needs for `MetricsByItem`.
#
# **Auth:** same delegated refresh-token workaround as
# `nb_bronze_capacity_metrics` — `executeQueries` empirically 403s under the
# `sp-fabric-cicd` Service Principal's own app-only identity regardless of
# tenant/workspace/App-Registration configuration (see that notebook's
# markdown for the full elimination process), so this uses the same
# `_auth_delegated` refresh token instead of
# `notebookutils.credentials.getToken("pbi")`.


# MARKDOWN ********************

# ### Configuration

# CELL ********************

DESTINATION_TABLE = "raw_capacity_cu_detail"

# Same tenant-wide Capacity Metrics app instance nb_bronze_capacity_metrics
# reads from — see that notebook's markdown for why these are fixed
# constants rather than environment-parameterized.
METRICS_WORKSPACE_ID = "aee1a4b3-ad5a-4bd8-a648-0120aad157ef"
METRICS_DATASET_ID = "a2354849-42a1-40b1-80ed-473e68401b75"

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path              : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exploration — confirm the real grain/key before writing the extraction query
#
# `CUDetail`'s column list (browsed in the portal) shows `SKU`,
# `StartOfHour`, `StartOf6min`, `WindowStartTime`, `WindowEndTime` — but no
# capacity id/name column, even though the app's own report lets you pick a
# capacity from a slicer. That suggests either this tenant only has one
# capacity in scope, or there's a key column the portal's field list didn't
# surface. **Run this cell and inspect the actual JSON before assuming
# anything** — same lesson as `nb_bronze_capacity_metrics`'s `Items[...]`
# case, where the portal's column browser wasn't the full story.

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType, DoubleType, LongType
from delta.tables import DeltaTable

def _get_delegated_token(scope: str) -> str:
    auth_row = spark.read.format("delta").load(AUTH_TABLE_PATH).collect()[0]
    resp = requests.post(
        f"https://login.microsoftonline.com/{auth_row['tenant_id']}/oauth2/v2.0/token",
        data={
            "grant_type": "refresh_token",
            "client_id": auth_row["client_id"],
            "refresh_token": auth_row["refresh_token"],
            "scope": f"{scope} offline_access",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token_data = resp.json()

    new_refresh_token = token_data.get("refresh_token", auth_row["refresh_token"])
    spark.createDataFrame([{
        "tenant_id": auth_row["tenant_id"],
        "client_id": auth_row["client_id"],
        "refresh_token": new_refresh_token,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }]).write.format("delta").mode("overwrite").save(AUTH_TABLE_PATH)

    return token_data["access_token"]

def _execute_dax(workspace_id: str, dataset_id: str, dax_query: str) -> list[dict]:
    token = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
    resp = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["results"][0]["tables"][0]["rows"]

cu_detail_rows = _execute_dax(METRICS_WORKSPACE_ID, METRICS_DATASET_ID, "EVALUATE CUDetail")
print(f"CUDetail rows: {len(cu_detail_rows)}")
print(_json.dumps(cu_detail_rows[:5], indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed grain (2026-07-28)
#
# Real grain is **30 seconds** (`WindowStartTime`/`WindowEndTime` are
# exactly 30s apart) — `StartOfHour`/`StartOf6min` are just pre-computed
# bucket labels carried on every row for later grouping, not separate
# grains. No capacity id/name column exists, only `SKU` ("FT1") — this
# tenant has one capacity in scope, so `SKU` is a sufficient identifier for
# now. 15,862 rows ≈ 5.5 days of retention at this fine grain (shorter than
# the app's own 14-day report view, which likely aggregates to a coarser
# level before display) — another reason to capture and accumulate this
# daily rather than rely on the app to keep it around.
#
# `WindowStartTime` is unique per row (each 30s bucket happens once), so
# it's the natural key for append-only dedup, same shape as
# `nb_bronze_refresh_history`'s dedup by `refresh_id`. The duplicate
# `"Start of Hour"` column (space-separated, same values as `StartOfHour`)
# is dropped as redundant.

# CELL ********************

# Explicit schema instead of relying on Spark's type inference: several
# Peak6min* columns come back null in every single row of a run (Preview
# features unused on this trial capacity), and an all-null column across the
# whole sample makes inference fail outright with CANNOT_DETERMINE_TYPE.
ROW_SCHEMA = StructType([
    StructField("sku", StringType()),
    StructField("window_start_time", StringType()),
    StructField("window_end_time", StringType()),
    StructField("start_of_hour", StringType()),
    StructField("start_of_6min", StringType()),
    StructField("cus", DoubleType()),
    StructField("interactive", DoubleType()),
    StructField("background", DoubleType()),
    StructField("interactive_preview", DoubleType()),
    StructField("background_preview", DoubleType()),
    StructField("base_capacity_units", LongType()),
    StructField("autoscale_capacity_units", LongType()),
    StructField("cu_limit", DoubleType()),
    StructField("threshold", DoubleType()),
    StructField("interactive_delay_pct", DoubleType()),
    StructField("interactive_rejection_pct", DoubleType()),
    StructField("background_rejection_pct", DoubleType()),
    StructField("peak6min_interactive", DoubleType()),
    StructField("peak6min_background", DoubleType()),
    StructField("peak6min_interactive_preview", DoubleType()),
    StructField("peak6min_background_preview", DoubleType()),
    StructField("peak6min_interactive_delay_pct", DoubleType()),
    StructField("peak6min_interactive_rejection_pct", DoubleType()),
    StructField("peak6min_background_rejection_pct", DoubleType()),
])

# Same DAX-serializes-whole-numbers-as-int gotcha as nb_bronze_capacity_metrics
# — coerce every Double-typed field through float() so it always matches the
# explicit schema regardless of what the source happened to serialize it as.
def _f(x):
    return float(x) if x is not None else None

def _i(x):
    return int(x) if x is not None else None

rows = [
    {
        "sku": r["CUDetail[SKU]"],
        "window_start_time": r["CUDetail[WindowStartTime]"],
        "window_end_time": r["CUDetail[WindowEndTime]"],
        "start_of_hour": r["CUDetail[StartOfHour]"],
        "start_of_6min": r["CUDetail[StartOf6min]"],
        "cus": _f(r["CUDetail[CUs]"]),
        "interactive": _f(r["CUDetail[Interactive]"]),
        "background": _f(r["CUDetail[Background]"]),
        "interactive_preview": _f(r["CUDetail[InteractivePreview]"]),
        "background_preview": _f(r["CUDetail[BackgroundPreview]"]),
        "base_capacity_units": _i(r["CUDetail[BaseCapacityUnits]"]),
        "autoscale_capacity_units": _i(r["CUDetail[AutoScaleCapacityUnits]"]),
        "cu_limit": _f(r["CUDetail[CU Limit]"]),
        "threshold": _f(r["CUDetail[Threshold]"]),
        "interactive_delay_pct": _f(r["CUDetail[Interactive Delay %]"]),
        "interactive_rejection_pct": _f(r["CUDetail[Interactive Rejection %]"]),
        "background_rejection_pct": _f(r["CUDetail[Background Rejection %]"]),
        "peak6min_interactive": _f(r["CUDetail[Peak6minInteractive]"]),
        "peak6min_background": _f(r["CUDetail[Peak6minBackground]"]),
        "peak6min_interactive_preview": _f(r["CUDetail[Peak6minInteractivePreview]"]),
        "peak6min_background_preview": _f(r["CUDetail[Peak6minBackgroundPreview]"]),
        "peak6min_interactive_delay_pct": _f(r["CUDetail[Peak6min Interactive Delay %]"]),
        "peak6min_interactive_rejection_pct": _f(r["CUDetail[Peak6min Interactive Rejection %]"]),
        "peak6min_background_rejection_pct": _f(r["CUDetail[Peak6min Background Rejection %]"]),
    }
    for r in cu_detail_rows
]

df = spark.createDataFrame(rows, schema=ROW_SCHEMA)
df = (
    df.withColumn("window_start_time", F.to_timestamp("window_start_time"))
      .withColumn("window_end_time", F.to_timestamp("window_end_time"))
      .withColumn("start_of_hour", F.to_timestamp("start_of_hour"))
      .withColumn("start_of_6min", F.to_timestamp("start_of_6min"))
      .withColumn("ingestion_ts", F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"))
      .withColumn("ingestion_date", F.lit(datetime.now(timezone.utc).date().isoformat()).cast(DateType()))
)

print(f"Rows fetched: {df.count()}")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Append-Only Write (deduplicated by sku + window_start_time)

# CELL ********************

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    existing_keys = (
        spark.read.format("delta").load(BRONZE_PATH)
        .select("sku", "window_start_time").distinct()
    )
    df_new = df.join(existing_keys, on=["sku", "window_start_time"], how="left_anti")
else:
    df_new = df

new_count = df_new.count()
print(f"New 30s buckets to write: {new_count}")

if new_count > 0:
    (df_new.write.format("delta").mode("append")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{new_count} records written to {BRONZE_PATH}")
elif not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    (df_new.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"No buckets yet — initialized empty table at {BRONZE_PATH}")
else:
    print("No new buckets. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(BRONZE_PATH)
    .groupBy(F.to_date("window_start_time").alias("day"))
    .agg(F.count("*").alias("buckets"), F.sum("cus").alias("total_cu"))
    .orderBy(F.desc("day"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### (Exploration only — do not schedule) Item × day × Background/Interactive grain
#
# Neither source captured so far crosses all three axes we need for "which
# item was the biggest offender today, and was it Background or
# Interactive load": `MetricsByItem` has item + CU but no Background/
# Interactive split; `CUDetail` has the Background/Interactive split but no
# ItemId (capacity-level only). Two tables spotted in the original table
# listing but never queried — `MetricsByItemAndDayForWorkloadAutoscale` and
# `CUDetailForWorkLoadAutoscale` — are candidates for combining item, day,
# and workload type in one place. `TOPN` caps the pull since these could be
# large; this is purely to see the real columns before deciding anything.

# CELL ********************

sample_a = _execute_dax(
    METRICS_WORKSPACE_ID, METRICS_DATASET_ID,
    "EVALUATE TOPN(5, MetricsByItemAndDayForWorkloadAutoscale)",
)
print(f"MetricsByItemAndDayForWorkloadAutoscale sample rows: {len(sample_a)}")
print(_json.dumps(sample_a, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sample_b = _execute_dax(
    METRICS_WORKSPACE_ID, METRICS_DATASET_ID,
    "EVALUATE TOPN(5, CUDetailForWorkLoadAutoscale)",
)
print(f"CUDetailForWorkLoadAutoscale sample rows: {len(sample_b)}")
print(_json.dumps(sample_b, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
