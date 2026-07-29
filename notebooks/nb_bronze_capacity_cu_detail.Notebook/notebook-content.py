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
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

def _execute_dax(workspace_id: str, dataset_id: str, dax_query: str) -> list[dict]:
    token = notebookutils.credentials.getToken("pbi")
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

rows = [
    {
        "sku": r["CUDetail[SKU]"],
        "window_start_time": r["CUDetail[WindowStartTime]"],
        "window_end_time": r["CUDetail[WindowEndTime]"],
        "start_of_hour": r["CUDetail[StartOfHour]"],
        "start_of_6min": r["CUDetail[StartOf6min]"],
        "cus": r["CUDetail[CUs]"],
        "interactive": r["CUDetail[Interactive]"],
        "background": r["CUDetail[Background]"],
        "interactive_preview": r["CUDetail[InteractivePreview]"],
        "background_preview": r["CUDetail[BackgroundPreview]"],
        "base_capacity_units": r["CUDetail[BaseCapacityUnits]"],
        "autoscale_capacity_units": r["CUDetail[AutoScaleCapacityUnits]"],
        "cu_limit": r["CUDetail[CU Limit]"],
        "threshold": r["CUDetail[Threshold]"],
        "interactive_delay_pct": r["CUDetail[Interactive Delay %]"],
        "interactive_rejection_pct": r["CUDetail[Interactive Rejection %]"],
        "background_rejection_pct": r["CUDetail[Background Rejection %]"],
        "peak6min_interactive": r["CUDetail[Peak6minInteractive]"],
        "peak6min_background": r["CUDetail[Peak6minBackground]"],
        "peak6min_interactive_preview": r["CUDetail[Peak6minInteractivePreview]"],
        "peak6min_background_preview": r["CUDetail[Peak6minBackgroundPreview]"],
        "peak6min_interactive_delay_pct": r["CUDetail[Peak6min Interactive Delay %]"],
        "peak6min_interactive_rejection_pct": r["CUDetail[Peak6min Interactive Rejection %]"],
        "peak6min_background_rejection_pct": r["CUDetail[Peak6min Background Rejection %]"],
    }
    for r in cu_detail_rows
]

df = spark.createDataFrame(rows)
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
