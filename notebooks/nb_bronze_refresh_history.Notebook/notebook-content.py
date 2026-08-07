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

# ### nb_bronze_refresh_history
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** Power BI Admin REST API — `GET /v1.0/myorg/admin/capacities/refreshables`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_refresh_history`
# **Schedule:** Daily
#
# This is the direct complement to nb_bronze_activity_events: the Activity Log
# only records a "RefreshDataset" activity with a UserId when a refresh was
# triggered on-demand by a person. Scheduled refreshes never appear there as a
# user action — but every refresh, scheduled or on-demand, shows up here, with
# status and (on failure) an error message. Joining the two in Gold is what
# lets us answer "did this fail because of something someone changed?".
#
# **Corrected endpoint (2026-08-03):** this notebook originally called
# `GET /admin/datasets/{id}/refreshhistory` per dataset — that endpoint does
# not exist in the Power BI REST API (confirmed: every dataset returned 404
# "No HTTP resource was found that matches the request URI", which is what
# sent us looking). The real, documented endpoint is
# `Admin - Get Refreshables` (`GET /admin/capacities/refreshables`), a single
# **tenant-wide** call — no per-dataset loop, and no dependency on raw_items
# needed to know which datasets to check.
#
# It has different semantics than the old (invented) design assumed: instead
# of a full list of past refresh runs per dataset, each refreshable exposes
# rolling-window aggregates (`refreshCount`/`refreshFailures`/`averageDuration`
# over "up to the last 7 days or 60 refreshes, whichever is less" — same
# rolling-window caveat as MetricsByItem, see nb_bronze_capacity_metrics) plus
# a `lastRefresh` sub-object with the single most recent completed/failed run.
#
# To reconstruct a genuine append-only event history from a value that's only
# ever "the latest one" at each daily snapshot, this notebook keeps the exact
# same dedup trick the old design already had: append-only, deduplicated by
# `refresh_id` (here, `lastRefresh.requestId`) against everything already in
# Bronze. The first day a given refresh is seen as "last", it gets written;
# once written, seeing it again as still-the-latest on a later day is a no-op.
# **Caveat:** if a dataset refreshes more than once between two daily pipeline
# runs, only the last of those runs is ever observed — under-counting is
# possible for datasets refreshed more than once a day. Fine for this
# project's daily cadence and mostly-scheduled-once-daily refresh patterns;
# worth knowing if reused against a tenant with frequent on-demand refreshes.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType, LongType, DoubleType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_refresh_history"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()
REFRESHABLES_TOP = 5000  # tenant-wide, single call — comfortably above this tenant's dataset count;
                          # a much larger tenant would need $skip-based pagination (not implemented here)

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path   : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Delegated Authentication
#
# `/admin/*` endpoints (both Fabric's and Power BI's) rejected every app-only
# Service Principal token tried here — `notebookutils.credentials.getToken()`
# and a raw `client_credentials` OAuth call both got 403/401, while an
# interactively-obtained delegated user token succeeded immediately. This
# isn't a tenant misconfiguration; some Admin APIs simply don't support
# app-only auth (confirmed against Microsoft Fabric community reports of the
# same symptom). See the README for the full diagnosis.
#
# The fix: exchange a refresh token — obtained once via an interactive
# device-code + MFA sign-in — for a fresh access token on every run, and
# persist the rotated refresh token Microsoft returns back into this same
# Delta table so the next run can still authenticate. See
# `nb_util_delegated_auth` for the shared implementation — it used to be
# duplicated verbatim here.

# CELL ********************

%run nb_util_delegated_auth

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# refreshables lives on api.powerbi.com — Power BI's resource, not Fabric's.
TOKEN = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Refreshables (tenant-wide, single call)

# CELL ********************

REFRESHABLES_URL = f"https://api.powerbi.com/v1.0/myorg/admin/capacities/refreshables?$top={REFRESHABLES_TOP}"
response = requests.get(REFRESHABLES_URL, headers=HEADERS, timeout=60)
response.raise_for_status()
refreshables = response.json().get("value", [])

print(f"Refreshables fetched: {len(refreshables)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame
#
# Only refreshables that have actually completed at least one refresh (i.e.
# have a `lastRefresh`) produce a row — a dataset with a schedule but no
# refresh yet has nothing to record. `refresh_count_7d`/`refresh_failures_7d`/
# `avg_duration_seconds_7d` are the rolling-window aggregates, kept as extra
# context columns alongside the individual `lastRefresh` event; downstream
# Silver/Gold/semantic model only ever consumed the per-event columns, so
# nothing further down the medallion needs to change.
#
# `name` from the API is deliberately NOT carried through as a `dataset_name`
# column here — nb_silver_refresh_history already resolves that by joining
# dataset_id against silver_items, and a same-named column from both sides
# would collide in that join.

# CELL ********************

def _f(v):
    return float(v) if v is not None else None

def _i(v):
    return int(v) if v is not None else None

rows = []
skipped_no_last_refresh = 0
for r in refreshables:
    last = r.get("lastRefresh")
    if not last:
        skipped_no_last_refresh += 1
        continue
    rows.append({
        "dataset_id": r.get("id"),
        "refresh_id": last.get("requestId"),
        "refresh_type": last.get("refreshType"),
        "start_time": last.get("startTime"),
        "end_time": last.get("endTime"),
        "status": last.get("status"),
        "error_json": _json.dumps(last.get("serviceExceptionJson")) if last.get("serviceExceptionJson") else None,
        "refresh_count_7d": _i(r.get("refreshCount")),
        "refresh_failures_7d": _i(r.get("refreshFailures")),
        "avg_duration_seconds_7d": _f(r.get("averageDuration")),
    })

print(f"Refresh events fetched: {len(rows)}")
print(f"Refreshables skipped (no lastRefresh yet): {skipped_no_last_refresh}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Typed DataFrame
#
# Explicit schema regardless of whether `rows` is empty — refresh_type/
# end_time/error_json are None for many runs (in-progress or non-scheduled
# refreshes), and Spark's type inference throws CANNOT_DETERMINE_TYPE if a
# column is null across every row in a run, same lesson as
# nb_bronze_activity_events.

# CELL ********************

schema = StructType([
    StructField("dataset_id", StringType()),
    StructField("refresh_id", StringType()),
    StructField("refresh_type", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
    StructField("status", StringType()),
    StructField("error_json", StringType()),
    StructField("refresh_count_7d", LongType()),
    StructField("refresh_failures_7d", LongType()),
    StructField("avg_duration_seconds_7d", DoubleType()),
])
df = spark.createDataFrame(rows, schema=schema)

df = (
    df.withColumn("start_time", F.to_timestamp("start_time"))
      .withColumn("end_time", F.to_timestamp("end_time"))
      .withColumn("ingestion_ts", F.lit(INGESTION_TS.isoformat()).cast("timestamp"))
      .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
)

print("Schema:")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Append-Only Write (deduplicated by refresh_id)

# CELL ********************

# refresh_id (lastRefresh.requestId) is a genuine unique ID per real refresh
# execution. Deduplicating against what's already in Bronze is what turns a
# value that's only ever "the latest as of today" into a real accumulated
# history over time: the first day a given refresh is seen as "last", it gets
# written; every later day it's still the latest (nothing new happened since),
# it's a no-op.
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    existing_ids = (
        spark.read.format("delta").load(BRONZE_PATH)
        .select("refresh_id").distinct()
    )
    df_new = df.join(existing_ids, on="refresh_id", how="left_anti")
else:
    df_new = df

new_count = df_new.count()
print(f"New refresh runs to write: {new_count}")

if new_count > 0:
    (df_new.write.format("delta").mode("append")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{new_count} records written to {BRONZE_PATH}")
elif not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    # First run with zero refresh history anywhere in the tenant yet (e.g. no
    # semantic model has been refreshed at all) — still create the table with
    # the right schema so downstream reads (Validation here, Silver later)
    # don't fail with PATH_NOT_FOUND on a table that was never initialized.
    (df_new.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"No refresh runs yet — initialized empty table at {BRONZE_PATH}")
else:
    print("No new refresh runs. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(BRONZE_PATH)
    .groupBy("status")
    .count()
    .orderBy(F.desc("count"))
    .show())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
