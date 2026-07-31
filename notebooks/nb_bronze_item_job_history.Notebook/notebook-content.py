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

# ### nb_bronze_item_job_history
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** Fabric Job Scheduler API — `GET /v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_item_job_history`
# **Schedule:** Daily
#
# `fact_refresh` only covers semantic model refreshes. Every other
# scheduled/triggered execution in the tenant — a Notebook run, a
# DataPipeline run (**including this project's own orchestration
# pipeline**) — has no failure/duration tracking at all today. This closes
# that gap with the same "who/what broke and how long did it take" shape
# `fact_refresh` already has, just for Notebook and DataPipeline items.
#
# Append-only: a job instance, once finished, does not change — no SCD
# needed, same reasoning as `raw_refresh_history`.
#
# **Coverage limitation, stated plainly:** this API is scoped per
# workspace/item, not tenant-admin — the caller needs actual access to
# that workspace, same as any regular user/app calling it. There's no
# `/admin/*` tenant-wide equivalent for job instances (unlike
# `/admin/items`). So this only covers items in workspaces the SP already
# has access to (this project's own workspace, and any sibling project's
# workspace the SP was already granted). A tenant-wide inventory of
# job-history-monitorable items would need the SP added to every workspace
# of interest — a real, explicit limitation, not a bug to chase.
#
# **Auth:** unlike the `/admin/*` and `executeQueries` endpoints elsewhere
# in this project, this is a plain Fabric Core API — the same simple
# `notebookutils.credentials.getToken("pbi")` pattern `nb_bronze_capacities`
# uses for `/v1/capacities` should work here too (to be confirmed by
# actually running this against the real tenant, not assumed).


# MARKDOWN ********************

# ### Configuration

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_item_job_history"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
ITEMS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"

TOKEN = notebookutils.credentials.getToken("pbi")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path              : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exploration — confirm auth works and the real response shape
#
# Testing against this project's own Notebook and DataPipeline items
# first — the SP running this notebook is guaranteed to have access to
# them (it's the identity that deploys and runs them), which isolates the
# question "does this API accept our simple auth?" from the separate
# question "which other workspaces does the SP have access to?". **Run
# this cell and inspect the output before the next cell is written.**

# CELL ********************

if not DeltaTable.isDeltaTable(spark, ITEMS_PATH):
    raise RuntimeError("raw_items table not found — run nb_bronze_items before this cell.")

sample_items = [
    row.asDict() for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type in ('Notebook', 'DataPipeline')")
        .select("item_id", "item_name", "item_type", "workspace_id")
        .limit(3)
        .collect()
]
print(f"Sample items to test: {len(sample_items)}")
print(_json.dumps(sample_items, indent=2, default=str))

sample_results = []
for item in sample_items:
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{item['workspace_id']}/items/{item['item_id']}/jobs/instances",
        headers=HEADERS, timeout=30,
    )
    sample_results.append({
        "item_name": item["item_name"],
        "item_type": item["item_type"],
        "status_code": resp.status_code,
        "body": resp.json() if resp.ok else resp.text,
    })

print(_json.dumps(sample_results, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed (2026-07-31): simple SP auth works, real schema captured
#
# `notebookutils.credentials.getToken("pbi")` worked fine (200 OK) — this
# endpoint doesn't reject app-only auth the way `/admin/*` and
# `executeQueries` do elsewhere in this project. No pagination marker
# appeared in any sampled response (`{"value": [...]}` only) at the volumes
# this tenant has; not implemented here since there's nothing to verify it
# against — if a heavily-used item ever returns a huge history, revisit.
#
# Real fields: `id`, `itemId`, `jobType` (`PipelineRunNotebook`,
# `RunNotebookInteractive`, ...), `invokeType` (`Manual`, presumably also
# `Scheduled`), `status` (`Completed`, `Failed`, ...), `failureReason`
# (`null`, or an object with `errorCode`/`message`/`isRetriable`),
# `rootActivityId`, `startTimeUtc`, `endTimeUtc`. One of the three sample
# items (`nb_gold_crypto_model`, in the sibling crypto project) has real,
# detailed failures — useful, genuine test data for the correlation this
# table exists to support.

# CELL ********************

job_items = [
    row.asDict() for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type in ('Notebook', 'DataPipeline')")
        .select("item_id", "item_name", "item_type", "workspace_id")
        .collect()
]
print(f"Notebook/DataPipeline items to check: {len(job_items)}")

job_rows = []
failed_lookups = []
for item in job_items:
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{item['workspace_id']}/items/{item['item_id']}/jobs/instances",
        headers=HEADERS, timeout=30,
    )
    if not resp.ok:
        # Same reasoning as nb_bronze_refresh_history: an item the SP
        # can't reach (a workspace it isn't a member of, or a deleted
        # item still lingering in raw_items until its next SCD2 pass)
        # shouldn't fail the whole run — skip and keep going.
        failed_lookups.append(item["item_id"])
        continue
    for job in resp.json().get("value", []):
        failure = job.get("failureReason") or {}
        job_rows.append({
            "job_id": job.get("id"),
            "item_id": item["item_id"],
            "item_name": item["item_name"],
            "item_type": item["item_type"],
            "workspace_id": item["workspace_id"],
            "job_type": job.get("jobType"),
            "invoke_type": job.get("invokeType"),
            "status": job.get("status"),
            "failure_error_code": failure.get("errorCode"),
            "failure_message": failure.get("message"),
            "failure_is_retriable": failure.get("isRetriable"),
            "root_activity_id": job.get("rootActivityId"),
            "start_time": job.get("startTimeUtc"),
            "end_time": job.get("endTimeUtc"),
        })

print(f"Job instances fetched: {len(job_rows)}")
print(f"Items with no job history available: {len(failed_lookups)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType

ROW_SCHEMA = StructType([
    StructField("job_id", StringType()),
    StructField("item_id", StringType()),
    StructField("item_name", StringType()),
    StructField("item_type", StringType()),
    StructField("workspace_id", StringType()),
    StructField("job_type", StringType()),
    StructField("invoke_type", StringType()),
    StructField("status", StringType()),
    StructField("failure_error_code", StringType()),
    StructField("failure_message", StringType()),
    StructField("failure_is_retriable", BooleanType()),
    StructField("root_activity_id", StringType()),
    StructField("start_time", StringType()),
    StructField("end_time", StringType()),
])

if job_rows:
    df = spark.createDataFrame(job_rows, schema=ROW_SCHEMA)
else:
    df = spark.createDataFrame([], schema=ROW_SCHEMA)

df = (
    df.withColumn("start_time", F.to_timestamp("start_time"))
      .withColumn("end_time", F.to_timestamp("end_time"))
      .withColumn("duration_seconds", F.col("end_time").cast("long") - F.col("start_time").cast("long"))
      .withColumn("is_failure", F.col("status") == "Failed")
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

# ### Append-Only Write (deduplicated by job_id)

# CELL ********************

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    existing_ids = (
        spark.read.format("delta").load(BRONZE_PATH)
        .select("job_id").distinct()
    )
    df_new = df.join(existing_ids, on="job_id", how="left_anti")
else:
    df_new = df

new_count = df_new.count()
print(f"New job instances to write: {new_count}")

if new_count > 0:
    (df_new.write.format("delta").mode("append")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{new_count} records written to {BRONZE_PATH}")
elif not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    (df_new.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"No job history yet — initialized empty table at {BRONZE_PATH}")
else:
    print("No new job instances. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(BRONZE_PATH)
    .groupBy("item_name", "item_type", "status")
    .agg(F.count("*").alias("runs"), F.avg("duration_seconds").alias("avg_duration_s"))
    .orderBy(F.desc("runs"))
    .show(30, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
