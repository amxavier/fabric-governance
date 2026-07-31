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

# ### Next step
#
# Share the printed output: did the simple auth work (200) or reject the
# SP (401/403, meaning this needs the delegated-token workaround too)?
# What are the real field names for a job instance (status, start/end
# time, duration, failure reason/error message, invoke type — scheduled vs
# manual)? Is pagination involved (a `continuationToken` like the Admin
# API endpoints, or `continuationUri` like Activity Events)? The append-
# only write (same idempotency pattern as `nb_bronze_refresh_history`,
# deduplicated by job instance id) gets added right after.
