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

# ### nb_bronze_capacity_metrics
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** The "Fabric Capacity Metrics" app's own semantic model (DAX query via `executeQueries` REST API)
# **Destination:** `lh_governance_bronze` → Delta Table `raw_capacity_metrics`
# **Schedule:** Daily
#
# This is the "how much did it cost" source that completes the governance
# picture: `fact_activity` says who changed what, `fact_refresh` says what
# broke, and this adds what it actually cost in CU(s)/$ — so a diagnosis can
# extend from "why did this break" to "was this worth what it's costing" and
# "which process should we optimize first."
#
# Unlike the other Bronze sources, this is **not** an `/admin/*` endpoint —
# it's a normal DAX query against a semantic model, so it does not need the
# delegated refresh-token workaround used by the other four Bronze notebooks
# (see their markdown / README for that saga).
#
# **Why REST `executeQueries` instead of `semantic-link` (`sempy.fabric`):**
# `semantic-link`'s `list_datasets`/`list_tables`/`evaluate_dax` all go
# through the semantic model's **XMLA endpoint**. The workspace hosting the
# Capacity Metrics app in this tenant runs on a **Fabric Trial (FT1)
# capacity**, and FT1 does not expose an XMLA Endpoint setting at all — it's
# a structural limitation of the trial SKU, not a toggle we can enable, and
# it doesn't matter which workspace is targeted since XMLA never worked
# against this capacity to begin with. The plain `executeQueries` REST API
# doesn't use XMLA, so it isn't affected by this — it only depends on the
# tenant setting "Semantic Model Execute Queries REST API" being enabled
# (already the case here, scoped to the `gp_sec_sp` security group).
#
# The Capacity Metrics app only exposes a rolling 14-day window in its own
# report — this notebook's job is to capture it daily, append-only, so we
# keep history beyond that window (same reasoning as raw_activity_events).


# MARKDOWN ********************

# ### Configuration

# CELL ********************

DESTINATION_TABLE = "raw_capacity_metrics"

# Workspace/dataset where the "Fabric Capacity Metrics" app is installed in
# this tenant — a single, tenant-wide app, not one per dev/qa/prd
# environment, so these are fixed constants rather than environment-
# parameterized values. Confirmed via the portal (Onelake > dataset details
# URL) rather than guessed.
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

# ### (Historical / do not run) Exploration via semantic-link
#
# Kept only as a record of how the real schema below was discovered — these
# cells fail with `OperationException: ... does not have permission to call
# the Discover method` on this tenant's FT1 trial capacity, regardless of
# workspace permissions (confirmed: workspace Admin access, tenant-wide XMLA
# setting enabled, capacity settings for FT1 don't even expose an XMLA
# Endpoint option). Superseded by the `executeQueries` REST cells further
# below, which is what this notebook actually runs.

# CELL ********************

# import sempy.fabric as fabric
# datasets = fabric.list_datasets(workspace=METRICS_WORKSPACE_ID)
# tables = fabric.list_tables("Fabric Capacity Metrics", workspace=METRICS_WORKSPACE_ID)
# cols = fabric.list_columns("Fabric Capacity Metrics", workspace=METRICS_WORKSPACE_ID)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed schema (2026-07-28)
#
# `CUDetail` is capacity-level only (no ItemId/WorkspaceId) — not useful
# here. `Items` is a dimension (ItemId, ItemName, WorkspaceId,
# WorkspaceName, ItemKind — no CU numbers). The table that actually has what
# we need is **`MetricsByItem`**: grain per `ItemId`, with `sum_CU`,
# `sum_duration`, operation counts by status
# (`count_successful_operations`, `count_failure_operations`,
# `count_cancelled_operations`, `count_rejected_operations`), `WorkspaceId`,
# `PremiumCapacityId`.
#
# Confirmed via the app's own report (the "Items (14 days)" table): this is
# a **rolling 14-day rolled-up total per item**, not sliced per day — there
# is no date column on `MetricsByItem` to query against. That means this
# notebook can't ask the API "give me yesterday's CU(s) for this item" the
# way `nb_bronze_activity_events` can; it can only get "the last 14 days,
# as of right now." Consequence for the ingestion design below: write this
# as a **daily snapshot** (like the SCD2 dimension notebooks), stamped with
# `ingestion_date`, and let Silver derive real day-over-day deltas by
# diffing consecutive snapshots — rather than treat it as a naturally
# append-only, already-atomic-per-day fact like refresh history or activity
# events.

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

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

metrics_rows = _execute_dax(METRICS_WORKSPACE_ID, METRICS_DATASET_ID, "EVALUATE MetricsByItem")
print(f"MetricsByItem rows: {len(metrics_rows)}")
print(_json.dumps(metrics_rows[:3], indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items_rows = _execute_dax(METRICS_WORKSPACE_ID, METRICS_DATASET_ID, "EVALUATE Items")
print(f"Items rows: {len(items_rows)}")
print(_json.dumps(items_rows[:3], indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Next step
#
# Run the two cells above and share: (1) whether `_execute_dax` succeeds at
# all with `notebookutils.credentials.getToken("pbi")` (if it 403s, the
# `gp_sec_sp`-scoped tenant setting needs broadening or this identity needs
# adding to that group), and (2) the printed row samples — the exact
# bracketed column names (`MetricsByItem[sum_CU]` etc.) so the DataFrame
# build and the daily-snapshot append-only write can be finished.
