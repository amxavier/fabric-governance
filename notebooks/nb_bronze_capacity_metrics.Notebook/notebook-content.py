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
# **Source:** The "Fabric Capacity Metrics" app's own semantic model (DAX query via semantic-link)
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
# (see their markdown / README for that saga). It only needs whatever
# identity runs this notebook to have at least **Viewer** access on the
# workspace hosting the Capacity Metrics app. If the exploration cell below
# fails with a permissions error, that's the fix.
#
# The Capacity Metrics app only exposes a rolling 14-day window in its own
# report — this notebook's job is to capture it daily, append-only, so we
# keep history beyond that window (same reasoning as raw_activity_events).


# MARKDOWN ********************

# ### Configuration

# CELL ********************

DESTINATION_TABLE = "raw_capacity_metrics"

# Workspace where the "Fabric Capacity Metrics" app is installed in this
# tenant — a single, tenant-wide app, not one per dev/qa/prd environment,
# so this is a fixed constant rather than an environment-parameterized value.
METRICS_WORKSPACE_ID = "aee1a4b3-ad5a-4bd8-a648-0120aad157ef"

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

# ### Exploration — confirm the real schema before writing the extraction query
#
# The Capacity Metrics app's internal semantic model schema isn't publicly
# documented and shifts between app versions, so rather than guess table/
# column names, this cell lists what's actually there in this tenant. **Run
# this cell first and inspect the printed output before the next cell is
# written** — the DAX query below depends on the real table/column names it
# reveals.

# CELL ********************

import sempy.fabric as fabric

datasets = fabric.list_datasets(workspace=METRICS_WORKSPACE_ID)
print("Datasets in the Capacity Metrics workspace:")
print(datasets[["Dataset Name", "Dataset Id"]].to_string(index=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adjust this if the printed dataset name above isn't exactly this — the app
# has historically shipped under this exact display name, but confirm rather
# than assume.
METRICS_DATASET_NAME = "Fabric Capacity Metrics"

tables = fabric.list_tables(METRICS_DATASET_NAME, workspace=METRICS_WORKSPACE_ID)
print("Tables in the Capacity Metrics semantic model:")
print(tables.to_string(index=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# For each table, list its columns — look specifically for something at the
# grain of "one row per operation/item execution" with a CU(s)/duration
# measure, an item id/name, a workspace id, and a timestamp. That's the table
# this notebook's real extraction query will target.
for _, row in tables.iterrows():
    table_name = row["Name"] if "Name" in tables.columns else row.iloc[0]
    print(f"\n--- {table_name} ---")
    try:
        cols = fabric.list_columns(METRICS_DATASET_NAME, workspace=METRICS_WORKSPACE_ID, additional_xmla_properties=None)
        print(cols[cols["Table Name"] == table_name].to_string(index=False))
    except Exception as exc:
        print(f"  (could not list columns: {exc})")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed schema (2026-07-28)
#
# `CUDetail` is capacity-level only (no ItemId/WorkspaceId) — not useful here.
# `Items` is a dimension (ItemId, ItemName, WorkspaceId, WorkspaceName,
# ItemKind — no CU numbers). The table that actually has what we need is
# **`MetricsByItem`**: grain per `ItemId`, with `sum_CU`, `sum_duration`,
# operation counts by status (`count_successful_operations`,
# `count_failure_operations`, `count_cancelled_operations`,
# `count_rejected_operations`), `WorkspaceId`, `PremiumCapacityId`.
#
# Open question this cell answers: `MetricsByItem` has no visible date
# column, so it's unclear whether `EVALUATE MetricsByItem` returns totals
# already sliced per day, or one rolled-up total per item across the app's
# entire rolling 14-day window (in which case this notebook would need to
# capture it daily as a snapshot and let Silver derive day-over-day deltas,
# similar in spirit to the SCD2 dimension notebooks rather than the
# already-granular append-only fact notebooks).

# CELL ********************

DAX_METRICS_BY_ITEM = "EVALUATE MetricsByItem"
df_metrics_by_item = fabric.evaluate_dax(METRICS_DATASET_NAME, DAX_METRICS_BY_ITEM, workspace=METRICS_WORKSPACE_ID)

print(f"Rows returned: {len(df_metrics_by_item)}")
print(f"Distinct ItemIds: {df_metrics_by_item['MetricsByItem[ItemId]'].nunique() if 'MetricsByItem[ItemId]' in df_metrics_by_item.columns else df_metrics_by_item.filter(like='ItemId').iloc[:, 0].nunique()}")
df_metrics_by_item.head(20)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAX_ITEMS = "EVALUATE Items"
df_items = fabric.evaluate_dax(METRICS_DATASET_NAME, DAX_ITEMS, workspace=METRICS_WORKSPACE_ID)

print(f"Rows returned: {len(df_items)}")
df_items.head(20)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Next step
#
# Run the two cells above and share: (1) the row count and column names
# printed for `MetricsByItem`, (2) whether the same `ItemId` repeats more
# than once (and if so, what differs between those rows — that's the real
# grain/date key), and (3) the `Items` output so item/workspace names can be
# joined in. The append-only write (mirroring `nb_bronze_activity_events`'s
# idempotency pattern) gets added right after, once the grain is confirmed.
