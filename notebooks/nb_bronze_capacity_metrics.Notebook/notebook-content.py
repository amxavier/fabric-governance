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

# ### Next step
#
# Run the three cells above and share the printed output — the DAX query,
# the DataFrame build, and the append-only write (mirroring
# `nb_bronze_activity_events`'s idempotency pattern, keyed by whatever this
# model's unique row identifier turns out to be) will be added right after,
# once the real table/column names are confirmed.
