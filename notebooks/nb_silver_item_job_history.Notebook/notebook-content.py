# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "86b1a934-7bcd-488d-85fe-fddbf9ce837d",
# META       "default_lakehouse_name": "lh_governance_silver",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "86b1a934-7bcd-488d-85fe-fddbf9ce837d"
# META         },
# META         {
# META           "id": "7eb57c95-5be1-421d-aee0-8ac6bde14d68"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_silver_item_job_history
#
# **Layer:** Silver — Cleansing & Enrichment, append-only
# **Source:** `lh_governance_bronze` → `raw_item_job_history`, joined to `lh_governance_silver` → `silver_workspaces`
# **Destination:** `lh_governance_silver` → Delta Table `silver_item_job_history`
# **Depends on:** `nb_silver_workspaces`
# **Schedule:** Daily (after Bronze and nb_silver_workspaces)
#
# Adds the workspace's name — `item_name`/`item_type` already came through
# from Bronze directly (unlike `refresh_history`, this source's own API
# response is per-item, so there's no bare-GUID join needed for those).
# Same grain as Bronze, incremental by `job_id` — append-only, no SCD.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_item_job_history"
WORKSPACES_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_workspaces"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_item_job_history"

print(f"[Silver] Bronze path     : {BRONZE_PATH}")
print(f"[Silver] Workspaces path : {WORKSPACES_PATH}")
print(f"[Silver] Silver path     : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Load from Bronze (by job_id)

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    existing_ids = spark.read.format("delta").load(SILVER_PATH).select("job_id").distinct()
    df_new = df_bronze.join(existing_ids, on="job_id", how="left_anti")
else:
    df_new = df_bronze

new_count = df_new.count()
print(f"New job instances to process: {new_count}")

# Only skip if Silver already exists — a fresh deployment with zero job
# history anywhere yet still needs Silver initialized with the right
# schema, so Gold's read of silver_item_job_history doesn't fail with
# PATH_NOT_FOUND — same lesson as nb_silver_refresh_history.
if new_count == 0 and DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Silver is already up to date. Nothing to process.")
    notebookutils.notebook.exit("UP_TO_DATE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enrich with Workspace Name

# CELL ********************

df_workspaces = (
    spark.read.format("delta").load(WORKSPACES_PATH)
    .filter("is_current = true")
    .select(F.col("workspace_id"), F.col("workspace_name"))
)

df_silver = (
    df_new
    .join(df_workspaces, on="workspace_id", how="left")
    .dropDuplicates(["job_id"])
)

print(f"Rows to write: {df_silver.count()}")
df_silver.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Append to Silver

# CELL ********************

(df_silver.write.format("delta").mode("append")
    .option("mergeSchema", "true").save(SILVER_PATH))

print(f"{df_silver.count()} records written to {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(SILVER_PATH)
    .groupBy("item_name", "item_type", "is_failure")
    .agg(F.count("*").alias("runs"), F.avg("duration_seconds").alias("avg_duration_s"))
    .orderBy(F.desc("runs"))
    .show(30, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
