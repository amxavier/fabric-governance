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

# ### nb_silver_items
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_governance_bronze` → Delta Table `raw_items`, joined to `lh_governance_silver` → `silver_workspaces`
# **Destination:** `lh_governance_silver` → Delta Table `silver_items`
# **Depends on:** `nb_silver_workspaces`
# **Schedule:** Daily (after Bronze and nb_silver_workspaces)
#
# Adds the workspace's friendly name and capacity context to every item, so
# downstream Gold/reporting never has to join back through raw workspace IDs.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"
WORKSPACES_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_workspaces"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_items"

print(f"[Silver] Bronze path     : {BRONZE_PATH}")
print(f"[Silver] Workspaces path : {WORKSPACES_PATH}")
print(f"[Silver] Silver path     : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean and Enrich

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# Join against the workspace's FULL history (not just is_current), matched by
# validity window, for the same reason nb_silver_workspaces joins capacities
# by window rather than by is_current — see that notebook's comment.
df_workspaces = (
    spark.read.format("delta").load(WORKSPACES_PATH)
    .select(
        "workspace_id", "workspace_name", "capacity_name", "is_trial_sku",
        F.col("valid_from").alias("ws_valid_from"),
        F.col("valid_to").alias("ws_valid_to"),
    )
)

df_silver = (
    df_bronze.alias("i")
    .join(
        df_workspaces.alias("w"),
        on=(
            (F.col("i.workspace_id") == F.col("w.workspace_id"))
            & (F.col("i.valid_from") >= F.col("w.ws_valid_from"))
            & (F.col("w.ws_valid_to").isNull() | (F.col("i.valid_from") < F.col("w.ws_valid_to")))
        ),
        how="left",
    )
    .select("i.*", "workspace_name", "capacity_name", "is_trial_sku")
    .dropDuplicates(["item_id", "valid_from"])
)

print(f"Rows: {df_silver.count()}")
df_silver.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Overwrite Silver

# CELL ********************

(df_silver.write.format("delta").mode("overwrite")
    .option("mergeSchema", "true").option("overwriteSchema", "true")
    .save(SILVER_PATH))

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
    .filter("is_current = true")
    .groupBy("item_type")
    .count()
    .orderBy(F.desc("count"))
    .show())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
