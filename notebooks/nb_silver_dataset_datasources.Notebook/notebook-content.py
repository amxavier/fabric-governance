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

# ### nb_silver_dataset_datasources
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_governance_bronze` → `raw_dataset_datasources`, joined to `lh_governance_silver` → `silver_items`
# **Destination:** `lh_governance_silver` → Delta Table `silver_dataset_datasources`
# **Depends on:** `nb_silver_items`
# **Schedule:** Daily (after Bronze and nb_silver_items)
#
# Adds the dataset's own name and workspace — a bare dataset GUID and a
# gateway GUID aren't where an incident diagnosis actually starts from.
# Same grain and SCD2 shape as Bronze.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_dataset_datasources"
ITEMS_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_items"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_dataset_datasources"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Items path  : {ITEMS_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enrich with Dataset Name and Workspace

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

df_items = (
    spark.read.format("delta").load(ITEMS_PATH)
    .filter("is_current = true")
    .select(
        F.col("item_id").alias("dataset_id"),
        F.col("item_name").alias("dataset_name"),
        F.col("workspace_name"),
    )
)

# Left join: a datasource entry for a dataset not (yet) seen by
# nb_silver_items keeps its row with a null name rather than being
# dropped — the gateway/datasource dependency is still valid audit data.
df_silver = (
    df_bronze
    .join(df_items, on="dataset_id", how="left")
    .dropDuplicates(["dataset_id", "datasource_id", "valid_from"])
)

print(f"Rows: {df_silver.count()}")
df_silver.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Overwrite Silver (Silver mirrors Bronze's SCD2 state exactly, no separate merge needed)

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
    .select("dataset_name", "workspace_name", "datasource_type", "gateway_id", "connection_details_json")
    .show(truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
