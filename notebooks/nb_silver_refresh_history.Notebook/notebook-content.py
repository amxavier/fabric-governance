# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-0000000000e2",
# META       "default_lakehouse_name": "lh_governance_silver",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "00000000-0000-0000-0000-0000000000e2"
# META         },
# META         {
# META           "id": "00000000-0000-0000-0000-0000000000e1"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_silver_refresh_history
#
# **Layer:** Silver — Cleansing & Enrichment, append-only
# **Source:** `lh_governance_bronze` → Delta Table `raw_refresh_history`, joined to `lh_governance_silver` → `silver_items`
# **Destination:** `lh_governance_silver` → Delta Table `silver_refresh_history`
# **Depends on:** `nb_silver_items`
# **Schedule:** Daily (after Bronze and nb_silver_items)
#
# Adds duration and a standardized failure flag, plus the dataset's name and
# workspace — the fields an incident diagnosis actually starts from, instead
# of a bare dataset GUID and a raw status string.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_refresh_history"
ITEMS_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_items"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_refresh_history"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Items path  : {ITEMS_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Load from Bronze (by refresh_id)

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    existing_ids = spark.read.format("delta").load(SILVER_PATH).select("refresh_id").distinct()
    df_new = df_bronze.join(existing_ids, on="refresh_id", how="left_anti")
else:
    df_new = df_bronze

new_count = df_new.count()
print(f"New refresh runs to process: {new_count}")

if new_count == 0:
    print("Silver is already up to date. Nothing to process.")
    notebookutils.notebook.exit("UP_TO_DATE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enrich with Item Context and Derive Diagnostics

# CELL ********************

df_items = (
    spark.read.format("delta").load(ITEMS_PATH)
    .filter("is_current = true")
    .select(
        F.col("item_id").alias("dataset_id"),
        F.col("item_name").alias("dataset_name"),
        F.col("workspace_name"),
    )
)

df_silver = (
    df_new
    .join(df_items, on="dataset_id", how="left")
    .withColumn("duration_seconds", F.col("end_time").cast("long") - F.col("start_time").cast("long"))
    .withColumn("is_failure", F.col("status").rlike("(?i)fail"))
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
    .groupBy("dataset_name", "is_failure")
    .agg(F.count("*").alias("runs"), F.avg("duration_seconds").alias("avg_duration_s"))
    .orderBy(F.desc("runs"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
