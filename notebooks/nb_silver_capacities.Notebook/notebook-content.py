# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00000000-0000-0000-0000-0000000000e2",
# META       "default_lakehouse_name": "lh_silver_governance",
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

# ### nb_silver_capacities
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_bronze_governance` → Delta Table `raw_capacities`
# **Destination:** `lh_silver_governance` → Delta Table `silver_capacities`
# **Schedule:** Daily (after Bronze)
#
# Same grain and SCD2 shape as Bronze (already applied at that layer for this
# project) — Silver here is about typing and a governance-relevant derived
# flag, not about adding history.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_bronze_governance")
_lh_silver = notebookutils.lakehouse.get("lh_silver_governance")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_capacities"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_capacities"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean and Enrich

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# Trial/Fabric-trial SKUs auto-expire and are a common, easy-to-miss cause of
# sudden refresh failures across every item on that capacity — flagging it
# explicitly here saves a manual lookup during incident diagnosis.
df_silver = (
    df_bronze
    .withColumnRenamed("display_name", "capacity_name")
    .withColumn("is_trial_sku", F.col("sku").rlike("(?i)^(trial|ft1)"))
    .dropDuplicates(["capacity_id", "valid_from"])
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

# Bronze already resolved SCD2 (valid_from/valid_to/is_current); Silver just
# re-derives its own copy from Bronze's current full history each run rather
# than incrementally merging — cheap at this row volume (capacities are few)
# and avoids a second, redundant SCD2 implementation for the same grain.
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

spark.read.format("delta").load(SILVER_PATH).filter("is_current = true").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
