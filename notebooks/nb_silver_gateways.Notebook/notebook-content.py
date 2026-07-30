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

# ### nb_silver_gateways
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_governance_bronze` → Delta Table `raw_gateways`
# **Destination:** `lh_governance_silver` → Delta Table `silver_gateways`
# **Schedule:** Daily (after Bronze)
#
# Same grain and SCD2 shape as Bronze — Silver here is a straight pass-
# through, not real enrichment. `gateway_annotation` (a JSON-encoded string
# per Microsoft's `Gateway` object) is deliberately left unparsed: this
# tenant has zero real gateways to check its actual shape against, and
# guessing at nested field names here would repeat a mistake this project
# has hit before elsewhere. Parse it once a real gateway's annotation can
# be inspected.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_gateways"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_gateways"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Overwrite Silver (Silver mirrors Bronze's SCD2 state exactly, no separate merge needed)

# CELL ********************

# Bronze already resolved SCD2 (valid_from/valid_to/is_current); Silver just
# re-derives its own copy from Bronze's current full history each run —
# same reasoning as nb_silver_capacities, cheap at this row volume.
df_silver = spark.read.format("delta").load(BRONZE_PATH).dropDuplicates(["gateway_id", "valid_from"])

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
