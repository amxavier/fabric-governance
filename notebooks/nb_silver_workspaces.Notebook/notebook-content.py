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

# ### nb_silver_workspaces
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_bronze_governance` → Delta Table `raw_workspaces`, joined to `lh_silver_governance` → `silver_capacities`
# **Destination:** `lh_silver_governance` → Delta Table `silver_workspaces`
# **Depends on:** `nb_silver_capacities`
# **Schedule:** Daily (after Bronze and nb_silver_capacities)
#
# Enriches each workspace snapshot with its capacity's friendly name and
# trial-SKU flag — "this workspace is on a Trial capacity" is a much faster
# diagnostic signal than a bare capacity GUID.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F

_lh_bronze = notebookutils.lakehouse.get("lh_bronze_governance")
_lh_silver = notebookutils.lakehouse.get("lh_silver_governance")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_workspaces"
CAPACITIES_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_capacities"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_workspaces"

print(f"[Silver] Bronze path     : {BRONZE_PATH}")
print(f"[Silver] Capacities path : {CAPACITIES_PATH}")
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

# Join against the capacity's FULL history (not just is_current), matched by
# validity window — not just capacity_id — so a workspace snapshot from three
# months ago is enriched with what the capacity actually was back then, not
# with today's capacity_name/is_trial_sku. Joining only against is_current
# would silently rewrite history every time a capacity changes, defeating the
# whole point of carrying valid_from/valid_to through Bronze and Silver.
df_capacities = (
    spark.read.format("delta").load(CAPACITIES_PATH)
    .select(
        "capacity_id", "capacity_name", "is_trial_sku",
        F.col("valid_from").alias("cap_valid_from"),
        F.col("valid_to").alias("cap_valid_to"),
    )
)

df_silver = (
    df_bronze.alias("w")
    .join(
        df_capacities.alias("c"),
        on=(
            (F.col("w.capacity_id") == F.col("c.capacity_id"))
            & (F.col("w.valid_from") >= F.col("c.cap_valid_from"))
            & (F.col("c.cap_valid_to").isNull() | (F.col("w.valid_from") < F.col("c.cap_valid_to")))
        ),
        how="left",
    )
    .select("w.*", "capacity_name", "is_trial_sku")
    .dropDuplicates(["workspace_id", "valid_from"])
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
    .groupBy("is_trial_sku")
    .count()
    .show())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
