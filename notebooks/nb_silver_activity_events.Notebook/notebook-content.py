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

# ### nb_silver_activity_events
#
# **Layer:** Silver — Cleansing & Enrichment, append-only
# **Source:** `lh_governance_bronze` → Delta Table `raw_activity_events`, joined to `lh_governance_silver` → `silver_items`
# **Destination:** `lh_governance_silver` → Delta Table `silver_activity_events`
# **Depends on:** `nb_silver_items`
# **Schedule:** Daily (after Bronze and nb_silver_items)
#
# Append-only, same grain as Bronze — processed incrementally by ingestion_date
# (like Bronze's own idempotency check) rather than a full overwrite, since
# this table grows unbounded over time unlike the dimension tables above.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_activity_events"
ITEMS_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_items"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_activity_events"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Items path  : {ITEMS_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Load from Bronze

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    processed_dates = (
        spark.read.format("delta").load(SILVER_PATH)
        .select("ingestion_date").distinct()
    )
    df_new = df_bronze.join(processed_dates, on="ingestion_date", how="left_anti")
else:
    df_new = df_bronze

new_count = df_new.count()
print(f"New records to process: {new_count}")

if new_count == 0:
    print("Silver is already up to date. Nothing to process.")
    notebookutils.notebook.exit("UP_TO_DATE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Enrich with Item Type and Standardize Status

# CELL ********************

df_items = (
    spark.read.format("delta").load(ITEMS_PATH)
    .filter("is_current = true")
    .select(
        F.col("item_id").alias("object_id"),
        F.col("item_type"),
        F.col("workspace_name"),
    )
)

# object_id in the activity log matches item_id in Bronze/Silver items for most
# activity types (PublishReport, RefreshDataset, ...) — but not all (e.g.
# workspace-level or tenant-level activities have no object_id). A left join
# is intentional: unmatched rows keep item_type/workspace_name as NULL rather
# than being dropped, since the activity event itself is still valid audit data.
df_silver = (
    df_new
    .join(df_items, on="object_id", how="left")
    .withColumn("is_failure", F.col("result_status").rlike("(?i)fail"))
    .dropDuplicates(["event_id"])
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
    .groupBy("activity", "is_failure")
    .count()
    .orderBy(F.desc("count"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
