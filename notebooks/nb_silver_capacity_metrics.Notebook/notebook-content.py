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

# ### nb_silver_capacity_metrics
#
# **Layer:** Silver — Cleansing & Enrichment
# **Source:** `lh_governance_bronze` → `raw_capacity_metrics`, joined to `lh_governance_silver` → `silver_items`
# **Destination:** `lh_governance_silver` → Delta Table `silver_capacity_metrics`
# **Depends on:** `nb_silver_items`, `nb_bronze_capacity_metrics`
# **Schedule:** Daily
#
# **Important caveat, read before trusting `delta_cu` for anything precise:**
# `raw_capacity_metrics` is a **daily snapshot of a rolling 14-day total**
# per item (see `nb_bronze_capacity_metrics` markdown), not a lifetime
# cumulative counter. That means `sum_cu - previous day's sum_cu` is only an
# approximation of "what this item cost yesterday" — if a big CU day rolls
# out of the 14-day window between two snapshots, the total can drop even
# though nothing changed, producing a misleading negative delta. Treat
# `delta_cu` as a trend/spike indicator, not an exact daily cost ledger.
# `sum_cu` itself (the rolling total as of each snapshot) is the reliable
# number for "which item is most expensive right now" — that ranking is
# unaffected by the window-rollover issue.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_capacity_metrics"
ITEMS_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_items"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_capacity_metrics"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Items path  : {ITEMS_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Incremental Load from Bronze (by ingestion_date)
#
# `delta_cu` needs the immediately preceding snapshot to compute the window
# function correctly, so the full Bronze history is read every run (it's
# small — one row per item per day) and the `LAG` is computed over all of
# it; only the rows for dates not yet in Silver are actually written out.

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    processed_dates = spark.read.format("delta").load(SILVER_PATH).select("ingestion_date").distinct()
    new_dates = [
        row["ingestion_date"] for row in
        df_bronze.select("ingestion_date").distinct().join(processed_dates, on="ingestion_date", how="left_anti").collect()
    ]
else:
    new_dates = [row["ingestion_date"] for row in df_bronze.select("ingestion_date").distinct().collect()]

print(f"New ingestion dates to process: {len(new_dates)}")

if not new_dates and DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Silver is already up to date. Nothing to process.")
    notebookutils.notebook.exit("UP_TO_DATE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Compute Day-over-Day Delta and Enrich with Item Context

# CELL ********************

item_window = Window.partitionBy("item_id").orderBy("ingestion_date")

df_with_delta = (
    df_bronze
    .withColumn("prev_sum_cu", F.lag("sum_cu").over(item_window))
    .withColumn("delta_cu", F.col("sum_cu") - F.col("prev_sum_cu"))
    .drop("prev_sum_cu")
)

df_new = df_with_delta.filter(F.col("ingestion_date").isin(new_dates))

df_items = (
    spark.read.format("delta").load(ITEMS_PATH)
    .filter("is_current = true")
    .select("item_id", "item_name", "workspace_name")
)

# Left join: an item captured by the Capacity Metrics app but not (yet) seen
# by nb_bronze_items (e.g. a brand-new item) keeps its cost data with a null
# name rather than being dropped — the CU(s) number is still valid audit data.
df_silver = df_new.join(df_items, on="item_id", how="left")

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

# ### Validation — most expensive items as of the latest snapshot

# CELL ********************

latest_date = spark.read.format("delta").load(SILVER_PATH).agg(F.max("ingestion_date")).collect()[0][0]

(spark.read.format("delta").load(SILVER_PATH)
    .filter(F.col("ingestion_date") == latest_date)
    .select("item_name", "workspace_name", "artifact_kind", "sum_cu", "sum_duration_s", "delta_cu")
    .orderBy(F.desc("sum_cu"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
