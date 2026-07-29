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

# ### nb_silver_capacity_cu_detail
#
# **Layer:** Silver — Cleansing & Enrichment (grain rollup)
# **Source:** `lh_governance_bronze` → `raw_capacity_cu_detail` (30-second buckets)
# **Destination:** `lh_governance_silver` → Delta Table `silver_capacity_cu_detail` (daily grain)
# **Depends on:** `nb_bronze_capacity_cu_detail`
# **Schedule:** Daily
#
# Rolls the 30-second Bronze buckets up to **one row per SKU per day** —
# matching the daily grain the rest of this star schema uses (`dim_date`),
# and the right resolution for the "did we grow over months" / "do we need
# more capacity next year" questions this source exists to answer. Nobody
# needs 30-second resolution a year later for that; daily is both plenty and
# far cheaper to query/plot over a long history.
#
# `sum_cu` here is genuinely additive (unlike `silver_capacity_metrics`'s
# rolling-window `sum_cu`) — these are immutable past buckets, so summing
# a day, a month, or a year of them gives a real total.
#
# **Full recompute, not incremental append:** a calendar day's Bronze
# buckets can arrive across more than one Bronze run (e.g. today's partial
# data now, completed tomorrow), so aggregating only "new" rows per run
# risks under-counting a day that was previously seen as partial. Bronze
# volume is modest (~2,880 rows/day per SKU), so re-aggregating the full
# history every run and overwriting Silver is simple and avoids that class
# of bug — same tradeoff `nb_gold_governance_model` already makes for
# `fact_activity`/`fact_refresh`.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_capacity_cu_detail"
SILVER_PATH = f"{_lh_silver['properties']['abfsPath']}/Tables/silver_capacity_cu_detail"

print(f"[Silver] Bronze path : {BRONZE_PATH}")
print(f"[Silver] Silver path : {SILVER_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Aggregate to Daily Grain

# CELL ********************

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

# A full day at 30s buckets is 24h * 60min / 0.5min = 2,880 buckets — used
# below only as a transparency signal (buckets_captured), not a hard
# validation, since the first and most recent day in the history will
# always be partial (capture started mid-day / today isn't over yet).
df_silver = (
    df_bronze
    .withColumn("date_id", F.to_date("window_start_time"))
    .groupBy("sku", "date_id")
    .agg(
        F.count("*").alias("buckets_captured"),
        F.sum("cus").alias("sum_cu"),
        F.avg("cus").alias("avg_cu"),
        F.max("cus").alias("max_cu"),
        F.avg("interactive").alias("avg_interactive"),
        F.avg("background").alias("avg_background"),
        F.max("interactive_delay_pct").alias("max_interactive_delay_pct"),
        F.max("interactive_rejection_pct").alias("max_interactive_rejection_pct"),
        F.max("background_rejection_pct").alias("max_background_rejection_pct"),
        F.first("base_capacity_units").alias("base_capacity_units"),
    )
)

print(f"Rows to write: {df_silver.count()}")
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
    .option("overwriteSchema", "true").save(SILVER_PATH))

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
    .select("sku", "date_id", "buckets_captured", "sum_cu", "avg_cu")
    .orderBy(F.desc("date_id"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
