# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "50efdcbf-bfca-48a0-ab39-8b5a52ed407f",
# META       "default_lakehouse_name": "lh_governance_gold",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "50efdcbf-bfca-48a0-ab39-8b5a52ed407f"
# META         },
# META         {
# META           "id": "86b1a934-7bcd-488d-85fe-fddbf9ce837d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_gold_governance_model
#
# **Layer:** Gold — Governance Star Schema
# **Source:** `lh_governance_silver` → `silver_capacities`, `silver_workspaces`, `silver_items`, `silver_activity_events`, `silver_refresh_history`, `silver_capacity_metrics`, `silver_capacity_cu_detail`
# **Destination:** `lh_governance_gold` → `dim_capacity`, `dim_workspace`, `dim_item`, `dim_user`, `dim_date`, `fact_activity`, `fact_refresh`, `fact_capacity_consumption`, `fact_capacity_utilization`
# **Depends on:** all five Silver notebooks
# **Schedule:** Daily (last step in the pipeline)
#
# This is the schema the Semantic Model (`sm_governance_medallion`) is built
# directly on top of. The motivating question this project exists to answer —
# "did this refresh fail because someone changed something?" — is answered by
# joining `fact_refresh` failures against `fact_activity` for the same item in
# the preceding window, which both facts support via a shared `dim_item` key.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

_lh_silver = notebookutils.lakehouse.get("lh_governance_silver")
_lh_gold = notebookutils.lakehouse.get("lh_governance_gold")
SILVER_ABFS = _lh_silver["properties"]["abfsPath"]
GOLD_ABFS = _lh_gold["properties"]["abfsPath"]

print(f"[Gold] lh_governance_silver id : {_lh_silver['id']}")
print(f"[Gold] lh_governance_gold id   : {_lh_gold['id']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_capacity

# CELL ********************

dim_capacity = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_capacities")
    .filter("is_current = true")
    .select("capacity_id", "capacity_name", "sku", "region", "state", "is_trial_sku")
)

(dim_capacity.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/dim_capacity"))
print(f"dim_capacity: {dim_capacity.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_workspace

# CELL ********************

dim_workspace = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_workspaces")
    .filter("is_current = true")
    .select("workspace_id", "workspace_name", "workspace_type", "capacity_id", "state", "is_trial_sku")
)

(dim_workspace.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/dim_workspace"))
print(f"dim_workspace: {dim_workspace.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_item

# CELL ********************

dim_item = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_items")
    .filter("is_current = true")
    .select("item_id", "item_name", "item_type", "workspace_id", "state", "description")
)

(dim_item.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/dim_item"))
print(f"dim_item: {dim_item.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_user

# CELL ********************

# Sourced only from activity events — refresh_history has no user attribution
# (see nb_bronze_refresh_history markdown for why), so dim_user's population
# is inherently limited to users who triggered a logged activity.
dim_user = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_activity_events")
    .filter("user_id is not null")
    .select("user_id")
    .distinct()
    .withColumnRenamed("user_id", "user_key")
    .withColumn("user_email", F.col("user_key"))
)

(dim_user.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/dim_user"))
print(f"dim_user: {dim_user.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### dim_date

# CELL ********************

# Span from the earliest observed event/refresh to today, with a 30-day
# forward buffer so the semantic model's date table doesn't need a rebuild
# just because "today" moved past its last row.
import datetime

activity_dates = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_activity_events")
    .select(F.to_date("creation_time").alias("d"))
)
refresh_dates = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_refresh_history")
    .select(F.to_date("start_time").alias("d"))
)
capacity_metrics_dates = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_capacity_metrics")
    .select(F.col("ingestion_date").alias("d"))
)
cu_detail_dates = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_capacity_cu_detail")
    .select(F.col("date_id").alias("d"))
)
observed = (
    activity_dates.union(refresh_dates).union(capacity_metrics_dates).union(cu_detail_dates)
    .filter("d is not null")
)
bounds = observed.agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).collect()[0]

# On a fresh deployment (day 1), both fact sources are empty and min_d/max_d
# are None — fall back to today explicitly here in Python rather than relying
# on Spark-side null-coalescing, so the range is never accidentally empty/null.
today = datetime.date.today()
start_date = bounds["min_d"] if bounds["min_d"] is not None else today
end_date = (bounds["max_d"] if bounds["max_d"] is not None else today) + datetime.timedelta(days=30)

date_range = (
    spark.createDataFrame([(1,)], ["_"])
    .select(F.explode(F.sequence(
        F.lit(start_date).cast(DateType()),
        F.lit(end_date).cast(DateType()),
        F.expr("interval 1 day"),
    )).alias("date_id"))
)

dim_date = (
    date_range
    .withColumn("year", F.year("date_id"))
    .withColumn("month", F.month("date_id"))
    .withColumn("month_name", F.date_format("date_id", "MMMM"))
    .withColumn("day", F.dayofmonth("date_id"))
    .withColumn("day_name", F.date_format("date_id", "EEEE"))
    .withColumn("year_month", F.date_format("date_id", "yyyy-MM"))
    .withColumn("is_weekend", F.dayofweek("date_id").isin(1, 7))
)

(dim_date.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/dim_date"))
print(f"dim_date: {dim_date.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_activity

# CELL ********************

fact_activity = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_activity_events")
    .select(
        "event_id",
        "creation_time",
        F.to_date("creation_time").alias("date_id"),
        "activity",
        "user_id",
        F.col("object_id").alias("item_id"),
        "workspace_id",
        "refresh_type",
        "result_status",
        "is_failure",
    )
)

(fact_activity.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/fact_activity"))
print(f"fact_activity: {fact_activity.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_refresh

# CELL ********************

fact_refresh = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_refresh_history")
    .select(
        "refresh_id",
        F.col("dataset_id").alias("item_id"),
        F.to_date("start_time").alias("date_id"),
        "start_time",
        "end_time",
        "duration_seconds",
        "refresh_type",
        "status",
        "is_failure",
        "error_json",
    )
)

(fact_refresh.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/fact_refresh"))
print(f"fact_refresh: {fact_refresh.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_capacity_consumption
#
# Grain: one row per item per captured day. `date_id` is the snapshot's
# `ingestion_date`, not an event timestamp like the other two facts — see
# `nb_silver_capacity_metrics` for why `delta_cu` is a trend indicator, not
# an exact daily cost, due to the source's rolling-14-day-window nature.

# CELL ********************

fact_capacity_consumption = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_capacity_metrics")
    .select(
        "item_id",
        "workspace_id",
        F.col("ingestion_date").alias("date_id"),
        "artifact_kind",
        "billing_type",
        "sum_cu",
        "sum_duration_s",
        "delta_cu",
        "count_operations",
        "count_successful_operations",
        "count_failure_operations",
        "count_cancelled_operations",
        "count_rejected_operations",
        "count_users",
    )
)

(fact_capacity_consumption.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/fact_capacity_consumption"))
print(f"fact_capacity_consumption: {fact_capacity_consumption.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_capacity_utilization
#
# Grain: one row per SKU per day. Unlike `fact_capacity_consumption`
# (item-level, rolling-window `sum_cu` — a trend indicator), `sum_cu` here
# is a genuine daily total (rolled up from immutable 30-second buckets in
# `silver_capacity_cu_detail`) — safe to sum across any date range for
# real growth/capacity-planning numbers ("total CU used last quarter vs.
# this quarter", "are we trending toward the SKU's ceiling").

# CELL ********************

fact_capacity_utilization = (
    spark.read.format("delta").load(f"{SILVER_ABFS}/Tables/silver_capacity_cu_detail")
    .select(
        "sku",
        "date_id",
        "buckets_captured",
        "sum_cu",
        "avg_cu",
        "max_cu",
        "avg_interactive",
        "avg_background",
        "max_interactive_delay_pct",
        "max_interactive_rejection_pct",
        "max_background_rejection_pct",
        "base_capacity_units",
    )
)

(fact_capacity_utilization.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/fact_capacity_utilization"))
print(f"fact_capacity_utilization: {fact_capacity_utilization.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

for table in ["dim_capacity", "dim_workspace", "dim_item", "dim_user", "dim_date", "fact_activity", "fact_refresh", "fact_capacity_consumption", "fact_capacity_utilization"]:
    count = spark.read.format("delta").load(f"{GOLD_ABFS}/Tables/{table}").count()
    print(f"{table:20s}: {count} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
