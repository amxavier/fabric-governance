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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_gold_capacity_forecast
#
# **Layer:** Gold — Capacity Planning & Forecasting
# **Source:** `lh_governance_gold` → `fact_capacity_utilization`, `dim_capacity` (both already built by `nb_gold_governance_model`)
# **Destination:** `lh_governance_gold` → `fact_capacity_forecast`, `capacity_planning_summary`
# **Depends on:** `nb_gold_governance_model`
# **Schedule:** Daily (after the main Gold model)
#
# The native Fabric Capacity Metrics app only exposes a rolling 14-day
# window — structurally incapable of answering "at the current growth
# rate, when do we run out of capacity?" This project persists
# `fact_capacity_utilization` daily with full history (see
# `nb_silver_capacity_cu_detail`'s markdown for why that source is safely
# summable), so that question is finally answerable. This notebook is
# where it gets answered: aggregate to weekly grain to smooth day-of-week
# seasonality, fit a simple linear trend, and project it forward.
#
# **Deliberately not using MLlib or any forecasting library** — a plain
# ordinary-least-squares line, computed from explicit sum aggregates, is
# the whole model. That's a real limitation (no seasonality beyond weekly
# smoothing, no change-point detection, assumes the trend stays linear),
# but it's honest, auditable by anyone who remembers high-school algebra,
# and paired with `n_weeks`/`r_squared`/`forecast_confidence` on every
# output row so nobody mistakes it for more than it is.

# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

_lh_gold = notebookutils.lakehouse.get("lh_governance_gold")
GOLD_ABFS = _lh_gold["properties"]["abfsPath"]

# How far forward to project. Named and hoisted up here rather than
# buried inline in the math below — this is the one input a reader
# would reasonably expect to tune per run.
FORECAST_HORIZON_WEEKS = 52

# Seconds per day, used to turn the guardrail-style `base_capacity_units`
# figure already present on every fact_capacity_utilization row into an
# actual CU budget. Documented assumption, not a silent one: Fabric
# reports capacity in CU(s) — capacity units accumulated per second — so
# a day's budget is base_capacity_units seconds-worth of CU(s) per second,
# i.e. base_capacity_units * seconds_in_a_day.
SECONDS_PER_DAY = 86_400

print(f"[Gold] lh_governance_gold id : {_lh_gold['id']}")
print(f"[Gold] Forecast horizon      : {FORECAST_HORIZON_WEEKS} weeks")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Weekly aggregation
#
# `fact_capacity_utilization` is daily grain, one row per SKU per day.
# Daily CU has real day-of-week seasonality (weekday batch jobs vs. quiet
# weekends) that would otherwise dominate a day-level trend line — rolling
# up to ISO weeks (`date_trunc("week", ...)`, which in Spark truncates to
# the Monday of that week) smooths that out and matches the "did we grow
# over months" question this exists to answer, not "which day was busiest"
# (that's what the Capacity Cost report page is already for).

# CELL ********************

fact_capacity_utilization = spark.read.format("delta").load(f"{GOLD_ABFS}/Tables/fact_capacity_utilization")

weekly_raw = (
    fact_capacity_utilization
    .withColumn("week_start_date", F.date_trunc("week", F.col("date_id")).cast("date"))
    .groupBy("sku", "week_start_date")
    .agg(
        F.sum("sum_cu").alias("cu_actual"),
        # base_capacity_units is a property of the SKU, constant across a
        # week's rows in practice — max() rather than avg()/first() so a
        # genuine mid-week SKU resize (rare, but real) is reflected by the
        # larger of the two rather than silently averaged away.
        F.max("base_capacity_units").alias("base_capacity_units"),
    )
)

print(f"Weekly rows: {weekly_raw.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Week index and per-capacity regression stats
#
# Ordinary least squares, computed from sums rather than `pyspark.ml`
# (no model object to save/load, no extra dependency, and the formulas
# below are the entire algorithm — nothing hidden in a library call):
#
# ```
# slope     = (nΣxy − ΣxΣy) / (nΣx² − (Σx)²)
# intercept = (Σy − slope·Σx) / n
# ```
#
# `x` is `week_index` (0, 1, 2, ... per SKU, in chronological order — not
# a calendar value, so the slope is directly "CU change per week"), `y` is
# `cu_actual`. Grouped by `sku` throughout so this stays correct if this
# tenant ever has more than the one capacity it has today.

# CELL ********************

_week_order = Window.partitionBy("sku").orderBy("week_start_date")
weekly = weekly_raw.withColumn("week_index", F.row_number().over(_week_order) - 1)

regression_sums = (
    weekly.groupBy("sku")
    .agg(
        F.count("*").alias("n_weeks"),
        F.sum("week_index").alias("sum_x"),
        F.sum("cu_actual").alias("sum_y"),
        F.sum(F.col("week_index") * F.col("cu_actual")).alias("sum_xy"),
        F.sum(F.col("week_index") * F.col("week_index")).alias("sum_x2"),
        F.sum(F.col("cu_actual") * F.col("cu_actual")).alias("sum_y2"),
        F.max("base_capacity_units").alias("base_capacity_units"),
        F.max("week_start_date").alias("last_actual_week"),
    )
)

capacity_stats = (
    regression_sums
    .withColumn(
        "slope",
        (F.col("n_weeks") * F.col("sum_xy") - F.col("sum_x") * F.col("sum_y"))
        / (F.col("n_weeks") * F.col("sum_x2") - F.col("sum_x") * F.col("sum_x")),
    )
    .withColumn("intercept", (F.col("sum_y") - F.col("slope") * F.col("sum_x")) / F.col("n_weeks"))
    # R² = 1 − SS_res/SS_tot. SS_tot = Σ(y−ȳ)² = Σy² − (Σy)²/n (standard
    # identity — avoids a second pass over the data just for the mean).
    # SS_res needs actual residuals, computed in the next cell once
    # predictions exist; joined back in below.
    .withColumn("ss_tot", F.col("sum_y2") - (F.col("sum_y") * F.col("sum_y")) / F.col("n_weeks"))
)

capacity_stats.select("sku", "n_weeks", "slope", "intercept", "base_capacity_units").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Residuals, R², and the forecast confidence band
#
# `cu_forecast_lower`/`cu_forecast_upper` use ±1.96×(residual std dev) —
# a 95% interval under the standard assumption that residuals are
# approximately normal. With few data points this band is a rough guide,
# not a guarantee — exactly why `forecast_confidence` exists alongside it.

# CELL ********************

weekly_with_fit = (
    weekly
    .join(capacity_stats.select("sku", "slope", "intercept", "ss_tot", "n_weeks"), on="sku")
    .withColumn("cu_forecast", F.col("intercept") + F.col("slope") * F.col("week_index"))
    .withColumn("residual", F.col("cu_actual") - F.col("cu_forecast"))
)

residual_stats = (
    weekly_with_fit.groupBy("sku")
    .agg(
        F.sum(F.col("residual") * F.col("residual")).alias("ss_res"),
        F.stddev_samp("residual").alias("residual_std"),
    )
)

capacity_stats = (
    capacity_stats
    .join(residual_stats, on="sku")
    # A single-week or perfectly-fit history makes ss_tot/residual_std 0,
    # which would divide-by-zero r_squared or null out the confidence
    # band — guard both rather than let a sparse-data edge case crash a
    # daily pipeline run.
    .withColumn(
        "r_squared",
        F.when(F.col("ss_tot") > 0, 1 - (F.col("ss_res") / F.col("ss_tot"))).otherwise(F.lit(None)),
    )
    .withColumn("residual_std", F.coalesce(F.col("residual_std"), F.lit(0.0)))
    # Thresholds documented, not just asserted: 8 weeks is roughly the
    # minimum to distinguish a real trend from week-to-week noise; among
    # series with enough history, r_squared >= 0.5 (more than half the
    # variance explained by the linear trend) is the bar for "High" vs
    # "Medium" — both are judgment calls, called out here so a future
    # reader can revisit them rather than treat them as physics.
    .withColumn(
        "forecast_confidence",
        F.when(F.col("n_weeks") < 8, F.lit("Low"))
         .when(F.col("r_squared") >= 0.5, F.lit("High"))
         .otherwise(F.lit("Medium")),
    )
    .withColumn(
        "confidence_caveat",
        F.concat(
            F.lit("Projection based on "), F.col("n_weeks"), F.lit(" week(s) of history"),
            F.when(F.col("n_weeks") < 8, F.lit(" — confidence increases as history accumulates.")).otherwise(F.lit(".")),
        ),
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Project forward and assemble `fact_capacity_forecast`
#
# One row per SKU per week, actual history first (`is_forecast = false`,
# `cu_forecast` is the fitted value on that same week so the report can
# show the trend line against real data) followed by
# `FORECAST_HORIZON_WEEKS` future weeks (`cu_actual` null,
# `is_forecast = true`).

# CELL ********************

# Future rows: continue week_index past the last observed week, one row
# per SKU per offset. crossJoin is safe here — both sides are tiny
# (one row per SKU × FORECAST_HORIZON_WEEKS), nowhere near a real
# cardinality risk.
future_offsets = spark.range(1, FORECAST_HORIZON_WEEKS + 1).withColumnRenamed("id", "future_offset")

future = (
    capacity_stats
    .select("sku", "n_weeks", "slope", "intercept", "last_actual_week")
    .crossJoin(future_offsets)
    .withColumn("week_index", F.col("n_weeks") - 1 + F.col("future_offset"))
    .withColumn("week_start_date", F.date_add(F.col("last_actual_week"), (F.col("future_offset") * 7).cast("int")))
    .withColumn("cu_actual", F.lit(None).cast("double"))
    .withColumn("cu_forecast", F.col("intercept") + F.col("slope") * F.col("week_index"))
    .withColumn("is_forecast", F.lit(True))
    .select("sku", "week_start_date", "cu_actual", "cu_forecast", "week_index", "is_forecast")
)

actual = (
    weekly_with_fit
    .withColumn("is_forecast", F.lit(False))
    .select("sku", "week_start_date", "cu_actual", "cu_forecast", "week_index", "is_forecast")
)

fact_capacity_forecast = (
    actual.unionByName(future)
    .join(
        capacity_stats.select(
            "sku", "residual_std", "base_capacity_units",
        ),
        on="sku",
    )
    .withColumn("cu_forecast_lower", F.col("cu_forecast") - 1.96 * F.col("residual_std"))
    .withColumn("cu_forecast_upper", F.col("cu_forecast") + 1.96 * F.col("residual_std"))
    # Weekly budget derived from the same base_capacity_units already
    # shipped on every source row (see the SECONDS_PER_DAY comment above)
    # — a constant per SKU, repeated on every week's row so the report can
    # plot it as a flat reference line without a second lookup.
    .withColumn("capacity_weekly_cu_limit", F.col("base_capacity_units") * SECONDS_PER_DAY * 7)
    .select(
        "sku", "week_start_date", "cu_actual", "cu_forecast",
        "cu_forecast_lower", "cu_forecast_upper", "capacity_weekly_cu_limit", "is_forecast",
    )
)

(fact_capacity_forecast.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/fact_capacity_forecast"))
print(f"fact_capacity_forecast: {fact_capacity_forecast.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### `capacity_planning_summary`
#
# One row per capacity — the decision-card grain the report page needs.
# `projected_saturation_date` falls out naturally rather than needing an
# explicit "if slope <= 0" branch: with a flat or negative slope,
# `cu_forecast` never crosses `capacity_weekly_cu_limit` in any future
# row, so the filter below simply finds nothing and `min()` over an empty
# set is null — exactly the "no saturation predicted" answer we want.

# CELL ********************

dim_capacity = spark.read.format("delta").load(f"{GOLD_ABFS}/Tables/dim_capacity")

saturation = (
    fact_capacity_forecast
    .filter("is_forecast = true and cu_forecast >= capacity_weekly_cu_limit")
    .groupBy("sku")
    .agg(F.min("week_start_date").alias("projected_saturation_date"))
)

# "Current" headroom uses the single most recent actual week, not the
# regression average — a manager asking "how close are we today" wants
# where we actually are right now, not where the trend line says we
# should be.
latest_actual = (
    fact_capacity_forecast
    .filter("is_forecast = false")
    .withColumn("_rank", F.row_number().over(Window.partitionBy("sku").orderBy(F.col("week_start_date").desc())))
    .filter("_rank = 1")
    .select("sku", F.col("cu_actual").alias("latest_week_cu"), "capacity_weekly_cu_limit")
)

capacity_planning_summary = (
    capacity_stats
    .join(dim_capacity.select("capacity_id", "sku"), on="sku", how="left")
    .join(saturation, on="sku", how="left")
    .join(latest_actual, on="sku", how="left")
    .withColumn("growth_cu_per_week", F.col("slope"))
    .withColumn("growth_pct_per_week", F.col("slope") / (F.col("sum_y") / F.col("n_weeks")) * 100)
    .withColumn(
        "current_headroom_pct",
        (F.col("capacity_weekly_cu_limit") - F.col("latest_week_cu")) / F.col("capacity_weekly_cu_limit") * 100,
    )
    .withColumn("capacity_daily_cu_limit", F.col("base_capacity_units") * SECONDS_PER_DAY)
    .withColumn(
        "weeks_to_saturation",
        F.when(
            F.col("projected_saturation_date").isNotNull(),
            F.datediff(F.col("projected_saturation_date"), F.col("last_actual_week")) / 7,
        ),
    )
    .select(
        "capacity_id", "growth_cu_per_week", "growth_pct_per_week", "current_headroom_pct",
        "capacity_daily_cu_limit", "projected_saturation_date", "weeks_to_saturation",
        F.col("n_weeks").alias("n_weeks_history"), "r_squared", "forecast_confidence", "confidence_caveat",
    )
)

(capacity_planning_summary.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").save(f"{GOLD_ABFS}/Tables/capacity_planning_summary"))
print(f"capacity_planning_summary: {capacity_planning_summary.count()} rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

for table in ["fact_capacity_forecast", "capacity_planning_summary"]:
    count = spark.read.format("delta").load(f"{GOLD_ABFS}/Tables/{table}").count()
    print(f"{table:28s}: {count} rows")

print()
capacity_planning_summary.show(truncate=False)

print("fact_capacity_forecast, ordered by week (actual history followed by the forecast horizon):")
fact_capacity_forecast.orderBy("sku", "week_start_date").show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
