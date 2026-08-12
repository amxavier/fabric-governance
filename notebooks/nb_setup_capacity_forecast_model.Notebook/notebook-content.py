# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ### nb_setup_capacity_forecast_model
#
# **Run this manually, once per environment**, after `nb_gold_capacity_forecast`
# has run at least once (so `fact_capacity_forecast`/`capacity_planning_summary`
# exist as real Delta tables in `lh_governance_gold` — Direct Lake needs the
# table to exist before it can bind a partition to it).
#
# Adds the two new tables to the live `sm_governance_medallion` model via TOM
# (same reasoning as `nb_setup_semantic_model_relationships`: incremental TOM
# writes sidestep the bulk-TMDL-import "ambiguous paths" validator that a full
# redeploy would hit). Mirrors
# `semantic models/sm_governance_medallion.SemanticModel/definition/tables/fact_capacity_forecast.tmdl`
# and `.../capacity_planning_summary.tmdl` — if those change, update this to match.
#
# **Deliberately does NOT activate `fact_capacity_forecast.sku -> dim_capacity.sku`**
# — that relationship is added but left inactive here, same caution already
# applied to `fact_capacity_utilization.sku -> dim_capacity.sku`. Reactivating
# either is its own separate, reviewed step per
# `docs/design/capacity-planning-forecasting.md`'s guardrails — don't fold it
# into this run.
#
# **Must be run interactively (your own sign-in), not via the pipeline's
# Service Principal identity** — TOM/XMLA write operations reject app-only
# Service Principal tokens on this tenant (see README, "Delegated
# Authentication for /admin/*" — same rejection pattern).
#
# Edit `DATASET`/`WORKSPACE` below to point at the environment you're adding
# this to, then run all cells top to bottom.

# CELL ********************

DATASET = "sm_governance_medallion"    # change if testing against a throwaway copy
WORKSPACE = "lakehouses_dev"           # lakehouses_dev / lakehouses_qa / lakehouses_prd


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Install semantic-link-labs

# CELL ********************

get_ipython().run_line_magic("pip", "install semantic-link-labs")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Resolve the Direct Lake expression name live
#
# Not hardcoded — the expression name isn't guaranteed to match across
# environments (confirmed live 2026-08-12: DEV's is
# `'DirectLake - lh_governance_gold_dev'`, with a suffix the
# git-checked-in `expressions.tmdl` doesn't have, since that file is never
# auto-synced — `deploy.py` excludes the whole SemanticModel folder from
# deploy to protect the interactively-built Direct Lake binding).
#
# `tom.model.Expressions` is a .NET `NamedMetadataObjectCollection` —
# iterable, but its indexer only accepts a string name, not an int position
# (`Expressions[0]` raises `TypeError: No method matches ... get_Item:
# (<class 'int'>)`, confirmed live) — so pull the name via iteration, not
# positional indexing.

# CELL ********************

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=True) as tom:
    EXPRESSION = [e.Name for e in tom.model.Expressions][0]
print(f"Using Direct Lake expression: {EXPRESSION!r}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add the two tables (columns + Direct Lake partition) and their relationships

# CELL ********************

fact_capacity_forecast_columns = [
    # (column_name, data_type, format_string, summarize_by)
    ("sku", "String", None, "None"),
    ("week_start_date", "DateTime", "General Date", "None"),
    ("cu_actual", "Double", None, "Sum"),
    ("cu_forecast", "Double", None, "Sum"),
    ("cu_forecast_lower", "Double", None, "Sum"),
    ("cu_forecast_upper", "Double", None, "Sum"),
    ("capacity_weekly_cu_limit", "Double", None, "Sum"),
    ("is_forecast", "Boolean", None, "None"),
]

capacity_planning_summary_columns = [
    ("capacity_id", "String", None, "None"),
    ("growth_cu_per_week", "Double", None, "Sum"),
    ("growth_pct_per_week", "Double", None, "Sum"),
    ("current_headroom_pct", "Double", None, "Sum"),
    ("capacity_daily_cu_limit", "Double", None, "Sum"),
    ("projected_saturation_date", "DateTime", "General Date", "None"),
    ("weeks_to_saturation", "Double", None, "Sum"),
    ("n_weeks_history", "Int64", "0", "Sum"),
    ("r_squared", "Double", None, "Sum"),
    ("forecast_confidence", "String", None, "None"),
    ("confidence_caveat", "String", None, "None"),
]

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=False) as tom:
    for table_name, columns in [
        ("fact_capacity_forecast", fact_capacity_forecast_columns),
        ("capacity_planning_summary", capacity_planning_summary_columns),
    ]:
        tom.add_table(name=table_name)
        tom.add_entity_partition(table_name=table_name, entity_name=table_name, expression=EXPRESSION)
        for column_name, data_type, format_string, summarize_by in columns:
            tom.add_data_column(
                table_name=table_name, column_name=column_name, source_column=column_name,
                data_type=data_type, format_string=format_string, summarize_by=summarize_by,
            )
        print(f"  Added table {table_name} ({len(columns)} columns)")

    tom.add_relationship(
        from_table="fact_capacity_forecast", from_column="week_start_date",
        to_table="dim_date", to_column="date_id",
        from_cardinality="Many", to_cardinality="One", is_active=True,
    )
    tom.add_relationship(
        from_table="capacity_planning_summary", from_column="capacity_id",
        to_table="dim_capacity", to_column="capacity_id",
        from_cardinality="Many", to_cardinality="One", is_active=True,
    )
    # Added but left inactive on purpose — see markdown above.
    tom.add_relationship(
        from_table="fact_capacity_forecast", from_column="sku",
        to_table="dim_capacity", to_column="sku",
        from_cardinality="Many", to_cardinality="One", is_active=False,
    )
    print("Tables + relationships added, saving...")

print("Done — TOM connection closed/saved.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add the 5 capacity planning measures to `_measure`
#
# Mirrors `semantic models/sm_governance_medallion.SemanticModel/definition/tables/_measure.tmdl`.
# `Weeks to Saturation`/`Current Headroom %`/`Weekly CU Growth %` use the
# `isGeneralNumber` format hint rather than a DAX `%` format string — the
# source columns already store percentages as 0-100 numbers (e.g. `99.99`),
# and a `%` format string would multiply by 100 again on top of that.

# CELL ********************

measures = [
    (
        "Projected Saturation Date",
        "SELECTEDVALUE(capacity_planning_summary[projected_saturation_date])",
        "General Date",
    ),
    ("Weeks to Saturation", "SELECTEDVALUE(capacity_planning_summary[weeks_to_saturation])", None),
    ("Current Headroom %", "SELECTEDVALUE(capacity_planning_summary[current_headroom_pct])", None),
    ("Weekly CU Growth %", "SELECTEDVALUE(capacity_planning_summary[growth_pct_per_week])", None),
    (
        "Forecast Confidence",
        'VAR Confidence = SELECTEDVALUE(capacity_planning_summary[forecast_confidence])\n'
        'VAR Caveat = SELECTEDVALUE(capacity_planning_summary[confidence_caveat])\n'
        'RETURN Confidence & " — " & Caveat',
        None,
    ),
]

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=False) as tom:
    for name, expr, format_string in measures:
        tom.add_measure(table_name="_measure", measure_name=name, expression=expr, format_string=format_string)
    print(f"{len(measures)} measures added, saving...")

print("Done — TOM connection closed/saved.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Refresh — Direct Lake needs an initial frame before the new tables are queryable

# CELL ********************

labs.refresh_semantic_model(dataset=DATASET, workspace=WORKSPACE)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validate — confirm real data comes back through Direct Lake

# CELL ********************

import sempy.fabric as fabric

print(fabric.evaluate_dax(dataset=DATASET, workspace=WORKSPACE, dax_string="EVALUATE capacity_planning_summary"))
print(fabric.evaluate_dax(dataset=DATASET, workspace=WORKSPACE, dax_string="EVALUATE TOPN(10, fact_capacity_forecast)"))
print(fabric.evaluate_dax(dataset=DATASET, workspace=WORKSPACE, dax_string="EVALUATE ROW(\"Projected Saturation Date\", [Projected Saturation Date], \"Weeks to Saturation\", [Weeks to Saturation], \"Current Headroom %\", [Current Headroom %], \"Weekly CU Growth %\", [Weekly CU Growth %], \"Forecast Confidence\", [Forecast Confidence])"))

