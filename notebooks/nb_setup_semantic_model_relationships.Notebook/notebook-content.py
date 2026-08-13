# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ### nb_setup_semantic_model_relationships
#
# **Run this manually, once per environment**, right after
# `scripts/deploy_semantic_model.py <branch>` creates a fresh
# `sm_governance_medallion` with `relationships.tmdl` deliberately empty.
#
# **Why this exists:** a bulk TMDL/REST import that includes relationships
# marked `isActive: false` (needed to break ambiguous paths — e.g. two facts
# sharing two dimensions) is rejected by Fabric's import validator with an
# "ambiguous paths between X and Y" error, even though the exact same graph
# is valid once built incrementally. Adding relationships one at a time via
# TOM sidesteps that validator entirely — confirmed empirically against a
# throwaway test model in DEV on 2026-08-07 (see PR/commit history around
# that date for the investigation). This notebook is the automated
# replacement for reconstructing the model by hand in the portal.
#
# Run interactively today, but **TOM/XMLA writes are confirmed to work
# fine under the pipeline's Service Principal identity too** (verified
# live 2026-08-13 — a real add/verify/remove measure round-trip via the
# SP succeeded) — this is a different auth path from the `/admin/*` REST
# rejection documented in the README, and assuming the same restriction
# applied without testing it was wrong. Stays a manual/occasional
# notebook because it's meant to run once per environment (a second run
# fails on "relationship already exists"), not because of any credential
# restriction.
#
# Edit `DATASET`/`WORKSPACE` below to point at the environment you just
# created the model in, then run all cells top to bottom.

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
#
# `sempy.fabric` (Semantic Link) ships built into the Fabric runtime;
# `sempy_labs` (Semantic Link Labs, the TOM/XMLA write helpers) is a
# separate package and needs installing per session.

# CELL ********************

get_ipython().run_line_magic("pip", "install semantic-link-labs")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add the 14 relationships via TOM
#
# Mirrors `semantic models/sm_governance_medallion.SemanticModel/definition/relationships.tmdl`
# exactly — if that file changes (new table, new relationship), update this
# list to match. TOM requires the "from" side of `add_relationship` to
# always be the "Many" side (unlike the portal's Manage Relationships
# dialog, which lets you pick either table as "From") — every tuple below
# is already `(fact_table, fact_column, dim_table, dim_column, is_active)`
# with that constraint in mind.

# CELL ********************

import sempy_labs as labs
from sempy_labs.tom import connect_semantic_model

relationships = [
    ("fact_activity", "item_id", "dim_item", "item_id", True),
    ("fact_activity", "workspace_id", "dim_workspace", "workspace_id", False),
    ("fact_activity", "user_id", "dim_user", "user_key", True),
    ("fact_activity", "date_id", "dim_date", "date_id", True),
    ("fact_refresh", "item_id", "dim_item", "item_id", True),
    ("fact_refresh", "date_id", "dim_date", "date_id", False),
    ("dim_item", "workspace_id", "dim_workspace", "workspace_id", True),
    ("dim_workspace", "capacity_id", "dim_capacity", "capacity_id", True),
    ("fact_capacity_consumption", "workspace_id", "dim_workspace", "workspace_id", False),
    ("fact_capacity_consumption", "date_id", "dim_date", "date_id", False),
    ("fact_capacity_consumption", "item_id", "dim_item", "item_id", True),
    ("fact_capacity_utilization", "sku", "dim_capacity", "sku", False),
    ("fact_capacity_utilization", "date_id", "dim_date", "date_id", True),
    ("fact_item_lifecycle", "item_id", "dim_item", "item_id", True),
]

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=False) as tom:
    for from_table, from_col, to_table, to_col, active in relationships:
        tom.add_relationship(
            from_table=from_table, from_column=from_col,
            to_table=to_table, to_column=to_col,
            from_cardinality="Many", to_cardinality="One",
            is_active=active,
        )
    print(f"All {len(relationships)} relationships added, saving...")

print("Done — TOM connection closed/saved.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Refresh — Direct Lake needs an initial frame before it's queryable

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

fabric.evaluate_dax(dataset=DATASET, workspace=WORKSPACE, dax_string="EVALUATE dim_capacity")

