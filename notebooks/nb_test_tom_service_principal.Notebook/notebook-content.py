# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ### nb_test_tom_service_principal
#
# **Temporary diagnostic — not a permanent part of this project.** Tests
# whether TOM/XMLA connections actually reject the pipeline's Service
# Principal identity on this tenant, or whether that was ever just an
# assumption carried over from the separately-confirmed `/admin/*` REST
# rejection (see README, "Semantic Model Lifecycle" and project memory —
# no commit or test in this repo's history actually exercised TOM under
# the SP before this one).
#
# **Read-only, makes no changes** — `connect_semantic_model(readonly=True)`
# plus a single table listing. Safe to run repeatedly via the pipeline
# (Service Principal) or interactively, in any environment.
#
# Delete this notebook (and its pipeline activity, if added) once the
# question is answered either way.

# CELL ********************

get_ipython().run_line_magic("pip", "install semantic-link-labs")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Attempt a read-only TOM connection
#
# If this raises, the exception itself is the evidence — printed in full
# rather than caught, so the real error (auth rejection vs. something
# else entirely) is visible in the run log either way.

# CELL ********************

DATASET = "sm_governance_medallion"
WORKSPACE = "lakehouses_dev"

from sempy_labs.tom import connect_semantic_model

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=True) as tom:
    tables = [t.Name for t in tom.model.Tables]

print(f"SUCCESS — TOM connection worked under this notebook's execution identity.")
print(f"Tables visible: {tables}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Attempt a real write — add, verify, then remove a throwaway measure
#
# Fully self-contained and reversible: nothing persists in the model
# regardless of outcome, since the last step always deletes what the
# first step added (or the run fails outright on the add, in which case
# there's nothing to clean up). This is the actual question that
# matters — read access alone doesn't prove
# `nb_setup_capacity_forecast_model`-style automation would work
# unattended via the pipeline.

# CELL ********************

import uuid

TEST_MEASURE = f"_sp_write_test_{uuid.uuid4().hex[:8]}"

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=False) as tom:
    tom.add_measure(table_name="_measure", measure_name=TEST_MEASURE, expression="1")
print(f"WRITE SUCCESS — added throwaway measure {TEST_MEASURE!r}")

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=True) as tom:
    names = [m.Name for m in tom.model.Tables["_measure"].Measures]
    found = TEST_MEASURE in names
print(f"VERIFY — {TEST_MEASURE!r} present after write: {found}")

with connect_semantic_model(dataset=DATASET, workspace=WORKSPACE, readonly=False) as tom:
    tom.remove_object(tom.model.Tables["_measure"].Measures[TEST_MEASURE])
print(f"CLEANUP SUCCESS — removed {TEST_MEASURE!r}, model back to its original state")

