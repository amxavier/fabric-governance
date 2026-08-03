"""
Static sanity check on the Bronze notebook source (not a Spark/Delta execution
test — this repo's CI has no Fabric runtime available).

`spark.createDataFrame(rows)` with no explicit schema infers types from the
data, and throws CANNOT_DETERMINE_TYPE the moment a column is null across
every row of a run (a very plausible occurrence: e.g. `description` is null
for most Fabric items, `region` is null for Trial capacities, `object_id` is
null for most Activity Events). Every Bronze notebook that builds its main
DataFrame from a Python list must pass an explicit `schema=`.
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"

# Matches spark.createDataFrame(some_identifier) with no second (schema) arg —
# exactly the pattern that has caused CANNOT_DETERMINE_TYPE in this project
# more than once. Calls that pass a schema (`, schema=...` or a positional
# second arg) or a literal single-row dict (`[{...}]`, used only for the
# single-row _auth_delegated write) don't match.
UNSAFE_CREATE_DATAFRAME = re.compile(r"spark\.createDataFrame\(\s*\w+\s*\)")


def test_bronze_notebooks_have_no_unschema_dataframe_calls():
    for path in sorted(NOTEBOOKS_DIR.glob("nb_bronze_*.Notebook/notebook-content.py")):
        source = path.read_text(encoding="utf-8")
        assert not UNSAFE_CREATE_DATAFRAME.search(source), (
            f"{path.relative_to(REPO_ROOT)} calls spark.createDataFrame(rows) "
            f"without an explicit schema= — this breaks with "
            f"CANNOT_DETERMINE_TYPE the moment a column is null across every "
            f"row of a run."
        )
