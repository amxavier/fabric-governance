"""
Static sanity checks on the Bronze notebook source (not a Spark/Delta execution
test — this repo's CI has no Fabric runtime available). These checks enforce the
architectural split confirmed for this project: dimension-style Bronze tables
(capacities, workspaces, items) use SCD Type 2; event-style Bronze tables
(activity_events, refresh_history) are append-only, with no SCD control columns.
"""
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = REPO_ROOT / "notebooks"

SCD2_DIMENSION_NOTEBOOKS = [
    "nb_bronze_capacities",
    "nb_bronze_workspaces",
    "nb_bronze_items",
]

APPEND_ONLY_FACT_NOTEBOOKS = [
    "nb_bronze_activity_events",
    "nb_bronze_refresh_history",
]

SCD2_CONTROL_COLUMNS = ["valid_from", "valid_to", "is_current"]


def _notebook_source(name: str) -> str:
    path = NOTEBOOKS_DIR / f"{name}.Notebook" / "notebook-content.py"
    assert path.exists(), f"Notebook source not found: {path}"
    return path.read_text(encoding="utf-8")


def test_scd2_dimension_notebooks_have_control_columns():
    for name in SCD2_DIMENSION_NOTEBOOKS:
        source = _notebook_source(name)
        for column in SCD2_CONTROL_COLUMNS:
            assert f'"{column}"' in source, (
                f"{name} is expected to apply SCD Type 2 but is missing the "
                f"'{column}' control column"
            )


def test_scd2_dimension_notebooks_merge_on_is_current():
    for name in SCD2_DIMENSION_NOTEBOOKS:
        source = _notebook_source(name)
        assert ".merge(" in source, f"{name} is expected to use a Delta MERGE for SCD2"
        assert "is_current = true" in source, (
            f"{name} SCD2 merge condition should match on is_current = true"
        )


def test_append_only_fact_notebooks_have_no_scd_columns():
    for name in APPEND_ONLY_FACT_NOTEBOOKS:
        source = _notebook_source(name)
        for column in SCD2_CONTROL_COLUMNS:
            assert f'"{column}"' not in source, (
                f"{name} is an append-only fact source and should not apply SCD2 "
                f"(found '{column}' control column)"
            )
        assert ".merge(" not in source, (
            f"{name} is append-only and should not use a Delta MERGE"
        )
