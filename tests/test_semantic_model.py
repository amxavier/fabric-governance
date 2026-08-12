import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SM_PATH = REPO_ROOT / "semantic models" / "sm_governance_medallion.SemanticModel"
DEFINITION_PATH = SM_PATH / "definition"

EXPECTED_MEASURES = [
    "Total Refreshes",
    "Failed Refreshes",
    "Success Rate %",
    "Avg Duration (s)",
    "Distinct Publishers",
    "Days Since Refresh",
    "Total CU (Item Trend)",
    "Total CU",
    "Total CU (Item, by Date)",
    "Avg Daily CU",
    "Total Items",
    "Compute",
    "Data",
    "Presentation",
    "Interactive %",
    "Background %",
    "CU % of Base",
    "Failed Refreshes Color",
    "Refresh Success Rate Color",
    "Monthly Cost (USD)",
    "Idle Cost (USD)",
    "Tracked Items",
    "Active Items",
    "Inactive Items",
    "Cleanup Candidates",
    "No Signal Items",
    "Projected Saturation Date",
    "Weeks to Saturation",
    "Current Headroom %",
    "Weekly CU Growth %",
    "Forecast Confidence",
    "Forecast Confidence Color",
]

# Table/measure home is "_measure" (not "measure") — renamed when the model
# was rebuilt interactively so the Display Folder groups independently of
# any real data table instead of nesting under whichever table a measure
# happened to be created on (see project memory for why).
EXPECTED_TABLES = [
    "dim_capacity", "dim_workspace", "dim_item", "dim_user", "dim_date",
    "fact_activity", "fact_refresh",
    "fact_capacity_consumption", "fact_capacity_utilization",
    "fact_item_lifecycle",
    "fact_capacity_forecast", "capacity_planning_summary",
    "_measure",
]

# Order-independent pairs — TMDL's fromColumn/toColumn direction isn't a
# reliable signal of which side is "many" (see EXPECTED_INACTIVE_PAIRS
# below and the 2026-08-07 investigation): relationships built via the
# portal wizard can list either table as "from", and Fabric infers
# cardinality from actual data uniqueness rather than a declared field
# when the TMDL has no explicit fromCardinality/toCardinality — the
# relationships.tmdl format used here never does. That's exactly how the
# fact_capacity_utilization/dim_date cardinality bug slipped in without
# this file changing shape at all, so don't expect a fixed direction here.
EXPECTED_RELATIONSHIP_PAIRS = [
    frozenset(("fact_activity.item_id", "dim_item.item_id")),
    frozenset(("fact_activity.workspace_id", "dim_workspace.workspace_id")),
    frozenset(("fact_activity.user_id", "dim_user.user_key")),
    frozenset(("fact_activity.date_id", "dim_date.date_id")),
    frozenset(("fact_refresh.item_id", "dim_item.item_id")),
    frozenset(("fact_refresh.date_id", "dim_date.date_id")),
    frozenset(("dim_item.workspace_id", "dim_workspace.workspace_id")),
    frozenset(("dim_workspace.capacity_id", "dim_capacity.capacity_id")),
    frozenset(("fact_capacity_consumption.item_id", "dim_item.item_id")),
    frozenset(("fact_capacity_consumption.workspace_id", "dim_workspace.workspace_id")),
    frozenset(("fact_capacity_consumption.date_id", "dim_date.date_id")),
    frozenset(("fact_capacity_utilization.date_id", "dim_date.date_id")),
    frozenset(("fact_capacity_utilization.sku", "dim_capacity.sku")),
    frozenset(("fact_item_lifecycle.item_id", "dim_item.item_id")),
    frozenset(("fact_capacity_forecast.week_start_date", "dim_date.date_id")),
    frozenset(("capacity_planning_summary.capacity_id", "dim_capacity.capacity_id")),
    frozenset(("fact_capacity_forecast.sku", "dim_capacity.sku")),
]

# Pairs that MUST be inactive to avoid an ambiguous relationship path (two
# fact tables sharing two dimensions — see relationships.tmdl's own
# comments/history, and the 2026-08-07 TOM validation). A bulk TMDL import
# that gets any of these wrong either fails outright ("ambiguous paths
# between X and Y") or — worse — imports fine but leaves a filter silently
# not propagating (the fact_capacity_utilization/dim_date bug: same failure
# family, opposite symptom).
EXPECTED_INACTIVE_PAIRS = [
    frozenset(("fact_activity.workspace_id", "dim_workspace.workspace_id")),
    frozenset(("fact_refresh.date_id", "dim_date.date_id")),
    frozenset(("fact_capacity_consumption.workspace_id", "dim_workspace.workspace_id")),
    frozenset(("fact_capacity_consumption.date_id", "dim_date.date_id")),
    frozenset(("fact_capacity_utilization.sku", "dim_capacity.sku")),
    frozenset(("fact_capacity_forecast.sku", "dim_capacity.sku")),
]


def _parse_relationships(content: str) -> list[dict]:
    """Parse relationships.tmdl into [{"pair": frozenset, "is_active": bool}, ...].

    Real parsing instead of substring checks — a substring check can't
    tell isActive apart from a coincidental match elsewhere in the file,
    and can't validate that fromColumn/toColumn belong to the SAME
    relationship block rather than two different ones.
    """
    # Leading "\n" guarantees the split pattern also matches a relationship
    # block that happens to be the very first line in the file (no newline
    # precedes it there) — without this, that first block silently merges
    # into the discarded preamble instead of being parsed.
    blocks = re.split(r"\nrelationship ", "\n" + content)[1:]
    parsed = []
    for block in blocks:
        is_active = "isActive: false" not in block
        from_match = re.search(r"fromColumn:\s*(\S+)", block)
        to_match = re.search(r"toColumn:\s*(\S+)", block)
        assert from_match and to_match, f"Malformed relationship block (missing fromColumn/toColumn): {block[:80]!r}"
        parsed.append({
            "pair": frozenset((from_match.group(1), to_match.group(1))),
            "is_active": is_active,
        })
    return parsed


def test_semantic_model_folder_exists():
    assert SM_PATH.is_dir(), f"SemanticModel folder not found: {SM_PATH}"


def test_platform_file_is_valid():
    platform = SM_PATH / ".platform"
    assert platform.exists(), ".platform file missing"
    data = json.loads(platform.read_text(encoding="utf-8"))
    assert data["metadata"]["displayName"] == "sm_governance_medallion"
    assert data["metadata"]["type"] == "SemanticModel"


def test_required_tmdl_files_exist():
    required = ["model.tmdl", "expressions.tmdl", "relationships.tmdl", "database.tmdl"]
    for name in required:
        assert (DEFINITION_PATH / name).exists(), f"Missing required TMDL file: {name}"


def test_all_tables_present():
    tables_dir = DEFINITION_PATH / "tables"
    assert tables_dir.is_dir(), "tables/ directory missing inside definition/"
    present = {f.stem for f in tables_dir.glob("*.tmdl")}
    for table in EXPECTED_TABLES:
        assert table in present, f"Missing table TMDL: {table}.tmdl"


def test_all_measures_defined():
    # TMDL only quotes an identifier when it needs to (spaces, %, parens,
    # etc.) — a single-word name like "Compute" is written unquoted
    # (`measure Compute =`), not `measure 'Compute' =`, so both forms are
    # legal and must be accepted.
    content = (DEFINITION_PATH / "tables" / "_measure.tmdl").read_text(encoding="utf-8")
    for measure in EXPECTED_MEASURES:
        quoted = f"measure '{measure}'" in content
        unquoted = f"measure {measure} " in content or f"measure {measure}=" in content
        assert quoted or unquoted, f"Missing DAX measure: {measure}"


def test_all_relationships_defined():
    content = (DEFINITION_PATH / "relationships.tmdl").read_text(encoding="utf-8")
    actual_pairs = {r["pair"] for r in _parse_relationships(content)}
    for expected in EXPECTED_RELATIONSHIP_PAIRS:
        assert expected in actual_pairs, f"Missing relationship: {set(expected)}"


def test_ambiguous_path_relationships_are_inactive():
    # This is the test that would have caught the cardinality-adjacent
    # class of bug found 2026-08-07: not the exact cardinality (TMDL
    # doesn't declare it explicitly for these, so it can't be asserted
    # statically — see module docstring above), but at least that the
    # specific relationships known to create a cycle if left active stay
    # inactive after any future pull/rebuild.
    content = (DEFINITION_PATH / "relationships.tmdl").read_text(encoding="utf-8")
    by_pair = {r["pair"]: r["is_active"] for r in _parse_relationships(content)}
    for expected in EXPECTED_INACTIVE_PAIRS:
        assert expected in by_pair, f"Missing relationship: {set(expected)}"
        assert not by_pair[expected], (
            f"{set(expected)} must be isActive: false to avoid an ambiguous "
            f"relationship path — see relationships.tmdl history."
        )


def test_expressions_tmdl_has_valid_directlake_source():
    # This file is only ever populated by pulling a real, live model back
    # from whichever environment last had it manually corrected in the
    # portal (DEV or QA, historically) — deploy.py deliberately never
    # writes to it (see scripts/deploy.py). So it legitimately holds a
    # different environment's real workspace/lakehouse GUID depending on
    # when it was last pulled; asserting one specific hardcoded GUID here
    # (as this test used to) goes stale the next time it's pulled from a
    # different environment, exactly as happened 2026-08-07. Validate
    # shape instead of a specific value: a real OneLake DataLake URL with
    # two well-formed GUIDs (workspace/lakehouse), not a placeholder.
    content = (DEFINITION_PATH / "expressions.tmdl").read_text(encoding="utf-8")
    match = re.search(
        r'onelake\.dfs\.fabric\.microsoft\.com/'
        r'([0-9a-f-]{36})/([0-9a-f-]{36})',
        content,
    )
    assert match, "No valid OneLake DirectLake source URL found in expressions.tmdl"
    workspace_guid, lakehouse_guid = match.groups()
    assert workspace_guid != "00000000-0000-0000-0000-000000000000", "Workspace GUID is a placeholder, not real"
    assert lakehouse_guid != "00000000-0000-0000-0000-000000000000", "Lakehouse GUID is a placeholder, not real"
