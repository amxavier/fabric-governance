import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SM_PATH = REPO_ROOT / "semantic models" / "sm_governance_medallion.SemanticModel"
DEFINITION_PATH = SM_PATH / "definition"

EXPECTED_MEASURES = [
    "Total Refreshes",
    "Failed Refreshes",
    "Refresh Success Rate %",
    "Avg Refresh Duration (s)",
    "Distinct Publishers",
    "Days Since Last Successful Refresh",
    "Total CU (Item Trend)",
    "Total CU (Capacity, Real)",
    "Total CU (Item, by Date)",
    "Avg Daily CU (Capacity)",
    "Total Items",
    "Compute Items",
    "Data Items",
    "Presentation Items",
    "Interactive Share % (Latest Day)",
    "Background Share % (Latest Day)",
    "Avg CU % of Base (Capacity)",
    "Failed Refreshes Color",
    "Refresh Success Rate Color",
    "Monthly Capacity Cost (Reserved, USD)",
    "Est. Idle Capacity Cost (USD)",
    "Total Tracked Items",
    "Active Items",
    "Inactive Items",
    "Cleanup Candidate Items",
    "No Signal Items",
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
    "_measure",
]

EXPECTED_RELATIONSHIPS = [
    ("fact_activity.item_id", "dim_item.item_id"),
    ("fact_activity.workspace_id", "dim_workspace.workspace_id"),
    ("fact_activity.user_id", "dim_user.user_key"),
    ("fact_activity.date_id", "dim_date.date_id"),
    ("fact_refresh.item_id", "dim_item.item_id"),
    ("fact_refresh.date_id", "dim_date.date_id"),
    ("dim_item.workspace_id", "dim_workspace.workspace_id"),
    ("dim_workspace.capacity_id", "dim_capacity.capacity_id"),
    ("dim_item.item_id", "fact_capacity_consumption.item_id"),
    ("fact_capacity_consumption.workspace_id", "dim_workspace.workspace_id"),
    ("fact_capacity_consumption.date_id", "dim_date.date_id"),
    ("dim_date.date_id", "fact_capacity_utilization.date_id"),
    ("fact_capacity_utilization.sku", "dim_capacity.sku"),
    ("dim_item.item_id", "fact_item_lifecycle.item_id"),
]

# Real DEV workspace (reused from the sibling microsoft-fabric-medallion-lakehouse
# project's workspace) and lh_governance_gold (created directly for this project).
DEV_WORKSPACE_GUID = "dc072922-4ffb-4424-868c-28087b02ecba"
DEV_LH_GOLD_GUID = "50efdcbf-bfca-48a0-ab39-8b5a52ed407f"


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
    content = (DEFINITION_PATH / "tables" / "_measure.tmdl").read_text(encoding="utf-8")
    for measure in EXPECTED_MEASURES:
        assert f"measure '{measure}'" in content, f"Missing DAX measure: {measure}"


def test_all_relationships_defined():
    content = (DEFINITION_PATH / "relationships.tmdl").read_text(encoding="utf-8")
    for from_col, to_col in EXPECTED_RELATIONSHIPS:
        assert f"fromColumn: {from_col}" in content, f"Missing relationship fromColumn: {from_col}"
        assert f"toColumn: {to_col}" in content, f"Missing relationship toColumn: {to_col}"


def test_expressions_tmdl_contains_dev_placeholders():
    # DEV placeholders must be present so the deploy script (or a manual pass once
    # the real DEV workspace exists) can perform environment substitution.
    content = (DEFINITION_PATH / "expressions.tmdl").read_text(encoding="utf-8")
    assert DEV_WORKSPACE_GUID in content
    assert DEV_LH_GOLD_GUID in content
