import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = REPO_ROOT / "report"


def _report_path() -> Path:
    for entry in REPORT_DIR.iterdir():
        if entry.is_dir() and entry.name.endswith(".Report"):
            return entry
    raise FileNotFoundError(f"No .Report folder found in {REPORT_DIR}")


def test_report_folder_exists():
    assert REPORT_DIR.is_dir(), "report/ directory not found"
    assert _report_path().is_dir()


def test_platform_file_is_valid():
    platform = _report_path() / ".platform"
    assert platform.exists(), ".platform file missing"
    data = json.loads(platform.read_text(encoding="utf-8"))
    assert data["metadata"]["type"] == "Report"
    assert "displayName" in data["metadata"]


def test_definition_pbir_is_valid_json():
    pbir = _report_path() / "definition.pbir"
    assert pbir.exists(), "definition.pbir missing"
    data = json.loads(pbir.read_text(encoding="utf-8"))
    assert "$schema" in data
    assert "datasetReference" in data


def test_definition_pbir_uses_bypath_not_byconnection():
    # Repository must store byPath. The deploy script converts it to byConnection
    # at deploy time — committing byConnection would break Git-based diffing.
    data = json.loads((_report_path() / "definition.pbir").read_text(encoding="utf-8"))
    ref = data["datasetReference"]
    assert "byPath" in ref, "definition.pbir must use byPath in the repository"
    assert "byConnection" not in ref, "byConnection must not be committed — it is injected at deploy time"


def test_definition_pbir_references_correct_semantic_model():
    data = json.loads((_report_path() / "definition.pbir").read_text(encoding="utf-8"))
    path = data["datasetReference"]["byPath"]["path"]
    assert "sm_governance_medallion.SemanticModel" in path, f"Unexpected SM reference in byPath: {path}"


def _visual_files() -> list[Path]:
    return list((_report_path() / "definition" / "pages").glob("*/visuals/*/visual.json"))


def test_report_definition_json_exists():
    # PBIR (full) format: report.json lives under definition/, one page.json
    # per page under definition/pages/, one visual.json per visual under
    # definition/pages/{page}/visuals/{visual}/ — not the old flat report.json
    # with escaped-string visualContainers.
    assert (_report_path() / "definition" / "report.json").exists(), "definition/report.json missing"


def test_report_has_at_least_one_page():
    pages = list((_report_path() / "definition" / "pages").glob("*/page.json"))
    assert len(pages) >= 1, "Report must have at least one page"


def test_report_references_expected_measures():
    visual_files = _visual_files()
    assert visual_files, "No visual.json files found under definition/pages"
    all_text = " ".join(f.read_text(encoding="utf-8") for f in visual_files)
    for measure in [
        "Total Refreshes", "Failed Refreshes", "Refresh Success Rate %",
        "Total Items", "Compute Items", "Data Items", "Presentation Items",
        "Total CU (Capacity, Real)", "Avg Daily CU (Capacity)", "Total CU (Item Trend)",
        "Interactive Share % (Latest Day)", "Background Share % (Latest Day)",
        "Avg CU % of Base (Capacity)",
    ]:
        assert measure in all_text, f"No visual references measure: {measure}"
