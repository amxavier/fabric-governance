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


def test_report_json_exists():
    assert (_report_path() / "report.json").exists(), "report.json missing"


def test_report_json_is_valid_and_references_measures():
    report_json = json.loads((_report_path() / "report.json").read_text(encoding="utf-8"))
    sections = report_json["sections"]
    assert len(sections) >= 1, "Report must have at least one page"
    visual_containers = sections[0]["visualContainers"]
    all_configs = " ".join(v["config"] for v in visual_containers)
    for measure in ["Total Refreshes", "Failed Refreshes", "Refresh Success Rate %"]:
        assert measure in all_configs, f"Report page does not reference measure: {measure}"
