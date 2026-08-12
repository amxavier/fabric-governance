"""
Real unit tests (not just static source checks) for scripts/utils.py's pipeline
patching — these are pure functions (JSON in, JSON out, no Fabric/network calls),
so unlike the rest of tests/ there's no reason to fall back to string matching.

Covers the exact bug class found live 2026-08-12: a pipeline deployed to a new
environment silently pointing its TridentNotebook activities and/or its
PBISemanticModelRefresh activity at whichever environment it was last captured
from, instead of the target workspace.
"""
import base64
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from utils import patch_pipeline_notebook_ids  # noqa: E402


def _make_parts(pipeline_content: dict) -> list[dict]:
    payload = base64.b64encode(json.dumps(pipeline_content).encode("utf-8")).decode("ascii")
    return [{"path": "pipeline-content.json", "payload": payload, "payloadType": "InlineBase64"}]


def _decode(parts: list[dict]) -> dict:
    for part in parts:
        if part["path"] == "pipeline-content.json":
            return json.loads(base64.b64decode(part["payload"]))
    raise AssertionError("pipeline-content.json not found in parts")


def _sample_pipeline() -> dict:
    return {
        "properties": {
            "activities": [
                {
                    "name": "update_bronze_capacities",
                    "type": "TridentNotebook",
                    "typeProperties": {
                        "notebookId": "some-logical-id-1234",
                        "workspaceId": "stale-workspace-id",
                    },
                },
                {
                    "name": "refresh_semantic_model",
                    "type": "PBISemanticModelRefresh",
                    "typeProperties": {
                        "method": "POST",
                        "operationType": "SemanticModelRefresh",
                        "groupId": "stale-workspace-id",
                        "datasetId": "stale-dataset-id",
                        "waitOnCompletion": True,
                        "commitMode": "Transactional",
                    },
                    "externalReferences": {"connection": "shared-connection-guid"},
                },
            ]
        }
    }


def test_notebook_id_resolved_via_logicalid_map():
    parts = _make_parts(_sample_pipeline())
    patched = patch_pipeline_notebook_ids(
        parts,
        logicalid_to_itemid={"some-logical-id-1234": "real-target-notebook-id"},
        workspace_id="target-workspace-id",
    )
    content = _decode(patched)
    nb_activity = content["properties"]["activities"][0]
    assert nb_activity["typeProperties"]["notebookId"] == "real-target-notebook-id"
    assert nb_activity["typeProperties"]["workspaceId"] == "target-workspace-id"


def test_notebook_id_left_alone_when_logicalid_unknown():
    # A real captured GUID (not a logicalId) has no entry in the map — must
    # pass through unchanged rather than being blanked out.
    parts = _make_parts(_sample_pipeline())
    patched = patch_pipeline_notebook_ids(
        parts, logicalid_to_itemid={}, workspace_id="target-workspace-id",
    )
    content = _decode(patched)
    nb_activity = content["properties"]["activities"][0]
    assert nb_activity["typeProperties"]["notebookId"] == "some-logical-id-1234"


def test_semantic_model_refresh_patched_when_sm_item_id_known():
    parts = _make_parts(_sample_pipeline())
    patched = patch_pipeline_notebook_ids(
        parts,
        logicalid_to_itemid={},
        workspace_id="target-workspace-id",
        sm_item_id="target-dataset-id",
    )
    content = _decode(patched)
    refresh_activity = content["properties"]["activities"][1]
    tp = refresh_activity["typeProperties"]
    assert tp["groupId"] == "target-workspace-id"
    assert tp["datasetId"] == "target-dataset-id"
    # The connection binding is reusable across environments (confirmed live:
    # identical GUID on DEV and QA after QA's independent portal correction)
    # — must never be touched by this function.
    assert refresh_activity["externalReferences"]["connection"] == "shared-connection-guid"


def test_semantic_model_refresh_left_alone_when_sm_item_id_unknown():
    # The semantic model hasn't been provisioned in the target workspace yet
    # (e.g. a brand-new environment) — must not write a bogus/empty reference.
    parts = _make_parts(_sample_pipeline())
    patched = patch_pipeline_notebook_ids(
        parts, logicalid_to_itemid={}, workspace_id="target-workspace-id", sm_item_id=None,
    )
    content = _decode(patched)
    refresh_activity = content["properties"]["activities"][1]
    tp = refresh_activity["typeProperties"]
    assert tp["groupId"] == "stale-workspace-id"
    assert tp["datasetId"] == "stale-dataset-id"
