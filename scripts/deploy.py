#!/usr/bin/env python3
"""
Deploy Fabric artifacts directly to the target workspace via REST API.

Deployment order (handles cross-item dependencies):
  Phase 1 — Notebooks + SemanticModel  (no internal dependencies)
  Phase 2 — DataPipeline               (references notebooks by item ID)
  Phase 3 — Report                     (references semantic model by item ID)

Environment variables expected (from GitHub Actions secrets):
  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Optional:
  DEPLOY_MODE       — "selective" | "full" (default: selective)
  GITHUB_REF_NAME   — branch name, injected by GitHub Actions
"""
import base64
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from fabric_client import FabricClient
from utils import (
    get_changed_items,
    get_display_name,
    get_item_type,
    get_notebook_logicalid_map,
    load_valuesets,
    patch_pipeline_notebook_ids,
    patch_report_byconnection,
    read_item_parts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SEMANTIC_MODEL_NAME = "sm_governance_medallion"

ARTIFACT_DIRS = [
    REPO_ROOT / "notebooks",
    REPO_ROOT / "pipelines",
    REPO_ROOT / "semantic models",
    REPO_ROOT / "report",
]


def _current_branch() -> str:
    env_branch = os.getenv("GITHUB_REF_NAME") or os.getenv("BRANCH_NAME")
    if env_branch:
        return env_branch
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    return result.stdout.strip()


def _all_artifacts() -> list[Path]:
    items = []
    for artifact_dir in ARTIFACT_DIRS:
        if not artifact_dir.exists():
            continue
        for entry in sorted(artifact_dir.iterdir()):
            if entry.is_dir() and get_item_type(entry.name):
                items.append(entry)
    return items


def _deploy_item(client: FabricClient, workspace_id: str, existing: dict[str, str],
                 item_path: Path, parts: list[dict]) -> bool:
    display_name = get_display_name(item_path)
    item_type = get_item_type(item_path.name)
    try:
        if display_name in existing:
            if item_type == "Report":
                # updateDefinition does not propagate layout changes for Reports;
                # delete + create ensures the full PBIR definition (report.json, theme)
                # is applied correctly every time.
                print(f"  Recreating [{item_type}] {display_name}")
                client.delete_item(workspace_id, existing[display_name])
                client.create_item(workspace_id, display_name, item_type, parts)
            else:
                print(f"  Updating  [{item_type}] {display_name}")
                client.update_item_definition(workspace_id, existing[display_name], parts)
        else:
            print(f"  Creating  [{item_type}] {display_name}")
            client.create_item(workspace_id, display_name, item_type, parts)
        print(f"  OK: {display_name}\n")
        return True
    except Exception as exc:
        print(f"  FAILED: {display_name} — {exc}\n")
        return False


def main() -> None:
    mode = os.getenv("DEPLOY_MODE", "selective")
    branch = _current_branch()

    print("=== Fabric Deploy ===")
    print(f"Branch : {branch}")
    print(f"Mode   : {mode}")

    target_config = load_valuesets(REPO_ROOT, branch)
    dev_config = load_valuesets(REPO_ROOT, "dev")
    workspace_id = target_config["workspace_id"]
    print(f"Target workspace: {workspace_id}\n")

    # Build OneLake URL replacement for DirectLake expressions.tmdl
    replacements: dict[str, str] = {}
    dev_url = dev_config.get("onelake_url", "")
    target_url = target_config.get("onelake_url", "")
    if dev_url and target_url and dev_url != target_url:
        replacements[dev_url] = target_url

    # notebook_replacements patches workspace/lakehouse IDs in notebook METADATA.
    # Notebooks store DEV IDs in their .py METADATA block; without this patch
    # Fabric fails to attach lakehouses when running notebooks in QA/PRD.
    notebook_replacements: dict[str, str] = {}
    dev_workspace_id = dev_config["workspace_id"]
    if dev_workspace_id != workspace_id:
        notebook_replacements[dev_workspace_id] = workspace_id

    # Determine artifact set
    if mode == "full":
        items = _all_artifacts()
    else:
        items = get_changed_items(REPO_ROOT)
        if not items:
            print("No artifact changes detected — nothing to deploy.")
            return

    # Exclude lakehouses (managed separately)
    deployable = [p for p in items if get_item_type(p.name) and not p.name.endswith(".Lakehouse")]
    if not deployable:
        print("No deployable artifacts in changeset.")
        return

    print(f"Artifacts to deploy ({len(deployable)}):")
    for p in deployable:
        print(f"  {p.name}")
    print()

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    # Resolve lakehouse ID replacements for notebook METADATA by comparing
    # DEV vs target workspace items — avoids hardcoding IDs in config.
    if notebook_replacements:
        dev_items = {i["displayName"]: i["id"] for i in client.get_workspace_items(dev_workspace_id) if i.get("type") == "Lakehouse"}
        target_items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id) if i.get("type") == "Lakehouse"}
        for lh_name in ["lh_bronze", "lh_silver", "lh_gold"]:
            if lh_name in dev_items and lh_name in target_items:
                notebook_replacements[dev_items[lh_name]] = target_items[lh_name]
        print(f"Notebook lakehouse ID map: {len(notebook_replacements) - 1} lakehouses resolved\n")

    # Split into three phases based on dependency order
    phase1 = [p for p in deployable if not p.name.endswith((".DataPipeline", ".Report"))]
    phase2 = [p for p in deployable if p.name.endswith(".DataPipeline")]
    phase3 = [p for p in deployable if p.name.endswith(".Report")]

    success, failed = 0, []

    # ── Phase 1: Notebooks + SemanticModel ───────────────────────────────────
    if phase1:
        print("── Phase 1: Notebooks + SemanticModel ──")
    for item_path in phase1:
        existing = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
        parts = read_item_parts(
            item_path,
            replacements or None,
            notebook_replacements or None,
        )
        if _deploy_item(client, workspace_id, existing, item_path, parts):
            success += 1
        else:
            failed.append(get_display_name(item_path))

    # ── Phase 2: DataPipeline (patch notebook IDs) ───────────────────────────
    if phase2:
        print("── Phase 2: DataPipeline ──")
        # Build logicalId → actual item ID map from the workspace
        workspace_items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
        logicalid_to_name = get_notebook_logicalid_map(REPO_ROOT)
        logicalid_to_itemid = {
            lid: workspace_items[name]
            for lid, name in logicalid_to_name.items()
            if name in workspace_items
        }
        print(f"  Notebook ID mapping: {len(logicalid_to_itemid)} notebooks resolved\n")

        for item_path in phase2:
            parts = read_item_parts(item_path, replacements or None)
            parts = patch_pipeline_notebook_ids(parts, logicalid_to_itemid, workspace_id)
            # Debug: confirm what is being sent to Fabric
            for p in parts:
                if p["path"] == "pipeline-content.json":
                    sent = json.loads(base64.b64decode(p["payload"]))
                    for act in sent.get("properties", {}).get("activities", []):
                        tp = act.get("typeProperties", {})
                        print(f"  [PATCH] {act['name']}: notebookId={tp.get('notebookId')} workspaceId={tp.get('workspaceId')}")
            if _deploy_item(client, workspace_id, workspace_items, item_path, parts):
                success += 1
            else:
                failed.append(get_display_name(item_path))

    # ── Phase 3: Report (patch byPath → byConnection) ────────────────────────
    if phase3:
        print("── Phase 3: Report ──")
        workspace_items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
        if SEMANTIC_MODEL_NAME not in workspace_items:
            print(f"  FAILED: {SEMANTIC_MODEL_NAME} not found in workspace — deploy it first.\n")
            failed.extend(get_display_name(p) for p in phase3)
        else:
            sm_item_id = workspace_items[SEMANTIC_MODEL_NAME]
            workspace_name = client.get_workspace_name(workspace_id)
            print(f"  Workspace name : {workspace_name}")
            print(f"  SemanticModel  : {SEMANTIC_MODEL_NAME} ({sm_item_id})\n")
            for item_path in phase3:
                parts = read_item_parts(item_path, replacements or None)
                parts = patch_report_byconnection(parts, workspace_name, SEMANTIC_MODEL_NAME, sm_item_id)
                if _deploy_item(client, workspace_id, workspace_items, item_path, parts):
                    success += 1
                else:
                    failed.append(get_display_name(item_path))

    print(f"=== Result: {success} deployed, {len(failed)} failed ===")
    if failed:
        print(f"Failed: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    main()
