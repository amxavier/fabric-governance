#!/usr/bin/env python3
"""
Provision sm_governance_medallion in a NEW environment via REST/TMDL — the
automated alternative to rebuilding the model by hand in the portal.

Deliberately NOT wired into deploy.py's normal full/selective flow: DEV and
QA already have working, interactively-built models, and a routine deploy
must never overwrite them. This script is only ever run explicitly, when
provisioning an environment's semantic model for the first time (or
intentionally rebuilding it from scratch).

What it does, and why:
  1. Deploys the TMDL definition via REST like any other item — BUT with
     definition/relationships.tmdl emptied first. A bulk TMDL import that
     includes relationships marked isActive: false to break an ambiguous
     path is rejected by Fabric's import validator ("ambiguous paths
     between X and Y") even though the same graph is perfectly valid once
     built incrementally — confirmed empirically against a throwaway test
     model (sm_test_rebind) in DEV on 2026-08-07. Importing with zero
     relationships sidesteps that validator entirely.
  2. Prints the new item's ID. The 14 relationships must then be added via
     TOM, one at a time in a single session (same reason: incremental TOM
     writes don't hit the bulk-import ambiguity check) — run
     notebooks/nb_setup_semantic_model_relationships.Notebook's cell against
     this dataset/workspace to finish the job, then refresh the model.

Requires AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in the
environment (same Service Principal credentials deploy.py uses) — creating
the item via REST works fine with the SP; it's only the TOM/XMLA write step
afterward that needs a delegated user token (see README, Delegated
Authentication section — the same rejection pattern as the /admin/* APIs).

Usage:
    python scripts/deploy_semantic_model.py <branch>

<branch> selects config/valueSets/<branch>.json, same convention as
deploy.py (e.g. "dev", "qa", "main" for PRD).
"""
import base64
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import os

from fabric_client import FabricClient
from utils import load_valuesets, read_item_parts

REPO_ROOT = Path(__file__).resolve().parent.parent
SM_PATH = REPO_ROOT / "semantic models" / "sm_governance_medallion.SemanticModel"
SEMANTIC_MODEL_NAME = "sm_governance_medallion"


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/deploy_semantic_model.py <branch>")
        sys.exit(1)
    branch = sys.argv[1]

    target_config = load_valuesets(REPO_ROOT, branch)
    dev_config = load_valuesets(REPO_ROOT, "dev")
    workspace_id = target_config["workspace_id"]

    replacements: dict[str, str] = {}
    dev_url = dev_config.get("onelake_url", "")
    target_url = target_config.get("onelake_url", "")
    if dev_url and target_url and dev_url != target_url:
        replacements[dev_url] = target_url
    print(f"Target workspace: {workspace_id}")
    print(f"OneLake URL replacement: {dev_url!r} -> {target_url!r}\n")

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    existing = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
    if SEMANTIC_MODEL_NAME in existing:
        print(f"FAILED: {SEMANTIC_MODEL_NAME} already exists ({existing[SEMANTIC_MODEL_NAME]}) in this "
              f"workspace — delete it first if you intend to rebuild it from scratch.")
        sys.exit(1)

    parts = read_item_parts(SM_PATH, replacements or None)
    for part in parts:
        if part["path"] == "definition/relationships.tmdl":
            part["payload"] = base64.b64encode(b"").decode("ascii")

    print(f"Creating {SEMANTIC_MODEL_NAME} in target workspace from {len(parts)} parts "
          f"(relationships.tmdl emptied — add via TOM next)...")
    client.create_item(workspace_id, SEMANTIC_MODEL_NAME, "SemanticModel", parts)

    items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
    print(f"\nCreated: {SEMANTIC_MODEL_NAME} ({items[SEMANTIC_MODEL_NAME]})")
    print("\nNext: run nb_setup_semantic_model_relationships.Notebook's cell against this "
          f"dataset (workspace={workspace_id}) to add the 14 relationships via TOM and refresh.")


if __name__ == "__main__":
    main()
