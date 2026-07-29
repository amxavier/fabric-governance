"""Manual utility: pull an item's current definition from a Fabric workspace
back into this repo.

Exists specifically for sm_governance_medallion: that semantic model is a
DirectLake model rebuilt interactively in the portal (see README) because
models deployed via the REST API have a broken OneLake binding, so deploy.py
no longer manages its content — this is the other direction, syncing the
live, working definition back into git so it stays a readable, versioned
snapshot instead of drifting invisibly out of sync with what's actually
running.

Usage:
    python scripts/pull_item_definition.py <workspace_id> <item_display_name> <target_folder>

Requires AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in the
environment (same Service Principal credentials deploy.py uses).
"""
import base64
import os
import shutil
import sys
from pathlib import Path

from fabric_client import FabricClient

REPO_ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python scripts/pull_item_definition.py <workspace_id> <item_display_name> <target_folder>")
        sys.exit(1)

    workspace_id, display_name, target_folder = sys.argv[1], sys.argv[2], sys.argv[3]

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    items = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
    if display_name not in items:
        print(f"FAILED: {display_name} not found in workspace {workspace_id}")
        sys.exit(1)
    item_id = items[display_name]

    parts = client.get_item_definition(workspace_id, item_id)
    print(f"Pulled {len(parts)} parts for {display_name} ({item_id})")

    item_path = REPO_ROOT / target_folder
    definition_path = item_path / "definition"
    if definition_path.exists():
        shutil.rmtree(definition_path)
        print(f"Cleared stale {definition_path}")

    for part in parts:
        raw = base64.b64decode(part["payload"])
        out_path = item_path / part["path"]
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(raw)
        print(f"  wrote {part['path']} ({len(raw)} bytes)")

    print("Done.")


if __name__ == "__main__":
    main()
