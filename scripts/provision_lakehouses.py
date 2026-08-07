#!/usr/bin/env python3
"""
Create lh_governance_bronze/silver/gold in a target workspace (if they don't
already exist) and update config/valueSets/<branch>.json's onelake_url with
the real lh_governance_gold ID — the step that was, until now, done by hand
for every new environment (DEV: commit c4c1f0f, QA: commit dae3a1a).

deploy.py already resolves lakehouse IDs dynamically for notebook METADATA
by name at deploy time — this script only needs to handle the one static
reference: onelake_url, used to patch the semantic model's DirectLake
source expression (scripts/deploy_semantic_model.py and, historically, the
model built by hand in the portal).

Requires AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET in the
environment (same Service Principal deploy.py uses — plain item creation
works fine with the SP; this isn't a TOM/XMLA write).

Usage:
    python scripts/provision_lakehouses.py <branch>
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import os

from fabric_client import FabricClient
from utils import load_valuesets

REPO_ROOT = Path(__file__).resolve().parent.parent
LAKEHOUSE_NAMES = ["lh_governance_bronze", "lh_governance_silver", "lh_governance_gold"]


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/provision_lakehouses.py <branch>")
        sys.exit(1)
    branch = sys.argv[1]

    config_path = REPO_ROOT / "config" / "valueSets" / f"{branch}.json"
    config = load_valuesets(REPO_ROOT, branch)
    workspace_id = config["workspace_id"]
    print(f"Target workspace: {workspace_id}\n")

    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    existing = {i["displayName"]: i["id"] for i in client.get_workspace_items(workspace_id)}
    for name in LAKEHOUSE_NAMES:
        if name in existing:
            print(f"  Exists: {name} ({existing[name]})")
            continue
        print(f"  Creating: {name}...")
        item_id = client.create_lakehouse(workspace_id, name)
        existing[name] = item_id
        print(f"  Created: {name} ({item_id})")

    gold_id = existing["lh_governance_gold"]
    new_onelake_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{gold_id}"

    if config.get("onelake_url") == new_onelake_url:
        print(f"\n{config_path.relative_to(REPO_ROOT)} already up to date.")
        return

    config["onelake_url"] = new_onelake_url
    config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    print(f"\nUpdated {config_path.relative_to(REPO_ROOT)}:")
    print(f"  onelake_url = {new_onelake_url}")


if __name__ == "__main__":
    main()
