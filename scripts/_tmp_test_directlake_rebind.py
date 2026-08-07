"""Throwaway validation script — NOT part of the real deploy tooling.

Creates a disposable copy of sm_governance_medallion (named sm_test_rebind)
in the DEV workspace via the same TMDL/REST path deploy.py would use, to
confirm the known "broken OneLake binding" bug reproduces on a fresh REST
deploy — then this is followed up by a notebook-side test of
sempy_labs.directlake.update_direct_lake_model_lakehouse_connection() to see
if it actually fixes the binding without an interactive rebuild.

Lives only on the test/directlake-rebind branch, never merged. Delete the
sm_test_rebind item and this file once validated either way.
"""
import base64
import os
from pathlib import Path

from fabric_client import FabricClient
from utils import read_item_parts

REPO_ROOT = Path(__file__).resolve().parent.parent
SM_PATH = REPO_ROOT / "semantic models" / "sm_governance_medallion.SemanticModel"
DEV_WORKSPACE_ID = "dc072922-4ffb-4424-868c-28087b02ecba"
TEST_MODEL_NAME = "sm_test_rebind"


def main() -> None:
    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )

    existing = {i["displayName"]: i["id"] for i in client.get_workspace_items(DEV_WORKSPACE_ID)}
    if TEST_MODEL_NAME in existing:
        print(f"{TEST_MODEL_NAME} already exists ({existing[TEST_MODEL_NAME]}) — delete it first.")
        return

    parts = read_item_parts(SM_PATH)
    # Test the two-phase hypothesis: strip all relationships from the initial
    # TMDL import (empty file), to see whether the bulk import succeeds when
    # there's no ambiguous-path graph to validate at all. Relationships would
    # then need to be added afterward via TOM, one at a time.
    for part in parts:
        if part["path"] == "definition/relationships.tmdl":
            part["payload"] = base64.b64encode(b"").decode("ascii")

    print(f"Creating {TEST_MODEL_NAME} in DEV from {len(parts)} parts (relationships.tmdl emptied)...")
    client.create_item(DEV_WORKSPACE_ID, TEST_MODEL_NAME, "SemanticModel", parts)

    items = {i["displayName"]: i["id"] for i in client.get_workspace_items(DEV_WORKSPACE_ID)}
    print(f"Created: {TEST_MODEL_NAME} ({items[TEST_MODEL_NAME]})")


if __name__ == "__main__":
    main()
