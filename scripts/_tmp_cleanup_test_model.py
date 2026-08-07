"""Throwaway cleanup: delete sm_test_rebind from DEV workspace if it exists
(orphaned by a failed ambiguous-path import in _tmp_test_directlake_rebind.py)."""
import os

from fabric_client import FabricClient

DEV_WORKSPACE_ID = "dc072922-4ffb-4424-868c-28087b02ecba"
TEST_MODEL_NAME = "sm_test_rebind"


def main() -> None:
    client = FabricClient(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    items = {i["displayName"]: i["id"] for i in client.get_workspace_items(DEV_WORKSPACE_ID)}
    if TEST_MODEL_NAME not in items:
        print(f"{TEST_MODEL_NAME} not found — nothing to clean up.")
        return
    item_id = items[TEST_MODEL_NAME]
    print(f"Deleting {TEST_MODEL_NAME} ({item_id})...")
    client.delete_item(DEV_WORKSPACE_ID, item_id)
    print("Deleted.")


if __name__ == "__main__":
    main()
