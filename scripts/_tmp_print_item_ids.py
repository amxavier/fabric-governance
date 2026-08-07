"""Throwaway: print item IDs for a workspace (IDs aren't secret, safe to log)."""
import os
import sys

from fabric_client import FabricClient

WORKSPACE_ID = sys.argv[1]

client = FabricClient(
    tenant_id=os.environ["AZURE_TENANT_ID"],
    client_id=os.environ["AZURE_CLIENT_ID"],
    client_secret=os.environ["AZURE_CLIENT_SECRET"],
)
for i in client.get_workspace_items(WORKSPACE_ID):
    print(f"{i['type']:20s} {i['displayName']:45s} {i['id']}")
