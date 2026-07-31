# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7eb57c95-5be1-421d-aee0-8ac6bde14d68",
# META       "default_lakehouse_name": "lh_governance_bronze",
# META       "default_lakehouse_workspace_id": "dc072922-4ffb-4424-868c-28087b02ecba",
# META       "known_lakehouses": [
# META         {
# META           "id": "7eb57c95-5be1-421d-aee0-8ac6bde14d68"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### nb_bronze_permissions
#
# **Layer:** Bronze — Raw Ingestion, SCD Type 2
# **Sources (two complementary angles, being tested here):**
#   - Fabric Core API — `GET /v1/workspaces/{workspaceId}/roleAssignments` → who has Admin/Member/Contributor/Viewer on each workspace (covers every Fabric-native item in this tenant: Lakehouse, Notebook, DataPipeline, SQLEndpoint — none of these have individual per-item sharing, access is workspace-scoped)
#   - Power BI Admin API — `GET /admin/datasets/{datasetId}/users` (and the `/reports`, `/dataflows` equivalents) → individual per-item sharing, which exists on top of workspace roles for classic Power BI artifact types
# **Destination:** `lh_governance_bronze` → `raw_workspace_role_assignments`, `raw_item_users`
# **Schedule:** Daily
#
# This is the LGPD/security-audit angle: who can access what, consolidated
# instead of checked one workspace at a time in the portal.
#
# Dimension-like (role assignments and sharing change over time) — SCD
# Type 2, same pattern as the other dimension Bronze tables.
#
# **Auth (to be confirmed empirically below, not assumed):** workspace role
# assignments is a plain Fabric Core API (like `/v1/capacities`), so the
# simple `notebookutils.credentials.getToken("pbi")` pattern is expected to
# work. Item users is a Power BI **Admin** API (`/admin/*`), so it's
# expected to need the same delegated refresh-token workaround as the
# other `/admin/*` Bronze notebooks — but expected is not confirmed, so
# both get tested here rather than assumed.


# MARKDOWN ********************

# ### Configuration

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
WORKSPACES_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_workspaces"
ITEMS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exploration A — workspace role assignments (simple SP auth)

# CELL ********************

SIMPLE_TOKEN = notebookutils.credentials.getToken("pbi")
SIMPLE_HEADERS = {"Authorization": f"Bearer {SIMPLE_TOKEN}", "Content-Type": "application/json"}

sample_workspaces = [
    row.asDict() for row in
    spark.read.format("delta").load(WORKSPACES_PATH)
        .filter("is_current = true")
        .select("workspace_id", "workspace_name")
        .limit(3)
        .collect()
]
print(f"Sample workspaces to test: {len(sample_workspaces)}")

role_results = []
for ws in sample_workspaces:
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws['workspace_id']}/roleAssignments",
        headers=SIMPLE_HEADERS, timeout=30,
    )
    role_results.append({
        "workspace_name": ws["workspace_name"],
        "status_code": resp.status_code,
        "body": resp.json() if resp.ok else resp.text,
    })

print(_json.dumps(role_results, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exploration B — per-item users (Power BI Admin API, delegated auth)

# CELL ********************

def _get_delegated_token(scope: str) -> str:
    auth_row = spark.read.format("delta").load(AUTH_TABLE_PATH).collect()[0]
    resp = requests.post(
        f"https://login.microsoftonline.com/{auth_row['tenant_id']}/oauth2/v2.0/token",
        data={
            "grant_type": "refresh_token",
            "client_id": auth_row["client_id"],
            "refresh_token": auth_row["refresh_token"],
            "scope": f"{scope} offline_access",
        },
        timeout=30,
    )
    resp.raise_for_status()
    token_data = resp.json()

    new_refresh_token = token_data.get("refresh_token", auth_row["refresh_token"])
    spark.createDataFrame([{
        "tenant_id": auth_row["tenant_id"],
        "client_id": auth_row["client_id"],
        "refresh_token": new_refresh_token,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }]).write.format("delta").mode("overwrite").save(AUTH_TABLE_PATH)

    return token_data["access_token"]

DELEGATED_TOKEN = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
DELEGATED_HEADERS = {"Authorization": f"Bearer {DELEGATED_TOKEN}", "Content-Type": "application/json"}

sample_datasets = [
    row.asDict() for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type = 'SemanticModel'")
        .select("item_id", "item_name")
        .limit(3)
        .collect()
]
print(f"Sample semantic models to test: {len(sample_datasets)}")

user_results = []
for ds in sample_datasets:
    resp = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{ds['item_id']}/users",
        headers=DELEGATED_HEADERS, timeout=30,
    )
    user_results.append({
        "item_name": ds["item_name"],
        "status_code": resp.status_code,
        "body": resp.json() if resp.ok else resp.text,
    })

print(_json.dumps(user_results, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Next step
#
# Share both printed outputs: (A) did `roleAssignments` work with simple
# auth, and what are the real field names (principal id/type/displayName,
# role)? (B) did `/admin/datasets/{id}/users` need delegated auth or did
# simple auth also work here, and what fields come back (accessRight,
# principal type, email/displayName)? The SCD2 merges for both
# `raw_workspace_role_assignments` and `raw_item_users` get written right
# after, once both are confirmed.
