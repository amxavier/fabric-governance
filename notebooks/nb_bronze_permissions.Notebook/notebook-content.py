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

# ### Confirmed (2026-07-31)
#
# **(A) Workspace role assignments** — simple SP auth worked. Real shape:
# `{"id": ..., "principal": {"id", "displayName", "type": "User"|"Group",
# "userDetails": {"userPrincipalName"} | "groupDetails": {"groupType"}},
# "role": "Admin"|...}`. One workspace (the special auto-provisioned "Admin
# monitoring" workspace) returned `400 WorkspaceTypeNotSupported` — a real,
# expected edge case for that workspace type, not a bug; skipped like any
# other per-item lookup failure elsewhere in this project.
#
# **(B) Per-item users** — needed the delegated token, consistent with
# every other Power BI-domain `/admin/*` endpoint in this project. Real
# shape: `{"datasetUserAccessRight", "emailAddress" (absent for some
# groups), "displayName", "identifier", "graphId", "principalType":
# "User"|"Group", "userType"}`. Built here for `SemanticModel` items only
# (Reports/Dataflows have the same-shaped `/admin/{type}/{id}/users`
# endpoints per Microsoft's docs, but untested against real data in this
# tenant — extending to them later should follow this exact pattern, not
# guess at the response shape first).

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType

INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

ROLES_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_workspace_role_assignments"
USERS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_item_users"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_workspace_role_assignments — Fetch, Build, and SCD2 Merge

# CELL ********************

all_workspaces = [
    row.asDict() for row in
    spark.read.format("delta").load(WORKSPACES_PATH)
        .filter("is_current = true")
        .select("workspace_id", "workspace_name")
        .collect()
]
print(f"Workspaces to check: {len(all_workspaces)}")

role_rows = []
skipped_workspaces = []
for ws in all_workspaces:
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws['workspace_id']}/roleAssignments",
        headers=SIMPLE_HEADERS, timeout=30,
    )
    if not resp.ok:
        # WorkspaceTypeNotSupported (special auto-provisioned workspaces
        # like "Admin monitoring") and similar — skip and keep going,
        # same reasoning as every other per-item loop in this project.
        skipped_workspaces.append((ws["workspace_id"], resp.status_code))
        continue
    for ra in resp.json().get("value", []):
        principal = ra.get("principal", {})
        role_rows.append({
            "workspace_id": ws["workspace_id"],
            "principal_id": principal.get("id"),
            "principal_display_name": principal.get("displayName"),
            "principal_type": principal.get("type"),
            "user_principal_name": (principal.get("userDetails") or {}).get("userPrincipalName"),
            "group_type": (principal.get("groupDetails") or {}).get("groupType"),
            "role": ra.get("role"),
        })

print(f"Role assignments fetched: {len(role_rows)}")
print(f"Workspaces skipped (unsupported type or no access): {len(skipped_workspaces)}")

df_roles = spark.createDataFrame(role_rows) if role_rows else spark.createDataFrame([], "workspace_id string, principal_id string, principal_display_name string, principal_type string, user_principal_name string, group_type string, role string")
df_roles_scd = (
    df_roles
    .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
    .withColumn("valid_from", F.col("ingestion_date"))
    .withColumn("valid_to", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
)

if not DeltaTable.isDeltaTable(spark, ROLES_PATH):
    (df_roles_scd.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(ROLES_PATH))
    print(f"{df_roles_scd.count()} records written to {ROLES_PATH} (initial load)")
else:
    dt = DeltaTable.forPath(spark, ROLES_PATH)
    current = spark.read.format("delta").load(ROLES_PATH).filter("is_current = true")

    changed = (
        df_roles_scd.alias("new")
        .join(current.alias("cur"), on=["workspace_id", "principal_id"], how="left")
        .where(
            "cur.workspace_id IS NULL OR "
            "NOT (new.principal_display_name <=> cur.principal_display_name) OR "
            "NOT (new.role <=> cur.role)"
        )
        .select("new.*")
    )
    print(f"Role assignments changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.workspace_id = source.workspace_id AND target.principal_id = source.principal_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(ROLES_PATH))
        print(f"{changed.count()} new versions written to {ROLES_PATH}")
    else:
        print("No role assignment changes detected. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_item_users — Fetch, Build, and SCD2 Merge

# CELL ********************

all_datasets = [
    row.asDict() for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type = 'SemanticModel'")
        .select("item_id", "item_name")
        .collect()
]
print(f"Semantic models to check: {len(all_datasets)}")

user_rows = []
skipped_datasets = []
for ds in all_datasets:
    resp = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{ds['item_id']}/users",
        headers=DELEGATED_HEADERS, timeout=30,
    )
    if not resp.ok:
        skipped_datasets.append((ds["item_id"], resp.status_code))
        continue
    for u in resp.json().get("value", []):
        user_rows.append({
            "item_id": ds["item_id"],
            "principal_identifier": u.get("identifier"),
            "principal_display_name": u.get("displayName"),
            "principal_type": u.get("principalType"),
            "email_address": u.get("emailAddress"),
            "graph_id": u.get("graphId"),
            "access_right": u.get("datasetUserAccessRight"),
            "user_type": u.get("userType"),
        })

print(f"Item user rows fetched: {len(user_rows)}")
print(f"Datasets skipped (no access/lookup unavailable): {len(skipped_datasets)}")

USER_SCHEMA = "item_id string, principal_identifier string, principal_display_name string, principal_type string, email_address string, graph_id string, access_right string, user_type string"
df_users = spark.createDataFrame(user_rows) if user_rows else spark.createDataFrame([], USER_SCHEMA)
df_users_scd = (
    df_users
    .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
    .withColumn("valid_from", F.col("ingestion_date"))
    .withColumn("valid_to", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
)

if not DeltaTable.isDeltaTable(spark, USERS_PATH):
    (df_users_scd.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(USERS_PATH))
    print(f"{df_users_scd.count()} records written to {USERS_PATH} (initial load)")
else:
    dt = DeltaTable.forPath(spark, USERS_PATH)
    current = spark.read.format("delta").load(USERS_PATH).filter("is_current = true")

    changed = (
        df_users_scd.alias("new")
        .join(current.alias("cur"), on=["item_id", "principal_identifier"], how="left")
        .where(
            "cur.item_id IS NULL OR "
            "NOT (new.access_right <=> cur.access_right)"
        )
        .select("new.*")
    )
    print(f"Item/user pairs changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.item_id = source.item_id AND target.principal_identifier = source.principal_identifier AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(USERS_PATH))
        print(f"{changed.count()} new versions written to {USERS_PATH}")
    else:
        print("No item/user changes detected. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

print("--- raw_workspace_role_assignments ---")
spark.read.format("delta").load(ROLES_PATH).filter("is_current = true") \
    .select("workspace_id", "principal_display_name", "principal_type", "role").show(50, truncate=False)

print("--- raw_item_users ---")
spark.read.format("delta").load(USERS_PATH).filter("is_current = true") \
    .select("item_id", "principal_display_name", "principal_type", "access_right").show(50, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
