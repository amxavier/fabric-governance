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

# ### nb_refresh_semantic_model
#
# **Layer:** Orchestration — final step, not a medallion layer
# **Purpose:** Trigger `sm_governance_medallion`'s own refresh right after
# Gold finishes writing, so nobody has to remember to click Refresh in the
# portal before looking at the report. Closes the exact gap found live
# today: Gold had real data, but the report showed blank `fact_refresh`-
# dependent visuals because the Direct Lake model hadn't been refreshed
# since the last write.
#
# **Auth:** using the delegated refresh-token from the start here, not
# testing simple SP auth first — `executeQueries` already taught this
# project that "not literally `/admin/*`" doesn't reliably predict whether
# an endpoint accepts app-only auth, and this is the same
# `analysis.windows.net/powerbi/api` surface. If a future check finds
# simple auth actually works fine here too, this can be simplified then —
# but that's a confirmed finding, not an assumption to build on now.
#
# **Pipeline position:** depends on `update_gold_governance_model`, which
# transitively depends on every other notebook in the pipeline finishing
# first — so by the time this runs, none of the `_auth_delegated`
# consumers are still active. No risk of repeating the concurrent-refresh-
# token race this project already hit twice.

# CELL ********************

import requests
from datetime import datetime, timezone
from delta.tables import DeltaTable

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
ITEMS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

SEMANTIC_MODEL_NAME = "sm_governance_medallion"

if not DeltaTable.isDeltaTable(spark, ITEMS_PATH):
    raise RuntimeError("raw_items table not found — run nb_bronze_items before this notebook.")

sm_rows = (
    spark.read.format("delta").load(ITEMS_PATH)
    .filter(f"is_current = true and item_type = 'SemanticModel' and item_name = '{SEMANTIC_MODEL_NAME}'")
    .select("item_id", "workspace_id")
    .collect()
)
if not sm_rows:
    raise RuntimeError(f"{SEMANTIC_MODEL_NAME} not found in raw_items — has it been created in this workspace?")

DATASET_ID = sm_rows[0]["item_id"]
WORKSPACE_ID = sm_rows[0]["workspace_id"]
print(f"Refreshing {SEMANTIC_MODEL_NAME} ({DATASET_ID}) in workspace {WORKSPACE_ID}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Trigger Refresh (delegated auth)

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

TOKEN = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

resp = requests.post(
    f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{DATASET_ID}/refreshes",
    headers=HEADERS,
    json={"notifyOption": "NoNotification"},
    timeout=30,
)

print(f"Status: {resp.status_code}")
print(resp.text if resp.text else "(no body — 202 Accepted is expected)")

# 202 Accepted means the refresh was queued — it runs asynchronously in the
# service, this notebook doesn't wait for it to finish. The *next* pipeline
# run's nb_bronze_refresh_history will pick up its result (Completed/Failed)
# the same way it already picks up manual on-demand refreshes triggered
# from the portal.
if resp.status_code not in (200, 202):
    resp.raise_for_status()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
