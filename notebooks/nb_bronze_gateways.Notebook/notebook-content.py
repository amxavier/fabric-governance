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

# ### nb_bronze_gateways
#
# **Layer:** Bronze — Raw Ingestion, SCD Type 2
# **Source:** Power BI Admin REST API — `GET /v1.0/myorg/admin/gateways`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_gateways`
# **Schedule:** Daily
#
# On-premises gateways are a classic silent-failure point — a gateway going
# offline doesn't raise an error anywhere obvious, it just makes every
# refresh that depends on it start failing. This closes that gap: gateway
# status becomes its own tracked dimension, joinable against
# `fact_refresh`/`fact_activity` the same way everything else in this
# project is, instead of a support call being the first signal.
#
# Dimension-like (a gateway's status/config changes over time, same shape
# as capacities/workspaces/items) — SCD Type 2, not append-only.
#
# **Auth:** `/admin/*` endpoint, so this needs the same delegated
# refresh-token workaround as the other four Bronze notebooks that hit
# Admin APIs (`/admin/*` rejects app-only Service Principal tokens
# outright — see README / the other notebooks' markdown for the full
# diagnosis). Same reasoning applies to `nb_bronze_capacity_metrics`'s
# `executeQueries` call, even though that one isn't literally `/admin/*`.


# MARKDOWN ********************

# ### Configuration

# CELL ********************

DESTINATION_TABLE = "raw_gateways"

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path              : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exploration — confirm the real response shape before writing the SCD2 merge
#
# The Admin API's gateway response schema isn't something we've verified
# against this tenant yet — same lesson as every other source in this
# project: read the real JSON before assuming field names. **Run this cell
# and inspect the output before the next cell is written.**

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone

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

resp = requests.get("https://api.powerbi.com/v1.0/myorg/admin/gateways", headers=HEADERS, timeout=30)
resp.raise_for_status()
gateways_raw = resp.json()

print(f"Top-level keys: {list(gateways_raw.keys())}")
value = gateways_raw.get("value", gateways_raw)
print(f"Gateways returned: {len(value) if isinstance(value, list) else 'n/a'}")
print(_json.dumps(value[:3] if isinstance(value, list) else value, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Next step
#
# Share the printed output: the real field names (does `gatewayAnnotation`
# come back as a nested object or a JSON-encoded string? is there a
# `status`/`type` field directly, or does status need a separate
# `/admin/gateways/{id}` or `/datasources` call per gateway?), and how many
# gateways exist in this tenant. The SCD2 merge (same pattern as
# `nb_bronze_capacities`/`nb_bronze_workspaces`) gets written right after.
