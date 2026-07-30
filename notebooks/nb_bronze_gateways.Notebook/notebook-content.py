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

# ### Confirmed (2026-07-30): zero gateways in this tenant
#
# `/admin/gateways` returned an empty `value` list — expected, since this
# entire project runs on native Fabric items (Lakehouse, Direct Lake,
# Notebooks) with no on-premises source requiring a gateway. The base
# gateway list also does **not** include datasource/connection details —
# that's a separate resource.
#
# The more useful axis for this project turns out not to be "loop over
# gateways" (there are none) but **`GET /admin/datasets/{datasetId}/datasources`**
# — per semantic model, still a tenant-admin endpoint (same permission
# model as everything else in this notebook), and it doesn't require the
# separate "gateway admin" role that `/gateways/{id}/datasources` would.
# This is testable right now against real semantic model ids from
# `raw_items`, gateway or no gateway — a cloud connection still shows up
# here, just with a null/empty gateway reference. That mapping (which
# datasource/gateway a given semantic model depends on) is the piece that
# actually lets a future correlation like "this refresh failed — was its
# gateway down?" work, once/if a gateway ever exists in this tenant.

# CELL ********************

if not DeltaTable.isDeltaTable(spark, f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"):
    raise RuntimeError("raw_items table not found — run nb_bronze_items before this cell.")

semantic_model_ids = [
    row["item_id"] for row in
    spark.read.format("delta").load(f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items")
        .filter("is_current = true and item_type = 'SemanticModel'")
        .select("item_id")
        .collect()
]
print(f"Semantic models to check: {len(semantic_model_ids)}")

sample_results = []
for dataset_id in semantic_model_ids[:5]:
    resp = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{dataset_id}/datasources",
        headers=HEADERS, timeout=30,
    )
    sample_results.append({
        "dataset_id": dataset_id,
        "status_code": resp.status_code,
        "body": resp.json() if resp.ok else resp.text,
    })

print(_json.dumps(sample_results, indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Next step
#
# Share the printed output for the 5 datasets sampled: the real field names
# for each datasource entry (datasourceId, gatewayId, datasourceType,
# connectionDetails — server/database or similar?), and whether models with
# no gateway dependency return an empty list vs. an entry with a null
# gatewayId. That decides the grain for `raw_dataset_datasources` (likely
# one row per dataset × datasource, SCD2 like the other dimension-style
# Bronze tables — a model's data source can change over time).
