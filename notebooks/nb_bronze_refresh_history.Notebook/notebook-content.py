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

# ### nb_bronze_refresh_history
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** Power BI Admin REST API — `GET /v1.0/myorg/admin/datasets/{id}/refreshhistory`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_refresh_history`
# **Depends on:** `nb_bronze_items` (needs the current list of SemanticModel ids)
# **Schedule:** Daily, after nb_bronze_items
#
# This is the direct complement to nb_bronze_activity_events: the Activity Log
# only records a "RefreshDataset" activity with a UserId when a refresh was
# triggered on-demand by a person. Scheduled refreshes never appear there as a
# user action — but every refresh, scheduled or on-demand, shows up here, with
# status and (on failure) an error message. Joining the two in Gold is what
# lets us answer "did this fail because of something someone changed?".
#
# Append-only: a refresh run, once finished, does not change — no SCD needed.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_refresh_history"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()
REFRESH_HISTORY_TOP = 30  # per dataset, per run — enough to cover a daily schedule with margin

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
ITEMS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path   : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Delegated Authentication
#
# `/admin/*` endpoints (both Fabric's and Power BI's) rejected every app-only
# Service Principal token tried here — `notebookutils.credentials.getToken()`
# and a raw `client_credentials` OAuth call both got 403/401, while an
# interactively-obtained delegated user token succeeded immediately. This
# isn't a tenant misconfiguration; some Admin APIs simply don't support
# app-only auth (confirmed against Microsoft Fabric community reports of the
# same symptom). See the README for the full diagnosis.
#
# The fix: exchange a refresh token — obtained once via an interactive
# device-code + MFA sign-in — for a fresh access token on every run, and
# persist the rotated refresh token Microsoft returns back into this same
# Delta table so the next run can still authenticate. tenant_id/client_id
# aren't secrets; only the refresh token is, and it never leaves this
# lakehouse (no Key Vault needed, no risk of it landing in git or in a public
# GitHub Actions log).

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

# refreshhistory lives on api.powerbi.com — Power BI's resource, not Fabric's.
TOKEN = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Load Current Semantic Model IDs from raw_items

# CELL ********************

if not DeltaTable.isDeltaTable(spark, ITEMS_PATH):
    raise RuntimeError(
        "raw_items table not found — run nb_bronze_items before this notebook."
    )

semantic_model_ids = [
    row["item_id"] for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type = 'SemanticModel'")
        .select("item_id")
        .collect()
]

print(f"Semantic models to check: {len(semantic_model_ids)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Refresh History per Semantic Model

# CELL ********************

rows = []
failed_lookups = []
for dataset_id in semantic_model_ids:
    url = (
        f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{dataset_id}/refreshhistory"
        f"?$top={REFRESH_HISTORY_TOP}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if not resp.ok:
        # Some datasets are not refreshable (e.g. push datasets, DirectLake-only
        # models with no scheduled refresh) — the API returns an error for those.
        # We skip and keep going rather than fail the whole notebook run.
        failed_lookups.append(dataset_id)
        continue
    for r in resp.json().get("value", []):
        rows.append({
            "dataset_id": dataset_id,
            "refresh_id": r.get("requestId"),
            "refresh_type": r.get("refreshType"),
            "start_time": r.get("startTime"),
            "end_time": r.get("endTime"),
            "status": r.get("status"),
            "error_json": _json.dumps(r.get("serviceExceptionJson")) if r.get("serviceExceptionJson") else None,
        })

print(f"Refresh runs fetched: {len(rows)}")
print(f"Datasets with no refresh history available: {len(failed_lookups)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame

# CELL ********************

if rows:
    df = spark.createDataFrame(rows)
else:
    schema = StructType([
        StructField("dataset_id", StringType()),
        StructField("refresh_id", StringType()),
        StructField("refresh_type", StringType()),
        StructField("start_time", StringType()),
        StructField("end_time", StringType()),
        StructField("status", StringType()),
        StructField("error_json", StringType()),
    ])
    df = spark.createDataFrame([], schema)

df = (
    df.withColumn("start_time", F.to_timestamp("start_time"))
      .withColumn("end_time", F.to_timestamp("end_time"))
      .withColumn("ingestion_ts", F.lit(INGESTION_TS.isoformat()).cast("timestamp"))
      .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
)

print("Schema:")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Append-Only Write (deduplicated by refresh_id)

# CELL ********************

# Unlike nb_bronze_activity_events (one pull per calendar day), refresh history
# can return the same refresh_id again on a later run before it appears as
# "old enough" to disappear from the API's own retention window — so we
# deduplicate by refresh_id against what's already in Bronze, instead of a
# per-day skip check.
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    existing_ids = (
        spark.read.format("delta").load(BRONZE_PATH)
        .select("refresh_id").distinct()
    )
    df_new = df.join(existing_ids, on="refresh_id", how="left_anti")
else:
    df_new = df

new_count = df_new.count()
print(f"New refresh runs to write: {new_count}")

if new_count > 0:
    (df_new.write.format("delta").mode("append")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{new_count} records written to {BRONZE_PATH}")
else:
    print("No new refresh runs. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(BRONZE_PATH)
    .groupBy("status")
    .count()
    .orderBy(F.desc("count"))
    .show())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
