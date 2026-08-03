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

# ### nb_bronze_activity_events
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** Power BI Admin REST API — `GET /v1.0/myorg/admin/activityevents`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_activity_events`
# **Schedule:** Daily
#
# This is the "who did it" source: publish, refresh, view, delete and other
# actions across the tenant, each with a UserId. Unlike the dimension notebooks
# in this layer, activity events are immutable by nature — an event that
# happened is never retroactively edited — so this table is append-only,
# no SCD needed.
#
# The API only accepts a window of at most 24 hours per call, so this notebook
# always pulls "yesterday, in full" — appropriate for a daily schedule. It also
# paginates via a full `continuationUri` in the response body, not a
# `continuationToken` query param like the Admin API endpoints used by the
# other Bronze notebooks — handled explicitly below.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_activity_events"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

# Pull the full previous UTC day — avoids partial-day gaps that would occur if
# we used "last 24h from now" on a schedule that doesn't run at exactly midnight.
# The API requires startDateTime/endDateTime to fall on the SAME UTC calendar
# day — spanning midnight-to-midnight (00:00:00 one day to 00:00:00 the next)
# fails with 400 Bad Request even though it's exactly 24h, because the end
# timestamp's date component is technically the following day. End at
# 23:59:59.999 of the same day instead.
TARGET_DATE = INGESTION_TS.date() - timedelta(days=1)
WINDOW_START = datetime.combine(TARGET_DATE, datetime.min.time(), tzinfo=timezone.utc)
WINDOW_END = datetime.combine(TARGET_DATE, datetime.max.time(), tzinfo=timezone.utc)

ACTIVITY_EVENTS_URL = (
    "https://api.powerbi.com/v1.0/myorg/admin/activityevents"
    f"?startDateTime='{WINDOW_START.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z'"
    f"&endDateTime='{WINDOW_END.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z'"
)

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] Window       : {WINDOW_START.isoformat()} -> {WINDOW_END.isoformat()}")
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

# Power BI's resource/audience, not Fabric's — activityevents lives on
# api.powerbi.com, unlike nb_bronze_workspaces/items which call
# api.fabric.microsoft.com.
TOKEN = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Activity Events (paginated via continuationUri)

# CELL ********************

events = []
url = ACTIVITY_EVENTS_URL
while url:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    page = response.json()
    events.extend(page.get("activityEventEntities", []))
    url = page.get("continuationUri")

print(f"Activity events fetched: {len(events)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame

# CELL ********************

import json as _json
from pyspark.sql.types import StructType, StructField, StringType

# Explicit schema, used whether or not there are events this day — every
# field here is optional per Activity type (e.g. object_id/refresh_type/
# result_status are None for most non-refresh activities), and Spark's type
# inference throws CANNOT_DETERMINE_TYPE if a column is null across every row
# in a run. An empty Python list needs the same schema too, or the write step
# below fails on a day with no activity at all.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("creation_time", StringType()),
    StructField("activity", StringType()),
    StructField("user_id", StringType()),
    StructField("workspace_id", StringType()),
    StructField("item_name", StringType()),
    StructField("object_id", StringType()),
    StructField("refresh_type", StringType()),
    StructField("result_status", StringType()),
    StructField("raw_json", StringType()),
])

rows = [
    {
        "event_id": e.get("Id"),
        "creation_time": e.get("CreationTime"),
        "activity": e.get("Activity"),
        "user_id": e.get("UserId"),
        "workspace_id": e.get("WorkspaceId"),
        "item_name": e.get("ItemName"),
        "object_id": e.get("ObjectId") or e.get("DatasetId") or e.get("ReportId"),
        "refresh_type": e.get("RefreshType"),
        "result_status": e.get("ResultStatus") or e.get("Status"),
        # Preserve the full payload — activity schemas vary a lot by Activity
        # value, and this is the audit source of truth; better to keep
        # everything than to silently drop fields we didn't anticipate.
        "raw_json": _json.dumps(e),
    }
    for e in events
]
df = spark.createDataFrame(rows, schema=EVENT_SCHEMA)

df = (
    df.withColumn("creation_time", F.to_timestamp("creation_time"))
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

# ### Idempotency Check and Append-Only Write

# CELL ********************

# Append-only: events are immutable, so we only need to guard against
# re-running the same day's pull twice, not merge/version anything.
already_ingested = False
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    count = (
        spark.read.format("delta").load(BRONZE_PATH)
        .filter(F.col("ingestion_date") == INGESTION_DATE)
        .count()
    )
    already_ingested = count > 0

if already_ingested:
    print(f"Activity events for {INGESTION_DATE} already ingested. Skipping.")
else:
    (df.write.format("delta").mode("append")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{df.count()} records written to {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

(spark.read.format("delta").load(BRONZE_PATH)
    .groupBy("ingestion_date", "activity")
    .count()
    .orderBy(F.desc("ingestion_date"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
