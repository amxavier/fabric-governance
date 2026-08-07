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

# ### nb_bronze_workspaces
#
# **Layer:** Bronze — Raw Ingestion, SCD Type 2
# **Source:** Fabric Admin REST API — `GET /v1/admin/workspaces`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_workspaces`
# **Schedule:** Daily
#
# Tenant-wide workspace inventory, independent of whether the Service Principal
# is a member of each workspace (Admin API scope, not per-workspace scope — see
# repo README prerequisites for the tenant setting this requires).


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_workspaces"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"
WORKSPACES_URL = "https://api.fabric.microsoft.com/v1/admin/workspaces"

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
# same symptom). See the README for the full diagnosis, and
# `nb_util_delegated_auth` for the token-exchange implementation shared by
# every notebook in this position — it used to be duplicated verbatim here.

# CELL ********************

%run nb_util_delegated_auth

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

TOKEN = _get_delegated_token("https://api.fabric.microsoft.com/.default")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Workspaces from Admin API (paginated)

# CELL ********************

# The Admin API paginates via continuationToken (a query param), unlike the
# Power BI Activity Events endpoint used in nb_bronze_activity_events, which
# paginates via a full continuationUri instead. Each source's pagination style
# is handled locally in its own notebook rather than shared, to keep each
# notebook self-contained and independently readable.
data = []
url = WORKSPACES_URL
while url:
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    page = response.json()
    # This Admin API nests results under "workspaces", not the generic "value"
    # key most other Fabric REST endpoints use — confirmed against Microsoft
    # Learn after a live run silently returned 0 rows with the wrong key.
    data.extend(page.get("workspaces", []))
    token = page.get("continuationToken")
    url = f"{WORKSPACES_URL}?continuationToken={token}" if token else None

print(f"Workspaces fetched: {len(data)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame

# CELL ********************

rows = [
    {
        "workspace_id": w.get("id"),
        "workspace_name": w.get("name"),
        "workspace_type": w.get("type"),
        "capacity_id": w.get("capacityId"),
        "state": w.get("state"),
    }
    for w in data
]

# Explicit schema — capacity_id is null for any workspace not assigned to a
# capacity, and Spark's type inference throws CANNOT_DETERMINE_TYPE when a
# column is null in every row of a fetch, same lesson as the other Bronze
# notebooks.
WORKSPACE_SCHEMA = StructType([
    StructField("workspace_id", StringType()),
    StructField("workspace_name", StringType()),
    StructField("workspace_type", StringType()),
    StructField("capacity_id", StringType()),
    StructField("state", StringType()),
])

df = spark.createDataFrame(rows, schema=WORKSPACE_SCHEMA)
df = (
    df.withColumn("ingestion_ts", F.lit(INGESTION_TS.isoformat()).cast("timestamp"))
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

# ### SCD Type 2 Merge into Bronze

# CELL ********************

df_scd = (
    df.withColumn("valid_from", F.col("ingestion_date"))
      .withColumn("valid_to", F.lit(None).cast(DateType()))
      .withColumn("is_current", F.lit(True))
)

if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    (df_scd.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(BRONZE_PATH))
    print(f"{df_scd.count()} records written to {BRONZE_PATH} (initial load)")
else:
    dt = DeltaTable.forPath(spark, BRONZE_PATH)

    # Only expire+reinsert workspaces whose tracked attributes actually changed —
    # e.g. renamed, moved to a different capacity, or state changed (Active/Deleted).
    # This is exactly the kind of change that can explain "why did this workspace's
    # refreshes suddenly start failing" when correlated with fact_refresh in Gold.
    # Uses <=> (null-safe equality) rather than <> — see nb_bronze_capacities for why.
    current = spark.read.format("delta").load(BRONZE_PATH).filter("is_current = true")
    changed = (
        df_scd.alias("new")
        .join(current.alias("cur"), on="workspace_id", how="left")
        .where(
            "cur.workspace_id IS NULL OR "
            "NOT (new.workspace_name <=> cur.workspace_name) OR "
            "NOT (new.capacity_id <=> cur.capacity_id) OR "
            "NOT (new.state <=> cur.state)"
        )
        .select("new.*")
    )
    print(f"Workspaces changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.workspace_id = source.workspace_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(BRONZE_PATH))
        print(f"{changed.count()} new versions written to {BRONZE_PATH}")
    else:
        print("No workspace changes detected. Nothing written.")

    # A business key present in `current` but absent from this run's fetch
    # (workspace deleted, or recreated under a new workspace_id) is never
    # touched by the `changed` merge above, since that only compares keys the
    # new fetch still has. Without this, the old workspace_id's row stays
    # is_current = true forever.
    removed = current.join(df_scd.select("workspace_id"), on="workspace_id", how="left_anti")
    print(f"Workspaces removed since last snapshot: {removed.count()}")

    if removed.count() > 0:
        dt.alias("target").merge(
            removed.withColumn("retired_on", F.lit(INGESTION_DATE).cast(DateType())).alias("source"),
            "target.workspace_id = source.workspace_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.retired_on",
            "is_current": "false",
        }).execute()
        print(f"{removed.count()} workspace(s) retired (no longer present in source)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

df_val = spark.read.format("delta").load(BRONZE_PATH)
print(f"Total rows (all versions): {df_val.count()}")
print(f"Current rows (is_current = True): {df_val.filter('is_current = true').count()}")
df_val.filter("is_current = true").groupBy("workspace_type", "state").count().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
