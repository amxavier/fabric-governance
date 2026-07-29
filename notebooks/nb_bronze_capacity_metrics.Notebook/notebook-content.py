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

# ### nb_bronze_capacity_metrics
#
# **Layer:** Bronze — Raw Ingestion, append-only
# **Source:** The "Fabric Capacity Metrics" app's own semantic model (DAX query via `executeQueries` REST API)
# **Destination:** `lh_governance_bronze` → Delta Table `raw_capacity_metrics`
# **Schedule:** Daily
#
# This is the "how much did it cost" source that completes the governance
# picture: `fact_activity` says who changed what, `fact_refresh` says what
# broke, and this adds what it actually cost in CU(s)/$ — so a diagnosis can
# extend from "why did this break" to "was this worth what it's costing" and
# "which process should we optimize first."
#
# This is **not** an `/admin/*` endpoint — it's a normal DAX query against a
# semantic model — but empirically it turned out to need the exact same
# delegated refresh-token workaround as the other four Bronze notebooks
# anyway (see below), just for a different reason.
#
# **Why REST `executeQueries` instead of `semantic-link` (`sempy.fabric`):**
# `semantic-link`'s `list_datasets`/`list_tables`/`evaluate_dax` all go
# through the semantic model's **XMLA endpoint**. The workspace hosting the
# Capacity Metrics app in this tenant runs on a **Fabric Trial (FT1)
# capacity**, and FT1 does not expose an XMLA Endpoint setting at all — it's
# a structural limitation of the trial SKU, not a toggle we can enable, and
# it doesn't matter which workspace is targeted since XMLA never worked
# against this capacity to begin with. The plain `executeQueries` REST API
# doesn't use XMLA, so it isn't affected by this.
#
# **Why `executeQueries` still needs delegated auth, not
# `notebookutils.credentials.getToken("pbi")`:** worked fine when run
# interactively (as a real user), but failed with `403 Forbidden` every time
# when run via the scheduled pipeline (as the `sp-fabric-cicd` Service
# Principal) — even after confirming, one by one: the tenant setting
# "Semantic Model Execute Queries REST API" enabled for the entire
# organization, the SP has Admin access on the Capacity Metrics workspace
# (both directly and via its security group), all relevant Developer
# settings enabled for that group, and the SP's App Registration already
# holds `Tenant.Read.All`/`Tenant.ReadWrite.All` (Application, admin-
# consented) — Power BI Service doesn't even offer a narrower Application
# permission than that. With every configuration angle ruled out, this
# matches the same platform-level pattern as the `/admin/*` endpoints:
# `executeQueries` behaves as if it doesn't support app-only Service
# Principal auth here either. The fix is the same one already proven for
# the other four Bronze notebooks: exchange a delegated user refresh token
# for an access token instead of using the SP's own identity.
#
# The Capacity Metrics app only exposes a rolling 14-day window in its own
# report — this notebook's job is to capture it daily, append-only, so we
# keep history beyond that window (same reasoning as raw_activity_events).


# MARKDOWN ********************

# ### Configuration

# CELL ********************

DESTINATION_TABLE = "raw_capacity_metrics"

# Workspace/dataset where the "Fabric Capacity Metrics" app is installed in
# this tenant — a single, tenant-wide app, not one per dev/qa/prd
# environment, so these are fixed constants rather than environment-
# parameterized values. Confirmed via the portal (Onelake > dataset details
# URL) rather than guessed.
METRICS_WORKSPACE_ID = "aee1a4b3-ad5a-4bd8-a648-0120aad157ef"
METRICS_DATASET_ID = "a2354849-42a1-40b1-80ed-473e68401b75"

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

# ### (Historical / do not run) Exploration via semantic-link
#
# Kept only as a record of how the real schema below was discovered — these
# cells fail with `OperationException: ... does not have permission to call
# the Discover method` on this tenant's FT1 trial capacity, regardless of
# workspace permissions (confirmed: workspace Admin access, tenant-wide XMLA
# setting enabled, capacity settings for FT1 don't even expose an XMLA
# Endpoint option). Superseded by the `executeQueries` REST cells further
# below, which is what this notebook actually runs.

# CELL ********************

# import sempy.fabric as fabric
# datasets = fabric.list_datasets(workspace=METRICS_WORKSPACE_ID)
# tables = fabric.list_tables("Fabric Capacity Metrics", workspace=METRICS_WORKSPACE_ID)
# cols = fabric.list_columns("Fabric Capacity Metrics", workspace=METRICS_WORKSPACE_ID)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed schema (2026-07-28)
#
# `CUDetail` is capacity-level only (no ItemId/WorkspaceId) — not useful
# here. `Items` is a dimension (ItemId, ItemName, WorkspaceId,
# WorkspaceName, ItemKind — no CU numbers). The table that actually has what
# we need is **`MetricsByItem`**: grain per `ItemId`, with `sum_CU`,
# `sum_duration`, operation counts by status
# (`count_successful_operations`, `count_failure_operations`,
# `count_cancelled_operations`, `count_rejected_operations`), `WorkspaceId`,
# `PremiumCapacityId`.
#
# Confirmed via the app's own report (the "Items (14 days)" table): this is
# a **rolling 14-day rolled-up total per item**, not sliced per day — there
# is no date column on `MetricsByItem` to query against. That means this
# notebook can't ask the API "give me yesterday's CU(s) for this item" the
# way `nb_bronze_activity_events` can; it can only get "the last 14 days,
# as of right now." Consequence for the ingestion design below: write this
# as a **daily snapshot** (like the SCD2 dimension notebooks), stamped with
# `ingestion_date`, and let Silver derive real day-over-day deltas by
# diffing consecutive snapshots — rather than treat it as a naturally
# append-only, already-atomic-per-day fact like refresh history or activity
# events.

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType, DoubleType, LongType
from delta.tables import DeltaTable

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

def _execute_dax(workspace_id: str, dataset_id: str, dax_query: str) -> list[dict]:
    token = _get_delegated_token("https://analysis.windows.net/powerbi/api/.default")
    resp = requests.post(
        f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"queries": [{"query": dax_query}], "serializerSettings": {"includeNulls": True}},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()["results"][0]["tables"][0]["rows"]

metrics_rows = _execute_dax(METRICS_WORKSPACE_ID, METRICS_DATASET_ID, "EVALUATE MetricsByItem")
print(f"MetricsByItem rows: {len(metrics_rows)}")
print(_json.dumps(metrics_rows[:3], indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

items_rows = _execute_dax(METRICS_WORKSPACE_ID, METRICS_DATASET_ID, "EVALUATE Items")
print(f"Items rows: {len(items_rows)}")
print(_json.dumps(items_rows[:3], indent=2, default=str))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Confirmed columns (2026-07-28) and build DataFrame
#
# `sum_CU`/`sum_duration` match the app report's "CU(s)"/"Duration(s)"
# columns exactly (spot-checked against the same item) — confirms units are
# CU-seconds and seconds, not some other scale. `sum_duration` itself has no
# `Ms` suffix unlike the percentile/avg columns, which are in milliseconds.
#
# `ItemId`/`WorkspaceId` here come back as **uppercase** GUIDs, while this
# project's other Bronze sources (`raw_items`, `raw_workspaces`, sourced
# from the Fabric Admin API) use lowercase — normalizing to lowercase here
# so Silver's join against `silver_items` actually matches instead of
# silently returning zero rows.
#
# Not persisting the `Items` query result — it's redundant with this
# project's own `raw_items`/`silver_items` (already sourced from the Fabric
# Admin API with proper SCD2 history), so Silver will join `item_id`
# straight into `silver_items` instead of carrying a second, competing
# source of item names.

# CELL ********************

# Explicit schema instead of relying on Spark's type inference — a column
# that's null across every row in a run (e.g. an item with no throttling
# data at all) makes inference fail outright with CANNOT_DETERMINE_TYPE,
# as found empirically with the sibling nb_bronze_capacity_cu_detail.
ROW_SCHEMA = StructType([
    StructField("item_id", StringType()),
    StructField("artifact_kind", StringType()),
    StructField("workspace_id", StringType()),
    StructField("premium_capacity_id", StringType()),
    StructField("billing_type", StringType()),
    StructField("sum_cu", DoubleType()),
    StructField("sum_duration_s", DoubleType()),
    StructField("avg_duration_ms", DoubleType()),
    StructField("percentile_duration_ms_50", DoubleType()),
    StructField("percentile_duration_ms_90", DoubleType()),
    StructField("count_operations", LongType()),
    StructField("count_successful_operations", LongType()),
    StructField("count_failure_operations", LongType()),
    StructField("count_cancelled_operations", LongType()),
    StructField("count_rejected_operations", LongType()),
    StructField("count_inprogress_operations", LongType()),
    StructField("count_invalid_operations", LongType()),
    StructField("count_users", LongType()),
    StructField("throttling_min", DoubleType()),
])

rows = [
    {
        "item_id": r["MetricsByItem[ItemId]"].lower(),
        "artifact_kind": r["MetricsByItem[ArtifactKind]"],
        "workspace_id": r["MetricsByItem[WorkspaceId]"].lower(),
        "premium_capacity_id": r["MetricsByItem[PremiumCapacityId]"],
        "billing_type": r["MetricsByItem[Billing type]"],
        "sum_cu": r["MetricsByItem[sum_CU]"],
        "sum_duration_s": r["MetricsByItem[sum_duration]"],
        "avg_duration_ms": r["MetricsByItem[avg_DurationMs]"],
        "percentile_duration_ms_50": r["MetricsByItem[percentile_DurationMs_50]"],
        "percentile_duration_ms_90": r["MetricsByItem[percentile_DurationMs_90]"],
        "count_operations": r["MetricsByItem[count_operations]"],
        "count_successful_operations": r["MetricsByItem[count_successful_operations]"],
        "count_failure_operations": r["MetricsByItem[count_failure_operations]"],
        "count_cancelled_operations": r["MetricsByItem[count_cancelled_operations]"],
        "count_rejected_operations": r["MetricsByItem[count_rejected_operations]"],
        "count_inprogress_operations": r["MetricsByItem[count_InProgress_operations]"],
        "count_invalid_operations": r["MetricsByItem[count_Invalid_operations]"],
        "count_users": r["MetricsByItem[count_users]"],
        "throttling_min": r["MetricsByItem[Throttling (min)]"],
    }
    for r in metrics_rows
]

df = spark.createDataFrame(rows, schema=ROW_SCHEMA)
df = (
    df.withColumn("ingestion_ts", F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp"))
      .withColumn("ingestion_date", F.lit(datetime.now(timezone.utc).date().isoformat()).cast(DateType()))
)

print(f"Rows to write: {df.count()}")
df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Daily Snapshot Write
#
# Unlike `raw_activity_events`/`raw_refresh_history` (naturally atomic,
# already-happened events, deduplicated by their own id), this source has no
# per-day key — the API only ever answers "the last 14 days, as of now." So
# the idempotency guard here is the same shape as the *dimension* Bronze
# notebooks: skip if this `ingestion_date` was already captured, otherwise
# append one snapshot row per item for today. Silver is where day-over-day
# deltas get derived from consecutive snapshots.

# CELL ********************

INGESTION_DATE = datetime.now(timezone.utc).date().isoformat()

already_ingested = False
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    count = (
        spark.read.format("delta").load(BRONZE_PATH)
        .filter(F.col("ingestion_date") == INGESTION_DATE)
        .count()
    )
    already_ingested = count > 0

if already_ingested:
    print(f"Capacity metrics snapshot for {INGESTION_DATE} already captured. Skipping.")
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
    .groupBy("ingestion_date")
    .agg(F.count("*").alias("items"), F.sum("sum_cu").alias("total_cu"))
    .orderBy(F.desc("ingestion_date"))
    .show(20, truncate=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
