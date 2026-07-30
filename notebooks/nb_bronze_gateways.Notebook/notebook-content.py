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
# **Sources:**
#   - Power BI Admin REST API — `GET /v1.0/myorg/admin/gateways` → `raw_gateways`
#   - Power BI Admin REST API — `GET /v1.0/myorg/admin/datasets/{datasetId}/datasources` → `raw_dataset_datasources`
# **Destination:** `lh_governance_bronze`
# **Schedule:** Daily
#
# On-premises gateways are a classic silent-failure point — a gateway going
# offline doesn't raise an error anywhere obvious, it just makes every
# refresh that depends on it start failing. `raw_gateways` tracks gateway
# health as its own dimension; `raw_dataset_datasources` tracks which
# datasource (and, through it, which gateway) each semantic model actually
# depends on — the join key a future "did this fail because its gateway
# went down" correlation needs against `fact_refresh`.
#
# **This tenant currently has zero gateways** (confirmed 2026-07-30 — every
# workload here is native Fabric: Direct Lake, Lakehouse, Notebooks; nothing
# needs an on-premises source). `raw_dataset_datasources` still returns real,
# useful rows for every model type present here — including Direct Lake:
# contrary to the original assumption when this notebook was written, a
# Direct Lake model *does* register as a datasource
# (`datasourceType: AzureDataLakeStorage`), just with `gatewayId: null` and
# the OneLake path itself (workspace/lakehouse GUIDs) in
# `connectionDetails` — real lineage data, not just a gateway-correlation
# join key. Confirmed against 20 real semantic models tenant-wide,
# including this project's own and the sibling crypto project's dev/qa/prd
# models. Both tables are built and kept live regardless of gateway
# presence, so this is a complete, working reference for an environment
# that does have gateways and Import/DirectQuery-mode models depending on
# them, not just a stub.
#
# Both are dimension-like (status/config changes over time) — SCD Type 2,
# same pattern as `nb_bronze_capacities`.
#
# **Auth:** `/admin/*` endpoints, so this needs the same delegated
# refresh-token workaround as the other Bronze notebooks that hit Admin
# APIs (`/admin/*` rejects app-only Service Principal tokens outright —
# see README / the other notebooks' markdown for the full diagnosis).


# MARKDOWN ********************

# ### Imports, Configuration, and Auth

# CELL ********************

import requests
import json as _json
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
GATEWAYS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_gateways"
DATASOURCES_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_dataset_datasources"
ITEMS_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/raw_items"
AUTH_TABLE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] raw_gateways path        : {GATEWAYS_PATH}")
print(f"[Bronze] raw_dataset_datasources  : {DATASOURCES_PATH}")


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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_gateways — Fetch and Build
#
# Field names confirmed against Microsoft's official `Gateway` object
# definition (not guessed): `id`, `name`, `type`, `gatewayStatus`,
# `gatewayAnnotation` (JSON-encoded string with contact/display metadata —
# kept as raw text, like `raw_json` elsewhere in this project, rather than
# parsed into fixed columns, since its shape varies by gateway type).
# `publicKey` (encryption key material) is intentionally excluded — not a
# governance signal, and not something this table should ever expose.

# CELL ********************

resp = requests.get("https://api.powerbi.com/v1.0/myorg/admin/gateways", headers=HEADERS, timeout=30)
resp.raise_for_status()
gateways_data = resp.json().get("value", [])
print(f"Gateways fetched: {len(gateways_data)}")

gateway_rows = [
    {
        "gateway_id": g.get("id"),
        "display_name": g.get("name"),
        "type": g.get("type"),
        "gateway_status": g.get("gatewayStatus"),
        "gateway_annotation": g.get("gatewayAnnotation"),
    }
    for g in gateways_data
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_gateways — SCD Type 2 Merge
#
# Same shape as `nb_bronze_capacities`: an empty tenant-wide list on a given
# day is a legitimate state (not an error) and must still initialize the
# table with the right schema — otherwise a later Silver/Gold read fails
# with PATH_NOT_FOUND the first time a gateway actually gets added.

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

GATEWAY_SCHEMA = StructType([
    StructField("gateway_id", StringType()),
    StructField("display_name", StringType()),
    StructField("type", StringType()),
    StructField("gateway_status", StringType()),
    StructField("gateway_annotation", StringType()),
])

df_gateways = spark.createDataFrame(gateway_rows, schema=GATEWAY_SCHEMA)
df_gateways_scd = (
    df_gateways
    .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
    .withColumn("valid_from", F.col("ingestion_date"))
    .withColumn("valid_to", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
)

if not DeltaTable.isDeltaTable(spark, GATEWAYS_PATH):
    (df_gateways_scd.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(GATEWAYS_PATH))
    print(f"{df_gateways_scd.count()} records written to {GATEWAYS_PATH} (initial load)")
else:
    dt = DeltaTable.forPath(spark, GATEWAYS_PATH)
    current = spark.read.format("delta").load(GATEWAYS_PATH).filter("is_current = true")

    # Null-safe equality (<=>), not <>: plain <> evaluates to NULL (not TRUE)
    # when a value changes to/from NULL, silently dropping a real change
    # instead of recording it — same lesson as nb_bronze_capacities.
    changed = (
        df_gateways_scd.alias("new")
        .join(current.alias("cur"), on="gateway_id", how="left")
        .where(
            "cur.gateway_id IS NULL OR "
            "NOT (new.display_name <=> cur.display_name) OR "
            "NOT (new.type <=> cur.type) OR "
            "NOT (new.gateway_status <=> cur.gateway_status) OR "
            "NOT (new.gateway_annotation <=> cur.gateway_annotation)"
        )
        .select("new.*")
    )
    print(f"Gateways changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.gateway_id = source.gateway_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(GATEWAYS_PATH))
        print(f"{changed.count()} new versions written to {GATEWAYS_PATH}")
    else:
        print("No gateway changes detected. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_dataset_datasources — Fetch and Build
#
# Looped per semantic model rather than a single tenant-wide call — this
# resource is scoped to one dataset at a time
# (`/admin/datasets/{datasetId}/datasources`). Rate limit per Microsoft's
# docs: 300 requests/hour — comfortably above this tenant's dataset count,
# but worth knowing if reusing this against a much larger tenant.
#
# `connectionDetails` is kept as a raw JSON string rather than flattened
# into fixed columns: its shape depends on `datasourceType` (a `Sql`
# datasource has `server`/`database`; an `Extension` datasource like this
# tenant's Capacity Metrics app has `path`/`kind` instead) — flattening
# would mean guessing at columns for datasource types this tenant doesn't
# currently have any examples of.

# CELL ********************

if not DeltaTable.isDeltaTable(spark, ITEMS_PATH):
    raise RuntimeError("raw_items table not found — run nb_bronze_items before this notebook.")

semantic_model_ids = [
    row["item_id"] for row in
    spark.read.format("delta").load(ITEMS_PATH)
        .filter("is_current = true and item_type = 'SemanticModel'")
        .select("item_id")
        .collect()
]
print(f"Semantic models to check: {len(semantic_model_ids)}")

datasource_rows = []
failed_lookups = []
for dataset_id in semantic_model_ids:
    resp = requests.get(
        f"https://api.powerbi.com/v1.0/myorg/admin/datasets/{dataset_id}/datasources",
        headers=HEADERS, timeout=30,
    )
    if not resp.ok:
        # Some datasets (e.g. push datasets) don't support this call —
        # skip and keep going rather than fail the whole notebook run,
        # same approach as nb_bronze_refresh_history.
        failed_lookups.append(dataset_id)
        continue
    for ds in resp.json().get("value", []):
        datasource_rows.append({
            "dataset_id": dataset_id,
            "datasource_id": ds.get("datasourceId"),
            "datasource_type": ds.get("datasourceType"),
            "gateway_id": ds.get("gatewayId"),
            "connection_details_json": _json.dumps(ds.get("connectionDetails")) if ds.get("connectionDetails") else None,
        })

print(f"Datasource rows fetched: {len(datasource_rows)}")
print(f"Datasets with no datasource lookup available: {len(failed_lookups)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### raw_dataset_datasources — SCD Type 2 Merge
#
# Grain: one row per (dataset_id, datasource_id) — a model can depend on
# more than one datasource, and which datasource(s) it depends on can
# change over time (e.g. a data source swap), which is exactly what SCD2
# is for.

# CELL ********************

DATASOURCE_SCHEMA = StructType([
    StructField("dataset_id", StringType()),
    StructField("datasource_id", StringType()),
    StructField("datasource_type", StringType()),
    StructField("gateway_id", StringType()),
    StructField("connection_details_json", StringType()),
])

df_datasources = spark.createDataFrame(datasource_rows, schema=DATASOURCE_SCHEMA)
df_datasources_scd = (
    df_datasources
    .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
    .withColumn("valid_from", F.col("ingestion_date"))
    .withColumn("valid_to", F.lit(None).cast(DateType()))
    .withColumn("is_current", F.lit(True))
)

if not DeltaTable.isDeltaTable(spark, DATASOURCES_PATH):
    (df_datasources_scd.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true").save(DATASOURCES_PATH))
    print(f"{df_datasources_scd.count()} records written to {DATASOURCES_PATH} (initial load)")
else:
    dt = DeltaTable.forPath(spark, DATASOURCES_PATH)
    current = spark.read.format("delta").load(DATASOURCES_PATH).filter("is_current = true")

    changed = (
        df_datasources_scd.alias("new")
        .join(current.alias("cur"), on=["dataset_id", "datasource_id"], how="left")
        .where(
            "cur.dataset_id IS NULL OR "
            "NOT (new.datasource_type <=> cur.datasource_type) OR "
            "NOT (new.gateway_id <=> cur.gateway_id) OR "
            "NOT (new.connection_details_json <=> cur.connection_details_json)"
        )
        .select("new.*")
    )
    print(f"Dataset/datasource pairs changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.dataset_id = source.dataset_id AND target.datasource_id = source.datasource_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(DATASOURCES_PATH))
        print(f"{changed.count()} new versions written to {DATASOURCES_PATH}")
    else:
        print("No dataset/datasource changes detected. Nothing written.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation

# CELL ********************

print("--- raw_gateways ---")
gw_val = spark.read.format("delta").load(GATEWAYS_PATH)
print(f"Total rows (all versions): {gw_val.count()}")
print(f"Current rows: {gw_val.filter('is_current = true').count()}")
gw_val.filter("is_current = true").select("gateway_id", "display_name", "type", "gateway_status").show(truncate=False)

print("--- raw_dataset_datasources ---")
ds_val = spark.read.format("delta").load(DATASOURCES_PATH)
print(f"Total rows (all versions): {ds_val.count()}")
print(f"Current rows: {ds_val.filter('is_current = true').count()}")
ds_val.filter("is_current = true").select("dataset_id", "datasource_type", "gateway_id", "connection_details_json").show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
