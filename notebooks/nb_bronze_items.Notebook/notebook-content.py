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

# ### nb_bronze_items
#
# **Layer:** Bronze — Raw Ingestion, SCD Type 2
# **Source:** Fabric Admin REST API — `GET /v1/admin/items`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_items`
# **Schedule:** Daily (after nb_bronze_workspaces)
#
# One generic table for every item type — Report, SemanticModel, Lakehouse,
# SQLEndpoint, DataPipeline, Notebook, Dataflow — with `item_type` as a
# discriminator column, rather than eight near-duplicate per-type tables.
# This matches how the Admin API itself returns items: a single flat list
# where `type` is just another field.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_items"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

TOKEN = notebookutils.credentials.getToken("pbi")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
ITEMS_URL = "https://api.fabric.microsoft.com/v1/admin/items"

_lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
BRONZE_PATH = f"{_lh_bronze['properties']['abfsPath']}/Tables/{DESTINATION_TABLE}"

print(f"[Bronze] lh_governance_bronze id : {_lh_bronze['id']}")
print(f"[Bronze] Write path   : {BRONZE_PATH}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fetch Items from Admin API (paginated)

# CELL ********************

data = []
url = ITEMS_URL
while url:
    response = requests.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()
    page = response.json()
    # This Admin API nests results under "itemEntities", not the generic
    # "value" key — same class of bug as nb_bronze_workspaces' "workspaces"
    # key, confirmed against Microsoft Learn.
    data.extend(page.get("itemEntities", []))
    token = page.get("continuationToken")
    url = f"{ITEMS_URL}?continuationToken={token}" if token else None

print(f"Items fetched: {len(data)}")


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
        "item_id": i.get("id"),
        "workspace_id": i.get("workspaceId"),
        "item_name": i.get("name") or i.get("displayName"),
        "item_type": i.get("type"),
        "state": i.get("state"),
        "description": i.get("description"),
    }
    for i in data
]

df = spark.createDataFrame(rows)
df = (
    df.withColumn("ingestion_ts", F.lit(INGESTION_TS.isoformat()).cast("timestamp"))
      .withColumn("ingestion_date", F.lit(INGESTION_DATE).cast(DateType()))
)

print("Schema:")
df.printSchema()
print("Items by type:")
df.groupBy("item_type").count().orderBy(F.desc("count")).show()


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

    # Item renames, workspace moves, or state changes (Active <-> Deleted) are
    # exactly the "who changed what" signal this project exists to capture —
    # e.g. a report or semantic model moved to a different workspace right
    # before its scheduled refresh started failing.
    # Uses <=> (null-safe equality) rather than <> — see nb_bronze_capacities for why.
    current = spark.read.format("delta").load(BRONZE_PATH).filter("is_current = true")
    changed = (
        df_scd.alias("new")
        .join(current.alias("cur"), on="item_id", how="left")
        .where(
            "cur.item_id IS NULL OR "
            "NOT (new.item_name <=> cur.item_name) OR "
            "NOT (new.workspace_id <=> cur.workspace_id) OR "
            "NOT (new.state <=> cur.state)"
        )
        .select("new.*")
    )
    print(f"Items changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.item_id = source.item_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(BRONZE_PATH))
        print(f"{changed.count()} new versions written to {BRONZE_PATH}")
    else:
        print("No item changes detected. Nothing written.")


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
df_val.filter("is_current = true").groupBy("item_type").count().orderBy(F.desc("count")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
