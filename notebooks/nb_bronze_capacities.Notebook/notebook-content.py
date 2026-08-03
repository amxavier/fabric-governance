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

# ### nb_bronze_capacities
#
# **Layer:** Bronze — Raw Ingestion, SCD Type 2
# **Source:** Fabric REST API (Core, not Admin) — `GET /v1/capacities`
# **Destination:** `lh_governance_bronze` → Delta Table `raw_capacities`
# **Schedule:** Daily (before workspaces/items, since workspaces reference capacityId)
#
# Capacity assignment is a governance signal in its own right — moving a workspace
# to a different (e.g. smaller, or Trial-expiring) capacity is a common root cause
# of refresh slowdowns or failures. This notebook snapshots the tenant's capacities
# and applies SCD Type 2 so capacity changes over time are preserved from the
# earliest possible layer.


# MARKDOWN ********************

# ### Imports and Configuration

# CELL ********************

import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StructType, StructField, StringType
from delta.tables import DeltaTable

DESTINATION_TABLE = "raw_capacities"
INGESTION_TS = datetime.now(timezone.utc)
INGESTION_DATE = INGESTION_TS.date().isoformat()

# Inside a Fabric notebook, a single "pbi" token audience is accepted by both
# api.fabric.microsoft.com and api.powerbi.com — no separate Fabric/Power BI
# credential handling is needed here (unlike scripts/fabric_client.py, which
# runs outside Fabric and must request each scope explicitly).
TOKEN = notebookutils.credentials.getToken("pbi")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
# There is no /v1/admin/capacities endpoint — capacity listing lives on the
# Core API (/v1/capacities), not the Admin API namespace like workspaces/items.
# With a Capacity.Read.All-equivalent grant this still returns every tenant
# capacity, not just ones the caller is explicitly assigned to.
CAPACITIES_URL = "https://api.fabric.microsoft.com/v1/capacities"

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

# ### Fetch Capacities from Admin API

# CELL ********************

response = requests.get(CAPACITIES_URL, headers=HEADERS, timeout=30)
response.raise_for_status()
data = response.json().get("value", [])

print(f"Capacities fetched: {len(data)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build DataFrame

# CELL ********************

# Keep only the fields we rely on downstream; capacityUserAccessRight and other
# tenant-specific extras are intentionally left out of the typed schema — if we
# ever need them, they should be added explicitly rather than left implicit.
rows = [
    {
        "capacity_id": c.get("id"),
        "display_name": c.get("displayName"),
        "sku": c.get("sku"),
        "region": c.get("region"),
        "state": c.get("state"),
    }
    for c in data
]

# Explicit schema — region (and in principle any of these) can be None across
# every capacity in a fetch (e.g. this tenant's Trial capacity), and Spark's
# type inference throws CANNOT_DETERMINE_TYPE when a column is null in every
# row, same lesson as nb_bronze_activity_events/nb_bronze_refresh_history.
CAPACITY_SCHEMA = StructType([
    StructField("capacity_id", StringType()),
    StructField("display_name", StringType()),
    StructField("sku", StringType()),
    StructField("region", StringType()),
    StructField("state", StringType()),
])

df = spark.createDataFrame(rows, schema=CAPACITY_SCHEMA)
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

# valid_from/valid_to/is_current applied here in Bronze (not Silver) so capacity
# change history is preserved from the earliest possible point — see notebook
# markdown header for why.
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

    # Only expire+reinsert capacities whose tracked attributes actually changed —
    # otherwise every daily run would insert a full new "version" for every
    # capacity even when nothing changed, defeating the purpose of SCD2.
    # Uses <=> (null-safe equality) rather than <>: plain <> evaluates to NULL
    # (not TRUE) when a value changes to/from NULL, which would silently drop
    # a real change instead of recording it.
    current = spark.read.format("delta").load(BRONZE_PATH).filter("is_current = true")
    changed = (
        df_scd.alias("new")
        .join(current.alias("cur"), on="capacity_id", how="left")
        .where(
            "cur.capacity_id IS NULL OR "
            "NOT (new.display_name <=> cur.display_name) OR "
            "NOT (new.sku <=> cur.sku) OR "
            "NOT (new.region <=> cur.region) OR "
            "NOT (new.state <=> cur.state)"
        )
        .select("new.*")
    )
    print(f"Capacities changed since last snapshot: {changed.count()}")

    if changed.count() > 0:
        dt.alias("target").merge(
            changed.alias("source"),
            "target.capacity_id = source.capacity_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.valid_from",
            "is_current": "false",
        }).execute()

        (changed.write.format("delta").mode("append")
            .option("mergeSchema", "true").save(BRONZE_PATH))
        print(f"{changed.count()} new versions written to {BRONZE_PATH}")
    else:
        print("No capacity changes detected. Nothing written.")

    # A business key present in `current` but absent from this run's fetch
    # (capacity deleted or recreated under a new capacity_id) is never touched
    # by the `changed` merge above, since that only compares keys the new
    # fetch still has. Without this, the old capacity_id's row stays
    # is_current = true forever.
    removed = current.join(df_scd.select("capacity_id"), on="capacity_id", how="left_anti")
    print(f"Capacities removed since last snapshot: {removed.count()}")

    if removed.count() > 0:
        dt.alias("target").merge(
            removed.withColumn("retired_on", F.lit(INGESTION_DATE).cast(DateType())).alias("source"),
            "target.capacity_id = source.capacity_id AND target.is_current = true"
        ).whenMatchedUpdate(set={
            "valid_to": "source.retired_on",
            "is_current": "false",
        }).execute()
        print(f"{removed.count()} capacity(ies) retired (no longer present in source)")


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
df_val.filter("is_current = true").select(
    "capacity_id", "display_name", "sku", "region", "state", "valid_from"
).show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
