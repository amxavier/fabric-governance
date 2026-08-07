# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ### nb_util_delegated_auth
#
# Shared via `%run nb_util_delegated_auth` (an isolated cell — no other code
# in that same cell, or Fabric errors) from every notebook that calls an
# `/admin/*` REST endpoint. Defines `_get_delegated_token(scope)` once
# instead of duplicating the same ~30 lines verbatim in 8 notebooks, which
# meant any future fix (this retry logic included) had to be copy-pasted
# into all 8 with no way to enforce they stayed in sync.
#
# **Why delegated auth at all:** `/admin/*` endpoints (both Fabric's and
# Power BI's) reject every app-only Service Principal token tried — a
# genuine platform limitation on this tenant, not a config error (see
# README, "Delegated Authentication for /admin/\*"). The fix: exchange a
# refresh token — obtained once via an interactive device-code + MFA
# sign-in, per environment — for a fresh access token on every run, and
# persist the rotated refresh token Microsoft returns back into the same
# Delta table so the next run can still authenticate.
#
# **Resolves `lh_governance_bronze` at call time**, not import time — since
# this file executes inside the *calling* notebook's session when %run'd,
# `notebookutils.lakehouse.get(...)` here reflects whichever workspace that
# caller is actually running in, exactly as if this code were pasted
# directly into it.

# CELL ********************

import random
import time
import requests
from datetime import datetime, timezone

# Same rate-limited identity shared across every /admin/*-calling notebook —
# schedule_ingestion.yml's own comments call out the hourly Admin API limit.
# A single transient 429/503 used to fail the whole notebook (and, via the
# pipeline's dependsOn chain, block everything after it) with no retry.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_RETRIES = 4


def _get_delegated_token(scope: str) -> str:
    _lh_bronze = notebookutils.lakehouse.get("lh_governance_bronze")
    auth_table_path = f"{_lh_bronze['properties']['abfsPath']}/Tables/_auth_delegated"
    auth_row = spark.read.format("delta").load(auth_table_path).collect()[0]

    resp = None
    for attempt in range(_MAX_RETRIES + 1):
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
        if resp.status_code not in _RETRYABLE_STATUS:
            break
        delay = (2 ** attempt) + random.uniform(0, 1)
        print(f"  [retry] token exchange -> {resp.status_code}, retrying in {delay:.1f}s "
              f"(attempt {attempt + 1}/{_MAX_RETRIES})")
        time.sleep(delay)
    resp.raise_for_status()
    token_data = resp.json()

    # Persist the rotated refresh token — Microsoft may issue a new one on
    # every redemption, and the old one can stop working once that happens.
    new_refresh_token = token_data.get("refresh_token", auth_row["refresh_token"])
    spark.createDataFrame([{
        "tenant_id": auth_row["tenant_id"],
        "client_id": auth_row["client_id"],
        "refresh_token": new_refresh_token,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }]).write.format("delta").mode("overwrite").save(auth_table_path)

    return token_data["access_token"]


print("nb_util_delegated_auth loaded — _get_delegated_token(scope) is available.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
