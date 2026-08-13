# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ### nb_test_admin_api_service_principal
#
# **Temporary diagnostic — not a permanent part of this project.** Re-tests
# whether `/admin/*` REST endpoints actually reject an app-only Service
# Principal token on this tenant, now that the SP has been added as
# Contributor directly on the FT1 trial capacity (fixed 2026-08-12 for
# `nb_bronze_capacities`'s `/v1/capacities` call). The original rejection
# (documented in `nb_bronze_workspaces` and the README) was real and
# tested at the time — but was tested *before* that capacity-level grant
# existed. The SP previously only had the tenant-level "Admin API
# settings" toggle enabled, not capacity-level Contributor — if that
# capacity grant is what these `/admin/*` calls actually needed (same
# root cause class as today's TOM/Connection findings), this may now
# succeed where it didn't before.
#
# **Read-only** — a single `GET /v1/admin/workspaces` call, no delegated
# auth, no refresh-token exchange. Prints the raw HTTP status and response
# body either way so the real evidence is visible, not just success/fail.
#
# Delete this notebook (and its pipeline activity, if added) once the
# question is answered either way.

# CELL ********************

import requests

TOKEN = notebookutils.credentials.getToken("pbi")
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
URL = "https://api.fabric.microsoft.com/v1/admin/workspaces"

response = requests.get(URL, headers=HEADERS, timeout=30)

print(f"HTTP status: {response.status_code}")
print(f"Response body (first 2000 chars): {response.text[:2000]}")

if response.status_code == 200:
    data = response.json().get("value", [])
    print(f"\nSUCCESS — {len(data)} workspace(s) returned via plain SP token, no delegated auth needed.")
else:
    print(f"\nSTILL REJECTED — status {response.status_code}, plain SP token not sufficient for this endpoint.")
