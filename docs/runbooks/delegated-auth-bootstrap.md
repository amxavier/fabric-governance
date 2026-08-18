# Delegated auth bootstrap (per environment)

Run this interactively in any notebook attached to the environment's `lh_governance_bronze` (delete the cell afterward — it briefly holds a device code, not a secret).

Prerequisites: the app registration needs **"Allow public client flows"** enabled, and a **Delegated** (not Application) `Tenant.Read.All` permission for Power BI Service with admin consent. The signed-in user needs whatever role grants Admin API visibility in the tenant.

```python
import requests
from datetime import datetime, timezone

TENANT_ID = "<tenant id>"                  # not a secret
CLIENT_ID = "<sp-fabric-cicd client id>"    # not a secret — App Registration needs
                                             # "Allow public client flows" = Yes

dc = requests.post(
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/devicecode",
    data={"client_id": CLIENT_ID, "scope": "https://api.fabric.microsoft.com/.default offline_access"},
).json()
print(dc["message"])  # open the URL, enter the code, sign in (MFA if prompted)

# Run this cell AFTER completing the browser sign-in above:
poll = requests.post(
    f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token",
    data={
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "client_id": CLIENT_ID,
        "device_code": dc["device_code"],
    },
).json()

_lh = notebookutils.lakehouse.get("lh_governance_bronze")
spark.createDataFrame([{
    "tenant_id": TENANT_ID,
    "client_id": CLIENT_ID,
    "refresh_token": poll["refresh_token"],
    "updated_at": datetime.now(timezone.utc).isoformat(),
}]).write.format("delta").mode("overwrite").save(f"{_lh['properties']['abfsPath']}/Tables/_auth_delegated")
```

The refresh token rotates on every use and stays valid on a sliding ~90-day window, so daily runs keep it alive indefinitely. If it ever expires, re-run this bootstrap for that environment.
