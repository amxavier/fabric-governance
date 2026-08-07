import time
import requests

_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
_FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_POWERBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"
_POWERBI_SCOPE = "https://analysis.windows.net/powerbi/api/.default"
_POLL_INTERVAL = 5
_POLL_TIMEOUT = 300


class FabricClient:
    """
    Thin REST wrapper over two related but distinct API surfaces:
      - Fabric REST API   (api.fabric.microsoft.com) — items, workspaces, admin/workspaces, admin/items
      - Power BI REST API (api.powerbi.com)           — admin/activityevents, admin/datasets/*/refreshhistory

    Both require the same Service Principal, but different token scopes/audiences,
    so tokens are cached separately per audience.
    """

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._tokens: dict[str, str] = {}

    def _get_token(self, scope: str) -> str:
        if scope in self._tokens:
            return self._tokens[scope]
        resp = requests.post(
            _TOKEN_URL.format(tenant=self._tenant_id),
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": scope,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self._tokens[scope] = token
        return token

    def _headers(self, scope: str) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token(scope)}",
            "Content-Type": "application/json",
        }

    def _poll(self, operation_url: str) -> None:
        deadline = time.time() + _POLL_TIMEOUT
        while time.time() < deadline:
            resp = requests.get(operation_url, headers=self._headers(_FABRIC_SCOPE), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            if status == "Succeeded":
                return
            if status == "Failed":
                error = data.get("error", {})
                raise RuntimeError(f"Operation failed: {error.get('message', data)}")
            time.sleep(_POLL_INTERVAL)
        raise TimeoutError(f"Operation did not complete within {_POLL_TIMEOUT}s")

    # ── Fabric REST API — workspace-scoped (used by deploy.py) ──────────────

    def get_workspace_name(self, workspace_id: str) -> str:
        resp = requests.get(
            f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}",
            headers=self._headers(_FABRIC_SCOPE),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["displayName"]

    def get_workspace_items(self, workspace_id: str) -> list[dict]:
        items = []
        url = f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items"
        while url:
            resp = requests.get(url, headers=self._headers(_FABRIC_SCOPE), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            url = data.get("continuationUri")
        return items

    def create_item(
        self,
        workspace_id: str,
        display_name: str,
        item_type: str,
        parts: list[dict],
    ) -> None:
        resp = requests.post(
            f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            headers=self._headers(_FABRIC_SCOPE),
            json={
                "displayName": display_name,
                "type": item_type,
                "definition": {"parts": parts},
            },
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            return
        resp.raise_for_status()

    def create_lakehouse(self, workspace_id: str, display_name: str) -> str:
        # Unlike create_item, a Lakehouse has no "definition" payload at all —
        # sending one (even {"parts": []}) is rejected. Returns the new
        # item's ID directly since callers need it immediately (e.g. to
        # patch config/valueSets), unlike create_item's fire-and-forget void
        # return.
        resp = requests.post(
            f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items",
            headers=self._headers(_FABRIC_SCOPE),
            json={"displayName": display_name, "type": "Lakehouse"},
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            items = {i["displayName"]: i["id"] for i in self.get_workspace_items(workspace_id)}
            return items[display_name]
        resp.raise_for_status()
        return resp.json()["id"]

    def get_item_definition(self, workspace_id: str, item_id: str, format: str | None = None) -> list[dict]:
        # format=TMDL is required for SemanticModel items — without it, Fabric
        # returns only the minimal .platform/definition.pbism stub instead of
        # the actual model/tables/relationships TMDL files.
        url = f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        if format:
            url += f"?format={format}"
        resp = requests.post(url, headers=self._headers(_FABRIC_SCOPE), timeout=60)
        if resp.status_code == 202:
            operation_url = resp.headers["Location"]
            self._poll(operation_url)
            resp = requests.get(f"{operation_url}/result", headers=self._headers(_FABRIC_SCOPE), timeout=30)
            resp.raise_for_status()
            return resp.json().get("definition", {}).get("parts", [])
        resp.raise_for_status()
        return resp.json().get("definition", {}).get("parts", [])

    def delete_item(self, workspace_id: str, item_id: str) -> None:
        resp = requests.delete(
            f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items/{item_id}",
            headers=self._headers(_FABRIC_SCOPE),
            timeout=30,
        )
        resp.raise_for_status()

    def update_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        parts: list[dict],
    ) -> None:
        resp = requests.post(
            f"{_FABRIC_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            headers=self._headers(_FABRIC_SCOPE),
            json={"definition": {"parts": parts}},
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            return
        resp.raise_for_status()

    # ── Fabric REST API — Admin (tenant-wide, used by Bronze notebooks) ─────
    # These mirror the notebook-side calls (which use notebookutils.credentials.getToken
    # instead of client_credentials) so the same pagination/shape assumptions can be
    # exercised locally, e.g. from tests or ad-hoc scripts run outside Fabric.

    def list_workspaces_admin(self) -> list[dict]:
        items = []
        url = f"{_FABRIC_BASE_URL}/admin/workspaces"
        while url:
            resp = requests.get(url, headers=self._headers(_FABRIC_SCOPE), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Nested under "workspaces", not the generic "value" key most other
            # Fabric REST endpoints use — confirmed against Microsoft Learn.
            items.extend(data.get("workspaces", []))
            token = data.get("continuationToken")
            url = f"{_FABRIC_BASE_URL}/admin/workspaces?continuationToken={token}" if token else None
        return items

    def list_items_admin(self, item_type: str | None = None) -> list[dict]:
        items = []
        url = f"{_FABRIC_BASE_URL}/admin/items"
        if item_type:
            url += f"?type={item_type}"
        while url:
            resp = requests.get(url, headers=self._headers(_FABRIC_SCOPE), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Nested under "itemEntities", not "value" — same as workspaces above.
            items.extend(data.get("itemEntities", []))
            token = data.get("continuationToken")
            if not token:
                url = None
            else:
                base = url.split("?")[0]
                url = f"{base}?type={item_type}&continuationToken={token}" if item_type else f"{base}?continuationToken={token}"
        return items

    def list_capacities_admin(self) -> list[dict]:
        # No /admin/capacities endpoint exists — capacities are listed via the
        # Core API (/v1/capacities), unlike workspaces/items which do have a
        # dedicated /admin/ namespace. Confirmed via a real 404 during the
        # first live deploy against the tenant.
        resp = requests.get(
            f"{_FABRIC_BASE_URL}/capacities",
            headers=self._headers(_FABRIC_SCOPE),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("value", [])

    # ── Power BI REST API — Admin (Activity Events, refresh history) ────────

    def get_activity_events(self, start_iso: str, end_iso: str) -> list[dict]:
        """
        Fetch activity events for a window <= 24h (Power BI Admin API constraint).
        start_iso/end_iso must fall on the SAME UTC calendar day — e.g.
        "2026-07-23T00:00:00.000Z" to "2026-07-23T23:59:59.999Z", not the
        following day's midnight, or the API returns 400 Bad Request.
        """
        events = []
        url = (
            f"{_POWERBI_BASE_URL}/admin/activityevents"
            f"?startDateTime='{start_iso}'&endDateTime='{end_iso}'"
        )
        while url:
            resp = requests.get(url, headers=self._headers(_POWERBI_SCOPE), timeout=60)
            resp.raise_for_status()
            data = resp.json()
            events.extend(data.get("activityEventEntities", []))
            # Activity Events paginates via a full continuation URL, not a token —
            # different shape from every other endpoint in this client.
            url = data.get("continuationUri")
        return events
