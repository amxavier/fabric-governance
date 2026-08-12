import base64
import json
import subprocess
from pathlib import Path

ITEM_TYPE_MAP = {
    ".Notebook": "Notebook",
    ".DataPipeline": "DataPipeline",
    ".SemanticModel": "SemanticModel",
    ".Report": "Report",
}

ARTIFACT_SUFFIXES = tuple(ITEM_TYPE_MAP.keys())

# .tmdl files carry DirectLake OneLake URLs; .py files carry notebook lakehouse IDs in METADATA
_TMDL_EXTENSIONS = {".tmdl"}
_NOTEBOOK_EXTENSIONS = {".py"}


def get_item_type(folder_name: str) -> str | None:
    for suffix, item_type in ITEM_TYPE_MAP.items():
        if folder_name.endswith(suffix):
            return item_type
    return None


def get_display_name(item_path: Path) -> str:
    platform_file = item_path / ".platform"
    if platform_file.exists():
        data = json.loads(platform_file.read_text(encoding="utf-8"))
        return data["metadata"]["displayName"]
    # Fallback: strip the type suffix from the folder name
    name = item_path.name
    for suffix in ARTIFACT_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def get_changed_items(repo_root: Path, depth: int = 1) -> list[Path]:
    result = subprocess.run(
        ["git", "diff", f"HEAD~{depth}", "--name-only"],
        capture_output=True,
        text=True,
        cwd=repo_root,
    )
    items: set[Path] = set()
    for f in result.stdout.strip().splitlines():
        parts = Path(f).parts
        for i, part in enumerate(parts):
            if any(part.endswith(s) for s in ARTIFACT_SUFFIXES):
                items.add(repo_root / Path(*parts[: i + 1]))
                break
    return list(items)


def read_item_parts(
    item_path: Path,
    replacements: dict[str, str] | None = None,
    notebook_replacements: dict[str, str] | None = None,
) -> list[dict]:
    parts = []
    for file_path in sorted(item_path.rglob("*")):
        if file_path.is_dir():
            continue
        relative = file_path.relative_to(item_path).as_posix()
        raw = file_path.read_bytes()
        if replacements and file_path.suffix in _TMDL_EXTENSIONS:
            text = raw.decode("utf-8")
            for old, new in replacements.items():
                text = text.replace(old, new)
            raw = text.encode("utf-8")
        elif notebook_replacements and file_path.suffix in _NOTEBOOK_EXTENSIONS:
            # Patch notebook METADATA: replace DEV workspace/lakehouse IDs with target IDs
            text = raw.decode("utf-8")
            for old, new in notebook_replacements.items():
                text = text.replace(old, new)
            raw = text.encode("utf-8")
        parts.append({
            "path": relative,
            "payload": base64.b64encode(raw).decode("ascii"),
            "payloadType": "InlineBase64",
        })
    return parts


def get_notebook_logicalid_map(repo_root: Path) -> dict[str, str]:
    """Return {logicalId: displayName} for all notebooks in the repo."""
    mapping = {}
    notebooks_dir = repo_root / "notebooks"
    for folder in sorted(notebooks_dir.iterdir()):
        if not (folder.is_dir() and folder.name.endswith(".Notebook")):
            continue
        platform_file = folder / ".platform"
        if not platform_file.exists():
            continue
        data = json.loads(platform_file.read_text(encoding="utf-8"))
        logical_id = data.get("config", {}).get("logicalId")
        display_name = data.get("metadata", {}).get("displayName")
        if logical_id and display_name:
            mapping[logical_id] = display_name
    return mapping


def patch_pipeline_notebook_ids(
    parts: list[dict],
    logicalid_to_itemid: dict[str, str],
    workspace_id: str | None = None,
    sm_item_id: str | None = None,
) -> list[dict]:
    """Replace notebook logicalIds, workspaceId, and the semantic-model-refresh
    activity's groupId/datasetId in pipeline-content.json with target workspace values.

    workspaceId "00000000..." works in the workspace where the pipeline was natively created,
    but fails with BadRequest when the pipeline is deployed via REST API to a different workspace.
    Setting it to the actual workspace ID fixes cross-environment execution.

    PBISemanticModelRefresh's groupId/datasetId are a captured-GUID field with no
    logicalId equivalent (unlike notebookId) — left unpatched, a pipeline deployed to
    a new environment silently refreshes whichever environment's semantic model it was
    last pulled from instead of its own (confirmed live 2026-08-12, across two
    separate environment promotions). externalReferences.connection is deliberately
    NOT touched: confirmed identical across DEV and QA's independently-corrected
    pipelines, so it's a reusable, workspace-agnostic auth binding, not something
    that needs recreating per environment.
    """
    patched = []
    for part in parts:
        if part["path"] == "pipeline-content.json":
            raw = base64.b64decode(part["payload"])
            content = json.loads(raw.decode("utf-8"))
            for activity in content.get("properties", {}).get("activities", []):
                if activity.get("type") == "TridentNotebook":
                    props = activity["typeProperties"]
                    logical_id = props.get("notebookId", "")
                    if logical_id in logicalid_to_itemid:
                        props["notebookId"] = logicalid_to_itemid[logical_id]
                    if workspace_id:
                        props["workspaceId"] = workspace_id
                elif activity.get("type") == "PBISemanticModelRefresh" and sm_item_id and workspace_id:
                    props = activity["typeProperties"]
                    props["groupId"] = workspace_id
                    props["datasetId"] = sm_item_id
            updated = json.dumps(content, indent=2).encode("utf-8")
            patched.append({
                "path": part["path"],
                "payload": base64.b64encode(updated).decode("ascii"),
                "payloadType": "InlineBase64",
            })
        else:
            patched.append(part)
    return patched


def patch_report_byconnection(parts: list[dict], workspace_name: str, sm_display_name: str, sm_item_id: str) -> list[dict]:
    """Replace byPath definition.pbir with a byConnection using the XMLA endpoint connection string."""
    connection_string = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace_name};"
        f"Initial Catalog={sm_display_name};"
        f"semanticModelId={sm_item_id}"
    )
    byconnection = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": connection_string,
            }
        },
    }
    patched = []
    for part in parts:
        if part["path"] == "definition.pbir":
            patched.append({
                "path": "definition.pbir",
                "payload": base64.b64encode(
                    json.dumps(byconnection, indent=2).encode("utf-8")
                ).decode("ascii"),
                "payloadType": "InlineBase64",
            })
        else:
            patched.append(part)
    return patched


def load_valuesets(repo_root: Path, branch: str) -> dict:
    path = repo_root / "config" / "valueSets" / f"{branch}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"No valueSet found for branch '{branch}'. Expected: {path}"
        )
    return json.loads(path.read_text(encoding="utf-8"))
