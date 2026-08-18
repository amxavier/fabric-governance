# Fabric Governance

[![CI](https://github.com/amxavier/fabric-governance/actions/workflows/ci.yml/badge.svg)](https://github.com/amxavier/fabric-governance/actions/workflows/ci.yml)
[![CD Deploy](https://github.com/amxavier/fabric-governance/actions/workflows/cd_deploy.yml/badge.svg)](https://github.com/amxavier/fabric-governance/actions/workflows/cd_deploy.yml)
[![Scheduled Scan](https://github.com/amxavier/fabric-governance/actions/workflows/schedule_ingestion.yml/badge.svg)](https://github.com/amxavier/fabric-governance/actions/workflows/schedule_ingestion.yml)

Tenant-wide governance for **Microsoft Fabric**, built as a Medallion pipeline. It tracks who published or refreshed what, when, whether it failed, what it cost, and which items nobody uses anymore, so questions like "why did this report stop refreshing" or "can we shrink our capacity" get answered from data instead of by digging through the Fabric portal workspace by workspace.

Runs in three environments (DEV/QA/PRD), each a full, independent copy of the pipeline on its own daily schedule.

Sibling project: [microsoft-fabric-medallion-lakehouse](https://github.com/amxavier/microsoft-fabric-medallion-lakehouse). This repo shares its CI/CD conventions and reuses its three Fabric workspaces and Service Principal, but scans the whole tenant via the Admin API instead of a single workspace.

---

## Why This Project

Fabric's **OneLake Catalog — Govern for admins** (currently in preview, and the successor to the old Purview Hub) already gives a daily snapshot of the tenant's current state: inventory, sensitivity label coverage, endorsement, and curation. Fabric Capacity Metrics does the same for capacity consumption. Both are native and first-party, and this project doesn't try to replace either.

What they don't do is keep history. Admin Monitoring retains roughly 30 days, Capacity Metrics 14 — enough to see today's state or a recent trend, not enough to answer "when did this item stop being endorsed" or "when did label coverage start dropping," and nothing survives once that window rolls past. This project exists to fill that specific gap: every scan is versioned with SCD Type 2 in its own lakehouse, under the project's own control, so history outlives whatever retention window the native tools apply. The Capacity Planning/forecast module is a direct consequence of that — a weekly trend and saturation projection needs more than 14 or 30 days of data to mean anything, which the native tools can't provide on their own.

The native tools are the "now." This project is the "over time."

---

## What it answers

- **Inventory** — every workspace, item (Report, SemanticModel, Lakehouse, SQLEndpoint, DataPipeline, Notebook, Dataflow, Warehouse, SQLDatabase, App) and capacity in the tenant, with full change history.
- **Why did this break?** Join a refresh failure against the activity log for the same item in the preceding window and see what changed right before it broke.
- **What does this cost?** Capacity Unit consumption by item and by day, cross-referenced against activity and refresh events.
- **What can we clean up?** Items with no refresh, job, or activity signal in 45+ days, classified Active / Inactive / Cleanup Candidate.
- **Who can see this?** Workspace role assignments and per-item sharing, an access-audit angle across every governed item.
- **When do we run out of capacity?** The native Capacity Metrics app only keeps a rolling 14-day window. This project keeps full daily history, so a weekly OLS trend can project forward and flag a saturation date, with a confidence rating that reflects how much history actually backs it instead of just a number.

---

## Architecture

```mermaid
flowchart LR
    ADM["Fabric Admin API\n/admin/workspaces, /admin/items"]
    CAP["Fabric Core API\n/capacities"]
    AE["Power BI Admin API\n/admin/activityevents"]
    RH["Power BI Admin API\n/admin/capacities/refreshables"]
    CM["Capacity Metrics app\nexecuteQueries (DAX)"]
    GW["Fabric Admin API\n/admin/*/datasources, gateways"]
    PERM["Fabric + Power BI Admin API\nrole assignments, item users"]
    JOB["Fabric Job Scheduler API\njob instances"]

    B["Bronze\nraw_* — SCD2 for metadata, append-only for events"]
    S["Silver\nsilver_* — cleaned, typed, enriched"]
    G["Gold\n17 tables — governance star schema"]
    SM["Semantic Model\nsm_governance_medallion (Direct Lake)"]
    PBI["Power BI Report\nrpt_governance_dashboard"]

    ADM --> B
    CAP --> B
    AE --> B
    RH --> B
    CM --> B
    GW --> B
    PERM --> B
    JOB --> B
    B -->|"Clean &\nEnrich"| S
    S -->|"Star Schema"| G
    G -->|"Direct Lake"| SM
    SM --> PBI
```

### Layer responsibilities

| Layer | Table(s) | Description |
|-------|----------|-------------|
| **Bronze** | `raw_capacities`, `raw_workspaces`, `raw_items`, `raw_gateways`, `raw_workspace_role_assignments` | Tenant-wide snapshots, SCD Type 2 — metadata changes preserved from the earliest layer |
| **Bronze** | `raw_activity_events`, `raw_refresh_history`, `raw_capacity_metrics`, `raw_capacity_cu_detail`, `raw_dataset_datasources`, `raw_item_users`, `raw_item_job_history` | Append-only or daily-snapshot — events and usage are point-in-time by nature |
| **Silver** | `silver_*` (12 notebooks) | Cleaned, typed, joined for readability |
| **Gold** | `dim_capacity`, `dim_workspace`, `dim_item`, `dim_user`, `dim_date`, `dim_gateway` | Governance star schema dimensions |
| **Gold** | `fact_activity`, `fact_refresh`, `fact_capacity_consumption`, `fact_capacity_utilization`, `fact_item_lifecycle`, `fact_item_job_history` | Event/measure facts, keyed to the dimensions above |
| **Gold** | `bridge_item_datasource`, `bridge_workspace_access`, `bridge_item_access` | Many-to-many lineage/access bridges |
| **Gold** | `fact_capacity_forecast`, `capacity_planning_summary` | Weekly OLS trend and 52-week projection, built from `fact_capacity_utilization`. Every row carries `n_weeks`/`r_squared`/`forecast_confidence` so the projection is never mistaken for more certainty than the history supports |

The core diagnostic pattern this schema enables: join `fact_refresh` failures against `fact_activity` for the same `item_id` in the preceding window to see what changed right before something broke.

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Platform | Microsoft Fabric |
| Storage | OneLake (Delta Lake) |
| Processing | PySpark (Spark 3.x) |
| Orchestration | Fabric Data Pipeline (~25 activities) |
| Semantic Layer | Power BI Semantic Model (Direct Lake), 13 tables, 17 relationships, DAX measures |
| Reporting | Power BI Report (PBIR format), 5 pages: Overview, Capacity Cost, Capacity & Planning, Refresh Health, Cleanup |
| CI/CD | GitHub Actions (6 workflows) |
| Auth | Azure AD Service Principal, with a delegated-user fallback for endpoints that reject app-only auth |
| Deployment | Fabric REST API (direct, 3-phase) — no native Deployment Pipelines |

---

## Environments

This project reuses the same three Fabric workspaces and Service Principal as the sibling [microsoft-fabric-medallion-lakehouse](https://github.com/amxavier/microsoft-fabric-medallion-lakehouse) project.

```
dev branch  →  DEV workspace  (lakehouses_dev)
qa branch   →  QA workspace   (lakehouses_qa)
main branch →  PRD workspace  (lakehouses_prd)
```

Within each workspace this project adds its own Lakehouse trio — `lh_governance_bronze`, `lh_governance_silver`, `lh_governance_gold` — so governance tables never mix with the sibling project's. `scripts/provision_lakehouses.py <branch>` creates them and wires the real IDs into `config/valueSets/<branch>.json` automatically; see [Getting Started](#getting-started).

All three environments run the full daily pipeline end to end. Changes are promoted `dev` → `qa` → `main`; `main` only receives a change once it's validated on the other two.

---

## Workflows

| File | Trigger | Purpose |
|------|---------|---------|
| `ci.yml` | Every push | Validates every Fabric artifact's structure; runs `tests/` |
| `cd_deploy.yml` | `ci.yml` succeeding on `dev`/`qa`/`main` | Deploys changed artifacts to the matching workspace via REST API |
| `schedule_ingestion.yml` | Daily at 06:00 UTC (PRD); manual for DEV/QA | Triggers `pl_governance_orchestration` |
| `provision_lakehouses.yml` | Manual | One-off: create the Lakehouse trio in a new environment |
| `deploy_semantic_model.yml` | Manual | One-off: provision `sm_governance_medallion` from scratch in a new environment |
| `pull_semantic_model.yml` | Manual | Syncs a live item's current definition (semantic model, pipeline, report) back into git |

`cd_deploy.yml` only runs after `ci.yml` succeeds, so a commit that fails a test never reaches a live workspace. `deploy.py` deploys in three phases — Notebooks + Semantic Model, then the Data Pipeline (patching notebook IDs and the refresh activity's target), then the Report (patching the semantic model connection) — and stops if an earlier phase failed rather than publish something that references an item that doesn't exist yet.

`sm_governance_medallion` is excluded from `cd_deploy.yml`'s normal flow; see below.

---

## Semantic Model Lifecycle

Fabric's import validator rejects Direct Lake models deployed in bulk via REST/TMDL whenever the relationship graph has an ambiguous path — even one deliberately broken with `isActive: false`. So `sm_governance_medallion` isn't managed by the normal deploy flow: it's provisioned once per environment (`scripts/deploy_semantic_model.py`, TMDL with relationships emptied) and then built up interactively, running `nb_setup_semantic_model_relationships.Notebook` and `nb_setup_capacity_forecast_model.Notebook` to add tables, relationships, and measures one at a time via TOM (`sempy_labs.tom`). Both notebooks run once per environment and stay manual for that reason, not because of any credential limit. `pull_semantic_model.yml` syncs the live definition back into git afterward for documentation.

One gotcha: a model provisioned via the Service Principal is SP-owned, which blocks the portal's **Edit tables**. Fix is to **Take ownership** of the item as yourself — safe for Direct Lake, since there's no stored credential tied to the owner and the item ID doesn't change.

`pl_governance_orchestration`'s `refresh_semantic_model` activity needs no manual re-pointing: `deploy.py` resolves its `groupId`/`datasetId` per environment and patches it on every deploy, same as it does for notebook IDs.

---

## Delegated Authentication for `/admin/*`

Fabric and Power BI's `/admin/*` endpoints — workspaces, items, activity events, refreshables, `executeQueries` against the Capacity Metrics app, per-item sharing — reject the Service Principal outright, regardless of configuration. Several are still Preview and don't support app-only auth yet.

Workaround: 8 Bronze notebooks exchange a stored refresh token for a fresh delegated access token on every run, through a shared helper (`nb_util_delegated_auth.Notebook`). The token lives in a Delta table inside `lh_governance_bronze` and never leaves the lakehouse. All 8 notebooks share the token, so they run as a single chain in the pipeline rather than in parallel — two concurrent redemptions of the same token invalidate each other.

Each new environment needs a one-time interactive bootstrap (device-code sign-in) to seed that token — see [`docs/runbooks/delegated-auth-bootstrap.md`](docs/runbooks/delegated-auth-bootstrap.md) for the script. It rotates automatically on every use afterward.

---

## Key Design Decisions

- **SCD Type 2 lives in Bronze, not Silver**, so a workspace or item's change history can't get smoothed over by a downstream transformation.
- **One generic `raw_items`/`dim_item` table** instead of one per item type, discriminated by `item_type` — mirrors how the Admin API itself returns items.
- **Two audit sources, joined in Gold**: the Activity Log only attributes a user to on-demand refreshes, so `raw_refresh_history` (every refresh) and `raw_activity_events` (who did what) are combined to make "did this fail because someone changed something?" answerable.
- **Two capacity-cost tables, not merged**: `MetricsByItem` gives an item-level trend but double-counts if summed across months; `CUDetail` is capacity-level but genuinely additive. `fact_capacity_consumption` and `fact_capacity_utilization` each keep their own honest use case.
- **Bronze only versions a row when something actually changed**, not on every run — tenant metadata is mostly stable, and versioning regardless would turn the history into noise.
- **Forecasting is plain OLS, not a forecasting library** — a weekly linear trend from explicit sum aggregates, auditable by anyone who remembers the math. Every row carries `n_weeks`/`r_squared`/`forecast_confidence` so a thin-history projection is never mistaken for a confident one.
- **`fact_capacity_forecast.sku → dim_capacity.sku` stays inactive** — both forecast and utilization facts already reach `dim_date` through `dim_capacity → dim_workspace → dim_item → fact_activity`, and activating it too would close a real relationship cycle. See `docs/design/capacity-planning-forecasting.md` for the full trace.

---

## Project Structure

```
fabric-governance/
│
├── lh_governance_bronze.Lakehouse/
├── lh_governance_silver.Lakehouse/
├── lh_governance_gold.Lakehouse/
│
├── .github/workflows/
│   ├── ci.yml
│   ├── cd_deploy.yml
│   ├── schedule_ingestion.yml
│   ├── provision_lakehouses.yml
│   ├── deploy_semantic_model.yml
│   └── pull_semantic_model.yml
│
├── config/valueSets/
│   └── dev.json / qa.json / main.json   # real workspace_id + onelake_url per environment
│
├── scripts/
│   ├── deploy.py                        # 3-phase deploy orchestration
│   ├── deploy_semantic_model.py         # provisions sm_governance_medallion in a new environment
│   ├── provision_lakehouses.py          # creates the Lakehouse trio in a new environment
│   ├── pull_item_definition.py          # syncs a live item's definition back into git
│   ├── fabric_client.py                 # Fabric + Power BI REST API wrapper, with retry/backoff
│   └── utils.py                         # Artifact helpers: patch, encode, diff
│
├── notebooks/
│   ├── nb_util_delegated_auth.Notebook/         # shared token-exchange helper, %run by 8 notebooks
│   ├── nb_setup_semantic_model_relationships.Notebook/
│   ├── nb_setup_capacity_forecast_model.Notebook/  # adds fact_capacity_forecast/capacity_planning_summary via TOM
│   ├── nb_bronze_*.Notebook/                    # 10 notebooks
│   ├── nb_silver_*.Notebook/                    # 12 notebooks
│   ├── nb_gold_governance_model.Notebook/
│   └── nb_gold_capacity_forecast.Notebook/      # weekly OLS trend + saturation forecast
│
├── pipelines/pl_governance_orchestration.DataPipeline/
├── semantic models/sm_governance_medallion.SemanticModel/
├── report/rpt_governance_dashboard.Report/
│
├── docs/design/                          # design docs for larger features, written before/alongside the code
├── tests/
├── requirements.txt
└── README.md
```

---

## Getting Started

### Prerequisites

The three Fabric workspaces and the Service Principal are reused from the sibling [microsoft-fabric-medallion-lakehouse](https://github.com/amxavier/microsoft-fabric-medallion-lakehouse) project. This project's only new infrastructure is its own Lakehouse trio inside those same workspaces, plus its own semantic model, report, and pipeline.

### Required GitHub Secrets

| Secret | Description |
|--------|-------------|
| `AZURE_TENANT_ID` | Azure AD Tenant ID |
| `AZURE_CLIENT_ID` | Shared Service Principal Application (Client) ID |
| `AZURE_CLIENT_SECRET` | Shared Service Principal Client Secret (Value, not ID) |
| `FABRIC_WORKSPACE_ID_DEV` / `_QA` / `_PRD` | Workspace GUIDs (also in `config/valueSets/*.json`) |
| `FABRIC_PIPELINE_ID_DEV` / `_QA` / `_PRD` | `pl_governance_orchestration` item GUID per environment, known after the first deploy |

### Standing up a new environment

1. `python scripts/provision_lakehouses.py <branch>` (or the `Provision Lakehouses` workflow) creates `lh_governance_bronze/silver/gold` and wires the real `lh_governance_gold` ID into `config/valueSets/<branch>.json`.
2. Run `CD - Deploy to Fabric` in **full** mode to publish all notebooks, the pipeline, and the report (the report step fails until step 3 is done — that's expected).
3. Provision the semantic model (see [Semantic Model Lifecycle](#semantic-model-lifecycle)), then re-run the full deploy so the report finds it and the pipeline's refresh activity gets auto-patched to this environment.
4. Run the [delegated-auth bootstrap](docs/runbooks/delegated-auth-bootstrap.md) once for this environment.
5. Trigger `schedule_ingestion.yml` manually to run the pipeline once and confirm real data reaches the report.

---

## Built With

- **Microsoft Fabric** — Lakehouse, Data Pipeline, Direct Lake semantic model, Power BI report
- **PySpark** / **Delta Lake** — Bronze/Silver/Gold transformations
- **GitHub Actions** — CI/CD, testing, and provisioning workflows
- **semantic-link-labs** (`sempy_labs.tom`) — programmatic Direct Lake model management
- **[Claude](https://claude.com/claude-code)** — AI pair-programming assistant used throughout development

---

## Author

**Andrelino Xavier** — Data Engineer
[GitHub](https://github.com/amxavier)

---

*A Data Engineering portfolio project built to demonstrate enterprise-realistic Fabric governance across three real environments.*
