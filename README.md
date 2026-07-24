# fabric-governance

Governance toolkit for Microsoft Fabric workspaces: inventory, access review, and compliance checks across environments, automated via GitHub Actions.

## Goals

- Inventory Fabric workspaces, items, and capacities via the Admin/REST APIs
- Review workspace access and role assignments against expected policy
- Track sensitivity labels and naming-convention compliance
- Publish governance reports on a schedule

## Structure

```
notebooks/   Fabric notebooks (governance scans, reports)
scripts/     Python scripts (Fabric REST API client, checks, reporting)
config/      Per-environment configuration (workspace IDs, policies)
.github/workflows/  CI/CD and scheduled scans
```

## Status

Project scaffold — implementation in progress.
