# Design: Capacity Planning & Forecasting

**Status:** Steps 1-7 done and validated live in DEV (2026-08-11/12), including the Step 6 guardrail
sub-step (see "sku relationship decision" below — decided: stay inactive, nothing further to do there).
Steps 8 (report layout), 9 (gate tests), 10 (README) remain.

## Mission

This project persists daily Fabric capacity telemetry with SCD Type 2, unlike the native
Capacity Metrics app, which only keeps a rolling 14-day window. Because full history is
retained, we can answer a question the native app structurally cannot: **at the current
growth rate, when will this capacity saturate, and do we need to provision more next
year?**

Build that capability end to end, Gold layer through semantic model, reusing the
automation and standards already in this repo — no manual portal work, no bypassing the
CI/CD gate.

## Investigation findings (Step 0 — confirmed against the real repo, not assumed)

- **CU source**: `fact_capacity_utilization` (Gold, built in `nb_gold_governance_model`,
  sourced from `silver_capacity_cu_detail`) already has `sum_cu`, `date_id`, `sku`, and
  **`base_capacity_units`** — the capacity's CU limit ships with the data itself. No
  hardcoded assumption or notebook parameter needed for the limit.
- **Capacity dimension**: `dim_capacity` (`capacity_id`, `capacity_name`, `sku`, `region`,
  `state`, `is_trial_sku`). The natural join key from `fact_capacity_utilization` is
  `sku`, not `capacity_id` — matches the existing (currently inactive)
  `fact_capacity_utilization.sku → dim_capacity.sku` relationship.
- **`dim_date` has no week grain.** Rather than modify `dim_date`, relate
  `fact_capacity_forecast.week_start_date → dim_date.date_id` directly — every
  `week_start_date` is itself a real date already present in `dim_date`.
- **Relationship graph risk, checked**: no fact table today has an *active* direct
  relationship to `dim_capacity` (the `sku` one is inactive — part of the ambiguous-path
  fix from the 2026-08-07 audit, since `dim_workspace.capacity_id → dim_capacity` is the
  active path today). Reactivating `fact_capacity_utilization.sku → dim_capacity.sku` and
  adding `fact_capacity_forecast`'s relationships to `dim_date`/`dim_capacity` does not,
  by itself, recreate a cycle — but this must be **verified interactively, not assumed**,
  exactly the class of bug the audit found. See guardrail below.

## Plan (10 steps)

1. New Gold notebook `nb_gold_capacity_forecast` (matches existing Gold notebook layout),
   reading `fact_capacity_utilization` from Gold (not Silver — it's already daily grain
   there).
2. Aggregate daily `sum_cu` → weekly (`week_start_date`, ISO week start), smoothing
   day-of-week seasonality.
3. Linear least-squares regression of weekly CU vs. week index. Explicit, commented math
   — no heavy ML dependency. Compute R² and `n_weeks`.
4. `capacity_daily_cu_limit` derived from `base_capacity_units` (already in the data) ×
   86,400 seconds — documented assumption, not a silent hardcode.
5. Write two idempotent (overwrite) Delta tables:
   - `fact_capacity_forecast` — one row per week: `week_start_date`, `cu_actual`,
     `cu_forecast`, `cu_forecast_lower`, `cu_forecast_upper` (CI from residual std dev),
     `capacity_weekly_cu_limit`, `is_forecast`.
   - `capacity_planning_summary` — one row per capacity: `capacity_id`,
     `growth_cu_per_week`, `growth_pct_per_week`, `current_headroom_pct`,
     `capacity_daily_cu_limit`, `projected_saturation_date` (null if never within horizon),
     `weeks_to_saturation`, `n_weeks_history`, `r_squared`, `forecast_confidence`
     (Low/Medium/High — Low when `n_weeks < 8`), `confidence_caveat` (human-readable
     string).
6. Add both tables + relationships to the Direct Lake model via `deploy_semantic_model.py`
   + the TOM/`sempy_labs` notebook, across DEV, QA, and PRD — **never by hand in the
   portal**. Reactivating `fact_capacity_utilization.sku → dim_capacity.sku` is its own
   isolated, reviewed sub-step (see guardrail).
7. 5 new DAX measures in `_measure`: `Projected Saturation Date`, `Weeks to Saturation`,
   `Current Headroom %`, `Weekly CU Growth %`, `Forecast Confidence` (surfaces the caveat
   string).
8. Report layout spec (not built) for a new **"Capacity & Planning"** page: combined
   actual-vs-forecast line, constant capacity-limit line, saturation-date marker, shaded
   forecast band; decision cards (Saturation Date, Weeks/Months to Saturation, Headroom %,
   Weekly Growth %); a visible confidence indicator bound to `forecast_confidence` +
   `confidence_caveat`.
9. Gate-blocking tests (wired into the existing CI gate, not just notebook prints):
   daily-to-weekly CU reconciliation (guards the exact repeated-wrong-total /
   inverted-cardinality bug class the 2026-08-07 audit found), forecast series continuity
   (no gap/overlap, `is_forecast` partitions correctly), saturation logic returns null on
   flat/negative slope, no negative CU anywhere.
10. Update README with the new capability.

## Guardrails

- Derive all schema from the real repo (done in Step 0); surface every assumption instead
  of silently hardcoding it.
- Reuse the existing deployment automation (`deploy_semantic_model.py`,
  `provision_lakehouses.py`, the TOM notebook) and respect the CI/CD gate — no manual
  portal changes, no gate bypass.
- Keep the regression explainable — a senior reviewer should be able to read the math
  directly.
- The confidence fields (`forecast_confidence`, `confidence_caveat`) are mandatory output,
  not optional polish — the whole point is to be honest about how much history backs the
  forecast.
- **sku → dim_capacity relationships — decided, 2026-08-12: stay inactive.** Mapped the
  full active-relationship graph by hand: `dim_capacity → dim_workspace → dim_item →
  fact_activity → dim_date` is already an active chain. `fact_capacity_utilization` and
  `fact_capacity_forecast` are both already active on `dim_date` (via `date_id` /
  `week_start_date`), so activating either one's `sku → dim_capacity.sku` relationship
  closes a real cycle (two distinct paths between `dim_capacity` and `dim_date`) —
  confirmed the same ambiguous-path bug class the 2026-08-07 audit found, not a false
  alarm. Fixing it would mean deactivating something already relied on elsewhere (e.g.
  `fact_activity.date_id → dim_date.date_id`), which has its own blast radius — user
  chose to leave both `sku` relationships inactive rather than take that on now. Anything
  needing capacity-level attributes joined to CU/forecast data should use
  `USERELATIONSHIP` in the measure (same pattern as `'Total CU (Item, by Date)'` in
  `_measure.tmdl`), not rely on auto filter propagation through `sku`.

## Execution order for the next session

Start cold with **Step 1, DEV only**. Do not touch QA/PRD until DEV's Gold notebook,
model changes, and tests are all validated against real data.
