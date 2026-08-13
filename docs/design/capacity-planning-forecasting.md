# Design: Capacity Planning & Forecasting

**Status:** Complete and live in all three environments — DEV, QA, and PRD (2026-08-11/12).
Notebook, semantic model (tables, relationships, 6 measures), the "Capacity & Planning" report
page (5 cards + 5-series line chart, built as PBIR JSON), gate-blocking tests, README, and the
pipeline wired to run the forecast daily — all validated live with real data in each
environment. `deploy.py` now auto-patches `refresh_semantic_model`'s target per environment
(see README's Semantic Model Lifecycle section), so promoting this project further needs no
manual pipeline reconfiguration. `nb_setup_capacity_forecast_model` (the one-time TOM setup
per environment) is confirmed to work fine under the Service Principal too (verified live
2026-08-13 — see README) — it's still run manually today, but purely by convention (it's a
once-per-environment step), not because of any auth restriction.

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

## Report layout spec — "Capacity & Planning" page (Step 8, not built)

New page in `rpt_governance_dashboard`, alongside the existing Overview / Capacity Cost /
Refresh Health / Cleanup Candidates pages. Spec only — build interactively in the portal,
same as the other pages (PBIR JSON is fragile to hand-edit; the semantic model is the part
worth automating, not the report canvas). Apply `report/theme_foundry.json` (View → Themes
→ Browse for themes) for consistent colors/fonts with the rest of the report.

**Top row — 4 decision cards** (Card visual, one measure each, `_measure` table):
1. `[Projected Saturation Date]` — title "Capacity Saturation Date". When null (current
   live state — headroom is 99.99%), Power BI's Card visual just shows blank; consider a
   conditional title/subtitle via a tooltip or a small text box underneath reading "No
   saturation predicted within the forecast horizon" bound to
   `ISBLANK([Projected Saturation Date])` via a rule-based text box, so blank doesn't read
   as "broken."
2. `[Weeks to Saturation]` — title "Weeks to Saturation". Same blank-state note as above.
3. `[Current Headroom %]` — title "Headroom %". Format as plain number with a "%" suffix
   in the card's display units / suffix setting (not a DAX `%` format — see the
   `_measure.tmdl` comment on why) — e.g. "99.99%" via the card's text suffix, not the
   measure's format string.
4. `[Weekly CU Growth %]` — title "Weekly Growth %". Same suffix approach as #3.

**Confidence indicator**, next to or below the cards: a Card or KPI visual bound to
`[Forecast Confidence]` (the text already reads e.g. "Low — Projection based on 5 week(s)
of history — confidence increases as history accumulates.") with its background or
accent color bound to `[Forecast Confidence Color]` via conditional formatting — this is
the guardrail's "confidence fields are mandatory output, not optional polish" made
visible, not buried in a tooltip.

**Main visual — actual vs. forecast trend**: a Line Chart, X axis `fact_capacity_forecast[week_start_date]`,
with these value series:
- `cu_actual` — solid line, `good` palette color (`#45B499`)
- `cu_forecast` — dashed/dotted line style (Format → line styles), same color family but
  lower opacity, so the actual/forecast boundary reads visually without needing a separate
  legend entry per point
- `cu_forecast_lower` / `cu_forecast_upper` — thin dotted lines in a muted neutral color
  (`#A89C86` from the theme), framing the confidence band. Power BI's native Line Chart
  doesn't fill an area between two lines, so this reads as a "channel" rather than a solid
  shaded band — acceptable given the goal is honesty about uncertainty, not polish; a
  filled band is a possible later enhancement via a Line and Area combo but not required
  for this spec.
- `capacity_weekly_cu_limit` — flat reference line, `bad` palette color (`#C76A5F`),
  distinct dash style, so the ceiling reads clearly against actual/forecast

**Saturation marker**: only meaningful once `projected_saturation_date` is non-null for at
least one capacity. When it is, add a vertical reference line via the visual's Analytics
pane bound to `[Projected Saturation Date]` (Power BI supports a dynamic reference line
value in recent versions) — don't hardcode a static date. Skip this element entirely for
now (current live data has no saturation date in the 52-week horizon) rather than build
against a value that doesn't exist yet; add it when the forecast actually produces one.

**Filter/slicer — skip for now, not a clean fit yet.** A `dim_capacity[capacity_name]`
slicer would correctly filter `capacity_planning_summary` (its relationship to
`dim_capacity` is active) but would silently NOT filter `fact_capacity_forecast` — its
`sku → dim_capacity` relationship is the one left inactive (see guardrail above), so the
main trend chart wouldn't respond to the slicer even though the cards would, which is a
worse trap than no slicer at all. Only one real capacity exists today so this has no
visible effect yet, but don't add the slicer until either the relationship question is
revisited or the chart's measures are rewritten with explicit `USERELATIONSHIP` against
`fact_capacity_forecast[sku] → dim_capacity[sku]`.

## Execution order for the next session

Start cold with **Step 1, DEV only**. Do not touch QA/PRD until DEV's Gold notebook,
model changes, and tests are all validated against real data.
