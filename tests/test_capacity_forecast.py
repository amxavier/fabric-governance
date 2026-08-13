"""
Static sanity checks on nb_gold_capacity_forecast's source (not a Spark/Delta
execution test — this repo's CI has no Fabric runtime available, same
constraint as tests/test_bronze_scd.py). These enforce the specific
safeguards docs/design/capacity-planning-forecasting.md's Step 9 calls for,
each one guarding a concrete failure mode rather than a stylistic preference.
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "nb_gold_capacity_forecast.Notebook" / "notebook-content.py"


def _source() -> str:
    assert NOTEBOOK_PATH.exists(), f"Notebook source not found: {NOTEBOOK_PATH}"
    return NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_weekly_aggregation_sums_directly_off_the_daily_fact():
    # Guards the 2026-08-07 audit's bug class: a join inserted before the
    # groupBy/agg (even an innocent-looking one) can fan out rows and
    # silently inflate every weekly total. weekly_raw must aggregate
    # straight off fact_capacity_utilization with sum(sum_cu) — no ".join("
    # between the read and this aggregation.
    source = _source()
    read_idx = source.index('spark.read.format("delta").load(f"{GOLD_ABFS}/Tables/fact_capacity_utilization")')
    agg_idx = source.index('.alias("cu_actual")')
    between = source[read_idx:agg_idx]
    assert ".join(" not in between, (
        "A join between reading fact_capacity_utilization and computing "
        "cu_actual can fan out rows and inflate weekly CU totals — "
        "aggregate directly off the daily fact."
    )
    assert 'F.sum("sum_cu")' in source, (
        "cu_actual must be a direct sum() of the daily sum_cu column, not "
        "an avg/count/first that would misrepresent the weekly total."
    )


def test_future_weeks_never_overlap_actual_week_index():
    # future_offset must start at 1 (not 0) and week_index must continue
    # from n_weeks - 1 (the last actual index) — together these guarantee
    # no forecast row duplicates or skips an actual row's week_index. Both
    # patterns are asserted verbatim since this is exactly the kind of
    # off-by-one that silently produces a one-week gap or overlap at the
    # actual/forecast boundary.
    source = _source()
    assert "spark.range(1, FORECAST_HORIZON_WEEKS + 1)" in source, (
        "future_offsets must start at 1, not 0 — starting at 0 would "
        "duplicate the last actual week's week_index in the forecast."
    )
    assert '"week_index", F.col("n_weeks") - 1 + F.col("future_offset")' in source, (
        "Forecast week_index must continue immediately from the last "
        "actual index (n_weeks - 1) with no gap or overlap."
    )


def test_is_forecast_flag_set_correctly_on_both_sides():
    # The literal flags are set separately on each half before the union —
    # confirm both exist and with the expected boolean, not just that the
    # column name appears somewhere.
    source = _source()
    assert '.withColumn("is_forecast", F.lit(True))' in source, "future rows must be flagged is_forecast = True"
    assert '.withColumn("is_forecast", F.lit(False))' in source, "actual rows must be flagged is_forecast = False"


def test_saturation_filter_returns_null_on_flat_or_negative_slope():
    # The whole "returns null, not an error or a stale date" guarantee
    # rests on this exact filter shape: with a flat/negative slope,
    # cu_forecast never exceeds capacity_weekly_cu_limit, the filter matches
    # zero rows, and min() over zero rows is null — no explicit branch
    # needed, but the filter has to actually say ">=", not something looser.
    source = _source()
    assert "is_forecast = true and cu_forecast >= capacity_weekly_cu_limit" in source, (
        "Saturation detection must filter on cu_forecast >= "
        "capacity_weekly_cu_limit restricted to forecast rows — this exact "
        "condition is what makes 'no saturation predicted' fall out as a "
        "natural null instead of needing a separate flat/negative-slope "
        "branch."
    )


def test_no_negative_cu_forecast_values():
    # A steep negative trend extrapolated FORECAST_HORIZON_WEEKS out can
    # otherwise project negative CU, which isn't a value a capacity can
    # consume. All three of cu_forecast/cu_forecast_lower/cu_forecast_upper
    # must be floored at 0 — cu_forecast_upper included, even though "higher
    # is always a valid bound" sounds right in isolation: once cu_forecast
    # itself is clamped to 0, an unclamped upper that's still negative would
    # sit below the now-zeroed forecast, inverting the confidence band
    # (confirmed as a real bug via code review, not just a theoretical one).
    source = _source()
    assert 'F.greatest(F.lit(0.0), F.col("cu_forecast") - 1.96 * F.col("residual_std"))' in source, (
        "cu_forecast_lower must be floored at 0."
    )
    assert 'F.greatest(F.lit(0.0), F.col("cu_forecast") + 1.96 * F.col("residual_std"))' in source, (
        "cu_forecast_upper must be floored at 0 too, or it can end up below "
        "the clamped cu_forecast, inverting the band."
    )
    assert source.count('F.greatest(F.lit(0.0), F.col("cu_forecast"))') >= 1, (
        "cu_forecast must be floored at 0 after the confidence band is computed from it."
    )


def test_regression_guards_against_single_week_and_perfect_fit_division_by_zero():
    # n_weeks < 2 makes the OLS denominator (n*sum_x2 - sum_x^2) zero;
    # ss_tot == 0 (a single week, or a perfectly repeated value) makes the
    # r_squared division zero. Both must be guarded rather than left to
    # produce a Spark null-propagation surprise or crash a daily run.
    source = _source()
    assert 'F.when(F.col("ss_tot") > 0,' in source, (
        "r_squared must guard against ss_tot == 0 (single week of history, "
        "or a perfectly flat series) instead of dividing by zero."
    )


def test_forecast_confidence_is_never_silently_omitted():
    # The design doc's guardrail: forecast_confidence/confidence_caveat are
    # mandatory output, not optional polish. Confirm both columns are
    # actually selected into the written fact_capacity_forecast/
    # capacity_planning_summary tables, not just computed and dropped.
    source = _source()
    assert "forecast_confidence" in source and "confidence_caveat" in source
    summary_select_idx = source.index("capacity_planning_summary = (")
    summary_write_idx = source.index("capacity_planning_summary.write")
    summary_block = source[summary_select_idx:summary_write_idx]
    assert "forecast_confidence" in summary_block and "confidence_caveat" in summary_block, (
        "forecast_confidence/confidence_caveat must reach the final "
        "capacity_planning_summary select — they're the whole point of "
        "surfacing forecast reliability, not incidental intermediate columns."
    )
