"""
Validates the actual OLS math nb_gold_capacity_forecast.py implements — not
just that the right code pattern appears (tests/test_capacity_forecast.py
already covers that), but that the algebra itself is correct against known
inputs/outputs.

**Honest limitation, stated plainly**: this is a pure-Python shadow
reimplementation of the same formulas documented in the notebook's markdown
(`slope = (nΣxy − ΣxΣy) / (nΣx² − (Σx)²)`, etc.), NOT an execution of the
real PySpark cells — this repo's CI has no Spark runtime (see every other
tests/ module's docstring for the same constraint). It catches a wrong
formula; it can't catch a Spark-specific bug (wrong column reference, wrong
join, wrong aggregation function) that tests/test_capacity_forecast.py's
source-pattern checks are aimed at instead. If the notebook's formula ever
changes, this file's copy must be updated to match — there's no shared
source of truth between the two, by design (the project's own guardrail is
to keep the regression inline and readable in the notebook, not hidden
behind a shared library).
"""
import math


def _ols(x: list[float], y: list[float]) -> tuple[float, float]:
    """Mirrors nb_gold_capacity_forecast.py's slope/intercept cell exactly."""
    n = len(x)
    sum_x, sum_y = sum(x), sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi * xi for xi in x)
    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
    intercept = (sum_y - slope * sum_x) / n
    return slope, intercept


def _r_squared(x: list[float], y: list[float], slope: float, intercept: float) -> float:
    """Mirrors the ss_tot/ss_res identity used in the notebook."""
    n = len(x)
    sum_y = sum(y)
    sum_y2 = sum(yi * yi for yi in y)
    ss_tot = sum_y2 - (sum_y * sum_y) / n
    ss_res = sum((yi - (intercept + slope * xi)) ** 2 for xi, yi in zip(x, y))
    if ss_tot <= 0:
        return None
    return 1 - (ss_res / ss_tot)


def test_ols_recovers_exact_slope_and_intercept_on_a_perfect_line():
    x = list(range(10))
    y = [3.0 * xi + 7.0 for xi in x]  # y = 3x + 7, no noise
    slope, intercept = _ols(x, y)
    assert math.isclose(slope, 3.0, abs_tol=1e-9)
    assert math.isclose(intercept, 7.0, abs_tol=1e-9)


def test_r_squared_is_one_for_a_perfect_line():
    x = list(range(10))
    y = [3.0 * xi + 7.0 for xi in x]
    slope, intercept = _ols(x, y)
    r2 = _r_squared(x, y, slope, intercept)
    assert math.isclose(r2, 1.0, abs_tol=1e-9), (
        "A perfectly linear series must fit with r_squared == 1.0 — if this "
        "fails, the ss_tot/ss_res identity itself is wrong, not just noisy data."
    )


def test_r_squared_is_near_zero_for_flat_data_with_symmetric_noise():
    x = list(range(10))
    # Symmetric noise around a flat mean — the best-fit line should be
    # ~flat, explaining almost none of the variance.
    y = [50.0, 52.0, 48.0, 51.0, 49.0, 53.0, 47.0, 50.0, 52.0, 48.0]
    slope, intercept = _ols(x, y)
    r2 = _r_squared(x, y, slope, intercept)
    assert abs(slope) < 1.0, f"Flat noisy data should fit a near-zero slope, got {slope}"
    assert r2 < 0.3, f"Flat noisy data should have low r_squared, got {r2}"


def test_single_week_history_hits_a_zero_ols_denominator():
    # A single week of history (n=1, x=[0]) makes the slope formula's own
    # denominator (n*sum_x2 - sum_x^2) zero — Spark's division-by-zero
    # returns null rather than raising, which is exactly why the notebook's
    # forecast_confidence/r_squared logic has to guard n_weeks/ss_tot
    # explicitly instead of assuming the regression always produces a value.
    x = [0]
    n = len(x)
    denom = n * sum(xi * xi for xi in x) - sum(x) * sum(x)
    assert denom == 0, "Single-point series must hit a zero OLS denominator, guarded upstream"


def test_forecast_confidence_thresholds_match_the_documented_rule():
    # Mirrors the exact F.when(n_weeks < 8, "Low").when(r_squared >= 0.5,
    # "High").otherwise("Medium") chain — a plain Python re-check of the
    # threshold logic itself, independent of how n_weeks/r_squared were
    # computed.
    def confidence(n_weeks: int, r_squared: float | None) -> str:
        if n_weeks < 8:
            return "Low"
        if r_squared is not None and r_squared >= 0.5:
            return "High"
        return "Medium"

    assert confidence(4, 0.9) == "Low"  # thin history overrides a strong fit
    assert confidence(10, 0.9) == "High"
    assert confidence(10, 0.2) == "Medium"
    assert confidence(10, None) == "Medium"  # ss_tot == 0 case, r_squared is None
