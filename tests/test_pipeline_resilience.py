"""
Real JSON parsing (not string matching) against pl_governance_orchestration's
committed definition — guards operational resilience properties that don't
show up as functional bugs until a transient Fabric platform error actually
happens mid-run (confirmed live, more than once, in this project's own
history: a generic "Something went wrong on our end" failure required a full
manual re-trigger of the entire pipeline because no activity had retry
configured).
"""
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_PATH = REPO_ROOT / "pipelines" / "pl_governance_orchestration.DataPipeline" / "pipeline-content.json"


def _activities() -> list[dict]:
    content = json.loads(PIPELINE_PATH.read_text(encoding="utf-8"))
    return content["properties"]["activities"]


def test_pipeline_content_is_valid_json():
    assert PIPELINE_PATH.exists(), f"pipeline-content.json not found: {PIPELINE_PATH}"
    activities = _activities()
    assert len(activities) > 0, "Pipeline has no activities"


def test_every_activity_retries_on_transient_failure():
    # retry: 0 means a single transient platform hiccup on any one of ~25
    # activities forces a full pipeline re-run from scratch instead of the
    # platform's own retry handling absorbing it.
    activities = _activities()
    no_retry = [a["name"] for a in activities if a.get("policy", {}).get("retry", 0) == 0]
    assert not no_retry, (
        f"{len(no_retry)} activity(ies) have retry: 0, so a single transient "
        f"Fabric error forces a full manual pipeline re-trigger: {no_retry}"
    )


def test_retry_interval_is_set_and_reasonable():
    # A retry with 0-second spacing hammers the platform immediately instead
    # of giving a transient condition time to clear — must be a real,
    # positive interval.
    activities = _activities()
    for a in activities:
        interval = a.get("policy", {}).get("retryIntervalInSeconds")
        assert interval and interval > 0, (
            f"{a['name']}: retryIntervalInSeconds must be a positive number, got {interval!r}"
        )
