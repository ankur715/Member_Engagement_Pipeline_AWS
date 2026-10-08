"""DAG integrity: imports cleanly, has tags/retries, every Redshift writer
uses the pool, and the dependency edges the design relies on exist."""
import os

import pytest
from airflow.models import DagBag

DAG_ID = "member_engagement_pipeline"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=os.path.join(os.path.dirname(__file__), "..", "dags"))
    assert bag.import_errors == {}
    return bag.get_dag(DAG_ID)


def test_dag_basics(dag):
    assert dag.tags
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2


def test_redshift_writers_use_pool(dag):
    for task_id in ("apply_migrations", "load_member_files", "ingest_salesforce_activities",
                    "ingest_events", "ingest_contact_preferences", "tag_sdoh_needs", "load_claims", "load_hra", "ingest_housing_violations", "ingest_weather_alerts", "classify_notes_llm"):
        assert dag.get_task(task_id).pool == "redshift", task_id


def test_roster_loads_are_sequential(dag):
    assert dag.get_task("load_member_files").depends_on_past


def test_sdoh_tagging_follows_salesforce(dag):
    assert "ingest_salesforce_activities" in dag.get_task("tag_sdoh_needs").upstream_task_ids
    assert "tag_sdoh_needs" in dag.get_task("classify_notes_llm").upstream_task_ids


def test_dq_waits_for_every_source(dag):
    assert {"load_member_files", "classify_notes_llm", "ingest_events", "ingest_contact_preferences", "load_claims", "load_hra", "ingest_housing_violations",
         "ingest_weather_alerts"} \
        <= dag.get_task("data_quality").upstream_task_ids


def test_kpis_publish_only_after_dq(dag):
    assert dag.get_task("publish_plan_kpis").upstream_task_ids == {"data_quality"}


def test_failure_callback_runs_triage_then_email(dag):
    from alerting import on_task_failure
    assert dag.default_args["on_failure_callback"] is on_task_failure


def test_every_task_is_in_the_triage_registry(dag):
    # The triage agent's get_pipeline_config tool describes each task; a new task must be added there.
    from pipeline.triage.tools import TASKS
    assert set(dag.task_ids) == set(TASKS)
