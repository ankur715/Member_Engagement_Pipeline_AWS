"""Member engagement data pipeline on AWS (S3 + Redshift Serverless).

Replaces legacy/run_nightly.sh + legacy/crontab with a DAG that has per-step
retries, dependency-aware parallelism, backfills, SLAs and alerting.

    apply_migrations
      -> drop_member_files (simulated health-plan SFTP drop) -> load_member_files (SCD2)
      -> ingest_salesforce_activities -> tag_sdoh_needs
      -> ingest_events
      -> ingest_contact_preferences (Google Sheet)
    all of the above -> data_quality -> publish_plan_kpis

Idempotency: every task is keyed on the logical date (`ds`). Roster files
are seeded from it; loads are MERGE / delete-insert on natural keys inside
one Redshift transaction; API watermarks advance only when their data
commits. Re-running any date, or clearing a single task, is safe.

Concurrency: Redshift writers share the `redshift` pool (1 slot). Redshift
uses serializable isolation, so two transactions writing the same tables at
once would abort one (error 1023); API reads still overlap freely.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException

# dags/ (for alerting.py) and the project root (for the `pipeline` package --
# the same modules that run standalone via `python -m pipeline.ingest...`).
_DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
for _path in (_DAGS_DIR, os.path.abspath(os.path.join(_DAGS_DIR, "..", ".."))):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from alerting import failure_email_notifier  # noqa: E402

# Airflow pool with 1 slot (create it: `airflow pools set redshift 1 ...`) so only
# one task writes to Redshift at a time.
REDSHIFT_POOL = "redshift"

# Settings every task inherits unless it overrides them.
default_args = {
    "retries": 2,                                    # retry twice before failing
    "retry_delay": timedelta(minutes=2),             # first retry after 2 min...
    "retry_exponential_backoff": True,               # ...then longer waits each time
    "execution_timeout": timedelta(minutes=20),      # kill a task that hangs
    "on_failure_callback": failure_email_notifier,   # email once retries are exhausted
}


@dag(
    dag_id="member_engagement_pipeline",
    schedule="0 6 * * *",          # daily, after health-plan files land overnight
    start_date=datetime(2026, 9, 20),
    catchup=False,                 # don't auto-run every missed day on first deploy (backfill on purpose instead)
    max_active_runs=1,             # SCD2 roster loads must apply in date order
    default_args=default_args,
    tags=["member-engagement", "aws", "redshift", "salesforce", "google-sheets"],
)
def member_engagement_pipeline():

    # Each @task is a thin wrapper around a pipeline module -- the same code can run
    # from the command line. Imports live inside the function so the DAG file
    # stays fast for Airflow's scheduler to parse.

    @task(pool=REDSHIFT_POOL)
    def apply_migrations():
        # Bring the Redshift schema up to date (no-op if nothing new).
        from pipeline import migrate
        return migrate.main()

    @task
    def drop_member_files(ds: str = None):
        """Stand-in for health plans' SFTP drop (AWS Transfer Family -> S3 in production)."""
        from pipeline.sources import generate_member_files
        return generate_member_files.main(ds)

    # depends_on_past: today's roster load waits for yesterday's to succeed (SCD2 must go in order).
    # `ds` is filled in by Airflow with the run's logical date, e.g. "2026-09-28".
    @task(pool=REDSHIFT_POOL, depends_on_past=True)
    def load_member_files(ds: str = None):
        from pipeline.ingest import member_files
        return member_files.main(ds)

    @task(pool=REDSHIFT_POOL)
    def ingest_salesforce_activities(ds: str = None):
        # Incremental pull of CHW activities since the last watermark.
        from pipeline.ingest import salesforce_activities
        return salesforce_activities.main(ds)

    @task(pool=REDSHIFT_POOL)
    def ingest_events(ds: str = None):
        # Incremental pull of community events + attendance.
        from pipeline.ingest import events
        return events.main(ds)

    @task(pool=REDSHIFT_POOL)
    def ingest_contact_preferences(ds: str = None):
        # Full snapshot of the do-not-contact Google Sheet.
        from pipeline.ingest import contact_preferences
        return contact_preferences.main(ds)

    @task(pool=REDSHIFT_POOL)
    def tag_sdoh_needs(ds: str = None):
        # Tag social needs in new/changed CHW notes.
        from pipeline.enrich import sdoh_rules
        return sdoh_rules.main(ds)

    @task
    def drop_claims_files(ds: str = None):
        """Stand-in for health plans' daily claims extracts. The first run also
        drops a 12-month history file, the way a new plan is onboarded."""
        from pipeline import s3_io
        from pipeline.sources import generate_claims
        onboarded = any("_history_" in k for k in s3_io.list_keys(generate_claims.PREFIX + "/"))
        return generate_claims.main(ds, history=not onboarded)

    @task(pool=REDSHIFT_POOL)
    def load_claims(ds: str = None):
        # Normalize messy claims and keep the latest version of each claim.
        from pipeline.ingest import claims
        return claims.main(ds)

    @task(retries=0)  # a DQ failure is a data problem; retrying won't fix it
    def data_quality(ds: str = None):
        from pipeline.quality import data_quality as dq
        try:
            return dq.run_checks(ds)
        except dq.DataQualityError as e:
            # Re-raise as an Airflow failure -> task turns red and the alert email fires.
            raise AirflowException(str(e))

    @task
    def publish_plan_kpis(ds: str = None):
        # Customer KPI report -> S3 export (+ Google Sheets if configured).
        from pipeline.publish import plan_kpis
        return plan_kpis.main(ds)

    # --- Build the task objects ---
    migrations = apply_migrations()
    members = load_member_files()
    activities = ingest_salesforce_activities()
    events = ingest_events()
    dnc = ingest_contact_preferences()
    sdoh = tag_sdoh_needs()
    claims = load_claims()
    dq = data_quality()

    # --- Wire the dependencies (">>" = "runs before") ---
    migrations >> drop_member_files() >> members          # schema first, then file drop, then load
    migrations >> [activities, events, dnc]               # the three API sources run in parallel
    activities >> sdoh                                    # tag notes only after they've landed
    migrations >> drop_claims_files() >> claims           # claims extracts, then load
    [members, sdoh, events, dnc, claims] >> dq >> publish_plan_kpis()  # KPIs go out only if DQ passes


# Calling the decorated function registers the DAG with Airflow.
member_engagement_pipeline()
