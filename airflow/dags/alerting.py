"""Failure path for every task: on_task_failure is the DAG's on_failure_callback.
It runs once, after the last retry fails, in this order:

  1. mark the load failed in ops.load_audit (reuse the row the load already
     wrote, or add one for a task that failed before loading anything)
  2. run the Pipeline Triage Agent on it (pipeline/triage/; skipped when
     LLM_PROVIDER=none), which saves its note on that audit row
  3. send the SMTP failure email, with the triage note when there is one

Each step is isolated and none may raise: an audit, agent, Bedrock or SMTP
problem is logged and never hides or replaces the task's own failure, which
is what Airflow keeps reporting. Email goes through the smtp_default Airflow
Connection (credentials live only in that connection).
"""
import logging
import os
from datetime import timezone

from airflow.providers.smtp.notifications.smtp import SmtpNotifier

log = logging.getLogger(__name__)

# Set ALERT_EMAIL in the environment (e.g. .env); the default is a placeholder.
ALERT_RECIPIENT = os.environ.get("ALERT_EMAIL", "alerts@example.com")

# Subject and body are Jinja templates: Airflow fills in {{ ti.* }} (the task
# instance) and {{ exception }}; on_task_failure adds load_id, triage_note and triage_status.
SUBJECT = "Airflow FAILED: {{ ti.dag_id }}.{{ ti.task_id }} (run {{ ti.run_id }})"
HTML_CONTENT = """
    <h3>Task failed</h3>
    <table>
      <tr><td><b>DAG</b></td><td>{{ ti.dag_id }}</td></tr>
      <tr><td><b>Task</b></td><td>{{ ti.task_id }}</td></tr>
      <tr><td><b>Run ID</b></td><td>{{ ti.run_id }}</td></tr>
      <tr><td><b>Try</b></td><td>{{ ti.try_number }} of {{ ti.max_tries + 1 }}</td></tr>
      {% if load_id %}<tr><td><b>Audit load_id</b></td><td>{{ load_id }}</td></tr>{% endif %}
    </table>
    <p><b>Exception:</b></p>
    <pre>{{ exception }}</pre>
    {% if triage_note %}
    <p><b>Triage note</b> (LLM-generated, {{ triage_status }}; verify before acting, fixes need human approval):</p>
    <pre>{{ triage_note | e }}</pre>
    {% endif %}
    <p><a href="{{ ti.log_url }}">View full log</a></p>
    """


def build_failure_notifier() -> SmtpNotifier:
    # A new notifier per email: rendering writes the rendered text back onto the
    # notifier's own fields, so a shared instance would reuse the first email.
    return SmtpNotifier(
        to=ALERT_RECIPIENT,
        from_email=ALERT_RECIPIENT,
        smtp_conn_id="smtp_default",       # SMTP host and login are stored in this Airflow Connection
        subject=SUBJECT,
        html_content=HTML_CONTENT,
    )


def _error_text(context) -> str:
    exc = context.get("exception")
    return f"{type(exc).__name__}: {exc}" if exc is not None else "unknown error"


def _run_started_at(context):
    # Audit timestamps are naive UTC; the DAG run's start bounds "this run's" failed rows.
    start = getattr(context.get("dag_run"), "start_date", None)
    if start is None:
        return None
    return start.astimezone(timezone.utc).replace(tzinfo=None) if start.tzinfo else start


def on_task_failure(context) -> None:
    ti = context["ti"]
    error = _error_text(context)
    batch_date = context.get("ds")
    load_id = triage = None

    # 1. Mark the load failed (always first).
    try:
        from pipeline.loaders import mark_task_failed
        from pipeline.triage.tools import TASKS
        since = _run_started_at(context)
        prefix = TASKS.get(ti.task_id, {}).get("entity_prefix") if since else None
        load_id = mark_task_failed(ti.task_id, prefix, batch_date, since, error)
    except Exception:
        log.exception("could not mark %s failed in ops.load_audit; the original failure stands", ti.task_id)

    # 2. Triage (triage_failed_load never raises; skipped when LLM_PROVIDER=none).
    if load_id:
        try:
            from pipeline.triage.agent import triage_failed_load
            triage = triage_failed_load(load_id, ti.task_id, batch_date, error)
        except Exception:
            log.exception("triage agent errored for %s; the original failure stands", load_id)

    # 3. The failure email, with the note when there is one.
    try:
        build_failure_notifier()(dict(context, load_id=load_id,
                                      triage_note=getattr(triage, "note", None),
                                      triage_status=getattr(triage, "status", None)))
    except Exception:
        log.exception("failure email errored; the original failure stands")
