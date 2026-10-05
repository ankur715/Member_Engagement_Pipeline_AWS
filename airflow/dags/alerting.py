"""Failure alert: SmtpNotifier with a readable subject/body, via the
smtp_default Airflow Connection (credentials live only in that connection).
"""
import os

from airflow.providers.smtp.notifications.smtp import SmtpNotifier

# Set ALERT_EMAIL in the environment (e.g. .env); the default is a placeholder.
ALERT_RECIPIENT = os.environ.get("ALERT_EMAIL", "alerts@example.com")

# Attached to every task as on_failure_callback (see default_args in the DAG). It
# fires after the last retry fails, not on every attempt.
failure_email_notifier = SmtpNotifier(
    to=ALERT_RECIPIENT,
    from_email=ALERT_RECIPIENT,
    smtp_conn_id="smtp_default",       # SMTP host and login are stored in this Airflow Connection
    # Subject and body are Jinja templates: Airflow fills in {{ ti.* }} (the task
    # instance) and {{ exception }} when the alert is sent.
    subject="Airflow FAILED: {{ ti.dag_id }}.{{ ti.task_id }} (run {{ ti.run_id }})",
    html_content="""
    <h3>Task failed</h3>
    <table>
      <tr><td><b>DAG</b></td><td>{{ ti.dag_id }}</td></tr>
      <tr><td><b>Task</b></td><td>{{ ti.task_id }}</td></tr>
      <tr><td><b>Run ID</b></td><td>{{ ti.run_id }}</td></tr>
      <tr><td><b>Try</b></td><td>{{ ti.try_number }} of {{ ti.max_tries + 1 }}</td></tr>
    </table>
    <p><b>Exception:</b></p>
    <pre>{{ exception }}</pre>
    <p><a href="{{ ti.log_url }}">View full log</a></p>
    """,
)
