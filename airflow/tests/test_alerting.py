"""The failure callback: mark failed -> triage -> email, in that order, with each
step isolated so none of them can mask or replace the task's own failure."""
import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
for p in (os.path.join(ROOT, "airflow", "dags"), ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

import alerting  # noqa: E402


def context():
    return {"ti": SimpleNamespace(task_id="load_claims", dag_id="member_engagement_pipeline", run_id="r1"),
            "ds": "2026-10-02",
            "exception": FileNotFoundError("No claims files for 2026-10-02 -- health-plan drop missing?"),
            "dag_run": SimpleNamespace(start_date=datetime(2026, 10, 2, 6, 0, tzinfo=timezone.utc))}


class Recorder:
    def __init__(self):
        self.calls, self.emails = [], []

    def notifier(self):
        def send(ctx):
            self.calls.append("email")
            self.emails.append(ctx)
        return send


@pytest.fixture
def rec(monkeypatch):
    r = Recorder()
    monkeypatch.setattr(alerting, "build_failure_notifier", r.notifier)
    return r


def test_marks_failed_then_triages_then_emails_with_the_note(monkeypatch, rec):
    from pipeline import loaders
    from pipeline.triage import agent

    def mark(task_id, prefix, batch_date, since, error):
        rec.calls.append("mark")
        assert (task_id, prefix, batch_date) == ("load_claims", "claims_", "2026-10-02")
        assert since == datetime(2026, 10, 2, 6, 0) and error.startswith("FileNotFoundError")
        return "load_claims-2026-10-02"

    def triage(load_id, task_id, batch_date, error):
        rec.calls.append("triage")
        return agent.TriageResult(status="answered", note="DIAGNOSIS: the claims drop is missing.")

    monkeypatch.setattr(loaders, "mark_task_failed", mark)
    monkeypatch.setattr(agent, "triage_failed_load", triage)
    alerting.on_task_failure(context())
    assert rec.calls == ["mark", "triage", "email"]
    email = rec.emails[0]
    assert email["triage_note"].startswith("DIAGNOSIS") and email["triage_status"] == "answered"
    assert email["load_id"] == "load_claims-2026-10-02"
    assert isinstance(email["exception"], FileNotFoundError)        # the original failure is what's reported


def test_every_step_failing_still_returns_cleanly(monkeypatch):
    from pipeline import loaders
    from pipeline.triage import agent

    def boom(*a, **k):
        raise RuntimeError("down")
    monkeypatch.setattr(loaders, "mark_task_failed", boom)
    monkeypatch.setattr(agent, "triage_failed_load", boom)
    monkeypatch.setattr(alerting, "build_failure_notifier", lambda: boom)
    assert alerting.on_task_failure(context()) is None


def test_agent_failure_still_sends_the_email_without_a_note(monkeypatch, rec):
    from pipeline import loaders
    from pipeline.triage import agent
    monkeypatch.setattr(loaders, "mark_task_failed", lambda *a: "load_claims-2026-10-02")
    monkeypatch.setattr(agent, "triage_failed_load", lambda *a: (_ for _ in ()).throw(RuntimeError("bedrock")))
    alerting.on_task_failure(context())
    assert rec.calls == ["email"] and rec.emails[0]["triage_note"] is None


def test_email_template_renders_the_note_escaped():
    from jinja2 import Template
    html = Template(alerting.HTML_CONTENT).render(
        ti=SimpleNamespace(dag_id="d", task_id="t", run_id="r", try_number=3, max_tries=2, log_url="u"),
        exception="E", load_id="l", triage_note="DIAGNOSIS: <b>x</b>", triage_status="answered")
    assert "DIAGNOSIS: &lt;b&gt;x&lt;/b&gt;" in html and "verify before acting" in html
    assert "Triage note" not in Template(alerting.HTML_CONTENT).render(
        ti=SimpleNamespace(dag_id="d", task_id="t", run_id="r", try_number=1, max_tries=0, log_url="u"),
        exception="E", load_id=None, triage_note=None)


def test_real_smtp_notifier_renders_the_triage_note():
    n = alerting.build_failure_notifier()
    ctx = dict(context(), load_id="load_claims-2026-10-02", triage_note="DIAGNOSIS: drop missing", triage_status="answered")
    ctx["ti"] = SimpleNamespace(dag_id="member_engagement_pipeline", task_id="load_claims", run_id="r1",
                                try_number=3, max_tries=2, log_url="http://log")
    n.render_template_fields(ctx)
    assert "DIAGNOSIS: drop missing" in n.html_content and "load_claims-2026-10-02" in n.html_content
    assert n.subject == "Airflow FAILED: member_engagement_pipeline.load_claims (run r1)"


def test_alert_recipient_is_read_at_send_time(monkeypatch):
    # Not frozen at DAG-parse time: a value set later (e.g. by .env loading) is used.
    monkeypatch.setenv("ALERT_EMAIL", "oncall@example.org")
    n = alerting.build_failure_notifier()
    assert n.to == "oncall@example.org" and n.from_email == "oncall@example.org"
    monkeypatch.setenv("ALERT_EMAIL", "")
    assert alerting.alert_recipient() == "alerts@example.com"
