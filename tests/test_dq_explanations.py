import pytest

from fake_llm import FakeClient, text_response
from pipeline import config
from pipeline.ai import llm
from pipeline.quality import data_quality as dq

FAILED = [{"check_name": "claim_files_loaded", "severity": "error", "passed": False, "observed_value": 1.0,
           "description": "Health-plan claims files loaded for this date"},
          {"check_name": "calls_after_opt_out", "severity": "warn", "passed": False, "observed_value": 36.0,
           "description": "Completed phone outreach dated after the member opted out"}]
HISTORY = {"claim_files_loaded": [("2026-10-01", "2.0"), ("2026-09-30", "2.0")]}
AUDIT = [{"entity": "claims_evergreen", "status": "succeeded", "rows_in": 5, "rows_staged": 4, "rows_rejected": 0}]


@pytest.fixture
def fake(monkeypatch):
    def install(responder):
        monkeypatch.setattr(config, "LLM_PROVIDER", "anthropic")
        client = FakeClient(responder)
        monkeypatch.setattr(llm, "client", lambda: client)
        return client
    return install


def test_no_explanation_when_disabled():
    assert dq.explain_failures("2026-10-02", FAILED, HISTORY, AUDIT) is None


def test_no_call_when_nothing_failed(fake):
    client = fake(lambda k, kw: text_response("x"))
    assert dq.explain_failures("2026-10-02", [], {}, []) is None and client.messages.calls == []


def test_prompt_has_only_aggregates_and_returns_note(fake):
    client = fake(lambda k, kw: text_response("- claims: harbor file missing; check the SFTP drop."))
    note = dq.explain_failures("2026-10-02", FAILED, HISTORY, AUDIT)
    assert note.startswith("- claims")
    prompt = client.messages.calls[0][1]["messages"][0]["content"]
    assert "claim_files_loaded [error] observed=1.0" in prompt
    assert "previous days: 2026-10-01: 2.0, 2026-09-30: 2.0" in prompt
    assert "claims_evergreen, succeeded, 5, 4, 0" in prompt
    assert "MEM" not in prompt                               # no member-level data


def test_llm_failure_degrades_to_none(fake):
    def boom(k, kw):
        raise llm.LLMUnavailable("down")
    fake(boom)
    assert dq.explain_failures("2026-10-02", FAILED, HISTORY, AUDIT) is None


def test_missing_sdk_or_any_error_never_breaks_the_checks(monkeypatch):
    # Found live: Airflow's venv lacked the anthropic package, and the ImportError
    # crashed the data_quality task. A missing SDK is now LLMUnavailable...
    import sys
    monkeypatch.setattr(config, "LLM_PROVIDER", "bedrock")
    monkeypatch.setitem(sys.modules, "anthropic", None)          # makes `import anthropic` fail
    with pytest.raises(llm.LLMUnavailable, match="isn't installed"):
        llm.text("s", "u")
    failed = [{"check_name": "x", "severity": "error", "observed_value": 1.0, "description": "d"}]
    assert dq.explain_failures("2026-10-07", failed, {}, []) is None
    # ...and any other error in the optional explanation is swallowed too.
    monkeypatch.setattr(llm, "text", lambda *a, **k: (_ for _ in ()).throw(KeyError("bug")))
    assert dq.explain_failures("2026-10-07", failed, {}, []) is None
