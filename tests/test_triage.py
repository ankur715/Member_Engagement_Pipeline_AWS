"""Pipeline Triage Agent: the Converse tool loop, caps, skip without an LLM, a
data quality failure, PHI safety and failure isolation -- with a mocked Bedrock
client and a fake read-only session (no AWS, no Redshift, no model)."""
import json
import re

import boto3
import pytest
from botocore.exceptions import ClientError

from pipeline import config, loaders
from pipeline.triage import agent, readonly, tools

LOAD_ID = "member_file-evergreen-2026-10-02"
BATCH = "2026-10-02"


class FakeBedrock:
    """Returns scripted Converse responses in order and records every request."""

    def __init__(self, responses):
        self.responses, self.requests = list(responses), []

    def converse(self, **kwargs):
        self.requests.append(json.loads(json.dumps(kwargs)))   # snapshot: the agent mutates messages later
        return self.responses.pop(0)


def tool_call(name, tool_input=None, tokens=(1000, 50), tool_use_id=None):
    return {"stopReason": "tool_use",
            "usage": {"inputTokens": tokens[0], "outputTokens": tokens[1]},
            "output": {"message": {"role": "assistant", "content": [
                {"text": "<thinking>checking</thinking>"},
                {"toolUse": {"toolUseId": tool_use_id or f"tu-{name}", "name": name, "input": tool_input or {}}}]}}}


def answer(text, tokens=(1500, 200)):
    return {"stopReason": "end_turn",
            "usage": {"inputTokens": tokens[0], "outputTokens": tokens[1]},
            "output": {"message": {"role": "assistant", "content": [{"text": text}]}}}


DIAGNOSIS = ("DIAGNOSIS: Evergreen's roster file did not arrive for 2026-10-02.\n"
             "EVIDENCE:\n- get_file_history: 0 raw files on 2026-10-02, 1 on each prior day\n"
             "SUGGESTED FIX (needs human approval): ask Evergreen to resend, then clear load_member_files.\n"
             "CONFIDENCE: high")


class FakeSession:
    """Stands in for ReadOnlySession: canned rows per named query, counts queries."""

    data = {
        "batch_loads": [
            {"load_id": LOAD_ID, "entity": "load_member_files", "status": "failed", "rows_in": None,
             "rows_staged": None, "rows_rejected": None, "started_at": "2026-10-02T06:01:00",
             "finished_at": "2026-10-02T06:01:00",
             "details": "FileNotFoundError: no roster for MEM10042, call (718) 555-0142 on 2026-10-02"},
            {"load_id": "member_file-harbor-2026-10-02", "entity": "member_file_harbor", "status": "succeeded",
             "rows_in": 31, "rows_staged": 30, "rows_rejected": 1, "started_at": "2026-10-02T06:00:30",
             "finished_at": "2026-10-02T06:00:40", "details": None},
        ],
        "dq_results": [
            {"batch_date": BATCH, "check_name": "member_files_loaded", "severity": "error", "passed": False,
             "observed_value": "1.0"},
            {"batch_date": BATCH, "check_name": "dnc_list_present", "severity": "error", "passed": True,
             "observed_value": "9.0"},
            {"batch_date": "2026-10-01", "check_name": "member_files_loaded", "severity": "error", "passed": True,
             "observed_value": "2.0"},
        ],
        "staging_counts": [{"staging_table": "member_eligibility", "load_id": "member_file-harbor-2026-10-02",
                            "row_count": 30}],
        "entity_history": [{"load_id": "member_file-evergreen-2026-10-01", "entity": "member_file_evergreen",
                            "status": "succeeded", "rows_in": 30, "rows_staged": 30, "rows_rejected": 0,
                            "started_at": "2026-10-01T06:00:00"}],
    }

    instances = []

    def __init__(self):
        self.queries_run, self.closed, self.names = 0, False, []
        FakeSession.instances.append(self)

    def query(self, name, **params):
        assert name in readonly.QUERIES
        if name not in self.names:
            self.queries_run += 1
        self.names.append(name)
        return self.data[name]

    def close(self):
        self.closed = True


@pytest.fixture
def llm_on(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "bedrock")
    monkeypatch.setattr(agent, "ReadOnlySession", FakeSession)
    FakeSession.instances.clear()
    monkeypatch.setattr(agent, "bedrock_client", lambda: pytest.fail("tests must inject a fake client"))


# --- skip, models ---------------------------------------------------------------

def test_no_llm_skips_without_touching_bedrock_or_redshift(monkeypatch):
    monkeypatch.setattr(config, "LLM_PROVIDER", "none")
    monkeypatch.setattr(agent, "bedrock_client", lambda: pytest.fail("no Bedrock client when LLM_PROVIDER=none"))
    monkeypatch.setattr(agent, "ReadOnlySession", lambda: pytest.fail("no Redshift when LLM_PROVIDER=none"))
    result = agent.triage_failed_load(LOAD_ID, "load_member_files", BATCH, "boom")
    assert result.status == "skipped" and result.note is None


def test_model_switch(monkeypatch):
    assert agent.model_id() == "us.amazon.nova-lite-v1:0"            # default
    monkeypatch.setattr(config, "TRIAGE_MODEL", "claude-haiku")
    assert agent.model_id() == "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    monkeypatch.setattr(config, "TRIAGE_MODEL", "us.amazon.nova-pro-v1:0")
    assert agent.model_id() == "us.amazon.nova-pro-v1:0"             # any full id passes through


# --- the tool loop ---------------------------------------------------------------

def test_tool_loop_runs_tools_and_returns_the_answer(llm_on, s3_bucket):
    fake = FakeBedrock([tool_call("get_load_audit"), tool_call("get_file_history", {"days": 2}), answer(DIAGNOSIS)])
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "FileNotFoundError: no roster", client=fake)

    assert result.status == "answered" and result.steps == 3
    assert result.tool_calls == ["get_load_audit", "get_file_history"]
    assert result.tokens == (1000 + 50) * 2 + 1500 + 200
    assert result.redshift_queries == 2                    # batch_loads + entity_history
    assert FakeSession.instances[0].closed
    assert result.note.startswith("DIAGNOSIS:") and "Suggested fixes need human approval" in result.note

    first = fake.requests[0]
    assert first["modelId"] == "us.amazon.nova-lite-v1:0"
    assert {t["toolSpec"]["name"] for t in first["toolConfig"]["tools"]} == set(tools.TOOL_NAMES)
    assert first["inferenceConfig"]["maxTokens"] == agent.MAX_OUTPUT_TOKENS
    # Each toolUse goes back as a toolResult with the same id, in the next user message.
    result_block = fake.requests[1]["messages"][-1]["content"][0]["toolResult"]
    assert result_block["toolUseId"] == "tu-get_load_audit"
    assert "status" not in result_block                    # Nova: no status field


def test_claude_tool_results_carry_a_status(llm_on, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MODEL", "claude-haiku")
    fake = FakeBedrock([tool_call("get_pipeline_config"), answer(DIAGNOSIS)])
    agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert fake.requests[1]["messages"][-1]["content"][0]["toolResult"]["status"] == "success"


def test_step_cap_stops_the_loop_and_keeps_partial_findings(llm_on, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MAX_STEPS", 3)
    fake = FakeBedrock([tool_call("get_load_audit")] * 3)
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert result.status == "step_cap" and result.steps == 3 and len(fake.requests) == 3
    assert agent.FINAL_STEP_NUDGE in json.dumps(fake.requests[-1]["messages"][-1])   # told to answer now
    assert "3 steps limit" in result.note


def test_default_step_cap_is_eight(llm_on):
    fake = FakeBedrock([tool_call("get_load_audit")] * 20)
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert result.status == "step_cap" and len(fake.requests) == 8


def test_token_cap_stops_the_loop(llm_on, monkeypatch):
    monkeypatch.setattr(config, "TRIAGE_MAX_TOKENS", 5000)
    fake = FakeBedrock([tool_call("get_load_audit", tokens=(4000, 1500)), tool_call("get_dq_results")])
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert result.status == "token_cap" and len(fake.requests) == 1
    assert "5000 tokens limit" in result.note


def test_thinking_tags_are_stripped(llm_on):
    fake = FakeBedrock([answer("<thinking>hmm</thinking>" + DIAGNOSIS)])
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert "<thinking>" not in result.note and result.note.startswith("DIAGNOSIS:")


# --- a data quality failure ------------------------------------------------------

def test_dq_failure_scenario(llm_on):
    note = ("DIAGNOSIS: member_files_loaded = 1 vs 2 expected; one plan's roster is missing.\n"
            "EVIDENCE:\n- get_dq_results: member_files_loaded failed today, 2.0 on 2026-10-01\n"
            "SUGGESTED FIX (needs human approval): check the missing plan's drop.\nCONFIDENCE: medium")
    fake = FakeBedrock([tool_call("get_dq_results"), answer(note)])
    result = agent.triage("data_quality-2026-10-02", "data_quality", BATCH,
                          "DataQualityError: Batch 2026-10-02 failed 1 data quality check(s)", client=fake)
    assert result.status == "answered" and result.tool_calls == ["get_dq_results"]

    sent = json.loads(fake.requests[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["text"])
    assert sent["checks_run"] == 2
    assert [f["check_name"] for f in sent["failed"]] == ["member_files_loaded"]
    assert sent["failed"][0]["description"]                                 # check description attached
    assert sent["previous_7_days_of_failed_checks"] == {"member_files_loaded": [["2026-10-01", "2.0"]]}


# --- PHI safety ------------------------------------------------------------------

def test_error_text_and_audit_details_are_redacted_but_dates_kept(llm_on):
    fake = FakeBedrock([tool_call("get_load_audit"), answer(DIAGNOSIS)])
    agent.triage(LOAD_ID, "load_member_files", BATCH, "ValueError: bad row for MEM10042 phone 718-555-0142",
                 client=fake)
    everything_sent = json.dumps(fake.requests)
    for secret in ("MEM10042", "555-0142"):
        assert secret not in everything_sent
    assert BATCH in everything_sent and LOAD_ID in everything_sent      # batch dates and load ids survive


def test_rejects_tool_returns_reason_counts_never_rows(s3_bucket):
    rows = ("member_id,first_name,phone,dob,reject_reason\n"
            "MEM10042,Maria,7185550142,1948-03-14,invalid_dob;\n"
            ",John,7185550199,1950-01-01,missing_member_id;invalid_phone;\n")
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key=f"rejects/member_files/dt={BATCH}/evergreen_rejects.csv", Body=rows.encode())
    box = tools.Toolbox(tools.TriageTarget(LOAD_ID, "load_member_files", BATCH), FakeSession())
    text, ok = box.run("get_rejects", {})
    assert ok
    assert json.loads(text)["reject_files"] == {"evergreen_rejects.csv": {
        "invalid_dob": 1, "missing_member_id": 1, "invalid_phone": 1}}
    for secret in ("MEM10042", "Maria", "John", "7185550142", "1948-03-14"):
        assert secret not in text


def test_file_history_lists_raw_files_by_day(s3_bucket):
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=s3_bucket, Key="raw/member_files/dt=2026-10-01/evergreen_roster_20261001.csv", Body=b"x")
    box = tools.Toolbox(tools.TriageTarget(LOAD_ID, "load_member_files", BATCH), FakeSession())
    out = json.loads(box.run("get_file_history", {"days": 2})[0])
    assert out["raw_files_by_date"] == {"2026-10-02": {"files": 0, "names": []},
                                        "2026-10-01": {"files": 1, "names": ["evergreen_roster_20261001.csv"]}}


# --- tools and the read-only session -----------------------------------------------

def test_unknown_tool_and_bad_arguments_go_back_as_errors():
    box = tools.Toolbox(tools.TriageTarget(LOAD_ID, "load_member_files", BATCH), FakeSession())
    text, ok = box.run("drop_table", {})
    assert not ok and "unknown tool" in text
    text, ok = box.run("get_load_audit", {"load_id": "someone-elses-load"})   # can't retarget a tool
    assert not ok and "bad arguments" in text
    text, ok = box.run("get_rejects", {"source": "core"})                    # only known S3 sources
    assert not ok and "pass a source" in text


def test_named_queries_are_single_selects_on_the_granted_tables_only():
    for name, sql in readonly.QUERIES.items():
        body = sql.strip().rstrip(";")
        assert ";" not in body, name
        assert re.match(r"^\s*(SELECT|WITH)\b", body, re.I), name
        assert not re.search(r"\b(insert|update|delete|merge|create|drop|alter|grant|copy|unload|call)\b", body, re.I)
        referenced = set(re.findall(r"\b(?:from|join)\s+([a-z_]+\.[a-z_]+)", body, re.I))
        assert referenced and referenced <= set(readonly.TABLES), (name, referenced)


def test_session_runs_only_named_queries_once_each(monkeypatch):
    from tests.conftest import FakeConnection

    class Conn(FakeConnection):
        def cursor(self):
            cur = super().cursor()
            cur.description, cur.fetchall = [("load_id",)], lambda: [("x",)]
            return cur

    conns = []
    monkeypatch.setattr(config, "TRIAGE_REDSHIFT_PASSWORD", "Secret123")
    monkeypatch.setattr(readonly, "get_connection",
                        lambda user, password: conns.append((user, Conn())) or conns[-1][1])
    s = readonly.ReadOnlySession()
    assert s.query("entity_history", entity_prefix="claims_", limit=10) == [{"load_id": "x"}]
    s.query("entity_history", entity_prefix="claims_", limit=10)              # cached
    assert s.queries_run == 1 and len(conns) == 1 and conns[0][0] == "triage_reader"
    assert conns[0][1].rolled_back                                              # never leaves a transaction open
    with pytest.raises(KeyError):
        s.query("DELETE FROM ops.load_audit")


def test_session_refuses_without_the_reader_password():
    with pytest.raises(RuntimeError, match="TRIAGE_REDSHIFT_PASSWORD"):
        readonly.ReadOnlySession().query("dq_results", batch_date=BATCH)


# --- failure isolation -------------------------------------------------------------

def _client_error():
    return ClientError({"Error": {"Code": "AccessDeniedException", "Message": "no model access"}}, "Converse")


class RaisingBedrock:
    def converse(self, **kwargs):
        raise _client_error()


def test_bedrock_error_never_raises(llm_on):
    result = agent.triage_failed_load(LOAD_ID, "load_member_files", BATCH, "boom", client=RaisingBedrock())
    assert result.status == "error" and result.note is None
    assert FakeSession.instances[0].closed                 # the Redshift connection is still closed


def test_tool_errors_go_back_to_the_model_and_the_loop_continues(llm_on, monkeypatch):
    def broken(self, name, **params):
        raise RuntimeError("Redshift unavailable")
    monkeypatch.setattr(FakeSession, "query", broken)
    fake = FakeBedrock([tool_call("get_load_audit"), answer(DIAGNOSIS)])
    result = agent.triage(LOAD_ID, "load_member_files", BATCH, "boom", client=fake)
    assert result.status == "answered"
    assert "ERROR:" in fake.requests[1]["messages"][-1]["content"][0]["toolResult"]["content"][0]["text"]


def test_saving_the_note_failing_still_returns_the_result(llm_on, monkeypatch):
    monkeypatch.setattr(agent, "record", lambda *a: (_ for _ in ()).throw(RuntimeError("db down")))
    result = agent.triage_failed_load(LOAD_ID, "load_member_files", BATCH, "boom", client=FakeBedrock([answer(DIAGNOSIS)]))
    assert result.status == "answered" and result.note


def test_note_is_saved_on_the_newest_failed_row(llm_on, monkeypatch):
    from tests.conftest import FakeConnection
    conn = FakeConnection()
    monkeypatch.setattr("pipeline.redshift.get_connection", lambda: conn)
    agent.triage_failed_load(LOAD_ID, "load_member_files", BATCH, "boom", client=FakeBedrock([answer(DIAGNOSIS)]))
    sql, params = conn.log[0]
    assert sql.startswith("UPDATE ops.load_audit SET triage_note") and "MAX(started_at)" in sql
    assert params[1] == "us.amazon.nova-lite-v1:0" and params[2] == 1700 and params[3] == LOAD_ID
    assert conn.committed


# --- marking the failed load (before triage) ----------------------------------------

class AuditConnection:
    def __init__(self, existing=None):
        self.existing, self.log, self.committed = existing, [], False

    def cursor(self):
        conn = self

        class Cur:
            def execute(self, sql, params=None):
                conn.log.append((" ".join(sql.split()), params))

            def fetchone(self):
                return (conn.existing,) if conn.existing else None

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        return Cur()

    def commit(self):
        self.committed = True

    def close(self):
        pass


def test_mark_task_failed_reuses_the_row_the_load_wrote(monkeypatch):
    conn = AuditConnection(existing=LOAD_ID)
    monkeypatch.setattr(loaders, "get_connection", lambda: conn)
    assert loaders.mark_task_failed("load_member_files", "member_file_", BATCH, "2026-10-02 06:00:00", "boom") == LOAD_ID
    assert len(conn.log) == 1 and not conn.committed          # nothing written


def test_mark_task_failed_adds_a_row_when_nothing_was_loaded(monkeypatch):
    conn = AuditConnection()
    monkeypatch.setattr(loaders, "get_connection", lambda: conn)
    load_id = loaders.mark_task_failed("data_quality", "data_quality", BATCH, "2026-10-02 06:00:00", "DQ failed")
    assert load_id == "data_quality-2026-10-02"
    sql, params = conn.log[-1]
    assert sql.startswith("INSERT INTO ops.load_audit") and "'failed'" in sql
    assert params[0] == load_id and params[1] == "data_quality" and conn.committed


def test_load_audit_keeps_the_latest_row_per_load_and_drops_other_loads_details(monkeypatch):
    rows = [dict(FakeSession.data["batch_loads"][0]),
            {"load_id": "claims-x-2026-10-02", "entity": "claims_x", "status": "failed", "rows_in": 9,
             "rows_staged": 0, "rows_rejected": 0, "started_at": "t1", "finished_at": "t1", "details": "old error"},
            {"load_id": "claims-x-2026-10-02", "entity": "claims_x", "status": "succeeded", "rows_in": 954,
             "rows_staged": 330, "rows_rejected": 610, "started_at": "t2", "finished_at": "t2", "details": None}]
    monkeypatch.setitem(FakeSession.data, "batch_loads", rows)
    out = json.loads(tools.Toolbox(tools.TriageTarget(LOAD_ID, "load_member_files", BATCH), FakeSession())
                     .run("get_load_audit", {})[0])
    assert out["failed_load"]["details"]                                   # the failed load keeps its error
    assert out["batch_loads"] == [{"load_id": "claims-x-2026-10-02", "entity": "claims_x", "status": "succeeded",
                                   "rows_in": 954, "rows_staged": 330, "rows_rejected": 610, "started_at": "t2",
                                   "attempts": 2}]
