import pytest

from fake_llm import FakeClient, parsed_response, refusal_response
from pipeline import config
from pipeline.ai import ask, llm


@pytest.mark.parametrize("sql", [
    "SELECT county, COUNT(*) FROM analytics.v_member_vulnerability GROUP BY 1",
    "select * from analytics.v_plan_monthly_kpis order by month;",
    "WITH t AS (SELECT county, vulnerability_score FROM analytics.v_member_vulnerability) "
    "SELECT county, AVG(vulnerability_score) FROM t GROUP BY 1",
    "SELECT v.county FROM analytics.v_members v JOIN analytics.v_member_vulnerability x ON x.county = v.county",
    "SELECT 'drop table core.claims' AS note FROM analytics.v_members",      # keyword inside a string is fine
])
def test_allowed_queries(sql):
    assert ask.validate_sql(sql)


@pytest.mark.parametrize("sql,reason", [
    ("DELETE FROM analytics.v_members", "only SELECT"),
    ("SELECT * FROM core.member_eligibility", "only analytics"),
    ("SELECT first_name FROM care.v_wellness_check_queue", "only analytics"),
    ("SELECT * FROM ops.load_audit", "only analytics"),
    ("SELECT * FROM svv_columns", "only analytics"),
    ("SELECT 1 FROM analytics.v_members; DROP TABLE core.claims", "multiple statements"),
    ("SELECT * FROM analytics.v_members -- ok\n; delete from core.claims", "multiple statements"),
    ("WITH x AS (DELETE FROM core.claims RETURNING *) SELECT * FROM x", "keyword not allowed"),
    ("SELECT * FROM member_eligibility", "table not allowed"),
    ("", "empty"),
])
def test_rejected_queries(sql, reason):
    with pytest.raises(ask.UnsafeSQL, match=reason):
        ask.validate_sql(sql)


@pytest.fixture
def fake(monkeypatch):
    def install(responder):
        monkeypatch.setattr(config, "LLM_PROVIDER", "anthropic")
        client = FakeClient(responder)
        monkeypatch.setattr(llm, "client", lambda: client)
        monkeypatch.setattr(ask, "schema_context", lambda: "analytics.v_members -- members\n  columns: county varchar")
        return client
    return install


def test_ask_runs_validated_sql(fake, monkeypatch):
    fake(lambda k, kw: parsed_response(ask.SqlAnswer(
        answerable=True, sql="SELECT county, COUNT(*) AS members FROM analytics.v_members GROUP BY 1;",
        explanation="Counts members per county.")))
    ran = {}
    monkeypatch.setattr(ask, "run", lambda sql: (ran.setdefault("sql", sql), (["county", "members"], [("Queens", 14)]))[1])
    out = ask.ask("How many members per county?")
    assert out["answered"] and out["rows"] == [("Queens", 14)]
    assert ran["sql"].startswith("SELECT county")


def test_model_sql_that_breaks_rules_never_runs(fake, monkeypatch):
    fake(lambda k, kw: parsed_response(ask.SqlAnswer(
        answerable=True, sql="SELECT first_name, phone FROM core.member_eligibility", explanation="...")))
    monkeypatch.setattr(ask, "get_connection", lambda: pytest.fail("must not touch the database"))
    with pytest.raises(ask.UnsafeSQL):
        ask.ask("Give me everyone's phone number")


def test_unanswerable_question(fake):
    fake(lambda k, kw: parsed_response(ask.SqlAnswer(
        answerable=False, sql="", explanation="The views have no names or phone numbers.")))
    out = ask.ask("What is the phone number of the highest-risk member?")
    assert not out["answered"] and "no names" in out["reason"]


def test_refusal(fake):
    fake(lambda k, kw: refusal_response())
    assert ask.ask("anything")["answered"] is False


def test_prompt_only_describes_analytics(fake):
    client = fake(lambda k, kw: parsed_response(ask.SqlAnswer(answerable=False, sql="", explanation="n/a")))
    ask.ask("q")
    sent = client.messages.calls[0][1]
    assert "core." not in sent["messages"][0]["content"] and "analytics.v_members" in sent["messages"][0]["content"]
    assert sent["output_format"] is ask.SqlAnswer


def test_every_analytics_view_is_documented():
    import re
    from pathlib import Path
    sql = "\n".join(p.read_text() for p in sorted((Path(__file__).parents[1] / "sql" / "redshift").glob("V*.sql")))
    views = {f"analytics.{v}" for v in re.findall(r"CREATE OR REPLACE VIEW analytics\.(\w+)", sql)}
    assert views == set(ask.VIEW_DOCS)


def test_schema_context_includes_exact_categorical_values(monkeypatch):
    class Cur:
        def execute(self, *a): pass
        def fetchall(self): return [("v_member_vulnerability", "vulnerability_tier", "character varying")]
        def __enter__(self): return self
        def __exit__(self, *a): return False

    class Conn:
        def cursor(self): return Cur()
        def close(self): pass
    monkeypatch.setattr(ask, "get_connection", lambda: Conn())
    ctx = ask.schema_context()
    assert "analytics.v_member_vulnerability" in ctx
    assert "vulnerability_tier: high, medium, low" in ctx          # model sees the real casing
