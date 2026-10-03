"""Ask the warehouse a question in plain English.

    python -m pipeline.ai.ask "Which county has the most high-vulnerability members?"

Claude turns the question into one Redshift SELECT over the de-identified
analytics.* views (structured output: sql + explanation), the SQL is checked
by validate_sql(), and only then does it run -- with a row cap, a statement
timeout, and inside a transaction that is rolled back.

Guardrails, in order:
  1. The model only sees the analytics.* schema (column catalog + view docs).
     Core/care/ops tables, which hold PHI and pipeline internals, are never
     described to it.
  2. validate_sql() rejects anything that isn't a single SELECT reading
     analytics.* (no DML/DDL, no other schemas, no system catalogs, no
     multiple statements) -- regardless of what the model returns.
  3. Execution: statement_timeout, fetchmany(row cap), rollback.
In production, step 3 would also run as the analyst_ro role (SELECT on
analytics only), so the database itself enforces the same boundary.
"""
import re
import sys

from pydantic import BaseModel, Field

from pipeline.ai import llm
from pipeline.redshift import get_connection

ROW_CAP = 200
TIMEOUT_MS = 30_000

# One line per view: what a row is and what it's for. Sent with the column list.
VIEW_DOCS = {
    "analytics.v_members": "one row per current member: plan, gender, age band, 3-digit ZIP, county, coverage dates",
    "analytics.v_member_plan_history": "SCD2 history of each member's plan (valid_from / valid_to)",
    "analytics.v_engagements": "one row per CHW activity (Salesforce): type, status, date, CHW name",
    "analytics.v_event_attendance": "one row per member per community event: event type, county, attended",
    "analytics.v_sdoh_needs": "one row per social need found in a CHW note: category, method (rules/llm), date, county",
    "analytics.v_member_engagement_monthly": "member x month: activities, completed, events attended, needs, is_engaged",
    "analytics.v_plan_monthly_kpis": "health plan x month: eligible, reached, engaged, engagement rate, attendance, needs",
    "analytics.v_ml_member_features": "one row per current member: features for engagement modelling",
    "analytics.v_zip_housing_conditions": "one row per NYC ZIP: open housing violations, class C, heat/hot water",
    "analytics.v_active_weather_alerts": "heat/cold alerts in effect now, one row per county per alert",
    "analytics.v_member_vulnerability": "one row per active member: vulnerability score, tier, per-factor points, reasons",
    "analytics.v_sdoh_method_agreement": "per need category: rules vs LLM tag agreement counts",
}

FORBIDDEN_KEYWORDS = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|grant|revoke|truncate|copy|unload|call|vacuum|"
    r"analyze|set|reset|begin|commit|rollback|execute|lock|cancel|prepare|deallocate)\b", re.I)
OTHER_SCHEMAS = re.compile(
    r"\b(core|care|staging|ops|public|pg_catalog|information_schema)\s*\.|\b(svv_|stl_|stv_|svl_|sys_|pg_)\w+", re.I)


class UnsafeSQL(Exception):
    pass


class SqlAnswer(BaseModel):
    answerable: bool = Field(description="False if the analytics views can't answer the question")
    sql: str = Field(description="One Amazon Redshift SELECT statement, or empty if not answerable")
    explanation: str = Field(description="One or two sentences on how the query answers the question")


SYSTEM_PROMPT = """You write SQL for analysts of a community-health program's Amazon Redshift \
warehouse. Answer each question with ONE SELECT statement that reads only the analytics views \
listed below, always schema-qualified (analytics.<view>). Use only columns that are listed. \
The views are de-identified: there are no names, phone numbers or member ids, so if a question \
asks for identities or for data the views don't contain, set answerable to false and leave sql \
empty. Prefer simple, readable SQL with clear column aliases, and order results sensibly."""


def _strip_literals_and_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)                 # line comments
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)    # block comments
    return re.sub(r"'(?:[^']|'')*'", "''", sql)         # string literals (so 'drop' in a value is fine)


def validate_sql(sql: str) -> str:
    """Return the cleaned SQL, or raise UnsafeSQL. Pure function -- unit tested."""
    cleaned = sql.strip().rstrip(";").strip()
    if not cleaned:
        raise UnsafeSQL("empty query")
    code = _strip_literals_and_comments(cleaned)
    if ";" in code:
        raise UnsafeSQL("multiple statements are not allowed")
    if not re.match(r"^\s*(select|with)\b", code, re.I):
        raise UnsafeSQL("only SELECT queries are allowed")
    if m := FORBIDDEN_KEYWORDS.search(code):
        raise UnsafeSQL(f"keyword not allowed: {m.group(0)}")
    if m := OTHER_SCHEMAS.search(code):
        raise UnsafeSQL(f"only analytics.* views may be queried (found {m.group(0)!r})")
    # Every FROM/JOIN target must be an analytics view or a CTE defined in the query.
    ctes = {c.lower() for c in re.findall(r"(?:\bwith|,)\s*([a-z_]\w*)\s+as\s*\(", code, re.I)}
    for target in re.findall(r"\b(?:from|join)\s+([a-z_][\w.]*)", code, re.I):
        t = target.lower()
        if not (t.startswith("analytics.") or t in ctes):
            raise UnsafeSQL(f"table not allowed: {target}")
    return cleaned


def schema_context() -> str:
    """Column catalog for analytics.* only, plus a one-line description per view."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name, column_name, data_type FROM svv_columns
                           WHERE table_schema = 'analytics' ORDER BY table_name, ordinal_position;""")
            rows = cur.fetchall()
    finally:
        conn.close()
    views: dict[str, list[str]] = {}
    for table, column, dtype in rows:
        views.setdefault(f"analytics.{table}", []).append(f"{column} {dtype}")
    return "\n".join(f"{v} -- {VIEW_DOCS.get(v, '')}\n  columns: {', '.join(cols)}" for v, cols in views.items())


def run(sql: str, row_cap: int = ROW_CAP) -> tuple[list[str], list[tuple]]:
    """Execute validated SQL read-only-style: timeout, row cap, and rollback."""
    sql = validate_sql(sql)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout TO {TIMEOUT_MS};")
            cur.execute(sql)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(row_cap)
        return columns, rows
    finally:
        conn.rollback()      # nothing an analyst question runs is ever kept
        conn.close()


def ask(question: str, usage: llm.Usage | None = None) -> dict:
    prompt = f"Analytics views:\n{schema_context()}\n\nQuestion: {question}"
    result = llm.parse(SYSTEM_PROMPT, prompt, SqlAnswer, usage=usage, max_tokens=4000)
    if result.parsed is None:
        return {"question": question, "answered": False, "reason": "the model declined this question"}
    answer = result.parsed
    if not answer.answerable or not answer.sql.strip():
        return {"question": question, "answered": False, "reason": answer.explanation}
    columns, rows = run(answer.sql)            # validate_sql() runs inside run(), before execution
    return {"question": question, "answered": True, "sql": validate_sql(answer.sql),
            "explanation": answer.explanation, "columns": columns, "rows": rows}


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print('usage: python -m pipeline.ai.ask "your question"')
        return 2
    if not llm.enabled():
        print("Set LLM_PROVIDER=anthropic or bedrock in .env to use the SQL assistant.")
        return 1
    try:
        out = ask(" ".join(argv[1:]))
    except UnsafeSQL as exc:
        print(f"Refused to run the generated SQL: {exc}")
        return 1
    if not out["answered"]:
        print(f"Can't answer that from the analytics views: {out['reason']}")
        return 0
    import pandas as pd
    print(f"SQL:\n{out['sql']}\n\n{out['explanation']}\n")
    print(pd.DataFrame(out["rows"], columns=out["columns"]).to_string(index=False))
    if len(out["rows"]) == ROW_CAP:
        print(f"(first {ROW_CAP} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
