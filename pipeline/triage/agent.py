"""Pipeline Triage Agent: when a DAG task fails (after retries) or a data quality
check fails, investigate with read-only tools and write a plain-English
diagnosis and suggested fix for a person to approve.

A plain tool-use loop on the Amazon Bedrock Runtime Converse API (boto3,
toolConfig) -- no Bedrock Agents, AgentCore, Knowledge Bases, Lambda or any
service outside this AWS account:

    user: "task T failed for batch D with error E"
    loop (at most TRIAGE_MAX_STEPS model calls, TRIAGE_MAX_TOKENS for the run):
        model -> text and/or toolUse blocks
        toolUse -> run the read-only tool (tools.py) -> toolResult back
        no toolUse -> that text is the diagnosis; stop

Guardrails:
  - tools are read-only, bound to the failed task and batch, and return counts
    and IDs only (never member PHI); the error text is redacted before it's sent
  - the note is a SUGGESTION: nothing here changes data or the DAG; a person
    reviews it (ops.load_audit.triage_note, the failure email) and applies any fix
  - LLM_PROVIDER=none skips the agent entirely (CI, tests, fresh clones)
  - triage_failed_load() never raises, so an agent problem can't hide or
    replace the pipeline's own failure
"""
import logging
import re
from dataclasses import dataclass, field

from pipeline import config, phi
from pipeline.triage.readonly import ReadOnlySession
from pipeline.triage.tools import TOOL_SPECS, Toolbox, TriageTarget

log = logging.getLogger(__name__)

MODELS = {
    "nova-lite": "us.amazon.nova-lite-v1:0",                           # default: cheap, does tool use well
    "claude-haiku": "us.anthropic.claude-haiku-4-5-20251001-v1:0",     # switch: stronger reasoning, ~15x the price
}
MAX_OUTPUT_TOKENS = 1500     # per model call
NOTE_MAX_CHARS = 8000        # ops.load_audit.triage_note is VARCHAR(8000)

SYSTEM_PROMPT = """You are the triage assistant for a healthcare member-engagement data pipeline on AWS.
Daily Airflow DAG: health-plan roster files (SCD2) and claims files from S3, Salesforce CHW activities
(incremental API pull), a community events API, a do-not-contact Google Sheet, HRA survey files, NYC housing
violations and NWS weather alerts. Each source lands raw in S3, is validated (bad rows go to rejects/ with a
reason), staged in Redshift and merged into core tables in one transaction, then data quality checks run.
A task or a data quality check just failed. Investigate with the tools, then explain what went wrong and
what a person should do.

Rules:
- Use the tools to gather evidence and quote the numbers you rely on. Never guess or invent data.
- The tools are read-only. You cannot change anything; your fix is a suggestion a person must approve.
- Never include member names, member ids, phone numbers or other personal data in your answer:
  counts, check names, load ids and file names only.
- Be brief: call only the tools you need, then answer.

Common causes:
- A health-plan or vendor file didn't arrive (FileNotFoundError, zero raw files for the date) or arrived
  twice or late; compare with the previous days' files and loads.
- Many rejects (load_reject_rate_pct fails): a partner changed its file layout or formats. Compare the
  reject reasons, the plans/files, and the source's previous loads to find which file changed and how.
- Rows in much lower than usual: an API pull stopped early (paging, expired token, rate limit 429, 5xx).
- Incremental pulls (Salesforce, events) use watermarks: get_watermarks shows if one stopped advancing,
  which means new records aren't landing even if each load "succeeds" with few or no rows.
- COPY / merge errors: the Parquet schema no longer matches the staging table (a column added or renamed).
- Redshift: usage limit reached, serializable isolation conflict (error 1023, concurrent writers),
  connection or statement timeouts.
- Data quality: a check failing today but passing on previous days points to today's load; a value that
  has been the same for days points to a long-running upstream issue. scd2_single_current_row failing can
  mean roster files were loaded out of date order. dnc_list_present = 0 means the do-not-contact sheet
  came back empty, which is never loaded on purpose.

Answer in plain text (no Markdown) with exactly these sections:
DIAGNOSIS: one or two sentences.
EVIDENCE: short lines starting with "- ", each citing a tool result.
SUGGESTED FIX (needs human approval): concrete steps for a person.
CONFIDENCE: high, medium or low."""

# Some models (Nova) wrap their reasoning in <thinking> tags in the text; the note keeps only the answer.
THINKING = re.compile(r"<thinking>.*?</thinking>", re.DOTALL | re.IGNORECASE)

FINAL_STEP_NUDGE = ("You have reached the step limit. Do not call any more tools: write your answer now, "
                    "in the required sections, from the evidence you already have.")


@dataclass
class TriageResult:
    status: str                    # answered | step_cap | token_cap | skipped | error
    note: str | None = None
    model: str | None = None
    tokens: int = 0
    steps: int = 0
    tool_calls: list[str] = field(default_factory=list)
    redshift_queries: int = 0


def enabled() -> bool:
    return config.LLM_PROVIDER != "none"


def model_id() -> str:
    """nova-lite / claude-haiku aliases, or any full Bedrock model id / inference profile."""
    return MODELS.get(config.TRIAGE_MODEL, config.TRIAGE_MODEL)


def bedrock_client():
    # Imported here so the pipeline, CI and LLM_PROVIDER=none never build a Bedrock client.
    import boto3
    from botocore.config import Config
    return boto3.client("bedrock-runtime", region_name=config.AWS_REGION,
                        config=Config(retries={"max_attempts": 3, "mode": "standard"}, read_timeout=60))


def _tool_result_block(tool_use_id: str, text: str, ok: bool, model: str) -> dict:
    block = {"toolUseId": tool_use_id, "content": [{"text": text}]}
    if "anthropic" in model:             # Claude models on Converse accept the status field
        block["status"] = "success" if ok else "error"
    elif not ok:
        block["content"] = [{"text": "ERROR: " + text}]
    return {"toolResult": block}


def triage(load_id: str, task_id: str, batch_date: str, error: str, client=None) -> TriageResult:
    """Investigate one failure. Raises only on Bedrock/AWS errors (see triage_failed_load)."""
    if not enabled():
        return TriageResult(status="skipped")

    model = model_id()
    session = ReadOnlySession()
    tools = Toolbox(TriageTarget(load_id=load_id, task_id=task_id, batch_date=batch_date), session)
    bedrock = client or bedrock_client()
    result = TriageResult(status="error", model=model)
    messages = [{"role": "user", "content": [{"text":
        f"Task '{task_id}' of batch {batch_date} failed (audit load_id {load_id}).\n"
        f"Error: {phi.redact(error, keep_dates=True)[:1500]}\nInvestigate and diagnose it."}]}]
    last_text = ""
    try:
        for step in range(1, config.TRIAGE_MAX_STEPS + 1):
            if result.tokens >= config.TRIAGE_MAX_TOKENS:
                result.status = "token_cap"
                break
            if step == config.TRIAGE_MAX_STEPS:
                messages[-1]["content"].append({"text": FINAL_STEP_NUDGE})

            response = bedrock.converse(
                modelId=model,
                system=[{"text": SYSTEM_PROMPT}],
                messages=messages,
                toolConfig={"tools": [{"toolSpec": spec} for spec in TOOL_SPECS]},
                inferenceConfig={"maxTokens": MAX_OUTPUT_TOKENS, "temperature": 0},
            )
            result.steps = step
            usage = response.get("usage", {})
            result.tokens += usage.get("inputTokens", 0) + usage.get("outputTokens", 0)

            message = response["output"]["message"]
            messages.append(message)
            texts = [THINKING.sub("", c["text"]) for c in message["content"] if "text" in c]
            if any(t.strip() for t in texts):
                last_text = "\n".join(t.strip() for t in texts if t.strip())
            tool_uses = [c["toolUse"] for c in message["content"] if "toolUse" in c]

            if response.get("stopReason") != "tool_use" or not tool_uses:
                result.status, result.note = "answered", last_text
                break
            if step == config.TRIAGE_MAX_STEPS:
                result.status = "step_cap"       # it still wanted tools on the last allowed call
                break

            blocks = []
            for tu in tool_uses:
                text, ok = tools.run(tu["name"], tu.get("input"))
                log.info("triage step %d: %s(%s) -> %s, %d chars", step, tu["name"], tu.get("input") or "",
                         "ok" if ok else "error", len(text))
                blocks.append(_tool_result_block(tu["toolUseId"], text, ok, model))
            messages.append({"role": "user", "content": blocks})
    finally:
        session.close()
        result.tool_calls = tools.calls
        result.redshift_queries = session.queries_run

    if result.status in ("step_cap", "token_cap"):
        cap = (f"{config.TRIAGE_MAX_STEPS} steps" if result.status == "step_cap"
               else f"{config.TRIAGE_MAX_TOKENS} tokens")
        result.note = (f"Triage stopped at its {cap} limit before a final diagnosis. Partial findings: "
                       f"{last_text or 'none'}")
    if result.note:
        # Belt and braces: the answer is built from counts and IDs, but redact it
        # anyway before it's stored or emailed.
        result.note = (f"{phi.redact(result.note, keep_dates=True)}\n\n[Triage agent, {result.status}: "
                       f"{result.steps} steps, {result.tokens} tokens, "
                       f"tools {', '.join(result.tool_calls) or 'none'}. "
                       f"Suggested fixes need human approval.]")[:NOTE_MAX_CHARS]
    return result


def record(load_id: str, result: TriageResult) -> None:
    """Write the note to the newest failed audit row for the load, as the ETL
    user (the triage user can't write)."""
    from pipeline.redshift import get_connection
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE ops.load_audit
                   SET triage_note = %s, triage_model = %s, triage_tokens = %s
                   WHERE load_id = %s AND status = 'failed'
                     AND started_at = (SELECT MAX(started_at) FROM ops.load_audit
                                       WHERE load_id = %s AND status = 'failed');""",
                (result.note, result.model, result.tokens, load_id, load_id),
            )
        conn.commit()
    finally:
        conn.close()


def triage_failed_load(load_id: str, task_id: str, batch_date: str, error: str,
                       client=None) -> TriageResult:
    """The failure-path entry point (DAG callback, CLI). NEVER raises: an agent,
    Bedrock or Redshift problem is logged and returned as status=error, so the
    task's own failure is what Airflow and the audit row keep showing."""
    try:
        result = triage(load_id, task_id, batch_date, error, client=client)
    except Exception as e:
        log.warning("triage of %s skipped (%s: %s); the original failure is unaffected",
                    load_id, type(e).__name__, e)
        return TriageResult(status="error", model=model_id())
    if result.status == "skipped":
        log.info("triage of %s skipped: LLM_PROVIDER=none", load_id)
        return result
    try:
        if result.note:
            record(load_id, result)
    except Exception:
        log.exception("could not save the triage note for %s", load_id)
    log.info("triage of %s: %s (%d steps, %d tokens, %d Redshift queries)", load_id, result.status,
             result.steps, result.tokens, result.redshift_queries)
    return result
