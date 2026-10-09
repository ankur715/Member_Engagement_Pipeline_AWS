"""The triage agent's tools: a fixed set of read-only Python functions bound to
ONE failed task and batch date. The model chooses which tools to call; it can't
choose another batch, a table or a query, and nothing here writes anything.

    tool                  reads                                               Redshift queries
    get_load_audit        ops.load_audit: the failed load + the batch's loads  1
    get_dq_results        ops.dq_results: the batch + 7-day history of fails   1
    get_staging_counts    ops.v_triage_staging_counts (counts per load)         1
    get_rejects           S3 rejects/<source>/dt=<date>/: counts by reason      0
    get_file_history      ops.load_audit history + S3 raw/ file listing         1
    get_pipeline_config   the local task/source registry below                  0

A whole investigation is at most 4 small queries on one connection. Results
are COUNTS and IDs only: reject files are read for their reject_reason column
alone, error text is run through phi.redact() (keeping batch dates), and no
tool returns a row of member data.
"""
import csv
import io
import json
from dataclasses import dataclass
from datetime import date, timedelta

from pipeline import phi, s3_io
from pipeline.reference_data import HEALTH_PLANS
from pipeline.triage.readonly import ReadOnlySession

MAX_RESULT_CHARS = 6000   # each tool result is truncated to this before it goes to the model

# S3 folders under raw/ and rejects/ (one per source).
SOURCES = ("member_files", "salesforce_activities", "events_api", "contact_preferences",
           "claims", "hra", "housing_violations", "weather_alerts")

_COMMON = {"dag_id": "member_engagement_pipeline", "schedule": "0 6 * * * (daily)",
           "retries": 2, "redshift_pool_slots": 1}

# The pipeline config the agent can read: what each DAG task does and where its data lives.
TASKS = {
    "apply_migrations": {"kind": "applies versioned SQL migrations (sql/redshift/V*.sql)", "source": None,
                         "entity_prefix": None, "staging_tables": [], "merge_procedure": None},
    "drop_member_files": {"kind": "simulated health-plan SFTP drop: writes one roster CSV per plan to S3 raw/",
                          "source": "member_files", "entity_prefix": None, "staging_tables": [], "merge_procedure": None},
    "load_member_files": {"kind": "health-plan roster files, one per plan per day, SCD2 history (loads must run in date order)",
                          "source": "member_files", "entity_prefix": "member_file_", "expected_loads": len(HEALTH_PLANS),
                          "staging_tables": ["member_eligibility"], "merge_procedure": "core.sp_merge_member_eligibility"},
    "ingest_salesforce_activities": {"kind": "Salesforce API pull, incremental on a LastModifiedDate watermark",
                                     "source": "salesforce_activities", "entity_prefix": "salesforce_activities",
                                     "staging_tables": ["engagements"], "merge_procedure": "core.sp_merge_engagements"},
    "ingest_events": {"kind": "community events REST API (events with nested attendees)", "source": "events_api",
                      "entity_prefix": "events_api", "staging_tables": ["events", "event_attendance"],
                      "merge_procedure": "core.sp_merge_events"},
    "ingest_contact_preferences": {"kind": "do-not-contact Google Sheet, full snapshot (an empty sheet fails the load)",
                                   "source": "contact_preferences", "entity_prefix": "contact_preferences",
                                   "staging_tables": ["contact_preferences"],
                                   "merge_procedure": "core.sp_merge_contact_preferences"},
    "tag_sdoh_needs": {"kind": "rule-based tagging of CHW notes already in core.engagements", "source": None,
                       "entity_prefix": "sdoh_rules", "staging_tables": ["member_sdoh_needs", "note_classifications"],
                       "merge_procedure": "core.sp_merge_note_classifications"},
    "classify_notes_llm": {"kind": "optional LLM tagging of CHW notes (skips when the LLM is off)", "source": None,
                           "entity_prefix": "sdoh_llm", "staging_tables": ["member_sdoh_needs", "note_classifications"],
                           "merge_procedure": "core.sp_merge_note_classifications"},
    "drop_claims_files": {"kind": "simulated drop of each plan's claims CSV to S3 raw/", "source": "claims",
                          "entity_prefix": None, "staging_tables": [], "merge_procedure": None},
    "load_claims": {"kind": "health-plan claims files, one per plan (latest version per claim wins)", "source": "claims",
                    "entity_prefix": "claims_", "expected_loads": len(HEALTH_PLANS), "staging_tables": ["claims"],
                    "merge_procedure": "core.sp_merge_claims"},
    "drop_hra_file": {"kind": "simulated survey-vendor drop of the HRA JSON Lines file", "source": "hra",
                      "entity_prefix": None, "staging_tables": [], "merge_procedure": None},
    "load_hra": {"kind": "HRA survey responses from the survey vendor (latest version per response)", "source": "hra",
                 "entity_prefix": "hra_responses", "staging_tables": ["hra_responses"],
                 "merge_procedure": "core.sp_merge_hra_responses"},
    "ingest_housing_violations": {"kind": "NYC Open Data housing violations, paged API, snapshot replace",
                                  "source": "housing_violations", "entity_prefix": "housing_violations",
                                  "staging_tables": ["housing_violations"],
                                  "merge_procedure": "core.sp_merge_housing_violations"},
    "ingest_weather_alerts": {"kind": "NWS active weather alerts API (zero alerts is normal)", "source": "weather_alerts",
                              "entity_prefix": "weather_alerts", "staging_tables": ["weather_alerts", "weather_alert_counties"],
                              "merge_procedure": "core.sp_merge_weather_alerts"},
    "data_quality": {"kind": "post-load data quality checks; failed error-level checks stop the run",
                     "source": None, "entity_prefix": "data_quality", "staging_tables": [], "merge_procedure": None},
    "publish_plan_kpis": {"kind": "publishes per-plan KPIs to Google Sheets and a CSV export (runs after data quality)",
                          "source": None, "entity_prefix": None, "staging_tables": [], "merge_procedure": None},
}

_SOURCE_PARAM = {"source": {"type": "string", "enum": list(SOURCES),
                            "description": "Which source's S3 folder to read. Defaults to the failed task's source."}}

TOOL_SPECS = [
    {"name": "get_load_audit",
     "description": "The failed load's audit row (status, rows in / staged / rejected, error details) and every "
                    "other load of the same batch date. Start here.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_dq_results",
     "description": "Data quality results for the batch date: each check's severity, pass/fail and observed value, "
                    "plus the previous 7 days for checks that failed (spike vs long-running issue).",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_staging_counts",
     "description": "Rows staged per staging table for each load of the batch date (counts only).",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
    {"name": "get_rejects",
     "description": "Rows that failed validation for a source on the batch date: reject files and counts by "
                    "reason (never the rows themselves).",
     "inputSchema": {"json": {"type": "object", "properties": _SOURCE_PARAM}}},
    {"name": "get_file_history",
     "description": "Recent loads of a source (status and row counts) and the raw files that landed in S3 on each "
                    "of the last few days, to spot a missing, late, duplicate or unusually small delivery.",
     "inputSchema": {"json": {"type": "object", "properties": {
         **_SOURCE_PARAM,
         "days": {"type": "integer", "description": "Days of S3 history (1-7).", "minimum": 1, "maximum": 7}}}}},
    {"name": "get_pipeline_config",
     "description": "What the failed task does: source type, S3 folders, staging tables, merge procedure, "
                    "expected loads per batch, schedule and retries.",
     "inputSchema": {"json": {"type": "object", "properties": {}}}},
]
TOOL_NAMES = [t["name"] for t in TOOL_SPECS]


@dataclass
class TriageTarget:
    load_id: str          # the failed load's audit row (written before triage runs)
    task_id: str          # the DAG task that failed
    batch_date: str       # the run's logical date, YYYY-MM-DD


def _redact(v):
    # Free text (error messages) can quote data values: strip identifiers first.
    return phi.redact(v, keep_dates=True) if isinstance(v, str) else v


class Toolbox:
    """Runs the tools for one target. Never writes anything."""

    def __init__(self, target: TriageTarget, session: ReadOnlySession):
        self.target, self.session = target, session
        self.calls: list[str] = []

    @property
    def _task(self) -> dict:
        return TASKS.get(self.target.task_id, {"kind": "unknown task", "source": None, "entity_prefix": None,
                                               "staging_tables": [], "merge_procedure": None})

    def _source(self, source: str | None) -> str:
        source = source or self._task["source"]
        if source not in SOURCES:
            raise ValueError(f"pass a source, one of {list(SOURCES)}")
        return source

    # --- the tools -------------------------------------------------------------

    def get_load_audit(self) -> dict:
        rows = self.session.query("batch_loads", load_id=self.target.load_id, batch_date=self.target.batch_date)
        rows = [{k: _redact(v) for k, v in r.items()} for r in rows]
        failed = [r for r in rows if r["load_id"] == self.target.load_id]
        # Other loads: the latest row per load_id (reruns add a row each time) with an
        # attempt count, and counts only (their error text stays in their own rows),
        # so a busy batch date still fits in one tool result.
        latest: dict[str, dict] = {}
        for r in rows:                                   # ordered by started_at, so the last one wins
            if r["load_id"] != self.target.load_id:
                attempts = latest.get(r["load_id"], {}).get("attempts", 0) + 1
                latest[r["load_id"]] = {**{k: v for k, v in r.items() if k not in ("details", "finished_at")},
                                        "attempts": attempts}
        others = list(latest.values())
        return {"failed_load": failed[-1] if failed else {"error": f"no audit row for {self.target.load_id}"},
                "batch_loads": others}

    def get_dq_results(self) -> dict:
        from pipeline.quality.data_quality import CHECKS
        described = {c.name: c.description for c in CHECKS}
        rows = self.session.query("dq_results", batch_date=self.target.batch_date)
        today = [r for r in rows if r["batch_date"] == self.target.batch_date]
        failed = [dict(r, description=described.get(r["check_name"], "")) for r in today if not r["passed"]]
        history: dict[str, list] = {}
        for r in rows:
            if r["batch_date"] != self.target.batch_date:
                history.setdefault(r["check_name"], []).append([r["batch_date"], r["observed_value"]])
        if not today:
            return {"checks_run": 0, "note": "no data quality results for this batch date (checks didn't run)"}
        return {"checks_run": len(today), "failed": failed, "previous_7_days_of_failed_checks": history,
                "passed": [r["check_name"] for r in today if r["passed"]]}

    def get_staging_counts(self) -> dict:
        rows = self.session.query("staging_counts", batch_date=self.target.batch_date)
        return {"staging_rows": rows or "no staged rows for this batch date",
                "expected_tables_for_task": self._task["staging_tables"]}

    def get_rejects(self, source: str | None = None) -> dict:
        source = self._source(source)
        prefix = f"rejects/{source}/dt={self.target.batch_date}/"
        files = {}
        for key in s3_io.list_keys(prefix):
            reader = csv.DictReader(io.StringIO(s3_io.get_bytes(key).decode("utf-8", errors="replace")))
            reasons: dict[str, int] = {}
            for row in reader:                     # only the reason column is kept -- never the row
                for reason in filter(None, (row.get("reject_reason") or "unknown").split(";")):
                    reasons[reason] = reasons.get(reason, 0) + 1
            files[key.rsplit("/", 1)[-1]] = reasons
        return {"source": source, "prefix": prefix, "reject_files": files or "no rejects for this source and date"}

    def get_file_history(self, source: str | None = None, days: int = 5) -> dict:
        source = self._source(source)
        days = max(1, min(int(days), 7))
        prefix = next((t["entity_prefix"] for t in TASKS.values()
                       if t["source"] == source and t["entity_prefix"]), source)
        loads = self.session.query("entity_history", entity_prefix=prefix, limit=10)
        end = date.fromisoformat(self.target.batch_date)
        landed = {}
        for i in range(days):
            d = (end - timedelta(days=i)).isoformat()
            keys = s3_io.list_keys(f"raw/{source}/dt={d}/")
            landed[d] = {"files": len(keys), "names": [k.rsplit("/", 1)[-1] for k in keys[:10]]}
        return {"source": source, "recent_loads": loads, "raw_files_by_date": landed}

    def get_pipeline_config(self) -> dict:
        return {"task_id": self.target.task_id, **_COMMON, **self._task,
                "raw_prefix": f"raw/{self._task['source']}/dt={self.target.batch_date}/" if self._task["source"] else None,
                "health_plans": [k for k, _, _ in HEALTH_PLANS]}

    # --- dispatch --------------------------------------------------------------

    def run(self, name: str, tool_input: dict | None) -> tuple[str, bool]:
        """(JSON result text for the model, ok). Unknown tools and tool errors are
        reported back to the model as errors, never raised."""
        self.calls.append(name)
        if name not in TOOL_NAMES:
            return json.dumps({"error": f"unknown tool {name!r}; available: {TOOL_NAMES}"}), False
        try:
            result = getattr(self, name)(**(tool_input or {}))
        except TypeError as e:            # the model sent arguments the tool doesn't take
            return json.dumps({"error": f"bad arguments for {name}: {e}"}), False
        except Exception as e:            # data/S3/Redshift problem: let the model see it and continue
            return json.dumps({"error": _redact(f"{type(e).__name__}: {e}")[:500]}), False
        text = json.dumps(result, default=str)
        if len(text) > MAX_RESULT_CHARS:
            text = text[:MAX_RESULT_CHARS] + ' ..."[truncated]"'
        return text, True
