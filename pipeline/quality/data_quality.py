"""Post-load data quality checks for one batch date.

Each check is a SQL query returning one number plus a pass rule. Every
result (pass or fail) is written to ops.dq_results, so DQ history is
queryable and trendable -- not just a log line.

severity=error fails the DAG run (and fires the alert email).
severity=warn is recorded and surfaced but doesn't block downstream.
"""
from dataclasses import dataclass
from datetime import date
from typing import Callable

from pipeline.redshift import get_connection
from pipeline.reference_data import HEALTH_PLANS


class DataQualityError(Exception):
    # A dedicated error type so the DAG can catch exactly this and turn it into
    # an Airflow failure (which then sends the alert email).
    pass


# One data quality rule: a SQL query that returns a single number, and a
# function that decides whether that number is acceptable.
@dataclass(frozen=True)
class Check:
    name: str
    severity: str                       # error | warn
    sql: str                            # returns one number; %(batch_date)s available
    passes: Callable[[float], bool]     # e.g. lambda v: v == 0
    description: str                    # human-readable, shown in ops.dq_results and alerts


# Activity types that are phone calls (used by the outreach-compliance check).
CALL_TYPES = "('Wellness Call', 'Care Gap Outreach', 'Welcome Call')"

CHECKS = [
    # --- integrity (Redshift doesn't enforce keys, so we do) ---
    Check("scd2_single_current_row", "error",
          """SELECT COUNT(*) FROM (SELECT member_id FROM core.member_eligibility
                                   WHERE is_current GROUP BY member_id HAVING COUNT(*) > 1)""",
          lambda v: v == 0, "Members with more than one current roster row"),
    Check("duplicate_activity_ids", "error",
          "SELECT COUNT(*) FROM (SELECT activity_id FROM core.engagements GROUP BY 1 HAVING COUNT(*) > 1)",
          lambda v: v == 0, "Duplicate Salesforce activity ids in core.engagements"),
    Check("attendance_without_event", "error",
          """SELECT COUNT(*) FROM core.event_attendance a
             LEFT JOIN core.events e ON e.event_id = a.event_id WHERE e.event_id IS NULL""",
          lambda v: v == 0, "Attendance rows pointing at an event we don't have"),

    # --- completeness / freshness ---
    Check("member_files_loaded", "error",
          """SELECT COUNT(DISTINCT entity) FROM ops.load_audit
             WHERE LEFT(entity, 12) = 'member_file_' AND status = 'succeeded'
               AND RIGHT(load_id, 10) = %(batch_date)s""",
          lambda v: v >= len(HEALTH_PLANS), "Health-plan roster files loaded for this date"),
    Check("dnc_list_present", "error",
          "SELECT COUNT(*) FROM core.contact_preferences",
          lambda v: v > 0, "Do-not-contact list is populated (empty = outreach compliance risk)"),
    Check("sources_breaching_sla", "warn",
          "SELECT COUNT(*) FROM ops.v_sla_status WHERE sla_breached",
          lambda v: v == 0, "Sources whose last successful load is older than their SLA"),

    # --- conformance ---
    Check("activities_unknown_member_pct", "warn",
          """SELECT COALESCE(100.0 * SUM(CASE WHEN m.member_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0)
             FROM core.engagements e
             LEFT JOIN (SELECT DISTINCT member_id FROM core.member_eligibility) m ON m.member_id = e.member_id
             WHERE e.activity_date > %(batch_date)s::DATE - 30""",
          lambda v: v <= 5, "% of last-30-day CHW activities whose member isn't on any roster"),
    Check("attendees_unknown_member_pct", "warn",
          """SELECT COALESCE(100.0 * SUM(CASE WHEN m.member_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0)
             FROM core.event_attendance a
             LEFT JOIN (SELECT DISTINCT member_id FROM core.member_eligibility) m ON m.member_id = a.member_id""",
          lambda v: v <= 10, "% of event attendees not on any roster (walk-ins / typos)"),

    # --- anomaly ---
    Check("activity_volume_vs_7day_avg_pct", "warn",
          """SELECT COALESCE(100.0 * ABS(t.n - h.avg_n) / NULLIF(h.avg_n, 0), 0) FROM
               (SELECT COUNT(*) AS n FROM core.engagements WHERE activity_date = %(batch_date)s) t,
               (SELECT AVG(n::FLOAT) AS avg_n FROM (
                   SELECT activity_date, COUNT(*) AS n FROM core.engagements
                   WHERE activity_date BETWEEN %(batch_date)s::DATE - 7 AND %(batch_date)s::DATE - 1
                   GROUP BY 1)) h""",
          lambda v: v <= 60, "Deviation of today's CHW activity count from the trailing 7-day average (%)"),

    # --- enrichment backlog ---
    Check("untagged_notes", "warn",
          """SELECT COUNT(*) FROM core.engagements e
             LEFT JOIN core.note_classifications c ON c.activity_id = e.activity_id AND c.method = 'rules'
             WHERE e.notes IS NOT NULL AND (c.activity_id IS NULL OR c.note_modified_at < e.last_modified_at)""",
          lambda v: v == 0, "CHW notes not yet tagged for SDoH needs"),

    # --- outreach compliance ---
    Check("opt_out_in_notes_not_on_dnc", "warn",
          """SELECT COUNT(DISTINCT n.member_id) FROM core.member_sdoh_needs n
             LEFT JOIN core.contact_preferences c ON c.member_id = n.member_id
             WHERE n.need_category = 'opt_out_request' AND c.member_id IS NULL""",
          lambda v: v == 0, "Members who asked a CHW to stop contact but aren't on the DNC sheet"),
    Check("calls_after_opt_out", "warn",
          f"""SELECT COUNT(*) FROM core.engagements e
              JOIN core.contact_preferences c
                ON c.member_id = e.member_id AND c.channel IN ('phone', 'all')
              WHERE e.activity_type IN {CALL_TYPES}
                AND e.status = 'Completed'
                AND e.activity_date > c.requested_date""",
          lambda v: v == 0, "Completed phone outreach dated after the member opted out"),

    # --- claims ---
    Check("claim_files_loaded", "error",
          """SELECT COUNT(DISTINCT entity) FROM ops.load_audit
             WHERE LEFT(entity, 7) = 'claims_' AND status = 'succeeded'
               AND RIGHT(load_id, 10) = %(batch_date)s""",
          lambda v: v >= len(HEALTH_PLANS), "Health-plan claims files loaded for this date"),
    Check("duplicate_claim_ids", "error",
          "SELECT COUNT(*) FROM (SELECT claim_id FROM core.claims GROUP BY 1 HAVING COUNT(*) > 1)",
          lambda v: v == 0, "More than one current version of a claim in core.claims"),
    Check("claims_unknown_member_pct", "warn",
          """SELECT COALESCE(100.0 * SUM(CASE WHEN m.member_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0)
             FROM core.claims c
             LEFT JOIN (SELECT DISTINCT member_id FROM core.member_eligibility) m ON m.member_id = c.member_id
             WHERE c.service_from > %(batch_date)s::DATE - 365""",
          lambda v: v <= 5, "% of last-12-month claims whose member isn't on any roster"),
    Check("claims_future_service_dates", "warn",
          "SELECT COUNT(*) FROM core.claims WHERE service_from > %(batch_date)s::DATE",
          lambda v: v == 0, "Claims with a service date after the batch date (bad dates upstream)"),

    # --- HRA surveys ---
    Check("hra_file_loaded", "error",
          """SELECT COUNT(*) FROM ops.load_audit
             WHERE entity = 'hra_responses' AND status = 'succeeded'
               AND RIGHT(load_id, 10) = %(batch_date)s""",
          lambda v: v >= 1, "Survey vendor's HRA file loaded for this date"),
    Check("hra_unknown_member_pct", "warn",
          """SELECT COALESCE(100.0 * SUM(CASE WHEN m.member_id IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0)
             FROM core.hra_responses h
             LEFT JOIN (SELECT DISTINCT member_id FROM core.member_eligibility) m ON m.member_id = h.member_id""",
          lambda v: v <= 5, "% of HRA responses whose member isn't on any roster"),
    Check("hra_coverage_pct", "warn",
          """SELECT COALESCE(100.0 * COUNT(DISTINCT h.member_id) / NULLIF(COUNT(DISTINCT m.member_id), 0), 0)
             FROM core.member_eligibility m
             LEFT JOIN core.hra_responses h
               ON h.member_id = m.member_id AND h.submitted_at > %(batch_date)s::DATE - 365
             WHERE m.is_current""",
          lambda v: v >= 50, "% of current members with an HRA in the last 12 months"),

    # --- public data: NYC housing violations ---
    Check("housing_violations_present", "warn",
          "SELECT COUNT(*) FROM core.housing_violations",
          lambda v: v > 0, "Open NYC housing violations loaded for member ZIPs (0 = pull likely failed)"),

    # --- public data: NOAA weather alerts ---
    Check("weather_alerts_pulled_today", "warn",
          """SELECT COUNT(*) FROM ops.load_audit
             WHERE entity = 'weather_alerts' AND status = 'succeeded'
               AND RIGHT(load_id, 10) = %(batch_date)s""",
          lambda v: v >= 1, "NWS alerts pulled for this date (zero alerts is fine; a failed pull is not)"),
    Check("weather_alerts_unmapped_counties", "warn",
          """SELECT COUNT(*) FROM core.weather_alert_counties ac
             JOIN core.weather_alerts a ON a.alert_id = ac.alert_id AND a.hazard IN ('heat', 'cold')
             LEFT JOIN core.county_fips f ON f.county_fips = ac.county_fips
             WHERE f.county_fips IS NULL""",
          lambda v: v == 0, "Heat/cold alert counties missing from core.county_fips (members there would be missed)"),

    # --- vulnerability index ---
    Check("vulnerability_index_covers_members", "error",
          """SELECT (SELECT COUNT(*) FROM core.member_eligibility
                     WHERE is_current AND (coverage_end IS NULL OR coverage_end >= CURRENT_DATE))
                  - (SELECT COUNT(*) FROM analytics.v_member_vulnerability)""",
          lambda v: v == 0, "Every active member has exactly one vulnerability score (no one silently dropped)"),
    Check("wellness_queue_excludes_opt_outs", "error",
          """SELECT COUNT(*) FROM care.v_wellness_check_queue q
             JOIN core.contact_preferences c ON c.member_id = q.member_id AND c.channel IN ('phone', 'all')""",
          lambda v: v == 0, "No member who opted out of phone contact is on the wellness-check call list"),
    Check("llm_rules_agreement_pct", "warn",
          """SELECT COALESCE(100.0 * SUM(both_methods) / NULLIF(SUM(both_methods + rules_only + llm_only), 0), 100)
             FROM analytics.v_sdoh_method_agreement""",
          lambda v: v >= 70, "LLM vs. rule-based SDoH tags agree on >= 70% of tags (drift monitor; 100 if not run)"),
    Check("analytics_exposes_no_identifiers", "error",
          """SELECT COUNT(*) FROM svv_columns
             WHERE table_schema = 'analytics'
               AND column_name IN ('member_id', 'first_name', 'last_name', 'phone', 'dob', 'notes', 'address')""",
          lambda v: v == 0, "analytics.* stays de-identified: no direct identifiers in any view"),
]


def evaluate(check: Check, value: float) -> dict:
    # Apply the pass rule to the observed number -> a result row.
    return {"check_name": check.name, "severity": check.severity, "passed": bool(check.passes(value)),
            "observed_value": value, "description": check.description}


def run_checks(batch_date: str) -> list[dict]:
    conn = get_connection()
    try:
        results = []
        with conn.cursor() as cur:
            # 1. Run every check query and evaluate its number.
            for check in CHECKS:
                cur.execute(check.sql, {"batch_date": batch_date})
                value = cur.fetchone()[0]
                # NULL (e.g. no rows to average) counts as 0.
                results.append(evaluate(check, float(value) if value is not None else 0.0))
            # 2. Save ALL results (passes too) for this date -- replacing any from a rerun.
            cur.execute("DELETE FROM ops.dq_results WHERE batch_date = %s;", (batch_date,))
            for r in results:
                cur.execute(
                    """INSERT INTO ops.dq_results (batch_date, check_name, severity, passed, observed_value, details)
                       VALUES (%s, %s, %s, %s, %s, %s);""",
                    (batch_date, r["check_name"], r["severity"], r["passed"], str(r["observed_value"]), r["description"]),
                )
        conn.commit()
    finally:
        conn.close()

    # 3. Print a readable summary to the task log.
    for r in results:
        print(f"[{'PASS' if r['passed'] else r['severity'].upper()}] {r['check_name']} = {r['observed_value']}")

    # 4. Only failed ERROR-level checks stop the pipeline; warnings are just recorded.
    errors = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if errors:
        raise DataQualityError(
            f"Batch {batch_date} failed {len(errors)} data quality check(s): "
            + "; ".join(f"{r['check_name']}={r['observed_value']} ({r['description']})" for r in errors)
        )
    return results


if __name__ == "__main__":
    import sys
    run_checks(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
