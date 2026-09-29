-- Fix: upcoming scheduled events created future months (e.g. next month
-- with 0 engagement) in the plan KPI report. KPIs should stop at the
-- current month.
-- Shipped as a new migration rather than an edit to V006: V006 is already
-- applied, and migrate.py rejects changes to applied files (checksum guard).
CREATE OR REPLACE VIEW analytics.v_plan_monthly_kpis AS
WITH months AS (
    SELECT DISTINCT month FROM analytics.v_member_engagement_monthly
    WHERE month <= DATE_TRUNC('month', CURRENT_DATE)
),
eligible AS (
    -- Members with coverage overlapping the month.
    SELECT mo.month, m.health_plan, COUNT(*) AS eligible_members
    FROM months mo
    JOIN core.member_eligibility m
      ON m.is_current
     AND m.coverage_start <= LAST_DAY(mo.month)
     AND (m.coverage_end IS NULL OR m.coverage_end >= mo.month)
    GROUP BY 1, 2
),
eng AS (
    SELECT health_plan, month,
           COUNT(DISTINCT CASE WHEN completed_activities > 0 THEN member_token END) AS members_reached,
           COUNT(DISTINCT CASE WHEN is_engaged THEN member_token END) AS members_engaged,
           SUM(events_attended) AS event_attendances,
           SUM(sdoh_needs) AS sdoh_needs_identified
    FROM analytics.v_member_engagement_monthly
    GROUP BY 1, 2
)
SELECT e.health_plan,
       e.month,
       e.eligible_members,
       COALESCE(g.members_reached, 0) AS members_reached,
       COALESCE(g.members_engaged, 0) AS members_engaged,
       ROUND(100.0 * COALESCE(g.members_engaged, 0) / NULLIF(e.eligible_members, 0), 1) AS engagement_rate_pct,
       COALESCE(g.event_attendances, 0) AS event_attendances,
       COALESCE(g.sdoh_needs_identified, 0) AS sdoh_needs_identified
FROM eligible e
LEFT JOIN eng g ON g.health_plan = e.health_plan AND g.month = e.month;
