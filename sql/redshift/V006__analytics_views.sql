-- De-identified analytics layer + the care team's operational view.
-- analytics.*: no names, DOBs, phones, member_ids, full ZIPs or CHW notes --
-- member_token, age band, 3-digit ZIP, county and plan only.
-- These are regular (schema-bound) views: a role can be granted SELECT on
-- the view with no access to core, and Redshift won't let a core table be
-- dropped out from under a dependent view.

CREATE OR REPLACE VIEW analytics.v_members AS
SELECT
    member_token,
    health_plan,
    plan_code,
    gender,
    CASE
        WHEN DATEDIFF(year, dob, CURRENT_DATE) < 65 THEN 'under_65'
        WHEN DATEDIFF(year, dob, CURRENT_DATE) < 75 THEN '65-74'
        WHEN DATEDIFF(year, dob, CURRENT_DATE) < 85 THEN '75-84'
        ELSE '85_plus'
    END AS age_band,
    LEFT(zip, 3) AS zip3,
    county,
    coverage_start,
    coverage_end
FROM core.member_eligibility
WHERE is_current;

-- SCD2 history: plan changes over time, for as-of reporting.
CREATE OR REPLACE VIEW analytics.v_member_plan_history AS
SELECT member_token, health_plan, plan_code, coverage_start, coverage_end,
       valid_from, valid_to, is_current
FROM core.member_eligibility;

CREATE OR REPLACE VIEW analytics.v_engagements AS
SELECT e.activity_id, m.member_token, m.health_plan, e.activity_type, e.status,
       e.activity_date, e.owner_name AS chw_name
FROM core.engagements e
LEFT JOIN core.member_eligibility m ON m.member_id = e.member_id AND m.is_current;

CREATE OR REPLACE VIEW analytics.v_event_attendance AS
SELECT ev.event_id, ev.event_name, ev.event_type, ev.county, ev.starts_at, ev.status AS event_status,
       m.member_token, m.health_plan, a.attended
FROM core.event_attendance a
JOIN core.events ev ON ev.event_id = a.event_id
LEFT JOIN core.member_eligibility m ON m.member_id = a.member_id AND m.is_current;

CREATE OR REPLACE VIEW analytics.v_sdoh_needs AS
SELECT m.member_token, m.health_plan, m.county, n.need_category, n.method, g.activity_date
FROM core.member_sdoh_needs n
JOIN core.engagements g ON g.activity_id = n.activity_id
LEFT JOIN core.member_eligibility m ON m.member_id = n.member_id AND m.is_current;

-- Member x month engagement. "Engaged" = a completed CHW activity or an
-- attended community event that month.
CREATE OR REPLACE VIEW analytics.v_member_engagement_monthly AS
WITH acts AS (
    SELECT member_id, DATE_TRUNC('month', activity_date)::DATE AS month,
           COUNT(*) AS total_activities,
           SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) AS completed_activities
    FROM core.engagements
    WHERE member_id IS NOT NULL
    GROUP BY 1, 2
),
evts AS (
    SELECT a.member_id, DATE_TRUNC('month', e.starts_at)::DATE AS month,
           SUM(CASE WHEN a.attended THEN 1 ELSE 0 END) AS events_attended
    FROM core.event_attendance a
    JOIN core.events e ON e.event_id = a.event_id
    GROUP BY 1, 2
),
needs AS (
    SELECT n.member_id, DATE_TRUNC('month', g.activity_date)::DATE AS month,
           COUNT(DISTINCT n.need_category) AS sdoh_needs
    FROM core.member_sdoh_needs n
    JOIN core.engagements g ON g.activity_id = n.activity_id
    GROUP BY 1, 2
),
member_months AS (
    SELECT member_id, month FROM acts
    UNION
    SELECT member_id, month FROM evts
)
SELECT m.member_token,
       m.health_plan,
       m.plan_code,
       k.month,
       COALESCE(a.total_activities, 0) AS total_activities,
       COALESCE(a.completed_activities, 0) AS completed_activities,
       COALESCE(v.events_attended, 0) AS events_attended,
       COALESCE(n.sdoh_needs, 0) AS sdoh_needs,
       COALESCE(a.completed_activities, 0) + COALESCE(v.events_attended, 0) > 0 AS is_engaged
FROM member_months k
JOIN core.member_eligibility m ON m.member_id = k.member_id AND m.is_current
LEFT JOIN acts a ON a.member_id = k.member_id AND a.month = k.month
LEFT JOIN evts v ON v.member_id = k.member_id AND v.month = k.month
LEFT JOIN needs n ON n.member_id = k.member_id AND n.month = k.month;

-- Health plan x month program KPIs -- what goes to each customer.
CREATE OR REPLACE VIEW analytics.v_plan_monthly_kpis AS
WITH months AS (
    SELECT DISTINCT month FROM analytics.v_member_engagement_monthly
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

-- One row per current member: a feature table the Data Science team can
-- train an engagement-propensity model on (and score from) directly.
CREATE OR REPLACE VIEW analytics.v_ml_member_features AS
WITH acts AS (
    SELECT member_id,
           SUM(CASE WHEN activity_date >= CURRENT_DATE - 90 THEN 1 ELSE 0 END) AS activities_90d,
           SUM(CASE WHEN activity_date >= CURRENT_DATE - 90 AND status = 'Completed' THEN 1 ELSE 0 END) AS completed_90d,
           SUM(CASE WHEN activity_date >= CURRENT_DATE - 90 AND status = 'No Answer' THEN 1 ELSE 0 END) AS no_answer_90d,
           MAX(CASE WHEN status = 'Completed' THEN activity_date END) AS last_completed_date
    FROM core.engagements
    GROUP BY 1
),
evts AS (
    SELECT a.member_id, SUM(CASE WHEN a.attended THEN 1 ELSE 0 END) AS events_attended_90d
    FROM core.event_attendance a
    JOIN core.events e ON e.event_id = a.event_id
    WHERE e.starts_at >= CURRENT_DATE - 90
    GROUP BY 1
),
needs AS (
    SELECT n.member_id, COUNT(DISTINCT n.need_category) AS distinct_needs_180d
    FROM core.member_sdoh_needs n
    JOIN core.engagements g ON g.activity_id = n.activity_id
    WHERE g.activity_date >= CURRENT_DATE - 180
    GROUP BY 1
),
versions AS (
    SELECT member_id, COUNT(*) - 1 AS plan_record_changes
    FROM core.member_eligibility
    GROUP BY 1
),
dnc AS (
    SELECT DISTINCT member_id FROM core.contact_preferences
)
SELECT m.member_token,
       m.health_plan,
       m.plan_code,
       m.gender,
       DATEDIFF(year, m.dob, CURRENT_DATE) AS age,
       m.county,
       DATEDIFF(month, m.coverage_start, CURRENT_DATE) AS tenure_months,
       COALESCE(a.activities_90d, 0) AS activities_90d,
       COALESCE(a.completed_90d, 0) AS completed_90d,
       COALESCE(a.no_answer_90d, 0) AS no_answer_90d,
       DATEDIFF(day, a.last_completed_date, CURRENT_DATE) AS days_since_last_completed,
       COALESCE(v.events_attended_90d, 0) AS events_attended_90d,
       COALESCE(n.distinct_needs_180d, 0) AS distinct_sdoh_needs_180d,
       COALESCE(r.plan_record_changes, 0) AS plan_record_changes,
       d.member_id IS NOT NULL AS has_opted_out
FROM core.member_eligibility m
LEFT JOIN acts a ON a.member_id = m.member_id
LEFT JOIN evts v ON v.member_id = m.member_id
LEFT JOIN needs n ON n.member_id = m.member_id
LEFT JOIN versions r ON r.member_id = m.member_id
LEFT JOIN dnc d ON d.member_id = m.member_id
WHERE m.is_current;

-- Care team's daily call list: active members, not opted out of phone
-- contact, no completed touch in 30 days. Highest priority first: never
-- reached, and members with recently identified social needs.
-- Minimum necessary PHI: name, phone, county -- no DOB, no notes.
CREATE OR REPLACE VIEW care.v_outreach_queue AS
WITH last_touch AS (
    SELECT member_id, MAX(activity_date) AS last_completed_date
    FROM core.engagements
    WHERE status = 'Completed'
    GROUP BY 1
),
recent_needs AS (
    SELECT n.member_id, COUNT(DISTINCT n.need_category) AS needs_90d
    FROM core.member_sdoh_needs n
    JOIN core.engagements g ON g.activity_id = n.activity_id
    WHERE g.activity_date >= CURRENT_DATE - 90
    GROUP BY 1
),
phone_opt_out AS (
    SELECT DISTINCT member_id FROM core.contact_preferences WHERE channel IN ('phone', 'all')
)
SELECT m.member_id,
       m.first_name,
       m.last_name,
       m.phone,
       m.county,
       m.health_plan,
       t.last_completed_date,
       DATEDIFF(day, t.last_completed_date, CURRENT_DATE) AS days_since_contact,
       COALESCE(r.needs_90d, 0) AS sdoh_needs_90d,
       CASE WHEN t.last_completed_date IS NULL THEN 3 ELSE 0 END
         + 2 * COALESCE(r.needs_90d, 0) AS priority_score
FROM core.member_eligibility m
LEFT JOIN last_touch t ON t.member_id = m.member_id
LEFT JOIN recent_needs r ON r.member_id = m.member_id
LEFT JOIN phone_opt_out o ON o.member_id = m.member_id
WHERE m.is_current
  AND (m.coverage_end IS NULL OR m.coverage_end >= CURRENT_DATE)
  AND o.member_id IS NULL
  AND (t.last_completed_date IS NULL OR t.last_completed_date < CURRENT_DATE - 30);
