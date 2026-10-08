-- Least-privilege access for the triage agent's read-only Redshift user
-- (created by `python -m pipeline.triage.setup_reader`, which grants it this role).
--
-- The agent needs staging row counts per load, but the staging tables hold
-- PHI (names, phones, notes). So it gets this view of COUNTS per load_id, and
-- no access to any staging, core or care table itself.
CREATE OR REPLACE VIEW ops.v_triage_staging_counts AS
          SELECT 'member_eligibility'     AS staging_table, load_id, COUNT(*) AS row_count FROM staging.member_eligibility     GROUP BY load_id
UNION ALL SELECT 'engagements',            load_id, COUNT(*) FROM staging.engagements            GROUP BY load_id
UNION ALL SELECT 'events',                 load_id, COUNT(*) FROM staging.events                 GROUP BY load_id
UNION ALL SELECT 'event_attendance',       load_id, COUNT(*) FROM staging.event_attendance       GROUP BY load_id
UNION ALL SELECT 'contact_preferences',    load_id, COUNT(*) FROM staging.contact_preferences    GROUP BY load_id
UNION ALL SELECT 'member_sdoh_needs',      load_id, COUNT(*) FROM staging.member_sdoh_needs      GROUP BY load_id
UNION ALL SELECT 'note_classifications',   load_id, COUNT(*) FROM staging.note_classifications   GROUP BY load_id
UNION ALL SELECT 'claims',                 load_id, COUNT(*) FROM staging.claims                 GROUP BY load_id
UNION ALL SELECT 'hra_responses',          load_id, COUNT(*) FROM staging.hra_responses          GROUP BY load_id
UNION ALL SELECT 'housing_violations',     load_id, COUNT(*) FROM staging.housing_violations     GROUP BY load_id
UNION ALL SELECT 'weather_alerts',         load_id, COUNT(*) FROM staging.weather_alerts         GROUP BY load_id
UNION ALL SELECT 'weather_alert_counties', load_id, COUNT(*) FROM staging.weather_alert_counties GROUP BY load_id;

-- SELECT on exactly three ops objects, all counts / IDs / pipeline metadata.
-- No INSERT/UPDATE/DELETE anywhere: writing the triage note is done by the ETL user.
CREATE ROLE triage_reader_ro;
GRANT USAGE ON SCHEMA ops TO ROLE triage_reader_ro;
GRANT SELECT ON ops.load_audit TO ROLE triage_reader_ro;
GRANT SELECT ON ops.dq_results TO ROLE triage_reader_ro;
GRANT SELECT ON ops.v_triage_staging_counts TO ROLE triage_reader_ro;
