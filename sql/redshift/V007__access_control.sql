-- Role-based access, three audiences with different PHI needs:
--   analyst_ro       -- de-identified analytics views only
--   care_team_ro     -- the outreach queue (name + phone to make calls, nothing else)
--   data_science_ro  -- core tables for feature work, with direct identifiers
--                       masked by Redshift dynamic data masking
-- The ETL/admin user owns everything.
CREATE ROLE analyst_ro;
CREATE ROLE care_team_ro;
CREATE ROLE data_science_ro;

GRANT USAGE ON SCHEMA analytics TO ROLE analyst_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO ROLE analyst_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO ROLE analyst_ro;

GRANT USAGE ON SCHEMA care TO ROLE care_team_ro;
GRANT SELECT ON care.v_outreach_queue TO ROLE care_team_ro;

GRANT USAGE ON SCHEMA analytics TO ROLE data_science_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO ROLE data_science_ro;
GRANT USAGE ON SCHEMA core TO ROLE data_science_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO ROLE data_science_ro;

CREATE MASKING POLICY mask_text_redacted
WITH (val VARCHAR(4000))
USING ('[REDACTED]'::VARCHAR(4000));

CREATE MASKING POLICY mask_short_text_redacted
WITH (val VARCHAR(100))
USING ('[REDACTED]'::VARCHAR(100));

CREATE MASKING POLICY mask_phone_last4
WITH (phone VARCHAR(10))
USING ('XXXXXX' || RIGHT(phone, 4));

CREATE MASKING POLICY mask_dob_year
WITH (dob DATE)
USING (DATE_TRUNC('year', dob)::DATE);

ATTACH MASKING POLICY mask_text_redacted ON core.engagements(notes) TO ROLE data_science_ro;
ATTACH MASKING POLICY mask_short_text_redacted ON core.member_eligibility(first_name) TO ROLE data_science_ro;
ATTACH MASKING POLICY mask_short_text_redacted ON core.member_eligibility(last_name) TO ROLE data_science_ro;
ATTACH MASKING POLICY mask_phone_last4 ON core.member_eligibility(phone) TO ROLE data_science_ro;
ATTACH MASKING POLICY mask_dob_year ON core.member_eligibility(dob) TO ROLE data_science_ro;
