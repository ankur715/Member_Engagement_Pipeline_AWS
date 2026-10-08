-- Fix forward for V017: Redshift also checks that the querying user has USAGE
-- on the schema of the tables behind a view, so ops.v_triage_staging_counts
-- failed for triage_reader with "permission denied for schema staging".
-- USAGE only lets the role resolve names in the schema; it grants SELECT on no
-- staging table, so the PHI in those tables stays unreadable (verified live:
-- the view returns counts, SELECT on any staging table is still denied).
GRANT USAGE ON SCHEMA staging TO ROLE triage_reader_ro;
