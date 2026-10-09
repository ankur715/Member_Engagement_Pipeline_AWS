-- Let the triage agent read the incremental-pull high-water marks, so it can
-- spot a stuck Salesforce / events pull (a watermark that stopped advancing).
-- ops.watermarks holds only source names and timestamps -- no member data.
GRANT SELECT ON ops.watermarks TO ROLE triage_reader_ro;
