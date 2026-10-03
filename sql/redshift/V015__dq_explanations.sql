-- LLM-written triage note for failed data quality checks (one note per
-- batch, stored on each failed check's row). Optional: NULL when the LLM is
-- disabled or unavailable. Built from aggregate check results only -- no PHI.
ALTER TABLE ops.dq_results ADD COLUMN explanation VARCHAR(4000);
