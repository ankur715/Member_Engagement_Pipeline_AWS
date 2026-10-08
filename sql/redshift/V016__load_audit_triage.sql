-- Pipeline Triage Agent (pipeline/triage/): when a task or a data quality check
-- fails, an LLM agent investigates with read-only tools and records its
-- diagnosis on the failed load's audit row. The note is a SUGGESTION for a
-- person to review -- nothing acts on it automatically. Counts and IDs only,
-- no member PHI. Redshift adds one column per ALTER TABLE.
ALTER TABLE ops.load_audit ADD COLUMN triage_note VARCHAR(8000);   -- diagnosis + suggested fix (plain text)
ALTER TABLE ops.load_audit ADD COLUMN triage_model VARCHAR(120);   -- Bedrock model id that wrote it
ALTER TABLE ops.load_audit ADD COLUMN triage_tokens INTEGER;       -- input + output tokens used (cost tracking)
