-- Layered warehouse:
--   staging   -- 1:1 landing tables for COPY, keyed by load_id, disposable
--   core      -- conformed, deduplicated tables (contains PHI; restricted)
--   care      -- operational views for the care team (minimum necessary PHI)
--   analytics -- de-identified views for Analytics / Data Science / BI
--   ops       -- pipeline metadata: migrations, load audit, DQ results, SLAs, watermarks
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS care;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;
