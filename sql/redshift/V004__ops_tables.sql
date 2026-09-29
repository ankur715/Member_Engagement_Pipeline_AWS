-- ops.schema_migrations is created by pipeline/migrate.py itself (it has to
-- exist before the first migration can be recorded).

-- One row per load: where the data came from, how much arrived, how much was
-- rejected, and whether it landed. This is the lineage/observability table.
CREATE TABLE IF NOT EXISTS ops.load_audit (
    load_id        VARCHAR(64)   NOT NULL,
    entity         VARCHAR(50)   NOT NULL,
    source_uri     VARCHAR(1000),
    rows_in        INTEGER,
    rows_staged    INTEGER,
    rows_rejected  INTEGER,
    status         VARCHAR(20)   NOT NULL,
    started_at     TIMESTAMP     NOT NULL,
    finished_at    TIMESTAMP,
    details        VARCHAR(2000)
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    batch_date     DATE          NOT NULL,
    check_name     VARCHAR(100)  NOT NULL,
    severity       VARCHAR(10)   NOT NULL,   -- error (fails the run) / warn (reported only)
    passed         BOOLEAN       NOT NULL,
    observed_value VARCHAR(200),
    details        VARCHAR(2000),
    checked_at     TIMESTAMP     NOT NULL DEFAULT GETDATE()
);

-- High-water marks for incremental API pulls (Salesforce LastModifiedDate, events API updated_at).
CREATE TABLE IF NOT EXISTS ops.watermarks (
    source        VARCHAR(100) NOT NULL,
    watermark_ts  TIMESTAMP    NOT NULL,
    updated_at    TIMESTAMP    NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (source)
);

-- Delivery SLAs per source: how stale is too stale. Analytics and the
-- health-plan KPI report depend on these landing daily.
CREATE TABLE IF NOT EXISTS ops.source_slas (
    source           VARCHAR(100) NOT NULL,   -- matches the prefix of ops.load_audit.entity
    max_hours_stale  INTEGER      NOT NULL,
    owner            VARCHAR(100),
    PRIMARY KEY (source)
);

INSERT INTO ops.source_slas (source, max_hours_stale, owner) VALUES
    ('member_file_', 26, 'data-engineering'),
    ('salesforce_activities', 26, 'data-engineering'),
    ('events_api', 26, 'data-engineering'),
    ('contact_preferences', 26, 'member-services-ops'),
    ('sdoh_rules', 26, 'data-engineering');

CREATE OR REPLACE VIEW ops.v_sla_status AS
SELECT s.source,
       s.owner,
       s.max_hours_stale,
       MAX(a.finished_at) AS last_success_at,
       DATEDIFF(hour, MAX(a.finished_at), GETDATE()) AS hours_since_success,
       COALESCE(DATEDIFF(hour, MAX(a.finished_at), GETDATE()) > s.max_hours_stale, TRUE) AS sla_breached
FROM ops.source_slas s
LEFT JOIN ops.load_audit a
       ON a.status = 'succeeded'
      AND LEFT(a.entity, LEN(s.source)) = s.source
GROUP BY s.source, s.owner, s.max_hours_stale;
