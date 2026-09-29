-- Landing tables for COPY ... FORMAT AS PARQUET. Parquet COPY maps columns
-- BY POSITION, so column order here must match pipeline/schemas.py exactly
-- (enforced by tests/test_staging_contract.py).
-- Every table carries load_id so a rerun only replaces its own staged rows.

CREATE TABLE IF NOT EXISTS staging.member_eligibility (
    load_id         VARCHAR(64),
    member_id       VARCHAR(20),
    member_token    VARCHAR(64),
    health_plan     VARCHAR(100),
    plan_code       VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    dob             DATE,
    gender          VARCHAR(1),
    phone           VARCHAR(10),
    zip             VARCHAR(5),
    county          VARCHAR(100),
    coverage_start  DATE,
    coverage_end    DATE,
    record_hash     VARCHAR(32),
    file_date       DATE,
    source_file     VARCHAR(500)
);

CREATE TABLE IF NOT EXISTS staging.engagements (
    load_id           VARCHAR(64),
    activity_id       VARCHAR(18),
    member_id         VARCHAR(20),
    activity_type     VARCHAR(50),
    subject           VARCHAR(255),
    status            VARCHAR(30),
    activity_date     DATE,
    owner_name        VARCHAR(100),
    notes             VARCHAR(4000),
    last_modified_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.events (
    load_id     VARCHAR(64),
    event_id    VARCHAR(20),
    event_name  VARCHAR(200),
    event_type  VARCHAR(50),
    venue_name  VARCHAR(200),
    county      VARCHAR(100),
    starts_at   TIMESTAMP,
    host_chw    VARCHAR(100),
    status      VARCHAR(20),
    capacity    INTEGER,
    updated_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.event_attendance (
    load_id        VARCHAR(64),
    event_id       VARCHAR(20),
    member_id      VARCHAR(20),
    registered_at  TIMESTAMP,
    attended       BOOLEAN,
    checked_in_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.contact_preferences (
    load_id         VARCHAR(64),
    member_id       VARCHAR(20),
    channel         VARCHAR(10),
    requested_date  DATE,
    requested_via   VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS staging.member_sdoh_needs (
    load_id        VARCHAR(64),
    activity_id    VARCHAR(18),
    member_id      VARCHAR(20),
    need_category  VARCHAR(50),
    method         VARCHAR(20),
    detected_at    TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.note_classifications (
    load_id           VARCHAR(64),
    activity_id       VARCHAR(18),
    method            VARCHAR(20),
    rule_version      VARCHAR(20),
    needs_found       INTEGER,
    note_modified_at  TIMESTAMP,
    classified_at     TIMESTAMP
);
