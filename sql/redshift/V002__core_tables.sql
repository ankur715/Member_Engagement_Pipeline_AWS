-- Redshift does NOT enforce PRIMARY KEY / UNIQUE / FOREIGN KEY constraints
-- (they're planner hints only). Idempotency lives in the merge procedures
-- (V005), and duplicate keys are caught by data_quality.py.
-- Member-grain tables are DISTKEY(member_id) so member-level joins stay
-- node-local.

-- Health-plan member rosters, SCD Type 2: one row per version of a member's
-- record. Exactly one is_current row per member_id.
CREATE TABLE IF NOT EXISTS core.member_eligibility (
    member_sk        BIGINT IDENTITY(1, 1),
    member_id        VARCHAR(20)  NOT NULL,
    member_token     VARCHAR(64)  NOT NULL,   -- HMAC of member_id; the only member key analytics sees
    health_plan      VARCHAR(100) NOT NULL,   -- the customer
    plan_code        VARCHAR(20)  NOT NULL,   -- e.g. MA-HMO, MA-PPO, D-SNP
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    dob              DATE,
    gender           VARCHAR(1),
    phone            VARCHAR(10),
    zip              VARCHAR(5),
    county           VARCHAR(100),
    coverage_start   DATE,
    coverage_end     DATE,
    record_hash      VARCHAR(32)  NOT NULL,   -- md5 of business columns; drives change detection
    valid_from       DATE         NOT NULL,
    valid_to         DATE         NOT NULL,   -- 9999-12-31 while current
    is_current       BOOLEAN      NOT NULL,
    source_file      VARCHAR(500) NOT NULL,   -- lineage back to the S3 object
    loaded_at        TIMESTAMP    NOT NULL DEFAULT GETDATE()
)
DISTKEY (member_id)
SORTKEY (member_id, valid_from);

-- Community Health Worker activity from Salesforce (Task object).
CREATE TABLE IF NOT EXISTS core.engagements (
    activity_id       VARCHAR(18)   NOT NULL,  -- Salesforce Id
    member_id         VARCHAR(20),
    activity_type     VARCHAR(50)   NOT NULL,  -- Wellness Call, Home Visit, ...
    subject           VARCHAR(255),
    status            VARCHAR(30)   NOT NULL,  -- Open / Completed / No Answer
    activity_date     DATE          NOT NULL,
    owner_name        VARCHAR(100),            -- the CHW
    notes             VARCHAR(4000),           -- free text, PHI -- never exposed in analytics
    last_modified_at  TIMESTAMP     NOT NULL,
    loaded_at         TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (activity_id)
)
DISTKEY (member_id)
SORTKEY (activity_date);

-- Community events (neighborhood groups, health fairs, exercise classes).
CREATE TABLE IF NOT EXISTS core.events (
    event_id     VARCHAR(20)   NOT NULL,
    event_name   VARCHAR(200)  NOT NULL,
    event_type   VARCHAR(50)   NOT NULL,
    venue_name   VARCHAR(200),
    county       VARCHAR(100),
    starts_at    TIMESTAMP     NOT NULL,
    host_chw     VARCHAR(100),
    status       VARCHAR(20)   NOT NULL,  -- scheduled / completed / cancelled
    capacity     INTEGER,
    updated_at   TIMESTAMP     NOT NULL,
    loaded_at    TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (event_id)
)
DISTSTYLE ALL;  -- small dimension, joined to member-grain attendance

CREATE TABLE IF NOT EXISTS core.event_attendance (
    event_id       VARCHAR(20)  NOT NULL,
    member_id      VARCHAR(20)  NOT NULL,
    registered_at  TIMESTAMP,
    attended       BOOLEAN      NOT NULL,
    checked_in_at  TIMESTAMP,
    PRIMARY KEY (event_id, member_id)
)
DISTKEY (member_id)
SORTKEY (event_id);

-- Do-not-contact list, maintained by the ops team in a Google Sheet.
-- The sheet is the source of truth: each load fully replaces this table.
CREATE TABLE IF NOT EXISTS core.contact_preferences (
    member_id       VARCHAR(20)  NOT NULL,
    channel         VARCHAR(10)  NOT NULL,   -- phone / mail / all
    requested_date  DATE,
    requested_via   VARCHAR(50),
    loaded_at       TIMESTAMP    NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (member_id, channel)
)
DISTKEY (member_id);

-- Social-determinants-of-health needs found in CHW notes. `method` records
-- how each was detected ('rules' today) so a different classifier can be
-- added later and compared against it on the same notes.
CREATE TABLE IF NOT EXISTS core.member_sdoh_needs (
    activity_id    VARCHAR(18)  NOT NULL,
    member_id      VARCHAR(20),
    need_category  VARCHAR(50)  NOT NULL,   -- food_insecurity, transportation, ...
    method         VARCHAR(20)  NOT NULL,
    detected_at    TIMESTAMP    NOT NULL
)
DISTKEY (member_id);

-- One row per classified note (even if nothing was found), so the pipeline
-- knows what's been processed and can re-run when a note or the rules change.
CREATE TABLE IF NOT EXISTS core.note_classifications (
    activity_id      VARCHAR(18)  NOT NULL,
    method           VARCHAR(20)  NOT NULL,
    rule_version     VARCHAR(20)  NOT NULL,
    needs_found      INTEGER      NOT NULL,
    note_modified_at TIMESTAMP    NOT NULL,  -- engagements.last_modified_at at classification time
    classified_at    TIMESTAMP    NOT NULL,
    PRIMARY KEY (activity_id, method)
);
