-- Medical claims from each health plan (one row per claim, latest version).
--
-- Claims are restated: a replacement (X12 frequency code 7) supersedes the
-- original (1), and a void (8) cancels it. Summing every version would
-- double-count utilization, so core keeps only the LATEST version per claim_id.

CREATE TABLE IF NOT EXISTS core.claims (
    claim_id          VARCHAR(30)    NOT NULL,
    member_id         VARCHAR(20),
    health_plan       VARCHAR(100)   NOT NULL,
    claim_type        VARCHAR(20)    NOT NULL,   -- professional (837P) / institutional (837I)
    place_of_service  VARCHAR(2),                -- 21 inpatient, 23 ER, 11 office, ...
    revenue_code      VARCHAR(4),                -- institutional: 0450 ER, 0100-0219 room & board
    cpt_code          VARCHAR(5),
    primary_dx        VARCHAR(8),                -- ICD-10, normalized with the dot (E11.9)
    service_from      DATE           NOT NULL,
    service_to        DATE,
    billed_amount     DECIMAL(12, 2),
    paid_amount       DECIMAL(12, 2),
    claim_status      VARCHAR(10)    NOT NULL,   -- paid / adjusted / void
    freq_code         VARCHAR(1)     NOT NULL,   -- 1 original, 7 replacement, 8 void
    received_date     DATE           NOT NULL,   -- when the plan received this version
    source_file       VARCHAR(500)   NOT NULL,
    loaded_at         TIMESTAMP      NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (claim_id)
)
DISTKEY (member_id)          -- joins to members/eligibility stay node-local
SORTKEY (service_from);      -- utilization windows filter on service date

CREATE TABLE IF NOT EXISTS staging.claims (
    load_id           VARCHAR(64),
    claim_id          VARCHAR(30),
    member_id         VARCHAR(20),
    health_plan       VARCHAR(100),
    claim_type        VARCHAR(20),
    place_of_service  VARCHAR(2),
    revenue_code      VARCHAR(4),
    cpt_code          VARCHAR(5),
    primary_dx        VARCHAR(8),
    service_from      DATE,
    service_to        DATE,
    billed_amount     DECIMAL(12, 2),
    paid_amount       DECIMAL(12, 2),
    claim_status      VARCHAR(10),
    freq_code         VARCHAR(1),
    received_date     DATE,
    source_file       VARCHAR(500)
);

-- Latest-version upsert. Within a load, rank versions by received_date then
-- frequency code (void > replacement > original); then skip any staged version
-- OLDER than what core already has, so replaying an old file can't roll a
-- claim back. Redshift MERGE has no "WHEN MATCHED AND <condition>", hence the
-- pre-filter on the temp table.
CREATE OR REPLACE PROCEDURE core.sp_merge_claims(p_load_id VARCHAR(64))
AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_claims;
    CREATE TEMP TABLE tmp_claims AS
    SELECT claim_id, member_id, health_plan, claim_type, place_of_service, revenue_code,
           cpt_code, primary_dx, service_from, service_to, billed_amount, paid_amount,
           claim_status, freq_code, received_date, source_file
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY claim_id
                   ORDER BY received_date DESC,
                            CASE freq_code WHEN '8' THEN 3 WHEN '7' THEN 2 ELSE 1 END DESC
               ) AS rn
        FROM staging.claims
        WHERE load_id = p_load_id
    )
    WHERE rn = 1;

    DELETE FROM tmp_claims
    USING core.claims c
    WHERE tmp_claims.claim_id = c.claim_id
      AND c.received_date > tmp_claims.received_date;

    MERGE INTO core.claims
    USING tmp_claims s
    ON core.claims.claim_id = s.claim_id
    WHEN MATCHED THEN UPDATE SET
        member_id = s.member_id,
        health_plan = s.health_plan,
        claim_type = s.claim_type,
        place_of_service = s.place_of_service,
        revenue_code = s.revenue_code,
        cpt_code = s.cpt_code,
        primary_dx = s.primary_dx,
        service_from = s.service_from,
        service_to = s.service_to,
        billed_amount = s.billed_amount,
        paid_amount = s.paid_amount,
        claim_status = s.claim_status,
        freq_code = s.freq_code,
        received_date = s.received_date,
        source_file = s.source_file,
        loaded_at = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (claim_id, member_id, health_plan, claim_type, place_of_service, revenue_code, cpt_code,
         primary_dx, service_from, service_to, billed_amount, paid_amount, claim_status, freq_code,
         received_date, source_file, loaded_at)
        VALUES (s.claim_id, s.member_id, s.health_plan, s.claim_type, s.place_of_service, s.revenue_code,
                s.cpt_code, s.primary_dx, s.service_from, s.service_to, s.billed_amount, s.paid_amount,
                s.claim_status, s.freq_code, s.received_date, s.source_file, GETDATE());

    DROP TABLE tmp_claims;
END;
$$ LANGUAGE plpgsql;

-- Freshness SLA: claim files are expected daily, like rosters.
INSERT INTO ops.source_slas (source, max_hours_stale, owner) VALUES ('claims_', 26, 'data-engineering');

-- Data Science works with claims for features; analysts only see the de-identified views.
GRANT SELECT ON core.claims TO ROLE data_science_ro;
