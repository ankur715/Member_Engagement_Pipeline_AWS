-- Health risk assessment (HRA) survey responses from the survey vendor.
-- One row per response (a member can complete several over time; views use
-- the latest). The vendor resends corrected responses under the same
-- response_id, so the merge keeps the most recently updated version.

CREATE TABLE IF NOT EXISTS core.hra_responses (
    response_id          VARCHAR(30)  NOT NULL,
    member_id            VARCHAR(20)  NOT NULL,
    submitted_at         TIMESTAMP    NOT NULL,
    updated_at           TIMESTAMP    NOT NULL,   -- vendor's last-modified time (corrections)
    is_complete          BOOLEAN      NOT NULL,   -- partial surveys still count for answered items
    lives_alone          BOOLEAN,                 -- NULL = not answered
    mobility_level       VARCHAR(10),             -- none / some / severe (NULL = not answered)
    has_working_heat     BOOLEAN,                 -- "sometimes" counts as FALSE (unreliable heat)
    has_ac               BOOLEAN,                 -- fan only / none = FALSE
    utility_cost_burden  BOOLEAN,                 -- trouble paying utility bills
    source_file          VARCHAR(500) NOT NULL,
    loaded_at            TIMESTAMP    NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (response_id)
)
DISTKEY (member_id)
SORTKEY (member_id, submitted_at);

CREATE TABLE IF NOT EXISTS staging.hra_responses (
    load_id              VARCHAR(64),
    response_id          VARCHAR(30),
    member_id            VARCHAR(20),
    submitted_at         TIMESTAMP,
    updated_at           TIMESTAMP,
    is_complete          BOOLEAN,
    lives_alone          BOOLEAN,
    mobility_level       VARCHAR(10),
    has_working_heat     BOOLEAN,
    has_ac               BOOLEAN,
    utility_cost_burden  BOOLEAN,
    source_file          VARCHAR(500)
);

-- Latest-correction upsert on response_id; an older resend never overwrites a newer one.
CREATE OR REPLACE PROCEDURE core.sp_merge_hra_responses(p_load_id VARCHAR(64))
AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_hra;
    CREATE TEMP TABLE tmp_hra AS
    SELECT response_id, member_id, submitted_at, updated_at, is_complete, lives_alone,
           mobility_level, has_working_heat, has_ac, utility_cost_burden, source_file
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY response_id ORDER BY updated_at DESC) AS rn
        FROM staging.hra_responses
        WHERE load_id = p_load_id
    )
    WHERE rn = 1;

    DELETE FROM tmp_hra
    USING core.hra_responses c
    WHERE tmp_hra.response_id = c.response_id
      AND c.updated_at > tmp_hra.updated_at;

    MERGE INTO core.hra_responses
    USING tmp_hra s
    ON core.hra_responses.response_id = s.response_id
    WHEN MATCHED THEN UPDATE SET
        member_id = s.member_id,
        submitted_at = s.submitted_at,
        updated_at = s.updated_at,
        is_complete = s.is_complete,
        lives_alone = s.lives_alone,
        mobility_level = s.mobility_level,
        has_working_heat = s.has_working_heat,
        has_ac = s.has_ac,
        utility_cost_burden = s.utility_cost_burden,
        source_file = s.source_file,
        loaded_at = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (response_id, member_id, submitted_at, updated_at, is_complete, lives_alone, mobility_level,
         has_working_heat, has_ac, utility_cost_burden, source_file, loaded_at)
        VALUES (s.response_id, s.member_id, s.submitted_at, s.updated_at, s.is_complete, s.lives_alone,
                s.mobility_level, s.has_working_heat, s.has_ac, s.utility_cost_burden, s.source_file, GETDATE());

    DROP TABLE tmp_hra;
END;
$$ LANGUAGE plpgsql;

INSERT INTO ops.source_slas (source, max_hours_stale, owner) VALUES ('hra_responses', 26, 'data-engineering');

GRANT SELECT ON core.hra_responses TO ROLE data_science_ro;
