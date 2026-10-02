-- Open NYC housing-code violations (HPD, NYC Open Data) in ZIPs where our
-- members live. Public data -- no PHI -- used as a building-conditions signal:
-- class C = "immediately hazardous"; heat (§27-2029) and hot water (§27-2031)
-- violations matter most before a cold snap.
-- Each run is a full snapshot of currently-open violations, so the table is
-- replaced (closed violations simply drop out).

CREATE TABLE IF NOT EXISTS core.housing_violations (
    violation_id        VARCHAR(20)   NOT NULL,
    zip                 VARCHAR(5)    NOT NULL,
    boro                VARCHAR(20),
    violation_class     VARCHAR(1)    NOT NULL,   -- A non-hazardous, B hazardous, C immediately hazardous
    is_heat_hot_water   BOOLEAN       NOT NULL,   -- §27-2029 heat / §27-2031 hot water
    inspection_date     DATE,
    nov_description     VARCHAR(500),
    loaded_at           TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (violation_id)
)
DISTSTYLE EVEN
SORTKEY (zip);

CREATE TABLE IF NOT EXISTS staging.housing_violations (
    load_id             VARCHAR(64),
    violation_id        VARCHAR(20),
    zip                 VARCHAR(5),
    boro                VARCHAR(20),
    violation_class     VARCHAR(1),
    is_heat_hot_water   BOOLEAN,
    inspection_date     DATE,
    nov_description     VARCHAR(500)
);

-- Snapshot replace in one transaction: readers see the old set or the new
-- one, never a half-loaded table.
CREATE OR REPLACE PROCEDURE core.sp_merge_housing_violations(p_load_id VARCHAR(64))
AS $$
BEGIN
    DELETE FROM core.housing_violations;

    INSERT INTO core.housing_violations
        (violation_id, zip, boro, violation_class, is_heat_hot_water, inspection_date, nov_description)
    SELECT DISTINCT violation_id, zip, boro, violation_class, is_heat_hot_water, inspection_date, nov_description
    FROM staging.housing_violations
    WHERE load_id = p_load_id;
END;
$$ LANGUAGE plpgsql;

-- ZIP-level building-conditions summary (public data, safe for analytics).
CREATE OR REPLACE VIEW analytics.v_zip_housing_conditions AS
SELECT zip,
       COUNT(*) AS open_violations,
       SUM(CASE WHEN violation_class = 'C' THEN 1 ELSE 0 END) AS open_class_c,
       SUM(CASE WHEN is_heat_hot_water THEN 1 ELSE 0 END) AS open_heat_hot_water
FROM core.housing_violations
GROUP BY zip;

INSERT INTO ops.source_slas (source, max_hours_stale, owner) VALUES ('housing_violations', 26, 'data-engineering');

GRANT SELECT ON core.housing_violations TO ROLE data_science_ro;
