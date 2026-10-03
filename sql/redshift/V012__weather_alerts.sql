-- NOAA / National Weather Service alerts for New York (api.weather.gov),
-- public data. Each pull is the set of alerts active right now; merging on
-- the alert id keeps a history, so "was there a heat advisory last July?"
-- stays answerable after the alert expires.
-- Alerts cover counties (6-digit SAME codes such as 036047 = Kings), so they
-- reach members through county FIPS codes (core.county_fips below).

CREATE TABLE IF NOT EXISTS core.weather_alerts (
    alert_id        VARCHAR(200)  NOT NULL,   -- NWS urn:oid:... id
    event           VARCHAR(100)  NOT NULL,   -- e.g. Heat Advisory, Extreme Cold Warning
    hazard          VARCHAR(10)   NOT NULL,   -- heat / cold / other
    severity        VARCHAR(20),
    urgency         VARCHAR(20),
    certainty       VARCHAR(20),
    message_type    VARCHAR(20),              -- Alert / Update / Cancel
    onset_at        TIMESTAMP,                -- UTC
    ends_at         TIMESTAMP,                -- UTC; NWS "ends", falling back to "expires"
    headline        VARCHAR(500),
    first_seen_at   TIMESTAMP     NOT NULL,
    last_seen_at    TIMESTAMP     NOT NULL,
    PRIMARY KEY (alert_id)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS core.weather_alert_counties (
    alert_id     VARCHAR(200)  NOT NULL,
    county_fips  VARCHAR(5)    NOT NULL,      -- 36047 = Kings County (Brooklyn)
    PRIMARY KEY (alert_id, county_fips)
)
DISTSTYLE ALL;

-- County name (as it appears on rosters) -> FIPS, for the member <-> alert join.
CREATE TABLE IF NOT EXISTS core.county_fips (
    county       VARCHAR(100)  NOT NULL,
    state        VARCHAR(2)    NOT NULL,
    county_fips  VARCHAR(5)    NOT NULL,
    is_nyc       BOOLEAN       NOT NULL,      -- NYC HPD housing data only covers the 5 boroughs
    PRIMARY KEY (county, state)
)
DISTSTYLE ALL;

INSERT INTO core.county_fips (county, state, county_fips, is_nyc) VALUES
    ('Bronx', 'NY', '36005', TRUE),
    ('Kings', 'NY', '36047', TRUE),
    ('New York', 'NY', '36061', TRUE),
    ('Queens', 'NY', '36081', TRUE),
    ('Richmond', 'NY', '36085', TRUE),
    ('Nassau', 'NY', '36059', FALSE),
    ('Westchester', 'NY', '36119', FALSE);

CREATE TABLE IF NOT EXISTS staging.weather_alerts (
    load_id         VARCHAR(64),
    alert_id        VARCHAR(200),
    event           VARCHAR(100),
    hazard          VARCHAR(10),
    severity        VARCHAR(20),
    urgency         VARCHAR(20),
    certainty       VARCHAR(20),
    message_type    VARCHAR(20),
    onset_at        TIMESTAMP,
    ends_at         TIMESTAMP,
    headline        VARCHAR(500),
    seen_at         TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.weather_alert_counties (
    load_id      VARCHAR(64),
    alert_id     VARCHAR(200),
    county_fips  VARCHAR(5)
);

-- Upsert alerts (keep first_seen_at, refresh everything else) and replace
-- each pulled alert's county list.
CREATE OR REPLACE PROCEDURE core.sp_merge_weather_alerts(p_load_id VARCHAR(64))
AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_alerts;
    CREATE TEMP TABLE tmp_alerts AS
    SELECT DISTINCT alert_id, event, hazard, severity, urgency, certainty, message_type,
           onset_at, ends_at, headline, seen_at
    FROM staging.weather_alerts
    WHERE load_id = p_load_id;

    MERGE INTO core.weather_alerts
    USING tmp_alerts s
    ON core.weather_alerts.alert_id = s.alert_id
    WHEN MATCHED THEN UPDATE SET
        event = s.event,
        hazard = s.hazard,
        severity = s.severity,
        urgency = s.urgency,
        certainty = s.certainty,
        message_type = s.message_type,
        onset_at = s.onset_at,
        ends_at = s.ends_at,
        headline = s.headline,
        last_seen_at = s.seen_at
    WHEN NOT MATCHED THEN INSERT
        (alert_id, event, hazard, severity, urgency, certainty, message_type, onset_at, ends_at,
         headline, first_seen_at, last_seen_at)
        VALUES (s.alert_id, s.event, s.hazard, s.severity, s.urgency, s.certainty, s.message_type,
                s.onset_at, s.ends_at, s.headline, s.seen_at, s.seen_at);

    DELETE FROM core.weather_alert_counties
    USING tmp_alerts s
    WHERE core.weather_alert_counties.alert_id = s.alert_id;

    INSERT INTO core.weather_alert_counties (alert_id, county_fips)
    SELECT DISTINCT alert_id, county_fips
    FROM staging.weather_alert_counties
    WHERE load_id = p_load_id;

    DROP TABLE tmp_alerts;
END;
$$ LANGUAGE plpgsql;

-- Heat/cold alerts in effect right now, one row per county (public data).
CREATE OR REPLACE VIEW analytics.v_active_weather_alerts AS
SELECT f.county, f.county_fips, a.alert_id, a.event, a.hazard, a.severity, a.onset_at, a.ends_at
FROM core.weather_alerts a
JOIN core.weather_alert_counties ac ON ac.alert_id = a.alert_id
JOIN core.county_fips f ON f.county_fips = ac.county_fips
WHERE a.hazard IN ('heat', 'cold')
  AND COALESCE(a.message_type, 'Alert') <> 'Cancel'
  AND COALESCE(a.onset_at, a.first_seen_at) <= GETDATE()
  AND (a.ends_at IS NULL OR a.ends_at > GETDATE());

INSERT INTO ops.source_slas (source, max_hours_stale, owner) VALUES ('weather_alerts', 26, 'data-engineering');

GRANT SELECT ON core.weather_alerts, core.weather_alert_counties, core.county_fips TO ROLE data_science_ro;
