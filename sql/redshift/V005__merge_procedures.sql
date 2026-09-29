-- Merge procedures: staging -> core. pipeline/loaders.py calls each one in
-- the same transaction as the COPY that filled staging, so a load lands
-- completely or not at all.


-- Member rosters: SCD Type 2. Each file is a full snapshot for one health
-- plan, so a changed record_hash means the member's record changed.
-- Re-running the same load is a no-op (every staged hash already matches a
-- current row). Loads must be applied in date order -- the DAG sets
-- depends_on_past on this task.
CREATE OR REPLACE PROCEDURE core.sp_merge_member_eligibility(p_load_id VARCHAR(64))
AS $$
BEGIN
    -- 1. Close out current versions whose attributes changed.
    UPDATE core.member_eligibility
    SET valid_to = s.file_date,
        is_current = FALSE
    FROM staging.member_eligibility s
    WHERE s.load_id = p_load_id
      AND core.member_eligibility.member_id = s.member_id
      AND core.member_eligibility.is_current
      AND core.member_eligibility.record_hash <> s.record_hash;

    -- 2. New current version for new members and the ones just closed.
    INSERT INTO core.member_eligibility (
        member_id, member_token, health_plan, plan_code, first_name, last_name, dob,
        gender, phone, zip, county, coverage_start, coverage_end, record_hash,
        valid_from, valid_to, is_current, source_file
    )
    SELECT s.member_id, s.member_token, s.health_plan, s.plan_code, s.first_name, s.last_name, s.dob,
           s.gender, s.phone, s.zip, s.county, s.coverage_start, s.coverage_end, s.record_hash,
           s.file_date, '9999-12-31'::DATE, TRUE, s.source_file
    FROM staging.member_eligibility s
    LEFT JOIN core.member_eligibility t
           ON t.member_id = s.member_id AND t.is_current
    WHERE s.load_id = p_load_id
      AND t.member_id IS NULL;
END;
$$ LANGUAGE plpgsql;


-- Salesforce activities: incremental upsert. A Task comes back every time
-- it's modified (Open -> Completed), so MERGE on activity_id keeping only
-- the latest version from this pull.
CREATE OR REPLACE PROCEDURE core.sp_merge_engagements(p_load_id VARCHAR(64))
AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_engagements;
    CREATE TEMP TABLE tmp_engagements AS
    SELECT activity_id, member_id, activity_type, subject, status, activity_date,
           owner_name, notes, last_modified_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY last_modified_at DESC) AS rn
        FROM staging.engagements
        WHERE load_id = p_load_id
    )
    WHERE rn = 1;

    MERGE INTO core.engagements
    USING tmp_engagements s
    ON core.engagements.activity_id = s.activity_id
    WHEN MATCHED THEN UPDATE SET
        member_id = s.member_id,
        activity_type = s.activity_type,
        subject = s.subject,
        status = s.status,
        activity_date = s.activity_date,
        owner_name = s.owner_name,
        notes = s.notes,
        last_modified_at = s.last_modified_at,
        loaded_at = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (activity_id, member_id, activity_type, subject, status, activity_date,
         owner_name, notes, last_modified_at, loaded_at)
        VALUES (s.activity_id, s.member_id, s.activity_type, s.subject, s.status, s.activity_date,
                s.owner_name, s.notes, s.last_modified_at, GETDATE());

    DROP TABLE tmp_engagements;
END;
$$ LANGUAGE plpgsql;


-- Events API: upsert the event, then replace its whole roster. The API
-- returns an event with its full attendee list whenever anything about it
-- changes, so the attendee list is authoritative per event.
CREATE OR REPLACE PROCEDURE core.sp_merge_events(p_load_id VARCHAR(64))
AS $$
BEGIN
    DROP TABLE IF EXISTS tmp_events;
    CREATE TEMP TABLE tmp_events AS
    SELECT event_id, event_name, event_type, venue_name, county, starts_at,
           host_chw, status, capacity, updated_at
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY updated_at DESC) AS rn
        FROM staging.events
        WHERE load_id = p_load_id
    )
    WHERE rn = 1;

    MERGE INTO core.events
    USING tmp_events s
    ON core.events.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
        event_name = s.event_name,
        event_type = s.event_type,
        venue_name = s.venue_name,
        county = s.county,
        starts_at = s.starts_at,
        host_chw = s.host_chw,
        status = s.status,
        capacity = s.capacity,
        updated_at = s.updated_at,
        loaded_at = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (event_id, event_name, event_type, venue_name, county, starts_at,
         host_chw, status, capacity, updated_at, loaded_at)
        VALUES (s.event_id, s.event_name, s.event_type, s.venue_name, s.county, s.starts_at,
                s.host_chw, s.status, s.capacity, s.updated_at, GETDATE());

    DELETE FROM core.event_attendance
    USING tmp_events s
    WHERE core.event_attendance.event_id = s.event_id;

    INSERT INTO core.event_attendance (event_id, member_id, registered_at, attended, checked_in_at)
    SELECT DISTINCT event_id, member_id, registered_at, attended, checked_in_at
    FROM staging.event_attendance
    WHERE load_id = p_load_id;

    DROP TABLE tmp_events;
END;
$$ LANGUAGE plpgsql;


-- Do-not-contact list: full snapshot replace. Inside one transaction, so
-- readers see either the old list or the new one, never an empty table.
-- Refuses an empty snapshot: a blank/broken sheet read must not silently
-- wipe everyone's opt-out and put them back in the outreach queue.
CREATE OR REPLACE PROCEDURE core.sp_merge_contact_preferences(p_load_id VARCHAR(64))
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_rows FROM staging.contact_preferences WHERE load_id = p_load_id;
    IF v_rows = 0 THEN
        RAISE EXCEPTION 'Refusing to replace contact_preferences with an empty snapshot (load %)', p_load_id;
    END IF;

    DELETE FROM core.contact_preferences;

    INSERT INTO core.contact_preferences (member_id, channel, requested_date, requested_via)
    SELECT member_id, channel, MIN(requested_date), MIN(requested_via)
    FROM staging.contact_preferences
    WHERE load_id = p_load_id
    GROUP BY member_id, channel;
END;
$$ LANGUAGE plpgsql;


-- SDoH needs: replace results for every note classified in this load.
CREATE OR REPLACE PROCEDURE core.sp_merge_note_classifications(p_load_id VARCHAR(64))
AS $$
BEGIN
    DELETE FROM core.member_sdoh_needs
    USING staging.note_classifications s
    WHERE s.load_id = p_load_id
      AND core.member_sdoh_needs.activity_id = s.activity_id
      AND core.member_sdoh_needs.method = s.method;

    DELETE FROM core.note_classifications
    USING staging.note_classifications s
    WHERE s.load_id = p_load_id
      AND core.note_classifications.activity_id = s.activity_id
      AND core.note_classifications.method = s.method;

    INSERT INTO core.member_sdoh_needs (activity_id, member_id, need_category, method, detected_at)
    SELECT activity_id, member_id, need_category, method, detected_at
    FROM staging.member_sdoh_needs
    WHERE load_id = p_load_id;

    INSERT INTO core.note_classifications (activity_id, method, rule_version, needs_found, note_modified_at, classified_at)
    SELECT activity_id, method, rule_version, needs_found, note_modified_at, classified_at
    FROM staging.note_classifications
    WHERE load_id = p_load_id;
END;
$$ LANGUAGE plpgsql;
