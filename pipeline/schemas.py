"""Staging table contracts: column order + Arrow types for each Parquet file
COPY'd into Redshift.

Parquet COPY maps columns by position, so these must match
sql/redshift/V003__staging_tables.sql column-for-column
(tests/test_staging_contract.py enforces it).
"""
from dataclasses import dataclass

import pyarrow as pa

TS = pa.timestamp("us")  # Redshift doesn't accept nanosecond Parquet timestamps
MONEY = pa.decimal128(12, 2)  # DECIMAL(12,2) in Redshift -- exact cents, no float drift


# frozen=True: specs are constants and can't be changed by accident at runtime.
@dataclass(frozen=True)
class StagingSpec:
    table: str            # staging.<table>
    fields: tuple         # ((column, arrow_type), ...), load_id first

    @property
    def columns(self) -> list[str]:
        # Every column name in order, including load_id -- the Parquet layout.
        return [name for name, _ in self.fields]

    @property
    def data_columns(self) -> list[str]:
        # The columns a source DataFrame must provide (load_id is added by the loader).
        return [name for name, _ in self.fields if name != "load_id"]

    @property
    def arrow_schema(self) -> pa.Schema:
        # The exact Parquet schema written to S3 (names, order, types).
        return pa.schema(list(self.fields))


# Health-plan roster rows (after cleanup), one per member per file.
MEMBER_ELIGIBILITY = StagingSpec("member_eligibility", (
    ("load_id", pa.string()),
    ("member_id", pa.string()),
    ("member_token", pa.string()),
    ("health_plan", pa.string()),
    ("plan_code", pa.string()),
    ("first_name", pa.string()),
    ("last_name", pa.string()),
    ("dob", pa.date32()),
    ("gender", pa.string()),
    ("phone", pa.string()),
    ("zip", pa.string()),
    ("county", pa.string()),
    ("coverage_start", pa.date32()),
    ("coverage_end", pa.date32()),
    ("record_hash", pa.string()),
    ("file_date", pa.date32()),
    ("source_file", pa.string()),
))

# Salesforce Task records: CHW calls/visits, including free-text notes.
ENGAGEMENTS = StagingSpec("engagements", (
    ("load_id", pa.string()),
    ("activity_id", pa.string()),
    ("member_id", pa.string()),
    ("activity_type", pa.string()),
    ("subject", pa.string()),
    ("status", pa.string()),
    ("activity_date", pa.date32()),
    ("owner_name", pa.string()),
    ("notes", pa.string()),
    ("last_modified_at", TS),
))

# Community events from the events platform API (one row per event).
EVENTS = StagingSpec("events", (
    ("load_id", pa.string()),
    ("event_id", pa.string()),
    ("event_name", pa.string()),
    ("event_type", pa.string()),
    ("venue_name", pa.string()),
    ("county", pa.string()),
    ("starts_at", TS),
    ("host_chw", pa.string()),
    ("status", pa.string()),
    ("capacity", pa.int32()),
    ("updated_at", TS),
))

# Registrations/check-ins, flattened out of each event's nested attendee list.
EVENT_ATTENDANCE = StagingSpec("event_attendance", (
    ("load_id", pa.string()),
    ("event_id", pa.string()),
    ("member_id", pa.string()),
    ("registered_at", TS),
    ("attended", pa.bool_()),
    ("checked_in_at", TS),
))

# The do-not-contact Google Sheet, normalized.
CONTACT_PREFERENCES = StagingSpec("contact_preferences", (
    ("load_id", pa.string()),
    ("member_id", pa.string()),
    ("channel", pa.string()),
    ("requested_date", pa.date32()),
    ("requested_via", pa.string()),
))

# One row per social need detected in a CHW note.
MEMBER_SDOH_NEEDS = StagingSpec("member_sdoh_needs", (
    ("load_id", pa.string()),
    ("activity_id", pa.string()),
    ("member_id", pa.string()),
    ("need_category", pa.string()),
    ("method", pa.string()),
    ("detected_at", TS),
))

# One row per note processed by a classifier (even if no needs were found).
NOTE_CLASSIFICATIONS = StagingSpec("note_classifications", (
    ("load_id", pa.string()),
    ("activity_id", pa.string()),
    ("method", pa.string()),
    ("rule_version", pa.string()),
    ("needs_found", pa.int32()),
    ("note_modified_at", TS),
    ("classified_at", TS),
))

# Medical claims from health plans (one row per claim version in a file).
CLAIMS = StagingSpec("claims", (
    ("load_id", pa.string()),
    ("claim_id", pa.string()),
    ("member_id", pa.string()),
    ("health_plan", pa.string()),
    ("claim_type", pa.string()),
    ("place_of_service", pa.string()),
    ("revenue_code", pa.string()),
    ("cpt_code", pa.string()),
    ("primary_dx", pa.string()),
    ("service_from", pa.date32()),
    ("service_to", pa.date32()),
    ("billed_amount", MONEY),
    ("paid_amount", MONEY),
    ("claim_status", pa.string()),
    ("freq_code", pa.string()),
    ("received_date", pa.date32()),
    ("source_file", pa.string()),
))

# Health risk assessment survey responses (normalized answers).
HRA_RESPONSES = StagingSpec("hra_responses", (
    ("load_id", pa.string()),
    ("response_id", pa.string()),
    ("member_id", pa.string()),
    ("submitted_at", TS),
    ("updated_at", TS),
    ("is_complete", pa.bool_()),
    ("lives_alone", pa.bool_()),
    ("mobility_level", pa.string()),
    ("has_working_heat", pa.bool_()),
    ("has_ac", pa.bool_()),
    ("utility_cost_burden", pa.bool_()),
    ("source_file", pa.string()),
))

# Every contract, so tests can check each one against the staging DDL.
ALL_SPECS = (MEMBER_ELIGIBILITY, ENGAGEMENTS, EVENTS, EVENT_ATTENDANCE, CONTACT_PREFERENCES,
             MEMBER_SDOH_NEEDS, NOTE_CLASSIFICATIONS, CLAIMS, HRA_RESPONSES)
