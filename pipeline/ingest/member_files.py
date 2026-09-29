"""Health-plan member roster files -> core.member_eligibility (SCD2).
This is the "customer data processing" core: each health plan is a customer
that sends us the members enrolled in the program.

Each plan sends its own CSV layout. A small per-plan spec maps it onto one
canonical schema; everything after that is shared pandas cleanup:
trim/case-normalize, parse dates with the plan's declared format, strip
phone formatting, validate, dedupe, tokenize, hash.

Rows that fail validation go to s3://.../rejects/ with a reason column
instead of silently vanishing, and the count lands in ops.load_audit.
"""
import hashlib
import io
from datetime import date

import pandas as pd

from pipeline import phi, s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.reference_data import HEALTH_PLANS
from pipeline.schemas import MEMBER_ELIGIBILITY as SPEC

PREFIX = "raw/member_files"

# plan_key -> display name, e.g. "evergreen" -> "Evergreen Health Plan".
PLAN_NAMES = {key: name for key, name, _ in HEALTH_PLANS}

# Per-plan "feed spec": how to rename that plan's columns to ours, and how it
# writes dates. Onboarding a new health plan = adding one entry here.
FEED_SPECS = {
    "evergreen": {
        "columns": {
            "MemberID": "member_id", "First Name": "first_name", "Last Name": "last_name",
            "DOB": "dob", "Gender": "gender", "Phone": "phone", "Zip": "zip", "County": "county",
            "Plan": "plan_code", "Eff Date": "coverage_start", "Term Date": "coverage_end",
        },
        "date_format": "%m/%d/%Y",
    },
    "harbor": {
        "columns": {
            "member_id": "member_id", "fname": "first_name", "lname": "last_name",
            "birth_date": "dob", "sex": "gender", "phone": "phone", "zip_code": "zip",
            "county_name": "county", "plan_code": "plan_code",
            "coverage_start": "coverage_start", "coverage_end": "coverage_end",
        },
        "date_format": "%Y-%m-%d",
    },
}

# The business columns whose change means "new version of this member" (SCD2).
HASH_COLUMNS = ["health_plan", "plan_code", "first_name", "last_name", "dob", "gender",
                "phone", "zip", "county", "coverage_start", "coverage_end"]


def normalize(raw: pd.DataFrame, plan_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejects). Pure pandas -- unit tested without AWS."""
    spec = FEED_SPECS[plan_key]
    # Rename to canonical names and keep only the columns we know about.
    df = raw.rename(columns=spec["columns"])[list(spec["columns"].values())].copy()
    # Everything as text, trimmed; empty strings become real missing values.
    df = df.astype("string").apply(lambda s: s.str.strip()).replace({"": pd.NA})

    # Standardize casing and formats so both plans look identical.
    df["member_id"] = df["member_id"].str.upper()                       # mem10002 -> MEM10002
    df["first_name"] = df["first_name"].str.title()                     # JANE -> Jane
    df["last_name"] = df["last_name"].str.title()
    df["gender"] = df["gender"].str.upper().str[:1]                     # f / Female -> F
    df["plan_code"] = df["plan_code"].str.upper()                       # ma-hmo -> MA-HMO
    df["county"] = df["county"].str.title()                             # QUEENS -> Queens
    df["phone"] = df["phone"].str.replace(r"\D", "", regex=True).str[-10:]  # keep the 10 digits
    df["zip"] = df["zip"].str[:5]                                       # 11201-0000 -> 11201
    for col in ("dob", "coverage_start", "coverage_end"):
        # Parse with the plan's declared format; anything unparseable -> missing (not a crash).
        df[col] = pd.to_datetime(df[col], format=spec["date_format"], errors="coerce").dt.date
    df["health_plan"] = PLAN_NAMES[plan_key]

    # Build a reason string per row; any non-empty reason means reject.
    reasons = pd.Series("", index=df.index)
    reasons[df["member_id"].isna()] += "missing_member_id;"
    reasons[df["dob"].isna()] += "invalid_dob;"
    reasons[df["plan_code"].isna()] += "missing_plan_code;"
    bad = reasons != ""
    rejects = raw.loc[bad].assign(reject_reason=reasons[bad])  # keep the ORIGINAL row for triage
    clean = df.loc[~bad]

    # Exact resends collapse to one row. Two DIFFERENT rows for one member in
    # the same snapshot is ambiguous -- keep the last and let DQ surface it.
    clean = clean.drop_duplicates().drop_duplicates(subset=["member_id"], keep="last")

    # record_hash = md5 of the business columns joined with "|". If any of
    # them changes between files, the hash changes and SCD2 opens a new version.
    as_text = clean[HASH_COLUMNS].apply(lambda col: col.map(lambda v: "" if pd.isna(v) else str(v)))
    clean = clean.assign(record_hash=as_text.agg("|".join, axis=1)
                         .map(lambda s: hashlib.md5(s.encode()).hexdigest()))
    return clean.reset_index(drop=True), rejects


def load_file(key: str, batch_date: str) -> dict:
    # "raw/member_files/dt=.../evergreen_roster_20260928.csv" -> "evergreen"
    plan_key = key.rsplit("/", 1)[-1].split("_")[0]
    # Read every column as text (dtype=str) and keep blanks as "" -- cleanup decides what's missing.
    raw = pd.read_csv(io.BytesIO(s3_io.get_bytes(key)), dtype=str, keep_default_na=False)
    clean, rejects = normalize(raw, plan_key)

    # Rejected rows go to S3 with their reason, so someone can fix/replay them.
    if len(rejects):
        s3_io.put_bytes(f"rejects/member_files/dt={batch_date}/{plan_key}_rejects.csv",
                        rejects.to_csv(index=False).encode(), "text/csv")

    clean = clean.assign(
        member_token=clean["member_id"].map(phi.member_token),  # de-identified key for analytics
        file_date=date.fromisoformat(batch_date),               # becomes SCD2 valid_from
        source_file=s3_io.uri(key),                              # lineage back to the exact file
    )
    # Parquet -> S3 -> COPY -> CALL core.sp_merge_member_eligibility, in one transaction.
    result = stage_and_merge(
        load_id_for(f"member_file-{plan_key}", batch_date), f"member_file_{plan_key}",
        [(SPEC, clean)], "core.sp_merge_member_eligibility",
        source_uri=s3_io.uri(key), rows_in=len(raw), rows_rejected=len(rejects),
    )
    # Counts only -- never log member-level rows.
    summary = {"plan": plan_key, "rows_in": len(raw), "loaded": len(clean), "rejected": len(rejects)}
    print(summary)
    return summary | {"staged": result.rows_staged}


def main(batch_date: str) -> list[dict]:
    # Every roster file that landed for this date (one per health plan).
    keys = s3_io.list_keys(f"{PREFIX}/dt={batch_date}/")
    if not keys:
        # Fail loudly: a missing customer file is an incident, not a quiet no-op.
        raise FileNotFoundError(f"No member files for {batch_date} -- health-plan drop missing?")
    return [load_file(k, batch_date) for k in keys]


if __name__ == "__main__":
    # Run standalone: python -m pipeline.ingest.member_files 2026-09-28
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
