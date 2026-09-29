import io

import pandas as pd
import pytest

from pipeline.ingest import member_files
from pipeline.reference_data import member_roster
from pipeline.sources import generate_member_files


def _raw(plan_key, day="2026-09-28"):
    data = generate_member_files.build_file(plan_key, day)
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


@pytest.mark.parametrize("plan_key", ["evergreen", "harbor"])
def test_normalize_cleans_and_rejects(plan_key):
    clean, rejects = member_files.normalize(_raw(plan_key), plan_key)
    plan_members = {m["member_id"] for m in member_roster() if m["plan_key"] == plan_key}
    assert set(clean["member_id"]) == plan_members      # duplicates collapsed, nobody lost
    assert clean["member_id"].is_unique
    assert len(rejects) == 2                             # one bad DOB + one blank member id
    assert set(rejects["reject_reason"]) <= {"missing_member_id;", "invalid_dob;"}
    assert clean["phone"].str.fullmatch(r"\d{10}").all()
    assert clean["zip"].str.fullmatch(r"\d{5}").all()
    assert clean["plan_code"].str.isupper().all()


def test_both_plan_layouts_normalize_to_the_same_canonical_values():
    roster = {m["member_id"]: m for m in member_roster()}
    for plan_key in ("evergreen", "harbor"):
        clean, _ = member_files.normalize(_raw(plan_key), plan_key)
        for _, row in clean.iterrows():
            m = roster[row["member_id"]]
            assert row["dob"] == m["dob"]
            assert row["first_name"] == m["first_name"].title()
            assert row["phone"] == m["phone"]
            assert row["county"] == m["county"]


def test_record_hash_stable_within_a_month():
    a, _ = member_files.normalize(_raw("harbor", "2026-09-10"), "harbor")
    b, _ = member_files.normalize(_raw("harbor", "2026-09-20"), "harbor")
    merged = a.merge(b, on="member_id", suffixes=("_a", "_b"))
    assert (merged["record_hash_a"] == merged["record_hash_b"]).all()   # SCD2 sees no change


def test_month_boundary_produces_scd2_changes():
    sep, _ = member_files.normalize(_raw("evergreen", "2026-09-28"), "evergreen")
    octo, _ = member_files.normalize(_raw("evergreen", "2026-10-02"), "evergreen")
    merged = sep.merge(octo, on="member_id", suffixes=("_s", "_o"))
    assert (merged["record_hash_s"] != merged["record_hash_o"]).any()


def test_roster_files_are_deterministic_per_date():
    assert generate_member_files.build_file("evergreen", "2026-09-28") == \
        generate_member_files.build_file("evergreen", "2026-09-28")
