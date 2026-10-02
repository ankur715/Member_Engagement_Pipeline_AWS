import json
from datetime import date, timedelta

import pytest

from pipeline.ingest import hra
from pipeline.sources import generate_hra


def _year_of_rows(end=date(2026, 9, 30)):
    return [r for d in range(365) for r in generate_hra.build_rows(end - timedelta(days=d))]


@pytest.mark.parametrize("v,expected", [("Yes", True), ("Y", True), (True, True), (1, True),
                                        ("no", False), ("N", False), (False, False), (0, False),
                                        ("", None), (None, None), ("maybe", None)])
def test_yes_no(v, expected):
    assert hra.yes_no(v) is expected


@pytest.mark.parametrize("v,expected", [("Uses walker", "severe"), ("Wheelchair", "severe"),
                                        ("Can't leave home without help", "severe"),
                                        ("Uses a cane", "some"), ("Slow on stairs", "some"),
                                        ("Some difficulty walking", "some"),
                                        ("No difficulty", "none"), ("Walks fine", "none"), ("", None)])
def test_mobility(v, expected):
    assert hra.mobility_level(v) == expected


def test_unreliable_heat_counts_as_no_heat():
    assert hra.working_heat("Sometimes") is False
    assert hra.working_heat("No heat since Nov") is False
    assert hra.working_heat("Working fine") is True


def test_fan_only_is_not_ac():
    assert hra.has_ac("fan only") is False
    assert hra.has_ac("window unit") is True
    assert hra.has_ac("no ac") is False


def test_cost_burden():
    assert hra.cost_burden("Often can't pay") is True
    assert hra.cost_burden("Never") is False


def test_normalize_a_year_of_vendor_files():
    clean, rejects = hra.normalize(_year_of_rows())
    assert len(clean) > 30
    assert clean["member_id"].str.fullmatch(r"MEM\d{5}").all()
    assert set(rejects["reject_reason"]) <= {"missing_member_id;invalid_submitted_at;no_answers;"}
    assert clean["submitted_at"].notna().all() and (clean["updated_at"] >= clean["submitted_at"]).all()
    assert set(clean["mobility_level"].dropna()) <= {"none", "some", "severe"}
    # Partial surveys keep their answered items; skipped ones stay NULL, not guessed.
    partial = clean[~clean["is_complete"]]
    assert len(partial) > 0 and partial["has_ac"].isna().all()


def test_corrected_resend_has_later_updated_at():
    clean, _ = hra.normalize(_year_of_rows())
    resent = clean[clean.duplicated("response_id", keep=False)].sort_values(["response_id", "updated_at"])
    assert len(resent) > 0
    for _, grp in resent.groupby("response_id"):
        assert grp["updated_at"].is_monotonic_increasing and grp["updated_at"].nunique() == len(grp)
        assert grp.iloc[-1]["is_complete"]          # the correction is the complete version


def test_answers_match_the_member_profile():
    clean, _ = hra.normalize(_year_of_rows())
    for _, row in clean.iterrows():
        p = generate_hra.member_profile(row["member_id"])
        if row["lives_alone"] is not None and not isinstance(row["lives_alone"], float):
            assert row["lives_alone"] == p["lives_alone"]


def test_empty_file_normalizes_to_contract_columns():
    clean, rejects = hra.normalize([])
    assert clean.empty and rejects.empty


def test_jsonl_round_trip():
    rows = generate_hra.build_rows(date(2026, 9, 30))
    text = generate_hra.to_jsonl(rows).decode()
    assert [json.loads(line) for line in text.splitlines() if line] == rows
