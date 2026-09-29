from datetime import datetime, timezone

import pytest

from mock_api.main import NOTES_COMPLETED, NOTES_NO_ANSWER
from pipeline.enrich import sdoh_rules
from pipeline.schemas import MEMBER_SDOH_NEEDS, NOTE_CLASSIFICATIONS


@pytest.mark.parametrize("note,expected", [
    ("Member mentioned food runs out before the end of the month", ["food_insecurity"]),
    ("Member has no ride to his cardiology appointment", ["transportation"]),
    ("Lives alone and feels lonely", ["social_isolation"]),
    ("Landlord is raising the rent, worried about eviction", ["housing_instability"]),
    ("Skipping doses because she can't afford the copay", ["medication_affordability"]),
    ("Member asked us to stop calling.", ["opt_out_request"]),
    ("Member doing well, no needs identified.", []),
    ("Current medications reviewed with the member's parent.", []),  # 'rent' inside words must not match
    (None, []),
])
def test_classify(note, expected):
    assert sdoh_rules.classify(note) == expected


def test_every_mock_note_is_handled():
    tagged = [n for n in NOTES_COMPLETED if sdoh_rules.classify(n)]
    assert len(tagged) == 11            # 10 needs + 1 opt-out; 3 neutral notes stay untagged
    assert all(not sdoh_rules.classify(n) for n in NOTES_NO_ANSWER)


def test_build_frames_records_every_classified_note():
    now = datetime(2026, 9, 28, tzinfo=timezone.utc)
    rows = [("00TA", "MEM10001", "Lives alone, and the landlord is raising the rent", now),
            ("00TB", "MEM10002", "Left voicemail.", now)]
    needs, classified = sdoh_rules.build_frames(rows, now)
    assert list(needs.columns) == MEMBER_SDOH_NEEDS.data_columns
    assert list(classified.columns) == NOTE_CLASSIFICATIONS.data_columns
    assert sorted(needs["need_category"]) == ["housing_instability", "social_isolation"]
    assert list(classified["needs_found"]) == [2, 0]    # the empty note is still marked as done
    assert (classified["rule_version"] == sdoh_rules.RULE_VERSION).all()
