"""Tag social-determinants-of-health (SDoH) needs in CHW notes.

Rule-based (versioned keyword patterns) -- transparent, cheap, and easy to
audit. Results carry method='rules' and RULE_VERSION, and every classified
note is recorded in core.note_classifications, so:
  - changing a pattern -> bump RULE_VERSION -> all notes get re-tagged
  - an edited note (newer last_modified_at) gets re-tagged
  - a different classifier can be added later as another `method` and
    compared against these results on the same notes

Notes are PHI: they're read from and written back to Redshift only, and
never printed.
"""
import re
from datetime import date, datetime, timezone

import pandas as pd

from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.redshift import fetch_all
from pipeline.schemas import MEMBER_SDOH_NEEDS, NOTE_CLASSIFICATIONS

METHOD = "rules"            # how needs were detected (a future LLM classifier would be another method)
RULE_VERSION = "2026.09.1"  # bump this after editing RULES -> every note gets re-tagged

# One regex per need. \b = word boundary, so "rent" doesn't match inside "current".
RULES = {
    "food_insecurity": r"\b(food (runs out|pantry|stamps)|skipp(ing|ed) meals|snap benefits|hungry|fridge is (nearly )?empty)\b",
    "transportation": r"\b(no ride|need(s)? a ride|transportation|bus route|can'?t get to)\b",
    "social_isolation": r"\b(lives alone|lonely|loneliness|no family nearby|hasn'?t left the house|isolated)\b",
    "housing_instability": r"\b(evict(ion|ed)?|rent|landlord|homeless|no heat|heat (is )?not working)\b",
    "medication_affordability": r"\b(can'?t afford|afford the copay|rationing|skipping doses)\b",
    "opt_out_request": r"\b(stop calling|do not call|don'?t call|remove (me |her |him )?from (the )?call list)\b",
}
# Compile once at import; re.I = case-insensitive.
_COMPILED = {k: re.compile(v, re.I) for k, v in RULES.items()}


def classify(note: str | None) -> list[str]:
    if not note:
        return []  # no note (e.g. an Open task) -> nothing to find
    # Every category whose pattern appears anywhere in the note.
    return [category for category, pattern in _COMPILED.items() if pattern.search(note)]


def notes_to_classify() -> list[tuple]:
    """Notes never tagged by this method, edited since, or tagged by older rules."""
    return fetch_all(
        """
        SELECT e.activity_id, e.member_id, e.notes, e.last_modified_at
        FROM core.engagements e
        LEFT JOIN core.note_classifications c
               ON c.activity_id = e.activity_id AND c.method = %s
        WHERE e.notes IS NOT NULL
          AND (c.activity_id IS NULL
               OR c.note_modified_at < e.last_modified_at
               OR c.rule_version <> %s);
        """,
        (METHOD, RULE_VERSION),
    )


def build_frames(rows: list[tuple], now: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    needs, classified = [], []
    # For each note: one row per need found, plus one "this note was processed" row.
    for activity_id, member_id, note, modified_at in rows:
        found = classify(note)
        needs += [{"activity_id": activity_id, "member_id": member_id, "need_category": c,
                   "method": METHOD, "detected_at": now} for c in found]
        classified.append({"activity_id": activity_id, "method": METHOD, "rule_version": RULE_VERSION,
                           "needs_found": len(found), "note_modified_at": modified_at, "classified_at": now})
    return (pd.DataFrame(needs, columns=MEMBER_SDOH_NEEDS.data_columns),
            pd.DataFrame(classified, columns=NOTE_CLASSIFICATIONS.data_columns))


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    rows = notes_to_classify()  # only new / edited / outdated notes -- not the whole history
    if not rows:
        print("No new or changed notes to tag.")
        return {"notes": 0}
    needs, classified = build_frames(rows, datetime.now(timezone.utc))
    # Replace any earlier results for these notes, atomically.
    stage_and_merge(load_id_for(f"sdoh_{METHOD}", batch_date), f"sdoh_{METHOD}",
                    [(MEMBER_SDOH_NEEDS, needs), (NOTE_CLASSIFICATIONS, classified)],
                    "core.sp_merge_note_classifications", source_uri="core.engagements.notes", rows_in=len(rows))
    # Counts per category only -- note text is PHI and is never printed.
    summary = {"notes": len(rows), "needs_found": len(needs),
               "by_category": needs["need_category"].value_counts().to_dict()}
    print(summary)
    return summary


if __name__ == "__main__":
    main()
