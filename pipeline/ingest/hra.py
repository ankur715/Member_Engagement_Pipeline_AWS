"""Health risk assessment (HRA) survey responses -> core.hra_responses.

The vendor's JSON Lines file nests answers under "answers"; pandas flattens
them, then each free-text answer is mapped onto a small, typed vocabulary:

  lives_alone          "Yes"/"Y"/true/1 -> True ; "No"/"N"/false/0 -> False
  mobility_level       walker/wheelchair/homebound -> severe ; cane/some/slow -> some ; fine -> none
  has_working_heat     "Sometimes" counts as FALSE -- unreliable heat is a risk in a cold snap
  has_ac               "fan only" / "none" -> FALSE
  utility_cost_burden  "Often can't pay"/"Sometimes" -> True

Unrecognised or skipped answers stay NULL (unknown), never guessed.
"""
import json
from datetime import date

import pandas as pd

from pipeline import s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.schemas import HRA_RESPONSES as SPEC

PREFIX = "raw/hra"         # where the survey vendor's daily files land in S3

# Survey questions we map (after flattening "answers.q_x" -> "q_x").
ANSWER_COLUMNS = ["q_lives_alone", "q_mobility", "q_heat_working", "q_cooling", "q_utility_cost"]


def _text(v) -> str:
    # Normalize any answer to trimmed lower-case text; None/NaN -> "" (blank).
    return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip().lower()


# Each mapper below returns True/False (or a level) when the answer is clear,
# and None when it's blank or unrecognised -- unknown is better than a guess.
def yes_no(v):
    t = _text(v)
    if t in {"yes", "y", "true", "1"}:
        return True
    if t in {"no", "n", "false", "0"}:
        return False
    return None


def mobility_level(v):
    t = _text(v)
    if not t:
        return None
    # Checked most-severe first, so "slow, uses a walker" -> severe, not some.
    if any(k in t for k in ("walker", "wheelchair", "can't leave", "cannot leave", "homebound", "bedbound")):
        return "severe"
    if t in {"none", "no"} or "no difficulty" in t or "fine" in t:
        return "none"
    if any(k in t for k in ("cane", "some", "slow", "difficulty")):
        return "some"
    return None


def working_heat(v):
    t = _text(v)
    if not t:
        return None
    if t.startswith("no") or "sometimes" in t:   # "No", "No heat since Nov", "Sometimes"
        return False
    if t in {"yes", "y"} or "working" in t:
        return True
    return None


def has_ac(v):
    t = _text(v)
    if not t:
        return None
    if "fan" in t or t in {"none", "no", "no ac"}:   # a fan is not air conditioning
        return False
    if "ac" in t or "central" in t or "window" in t:
        return True
    return None


def cost_burden(v):
    t = _text(v)
    if not t:
        return None
    if "often" in t or "sometimes" in t or "can't" in t:
        return True
    if "never" in t or "no problem" in t:
        return False
    return None


def normalize(records: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejects). Pure pandas -- unit tested without AWS."""
    if not records:
        return pd.DataFrame(columns=SPEC.data_columns), pd.DataFrame()
    raw = pd.json_normalize(records)                       # answers.q_mobility -> its own column
    raw.columns = [c.replace("answers.", "") for c in raw.columns]   # "answers.q_mobility" -> "q_mobility"
    for col in ANSWER_COLUMNS:                              # surveys missing a question entirely
        if col not in raw.columns:
            raw[col] = None

    # format="mixed": the vendor sends several date formats; unparseable -> NaT (then rejected).
    submitted = pd.to_datetime(raw["submitted_at"], format="mixed", errors="coerce", utc=True)
    updated = pd.to_datetime(raw["updated_at"], format="mixed", errors="coerce", utc=True)
    df = pd.DataFrame({
        "response_id": raw["response_id"].astype("string").str.strip(),
        "member_id": raw["member_ref"].astype("string").str.strip().str.upper().replace({"": pd.NA}),  # "mem10001" -> "MEM10001"
        "submitted_at": submitted,
        "updated_at": updated.fillna(submitted),            # no correction timestamp -> submission time
        "is_complete": raw["status"].astype("string").str.lower().eq("complete"),
        "lives_alone": raw["q_lives_alone"].map(yes_no),
        "mobility_level": raw["q_mobility"].map(mobility_level),
        "has_working_heat": raw["q_heat_working"].map(working_heat),
        "has_ac": raw["q_cooling"].map(has_ac),
        "utility_cost_burden": raw["q_utility_cost"].map(cost_burden),
    })

    # Reject rows we can't use; a row can collect several reasons.
    reasons = pd.Series("", index=df.index)
    reasons[df["member_id"].isna()] += "missing_member_id;"
    reasons[df["submitted_at"].isna()] += "invalid_submitted_at;"
    # A survey where every question is blank or unrecognised tells us nothing.
    answered = df[["lives_alone", "mobility_level", "has_working_heat", "has_ac", "utility_cost_burden"]].notna().any(axis=1)
    reasons[~answered] += "no_answers;"
    bad = reasons != ""
    rejects = raw.loc[bad].assign(reject_reason=reasons[bad])
    return df.loc[~bad].reset_index(drop=True), rejects


def load_file(key: str, batch_date: str) -> dict:
    # One JSON object per line. Parsed with json (not pd.read_json, which would
    # silently auto-convert *_at columns before our own mixed-format parsing).
    records = [json.loads(line) for line in s3_io.get_bytes(key).decode().splitlines() if line.strip()]
    clean, rejects = normalize(records)
    name = key.rsplit("/", 1)[-1].removesuffix(".jsonl")   # file name without folder or extension
    if len(rejects):
        s3_io.put_bytes(f"rejects/hra/dt={batch_date}/{name}_rejects.csv",
                        rejects.to_csv(index=False).encode(), "text/csv")
    clean = clean.assign(source_file=s3_io.uri(key))      # lineage: every row knows its source file
    # The merge keeps the latest version of each response (by updated_at), so a
    # vendor correction replaces the original and an older file can't undo it.
    stage_and_merge(load_id_for(f"hra-{name}", batch_date)[:64], "hra_responses",   # [:64] = load_id column width
                    [(SPEC, clean)], "core.sp_merge_hra_responses",
                    source_uri=s3_io.uri(key), rows_in=len(records), rows_rejected=len(rejects))
    summary = {"file": name, "rows_in": len(records), "loaded": len(clean), "rejected": len(rejects)}
    print(summary)  # counts only -- survey answers are PHI
    return summary


def main(batch_date: str) -> list[dict]:
    keys = s3_io.list_keys(f"{PREFIX}/dt={batch_date}/")
    if not keys:
        raise FileNotFoundError(f"No HRA file for {batch_date} -- survey vendor drop missing?")
    keys.sort(key=lambda k: (0 if "_history_" in k else 1, k))   # history first, then the day's file
    return [load_file(k, batch_date) for k in keys]


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
