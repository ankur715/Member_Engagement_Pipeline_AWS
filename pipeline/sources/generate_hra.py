"""Synthetic health risk assessment (HRA) survey responses, delivered by a
survey vendor as one JSON Lines file per day -- with the mess real survey
exports have:

  - yes/no answers as "Yes", "Y", "no", true, 1, "" ...
  - free-text mobility and heating/cooling answers
  - partial surveys (skipped questions are null)
  - member ids typed by hand; corrected resends under the same response_id

Each member completes an HRA about once a year (on a fixed, hashed day), and
answers are fixed per member so later surveys stay consistent. Frailer
members (same frailty as the claims feed) report more mobility trouble.

  python -m pipeline.sources.generate_hra 2026-09-30            # one day
  python -m pipeline.sources.generate_hra 2026-09-30 --history  # + 12-month backfill
"""
import hashlib
import json
import random
from datetime import date, datetime, time, timedelta, timezone

from pipeline import s3_io
from pipeline.reference_data import member_roster
from pipeline.sources import rng_for
from pipeline.sources.generate_claims import frailty

PREFIX = "raw/hra"

YES = ["Yes", "Y", "yes", True, 1]
NO = ["No", "N", "no", False, 0]
MOBILITY = {
    "none": ["No difficulty", "none", "Walks fine"],
    "some": ["Some difficulty walking", "Uses a cane", "Slow on stairs"],
    "severe": ["Uses walker", "Wheelchair", "Can't leave home without help"],
}
HEAT = {True: ["Yes", "Working fine"], False: ["No", "Sometimes", "No heat since Nov"]}
COOLING = {True: ["Central AC", "window unit", "Window AC"], False: ["fan only", "None", "no ac"]}
COST = {True: ["Often can't pay", "Sometimes"], False: ["Never", "No problem"]}


def _h(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


def member_profile(member_id: str) -> dict:
    """A member's true circumstances (stable across their surveys)."""
    f = frailty(member_id)
    r = random.Random(_h("hra-profile-" + member_id))
    return {
        "lives_alone": r.random() < 0.35,
        "mobility": "severe" if r.random() < 0.1 + 0.4 * f else ("some" if r.random() < 0.3 else "none"),
        "heat": r.random() > 0.12,
        "cooling": r.random() > 0.25,
        "cost_burden": r.random() < 0.3,
    }


def survey_day_of_year(member_id: str) -> int:
    return _h("hra-day-" + member_id) % 365


def _responses(day: date) -> list[dict]:
    """Real survey responses submitted on one day (members whose survey day is today)."""
    rng = rng_for("hra", day.isoformat())
    rows = []
    for m in member_roster():
        mid = m["member_id"]
        if survey_day_of_year(mid) != day.timetuple().tm_yday - 1:
            continue                                   # not this member's survey day
        if _h(f"hra-skip-{mid}-{day.year}") % 5 == 0:
            continue                                   # ~1 in 5 members skip their HRA this year
        p = member_profile(mid)
        partial = rng.random() < 0.15
        submitted = datetime.combine(day, time(rng.randint(9, 19), rng.randint(0, 59)), tzinfo=timezone.utc)
        answers = {
            "q_lives_alone": rng.choice(YES if p["lives_alone"] else NO),
            "q_mobility": rng.choice(MOBILITY[p["mobility"]]),
            "q_heat_working": rng.choice(HEAT[p["heat"]]),
            "q_cooling": rng.choice(COOLING[p["cooling"]]),
            "q_utility_cost": rng.choice(COST[p["cost_burden"]]),
        }
        if partial:                                    # skipped the second half of the survey
            answers.update({"q_cooling": None, "q_utility_cost": None})
        rows.append({
            "response_id": f"HRA-{mid[3:]}-{day:%Y%m%d}",
            "member_ref": f" {mid.lower()}" if rng.random() < 0.15 else mid,      # hand-typed id
            "status": "partial" if partial else "complete",
            "submitted_at": rng.choice([submitted.isoformat(), submitted.strftime("%m/%d/%Y %I:%M %p")]),
            "updated_at": submitted.isoformat(),
            "answers": answers,
        })
    return rows


def build_rows(day: date) -> list[dict]:
    """One day's vendor file: today's responses + a corrected resend + maybe garbage."""
    rng = rng_for("hra-extras", day.isoformat())
    rows = _responses(day)
    # Correction: the vendor re-sends a response from 3 days ago, now complete,
    # with a later updated_at -- the merge must keep this version.
    earlier = _responses(day - timedelta(days=3))
    if earlier:
        fixed = json.loads(json.dumps(earlier[0]))
        member = fixed["member_ref"].strip().upper()
        fixed["status"] = "complete"
        fixed["updated_at"] = datetime.combine(day, time(8, 0), tzinfo=timezone.utc).isoformat()
        fixed["answers"]["q_cooling"] = COOLING[member_profile(member)["cooling"]][0]
        fixed["answers"]["q_utility_cost"] = COST[member_profile(member)["cost_burden"]][0]
        rows.append(fixed)
    if rows and rng.random() < 0.3:                    # unusable record: no member, bad date
        rows.append({"response_id": f"HRA-X-{day:%Y%m%d}", "member_ref": "", "status": "complete",
                     "submitted_at": "not a date", "updated_at": "", "answers": {}})
    return rows


def to_jsonl(rows: list[dict]) -> bytes:
    return "\n".join(json.dumps(r) for r in rows).encode()


def main(batch_date: str, history: bool = False) -> list[str]:
    day = date.fromisoformat(batch_date)
    keys = []
    key = f"{PREFIX}/dt={batch_date}/hra_responses_{day:%Y%m%d}.jsonl"
    s3_io.put_bytes(key, to_jsonl(build_rows(day)), "application/x-ndjson")
    keys.append(key)
    if history:
        rows = [r for d in range(365, 0, -1) for r in build_rows(day - timedelta(days=d))]
        hkey = f"{PREFIX}/dt={batch_date}/hra_responses_history_{day:%Y%m%d}.jsonl"
        s3_io.put_bytes(hkey, to_jsonl(rows), "application/x-ndjson")
        keys.append(hkey)
    print(f"Dropped {len(keys)} HRA file(s) for {batch_date}" + (" (incl. 12-month history)" if history else ""))
    return keys


if __name__ == "__main__":
    import sys
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    main(args[0] if args else date.today().isoformat(), history="--history" in sys.argv)
