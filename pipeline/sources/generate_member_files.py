"""Daily member roster files, one per health plan (our customers), each in
that plan's own (inconsistent) format -- the classic "every partner sends a different
CSV" problem:

  evergreen: MemberID, First Name, Last Name, DOB (MM/DD/YYYY), Phone "(555) 123-4567", ...
  harbor:    member_id, fname, lname, birth_date (YYYY-MM-DD), phone digits, lowercase plan codes

Built-in mess: stray whitespace, duplicate rows, the occasional unparseable
DOB or blank member id. Plan changes and terminations happen at month
boundaries, which is what gives the SCD2 merge real work to do.
"""
import csv
import hashlib
import io
from datetime import date, timedelta

from pipeline import s3_io
from pipeline.reference_data import HEALTH_PLANS, member_roster
from pipeline.sources import rng_for

PREFIX = "raw/member_files"  # where health plans "drop" their files in S3


def _month_flag(member_id: str, month: str, modulo: int) -> bool:
    # Deterministic "coin flip" per member per month: hashing the pair means
    # the answer is the same every day of that month, and changes next month.
    return int(hashlib.md5(f"{member_id}-{month}".encode()).hexdigest(), 16) % modulo == 0


def current_state(member: dict, as_of: date) -> dict:
    """Member attributes as of a date: ~10% switch plan code in a given
    month, ~3% are termed at the end of the prior month."""
    month = as_of.strftime("%Y-%m")
    # 1 in 10 members is on their alternate plan code this month.
    plan_code = member["alt_plan_code"] if _month_flag(member["member_id"], month, 10) else member["base_plan_code"]
    coverage_end = None
    # 1 in 33 members had coverage end on the last day of the previous month.
    if _month_flag(member["member_id"], "term-" + month, 33):
        coverage_end = as_of.replace(day=1) - timedelta(days=1)
    return {**member, "plan_code": plan_code, "coverage_end": coverage_end}


def _evergreen_row(m: dict) -> dict:
    # Evergreen's layout: Title-Case headers, US dates, formatted phones,
    # padded first names and UPPERCASE last names.
    p = m["phone"]
    return {
        "MemberID": m["member_id"],
        "First Name": f" {m['first_name']} ",                   # stray spaces on purpose
        "Last Name": m["last_name"].upper(),
        "DOB": m["dob"].strftime("%m/%d/%Y"),                    # 03/14/1948
        "Gender": m["gender"],
        "Phone": f"({p[:3]}) {p[3:6]}-{p[6:]}",                  # (718) 555-0142
        "Zip": m["zip"],
        "County": m["county"],
        "Plan": m["plan_code"],
        "Eff Date": m["coverage_start"].strftime("%m/%d/%Y"),
        "Term Date": m["coverage_end"].strftime("%m/%d/%Y") if m["coverage_end"] else "",
    }


def _harbor_row(m: dict) -> dict:
    # Harbor's layout: snake_case headers, ISO dates, lowercase ids/codes,
    # ZIP+4 and UPPERCASE counties.
    return {
        "member_id": m["member_id"].lower(),                     # mem10002
        "fname": m["first_name"],
        "lname": m["last_name"],
        "birth_date": m["dob"].isoformat(),                      # 1948-03-14
        "sex": m["gender"].lower(),
        "phone": m["phone"],                                     # digits only
        "zip_code": m["zip"] + "-0000",                          # ZIP+4
        "county_name": m["county"].upper(),
        "plan_code": m["plan_code"].lower(),
        "coverage_start": m["coverage_start"].isoformat(),
        "coverage_end": m["coverage_end"].isoformat() if m["coverage_end"] else "",
    }


# Which row formatter each health plan uses.
ROW_BUILDERS = {"evergreen": _evergreen_row, "harbor": _harbor_row}


def build_file(plan_key: str, batch_date: str) -> bytes:
    # Seeded from plan + date: regenerating the same day gives the same file.
    rng = rng_for(f"elig-{plan_key}", batch_date)
    as_of = date.fromisoformat(batch_date)
    # One row per member of this plan, in the plan's own format.
    rows = [ROW_BUILDERS[plan_key](current_state(m, as_of)) for m in member_roster() if m["plan_key"] == plan_key]

    # Duplicates: partners resend rows.
    rows += [dict(r) for r in rng.sample(rows, k=2)]
    # Garbage: one bad DOB, one blank member id.
    bad = dict(rng.choice(rows))
    bad["DOB" if plan_key == "evergreen" else "birth_date"] = "00/00/0000"
    rows.append(bad)
    blank = dict(rng.choice(rows))
    blank["MemberID" if plan_key == "evergreen" else "member_id"] = ""
    rows.append(blank)
    rng.shuffle(rows)  # real files aren't sorted

    # Write the rows as CSV text in memory, then return it as bytes for S3.
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def main(batch_date: str) -> list[str]:
    keys = []
    for plan_key, _name, _codes in HEALTH_PLANS:
        # e.g. raw/member_files/dt=2026-09-28/evergreen_roster_20260928.csv
        key = f"{PREFIX}/dt={batch_date}/{plan_key}_roster_{batch_date.replace('-', '')}.csv"
        s3_io.put_bytes(key, build_file(plan_key, batch_date), "text/csv")
        keys.append(key)
    print(f"Dropped {len(keys)} member files for {batch_date}")
    return keys


if __name__ == "__main__":
    # Run standalone: python -m pipeline.sources.generate_member_files 2026-09-28
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
