"""Synthetic medical claims, one CSV per health plan per day -- deliberately
messy, the way claims extracts really arrive:

  - mixed date formats in one file (2026-09-15, 09/15/2026, 20260915)
  - amounts as text: "$1,234.50", "(12.50)" for reversals
  - revenue codes that lost their leading zero ("450" -- Excel did it)
  - ICD-10 codes without the dot (E119 instead of E11.9)
  - sloppy member ids, exact duplicate rows, a few unusable rows
  - RESTATEMENTS: replacements (freq 7) and voids (freq 8) of claims sent
    a week earlier -- the merge must keep only the latest version

A member's "frailty" (fixed per member) drives how often they show up in the
ER or hospital, so utilization differs realistically between members.

  python -m pipeline.sources.generate_claims 2026-09-30            # one day
  python -m pipeline.sources.generate_claims 2026-09-30 --history  # + 12-month backfill file
"""
import csv
import hashlib
import io
import random
from datetime import date, timedelta

from pipeline import s3_io
from pipeline.reference_data import CLAIM_DX_CODES, HEALTH_PLANS, VISIT_TYPES, member_roster
from pipeline.sources import rng_for

PREFIX = "raw/claims"
COLUMNS = ["CLAIM_ID", "FREQ_CD", "MEMBER_ID", "CLAIM_TYPE", "POS", "REV_CD", "CPT", "DX1",
           "FROM_DT", "THRU_DT", "BILLED_AMT", "PAID_AMT", "RECEIVED_DT"]


def frailty(member_id: str) -> float:
    # 0.0-1.0, skewed low: most members rarely use the ER, a few use it a lot.
    return (int(hashlib.md5(("frailty-" + member_id).encode()).hexdigest(), 16) % 100 / 100) ** 2


def _messy_date(rng: random.Random, d: date) -> str:
    # Same date, three formats -- whichever the upstream system felt like.
    return rng.choice([d.isoformat(), d.strftime("%m/%d/%Y"), d.strftime("%Y%m%d")])


def _messy_money(rng: random.Random, amount: float) -> str:
    if amount < 0:
        return f"({abs(amount):.2f})"                       # accounting-style negative
    return rng.choice([f"{amount:.2f}", f"${amount:,.2f}"])  # sometimes with $ and commas


def _visit_rows(plan_key: str, received: date, rng: random.Random) -> list[dict]:
    """Original (freq 1) claims received on one day for one plan."""
    members = [m["member_id"] for m in member_roster() if m["plan_key"] == plan_key]
    rows = []
    for n in range(1, rng.randint(6, 12) + 1):
        member = rng.choice(members)
        f = frailty(member)
        # Frail members are far more likely to have ER / inpatient claims.
        visit = rng.choices(["er", "inpatient", "office", "wellness", "lab"],
                            weights=[0.04 + 0.5 * f, 0.01 + 0.2 * f, 0.5, 0.15, 0.3])[0]
        claim_type, pos, rev, cpt, billed = VISIT_TYPES[visit]
        service_from = received - timedelta(days=rng.randint(3, 60))  # claims lag behind care
        service_to = service_from + timedelta(days=rng.randint(2, 7) if visit == "inpatient" else 0)
        billed = round(billed * rng.uniform(0.8, 1.3), 2)
        rows.append({
            "CLAIM_ID": f"{plan_key[:3].upper()}{received:%Y%m%d}{n:04d}",
            "FREQ_CD": "1",
            "MEMBER_ID": member,
            "CLAIM_TYPE": claim_type,
            "POS": pos,
            "REV_CD": rev,
            "CPT": cpt,
            "DX1": rng.choice(CLAIM_DX_CODES),
            "FROM_DT": service_from,
            "THRU_DT": service_to,
            "BILLED_AMT": billed,
            "PAID_AMT": round(billed * rng.uniform(0.4, 0.8), 2),
            "RECEIVED_DT": received,
        })
    return rows


def build_rows(plan_key: str, received: date) -> list[dict]:
    """One day's file: new claims + restatements of last week's claims + mess."""
    rng = rng_for(f"claims-{plan_key}", received.isoformat())
    rows = _visit_rows(plan_key, received, rng)

    # Restatements: re-derive the claims received 7 days ago (deterministic),
    # then replace one (freq 7, new paid amount) and maybe void another (freq 8).
    week_ago = received - timedelta(days=7)
    previous = _visit_rows(plan_key, week_ago, rng_for(f"claims-{plan_key}", week_ago.isoformat()))
    if previous:
        replaced = dict(rng.choice(previous), FREQ_CD="7", RECEIVED_DT=received)
        replaced["PAID_AMT"] = round(replaced["PAID_AMT"] * 0.9, 2)
        rows.append(replaced)
        if rng.random() < 0.5:
            voided = dict(rng.choice(previous), FREQ_CD="8", RECEIVED_DT=received)
            voided["PAID_AMT"] = -voided["PAID_AMT"]                 # reversal
            rows.append(voided)

    out = []
    for r in rows:
        member = r["MEMBER_ID"]
        out.append({
            **r,
            "MEMBER_ID": f" {member.lower()}" if rng.random() < 0.1 else member,   # sloppy ids
            "REV_CD": r["REV_CD"].lstrip("0") if r["REV_CD"] and rng.random() < 0.3 else r["REV_CD"],
            "DX1": r["DX1"].replace(".", "") if rng.random() < 0.4 else r["DX1"],  # dot dropped
            "FROM_DT": _messy_date(rng, r["FROM_DT"]),
            "THRU_DT": _messy_date(rng, r["THRU_DT"]),
            "BILLED_AMT": _messy_money(rng, r["BILLED_AMT"]),
            "PAID_AMT": _messy_money(rng, r["PAID_AMT"]),
            "RECEIVED_DT": _messy_date(rng, r["RECEIVED_DT"]),
        })

    if out:
        out.append(dict(rng.choice(out)))                                  # resent duplicate row
        out.append(dict(rng.choice(out), CLAIM_ID=""))                     # unusable: no claim id
        out.append(dict(rng.choice(out), CLAIM_ID=f"BAD{received:%Y%m%d}", FROM_DT="13/45/2026"))  # bad date
    rng.shuffle(out)
    return out


def to_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def main(batch_date: str, history: bool = False) -> list[str]:
    day = date.fromisoformat(batch_date)
    keys = []
    for plan_key, _name, _codes in HEALTH_PLANS:
        key = f"{PREFIX}/dt={batch_date}/claims_{plan_key}_{day:%Y%m%d}.csv"
        s3_io.put_bytes(key, to_csv(build_rows(plan_key, day)), "text/csv")
        keys.append(key)
        if history:
            # Onboarding backfill: the previous 12 months of daily files in one file.
            rows = [r for d in range(365, 0, -1) for r in build_rows(plan_key, day - timedelta(days=d))]
            hkey = f"{PREFIX}/dt={batch_date}/claims_{plan_key}_history_{day:%Y%m%d}.csv"
            s3_io.put_bytes(hkey, to_csv(rows), "text/csv")
            keys.append(hkey)
    print(f"Dropped {len(keys)} claims files for {batch_date}" + (" (incl. 12-month history)" if history else ""))
    return keys


if __name__ == "__main__":
    import sys
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    main(args[0] if args else date.today().isoformat(), history="--history" in sys.argv)
