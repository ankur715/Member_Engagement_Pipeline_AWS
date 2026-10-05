"""Health-plan medical claims -> core.claims (latest version per claim).

Cleanup is all pandas and unit-tested without AWS:
  - dates in any of three formats -> real dates (unparseable -> reject)
  - "$1,234.50" / "(12.50)" -> exact decimals
  - revenue codes re-padded to 4 digits ("450" -> "0450")
  - ICD-10 codes re-dotted ("E119" -> "E11.9")
  - member ids trimmed/upper-cased; exact duplicate rows collapsed
  - freq code -> claim_status (1 paid, 7 adjusted, 8 void)
Rows without a claim id, member id or service date go to rejects/ with a
reason. Versioning (which restatement wins) happens in core.sp_merge_claims.
"""
import io
from datetime import date

import pandas as pd

from pipeline import s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.reference_data import HEALTH_PLANS
from pipeline.schemas import CLAIMS as SPEC

PREFIX = "raw/claims"                                         # where health plans' claims files land
PLAN_NAMES = {key: name for key, name, _ in HEALTH_PLANS}     # "evergreen" -> "Evergreen Health Plan"
# Claim frequency code (from the UB-04/837 standard): 1 original, 7 replacement, 8 void.
STATUS_BY_FREQ = {"1": "paid", "7": "adjusted", "8": "void"}
CLAIM_TYPES = {"P": "professional", "I": "institutional"}     # doctor/office vs hospital/facility


def _to_money(s: pd.Series) -> pd.Series:
    # "(12.50)" -> -12.50 ; "$1,234.50" -> 1234.50 ; "" -> NaN
    neg = s.str.startswith("(") & s.str.endswith(")")       # accounting style: (x) means negative
    num = pd.to_numeric(s.str.replace(r"[$,()\s]", "", regex=True), errors="coerce")
    return num.where(~neg, -num)


def _to_date(s: pd.Series) -> pd.Series:
    # format="mixed" parses each value on its own (ISO, US, or compact YYYYMMDD).
    return pd.to_datetime(s, format="mixed", errors="coerce").dt.date


def _dot_icd10(code):
    # ICD-10 codes have a dot after the 3rd character when longer than 3 chars.
    if pd.isna(code) or not code:
        return None
    code = code.replace(".", "").upper()
    return code if len(code) <= 3 else f"{code[:3]}.{code[3:]}"


def normalize(raw: pd.DataFrame, plan_key: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejects). Pure pandas -- unit tested without AWS."""
    # Everything as trimmed text first; blank cells become NA (missing).
    df = raw.astype("string").apply(lambda s: s.str.strip()).replace({"": pd.NA})

    out = pd.DataFrame({
        "claim_id": df["CLAIM_ID"].str.upper(),
        "member_id": df["MEMBER_ID"].str.upper(),
        "health_plan": PLAN_NAMES[plan_key],
        "claim_type": df["CLAIM_TYPE"].str.upper().map(CLAIM_TYPES),
        "place_of_service": df["POS"].str.zfill(2),           # "9" -> "09" (POS codes are 2 digits)
        # Excel-mangled revenue codes: numeric and short -> zero-pad back to 4.
        "revenue_code": df["REV_CD"].where(df["REV_CD"].isna(), df["REV_CD"].str.zfill(4)),
        "cpt_code": df["CPT"],
        "primary_dx": df["DX1"].map(_dot_icd10),
        "service_from": _to_date(df["FROM_DT"]),
        "service_to": _to_date(df["THRU_DT"]),
        "billed_amount": _to_money(df["BILLED_AMT"]),
        "paid_amount": _to_money(df["PAID_AMT"]),
        "freq_code": df["FREQ_CD"].fillna("1"),               # missing -> original claim
        "received_date": _to_date(df["RECEIVED_DT"]),
    }, index=df.index)
    out["claim_status"] = out["freq_code"].map(STATUS_BY_FREQ).fillna("paid")

    # Reject rows we can't use, with every reason that applies.
    reasons = pd.Series("", index=out.index)
    reasons[out["claim_id"].isna()] += "missing_claim_id;"
    reasons[out["member_id"].isna()] += "missing_member_id;"
    reasons[out["service_from"].isna()] += "invalid_service_date;"
    reasons[out["claim_type"].isna()] += "unknown_claim_type;"
    bad = reasons != ""
    rejects = raw.loc[bad].assign(reject_reason=reasons[bad])

    clean = out.loc[~bad].drop_duplicates()  # resent identical rows -> one
    return clean.reset_index(drop=True), rejects


def load_file(key: str, batch_date: str) -> dict:
    # "raw/claims/dt=.../claims_evergreen_20260930.csv" -> "evergreen"
    plan_key = key.rsplit("/", 1)[-1].split("_")[1]
    # dtype=str + keep_default_na=False: read every cell as-is, so pandas can't
    # strip leading zeros from codes or turn "NA" text into missing values.
    raw = pd.read_csv(io.BytesIO(s3_io.get_bytes(key)), dtype=str, keep_default_na=False)
    clean, rejects = normalize(raw, plan_key)

    name = key.rsplit("/", 1)[-1].removesuffix(".csv")
    if len(rejects):
        s3_io.put_bytes(f"rejects/claims/dt={batch_date}/{name}_rejects.csv",
                        rejects.to_csv(index=False).encode(), "text/csv")

    # A received date the file didn't provide falls back to the file's date.
    clean["received_date"] = clean["received_date"].fillna(date.fromisoformat(batch_date))
    clean = clean.assign(source_file=s3_io.uri(key))

    # One load per file (history and daily files load independently).
    stage_and_merge(load_id_for(f"claims-{name}", batch_date)[:64], f"claims_{plan_key}",
                    [(SPEC, clean)], "core.sp_merge_claims",
                    source_uri=s3_io.uri(key), rows_in=len(raw), rows_rejected=len(rejects))
    summary = {"file": name, "rows_in": len(raw), "loaded": len(clean), "rejected": len(rejects)}
    print(summary)  # counts only -- never claim-level PHI
    return summary


def main(batch_date: str) -> list[dict]:
    keys = s3_io.list_keys(f"{PREFIX}/dt={batch_date}/")
    if not keys:
        raise FileNotFoundError(f"No claims files for {batch_date} -- health-plan drop missing?")
    # History files first, so the day's restatements land on top of them.
    keys.sort(key=lambda k: (0 if "_history_" in k else 1, k))
    return [load_file(k, batch_date) for k in keys]


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
