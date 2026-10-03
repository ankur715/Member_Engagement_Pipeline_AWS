"""Open NYC housing-code violations (HPD, NYC Open Data) -> core.housing_violations.

Public data, so no PHI -- but the same discipline applies: raw pages land in
S3, the pull is paged, and the load is one transaction (snapshot replace).

Pull: Socrata SoQL against dataset wvxf-dwi5, filtered server-side to open
class B/C violations from the last 3 years in the ZIPs where members live.
Paged with $limit/$offset until a short page comes back.

Cleanup: ZIP+4 -> 5-digit ZIP; blank ZIP -> reject (can't join to members);
class upper-cased (A/B/C only); heat and hot-water violations flagged by
their Housing Maintenance Code sections (§27-2029 heat, §27-2031 hot water),
which catches the "ADM CODE" / "ADMIN. CODE:" wording variants.
"""
import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from pipeline import config, http, s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.redshift import fetch_all
from pipeline.schemas import HOUSING_VIOLATIONS as SPEC

SOURCE = "housing_violations"
DATASET = "wvxf-dwi5"
PAGE_SIZE = 1000
FIELDS = "violationid,zip,boro,class,inspectiondate,novdescription,violationstatus"
HEAT_SECTIONS = ("27-2029", "27-2031")


def soql_where(zips: list[str], since: date) -> str:
    zip_list = ",".join(f"'{z}'" for z in sorted(zips))
    return (f"violationstatus='Open' AND class in('B','C') "
            f"AND inspectiondate > '{since.isoformat()}' AND zip in({zip_list})")


def fetch_pages(zips: list[str], since: date):
    s = http.session(auth=False)                       # public API: no bearer token
    if config.NYC_OPEN_DATA_APP_TOKEN:
        s.headers["X-App-Token"] = config.NYC_OPEN_DATA_APP_TOKEN
    url = f"{config.NYC_OPEN_DATA_URL.rstrip('/')}/resource/{DATASET}.json"
    offset = 0
    while True:
        page = http.get_json(s, url, {"$select": FIELDS, "$where": soql_where(zips, since),
                                      "$order": "violationid", "$limit": PAGE_SIZE, "$offset": offset})
        yield page
        if len(page) < PAGE_SIZE:                      # short page = last page
            return
        offset += PAGE_SIZE


def normalize(rows: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejects). Pure pandas -- unit tested without the API."""
    if not rows:
        return pd.DataFrame(columns=SPEC.data_columns), pd.DataFrame()
    raw = pd.DataFrame(rows).astype("string")
    for col in ("zip", "boro", "class", "inspectiondate", "novdescription"):
        if col not in raw.columns:                     # Socrata omits null fields entirely
            raw[col] = pd.NA
    desc = raw["novdescription"].fillna("")
    df = pd.DataFrame({
        "violation_id": raw["violationid"].str.strip(),
        "zip": raw["zip"].str.strip().str[:5].replace({"": pd.NA}),
        "boro": raw["boro"].str.strip().str.upper(),
        "violation_class": raw["class"].str.strip().str.upper(),
        "is_heat_hot_water": desc.str.contains("|".join(HEAT_SECTIONS), regex=True),
        "inspection_date": pd.to_datetime(raw["inspectiondate"], errors="coerce").dt.date,
        "nov_description": desc.str.slice(0, 500),
    })
    reasons = pd.Series("", index=df.index)
    reasons[df["violation_id"].isna()] += "missing_violation_id;"
    reasons[df["zip"].isna() | ~df["zip"].fillna("").str.fullmatch(r"\d{5}")] += "invalid_zip;"
    reasons[~df["violation_class"].isin(["A", "B", "C"])] += "invalid_class;"
    bad = reasons != ""
    rejects = raw.loc[bad].assign(reject_reason=reasons[bad])
    clean = df.loc[~bad].drop_duplicates(subset=["violation_id"])
    return clean.reset_index(drop=True), rejects


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    # Only ZIPs where current members live -- keeps the public pull small and relevant.
    zips = [r[0] for r in fetch_all("SELECT DISTINCT zip FROM core.member_eligibility WHERE is_current AND zip IS NOT NULL;")]
    since = date.fromisoformat(batch_date) - timedelta(days=3 * 365)

    rows = []
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for n, page in enumerate(fetch_pages(zips, since)):
        s3_io.put_bytes(f"raw/{SOURCE}/dt={batch_date}/{run_ts}_page{n:03d}.json",
                        json.dumps(page).encode(), "application/json")
        rows.extend(page)

    clean, rejects = normalize(rows)
    if len(rejects):
        s3_io.put_bytes(f"rejects/{SOURCE}/dt={batch_date}/rejects.csv", rejects.to_csv(index=False).encode(), "text/csv")
    stage_and_merge(load_id_for(SOURCE, batch_date), SOURCE, [(SPEC, clean)], "core.sp_merge_housing_violations",
                    source_uri=f"{config.NYC_OPEN_DATA_URL.rstrip('/')}/resource/{DATASET}.json",
                    rows_in=len(rows), rows_rejected=len(rejects))
    summary = {"zips_queried": len(zips), "open_violations": len(clean), "heat_hot_water": int(clean["is_heat_hot_water"].sum()),
               "rejected": len(rejects)}
    print(summary)
    return summary


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
