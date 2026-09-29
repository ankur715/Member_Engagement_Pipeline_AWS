"""Community Health Worker activity from Salesforce (Task object), pulled
incrementally.

- Real Salesforce when SF_USERNAME is set (simple-salesforce; a free
  Developer Edition org works with a Member_ID__c custom field on Task).
  Otherwise the Salesforce-shaped mock in mock_api/.
- Incremental on LastModifiedDate > watermark; handles Salesforce
  pagination (done / nextRecordsUrl).
- Every raw page lands in S3 before transformation -> replayable.
"""
import json
from datetime import date, datetime, timezone
from typing import Callable, Iterator

import pandas as pd

from pipeline import config, http, s3_io, watermarks
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.schemas import ENGAGEMENTS

SOURCE = "salesforce_activities"  # name used for the watermark, S3 prefix and audit rows
# Salesforce query (SOQL): only Tasks changed since the last run, oldest first.
# {wm} is filled in with the watermark timestamp at run time.
SOQL = (
    "SELECT Id, Member_ID__c, Type, Subject, Status, ActivityDate, Description, Owner.Name, LastModifiedDate "
    "FROM Task WHERE LastModifiedDate > {wm} ORDER BY LastModifiedDate"
)


def paginate(first_page: dict, fetch_next: Callable[[str], dict]) -> Iterator[dict]:
    # Salesforce returns big results in pages: each page says "done": true/false,
    # and if not done, gives a nextRecordsUrl to fetch the next page.
    page = first_page
    while True:
        yield page                                   # hand this page to the caller
        if page.get("done", True) or not page.get("nextRecordsUrl"):
            return                                   # last page -> stop
        page = fetch_next(page["nextRecordsUrl"])    # follow the link to the next page


def _salesforce_client():
    # Real Salesforce via the simple-salesforce library (imported only when used).
    from simple_salesforce import Salesforce
    sf = Salesforce(username=config.SF_USERNAME, password=config.SF_PASSWORD,
                    security_token=config.SF_SECURITY_TOKEN, domain=config.SF_DOMAIN)
    # Return two functions: "run this query" and "fetch the next page".
    return (lambda soql: sf.query(soql),
            lambda url: sf.query_more(url, identifier_is_url=True))


def _mock_client():
    # Same two functions, but against the local mock that mimics Salesforce's REST API.
    s = http.session()
    base = config.MOCK_API_URL.rstrip("/")
    return (lambda soql: http.get_json(s, f"{base}/services/data/v60.0/query", {"q": soql}),
            lambda url: http.get_json(s, f"{base}{url}"))


def to_frame(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=ENGAGEMENTS.data_columns)  # empty, but with the right columns
    # json_normalize flattens nested JSON: {"Owner": {"Name": "x"}} -> column "Owner.Name".
    df = pd.json_normalize(records)
    # CHW notes: trimmed, capped at the column size (VARCHAR 4000).
    notes = df["Description"].astype("string").str.strip().str.slice(0, 4000)
    return pd.DataFrame({
        "activity_id": df["Id"],                                  # Salesforce's 18-char record id
        "member_id": df["Member_ID__c"].astype("string").str.strip().str.upper(),  # hand-typed -> cleaned
        "activity_type": df["Type"].fillna("Other"),
        "subject": df.get("Subject"),
        "status": df["Status"],
        "activity_date": pd.to_datetime(df["ActivityDate"]).dt.date,
        "owner_name": df.get("Owner.Name"),
        "notes": notes.replace({"": pd.NA}),                      # blank note -> NULL
        "last_modified_at": pd.to_datetime(df["LastModifiedDate"], utc=True),  # drives the watermark
    })


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    watermark = watermarks.get(SOURCE)  # last LastModifiedDate we loaded
    # Real Salesforce if credentials are configured, otherwise the mock.
    query, query_more = _salesforce_client() if config.SF_USERNAME else _mock_client()
    soql = SOQL.format(wm=watermark.strftime("%Y-%m-%dT%H:%M:%SZ"))

    records = []
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    for n, page in enumerate(paginate(query(soql), query_more)):
        # Land every raw page in S3 before transforming it -> we can always replay.
        s3_io.put_bytes(f"raw/{SOURCE}/dt={batch_date}/{run_ts}_page{n:03d}.json",
                        json.dumps(page).encode(), "application/json")
        records.extend(page.get("records", []))

    df = to_frame(records)
    if df.empty:
        # Nothing changed since last run -- a normal, successful outcome.
        print(f"No Salesforce changes since {watermark}")
        return {"records": 0, "watermark": str(watermark)}

    # New watermark = newest record we just pulled (UTC, no tz for Redshift).
    new_wm = df["last_modified_at"].max().tz_convert("UTC").tz_localize(None).to_pydatetime()
    # Merge into core.engagements AND advance the watermark in one transaction:
    # if the merge fails, the watermark stays put and the next run re-pulls.
    stage_and_merge(
        f"{load_id_for(SOURCE, batch_date)}-{run_ts}", SOURCE, [(ENGAGEMENTS, df)],
        "core.sp_merge_engagements", source_uri=s3_io.uri(f"raw/{SOURCE}/dt={batch_date}/"),
        rows_in=len(records), extra_sql=watermarks.set_sql(SOURCE, new_wm),
    )
    summary = {"records": len(records), "old_watermark": str(watermark), "new_watermark": str(new_wm)}
    print(summary)
    return summary


if __name__ == "__main__":
    main()
