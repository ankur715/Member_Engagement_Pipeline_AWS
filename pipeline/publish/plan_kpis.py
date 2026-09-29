"""Monthly program KPIs per health plan -> the account-management team.

Written to a Google Sheet (one tab per health plan) when Sheets is
configured, and always exported as CSV to s3://.../exports/ so there's a
durable copy of exactly what was reported. Idempotent: a rerun replaces the
month's row rather than appending a duplicate.
Only de-identified aggregates from the analytics schema leave the warehouse.
"""
from datetime import date

import pandas as pd

from pipeline import config, s3_io
from pipeline.redshift import fetch_all

# This month's KPI row for every health plan, from the de-identified view.
# %(d)s is the batch date; DATE_TRUNC turns it into the first of its month.
KPI_SQL = """
SELECT health_plan, month, eligible_members, members_reached, members_engaged,
       engagement_rate_pct, event_attendances, sdoh_needs_identified
FROM analytics.v_plan_monthly_kpis
WHERE month = DATE_TRUNC('month', %(d)s::DATE)
ORDER BY health_plan;
"""
# Column order for the CSV and the Sheet (matches the SELECT above).
COLUMNS = ["health_plan", "month", "eligible_members", "members_reached", "members_engaged",
           "engagement_rate_pct", "event_attendances", "sdoh_needs_identified"]


def upsert_rows(existing: list[list[str]], header: list[str], row: list[str]) -> tuple[list[list[str]], bool]:
    """Replace the row for row[1] (the month) if present, else append. Pure, unit tested."""
    rows = existing or [header]                     # empty sheet -> start with a header row
    for i, r in enumerate(rows[1:], start=1):       # skip the header
        if len(r) > 1 and r[1] == row[1]:           # same month already there -> replace it
            rows = rows[:i] + [row] + rows[i + 1:]
            return rows, True
    return rows + [row], False                      # new month -> append


def publish_to_sheets(df: pd.DataFrame):
    import gspread  # only needed when Sheets is configured
    # Open the KPI spreadsheet with the service account's credentials.
    book = gspread.service_account(filename=config.GOOGLE_SERVICE_ACCOUNT_JSON).open_by_key(config.GOOGLE_KPI_SHEET_ID)
    for _, rec in df.iterrows():
        title = rec["health_plan"][:90]   # one tab per health plan (tab names max ~100 chars)
        try:
            ws = book.worksheet(title)
        except gspread.WorksheetNotFound:
            # First time we report to this plan -> create its tab.
            ws = book.add_worksheet(title=title, rows=100, cols=len(COLUMNS))
        rows, _ = upsert_rows(ws.get_all_values(), COLUMNS, [str(rec[c]) for c in COLUMNS])
        ws.update(range_name="A1", values=rows)  # write the whole tab back in one call


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    df = pd.DataFrame(fetch_all(KPI_SQL, {"d": batch_date}), columns=COLUMNS)
    # Always keep a CSV copy of exactly what was reported, per day.
    key = f"exports/plan_kpis/dt={batch_date}/plan_kpis.csv"
    s3_io.put_bytes(key, df.to_csv(index=False).encode(), "text/csv")

    # Push to Google Sheets only when credentials and a sheet id are configured.
    published = bool(config.GOOGLE_SERVICE_ACCOUNT_JSON and config.GOOGLE_KPI_SHEET_ID)
    if published:
        publish_to_sheets(df)
    print(f"KPIs for {len(df)} health plan(s) -> {s3_io.uri(key)}" + (" + Google Sheets" if published else ""))
    return {"plans": len(df), "sheets": published}


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
