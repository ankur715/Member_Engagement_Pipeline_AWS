"""Community events + attendance from the events platform's REST API.

Cursor-paginated, incremental on updated_since. Each event arrives with its
full nested attendee list; pandas flattens it into two tables
(events, event_attendance). The merge replaces an event's roster
wholesale, since the API's list is authoritative per event.
"""
import json
from datetime import date, datetime, timezone

import pandas as pd

from pipeline import config, http, s3_io, watermarks
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.schemas import EVENT_ATTENDANCE, EVENTS

SOURCE = "events_api"
PAGE_SIZE = 25  # events per API call


def fetch_pages(updated_since: datetime):
    s = http.session()  # auth + retries
    url = f"{config.MOCK_API_URL.rstrip('/')}/events/v1/events"
    # First call: "events changed since the watermark".
    params = {"updated_since": updated_since.strftime("%Y-%m-%dT%H:%M:%SZ"), "page_size": PAGE_SIZE}
    while True:
        page = http.get_json(s, url, params)
        yield page                                  # hand this page to the caller
        if not page.get("next_cursor"):
            return                                  # no cursor = last page
        # Later calls: just the opaque cursor the API gave us (it remembers the filter).
        params = {"cursor": page["next_cursor"], "page_size": PAGE_SIZE}


def to_frames(events: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not events:
        return pd.DataFrame(columns=EVENTS.data_columns), pd.DataFrame(columns=EVENT_ATTENDANCE.data_columns)

    # Table 1 -- events: one row per event; nested venue {name, county} becomes
    # columns "venue.name" / "venue.county".
    ev = pd.json_normalize(events)
    events_df = pd.DataFrame({
        "event_id": ev["id"],
        "event_name": ev["name"],
        "event_type": ev["type"],
        "venue_name": ev["venue.name"],
        "county": ev["venue.county"],
        "starts_at": pd.to_datetime(ev["starts_at"], utc=True),
        "host_chw": ev["host"],
        "status": ev["status"].str.lower(),   # Completed -> completed
        "capacity": ev["capacity"],
        "updated_at": pd.to_datetime(ev["updated_at"], utc=True),
    })

    # Table 2 -- attendance: explode each event's "attendees" list into its own
    # rows, carrying the parent event id along as column "event.id".
    att = pd.json_normalize(events, record_path="attendees", meta=["id"], meta_prefix="event.")
    if att.empty:
        return events_df, pd.DataFrame(columns=EVENT_ATTENDANCE.data_columns)
    attendance_df = pd.DataFrame({
        "event_id": att["event.id"],
        "member_id": att["member_id"].astype("string").str.strip().str.upper(),
        "registered_at": pd.to_datetime(att["registered_at"], utc=True),
        "attended": att["checked_in_at"].notna(),   # checked in = attended
        "checked_in_at": pd.to_datetime(att["checked_in_at"], utc=True),
    }).drop_duplicates(subset=["event_id", "member_id"], keep="last")  # one row per member per event
    return events_df, attendance_df


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    watermark = watermarks.get(SOURCE)  # newest updated_at already loaded
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    events = []
    for n, page in enumerate(fetch_pages(watermark)):
        # Keep every raw API page in S3 (replayable, auditable).
        s3_io.put_bytes(f"raw/{SOURCE}/dt={batch_date}/{run_ts}_page{n:03d}.json",
                        json.dumps(page).encode(), "application/json")
        events.extend(page["data"])

    events_df, attendance_df = to_frames(events)
    if events_df.empty:
        print(f"No event changes since {watermark}")
        return {"events": 0}

    # Next run asks for events updated after the newest one in this batch.
    new_wm = events_df["updated_at"].max().tz_convert("UTC").tz_localize(None).to_pydatetime()
    # Both tables + the watermark commit together (core.sp_merge_events
    # upserts events and replaces each event's attendee list).
    stage_and_merge(
        f"{load_id_for(SOURCE, batch_date)}-{run_ts}", SOURCE,
        [(EVENTS, events_df), (EVENT_ATTENDANCE, attendance_df)],
        "core.sp_merge_events", source_uri=s3_io.uri(f"raw/{SOURCE}/dt={batch_date}/"),
        rows_in=len(events), extra_sql=watermarks.set_sql(SOURCE, new_wm),
    )
    summary = {"events": len(events_df), "attendance_rows": len(attendance_df), "new_watermark": str(new_wm)}
    print(summary)
    return summary


if __name__ == "__main__":
    main()
