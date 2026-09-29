"""Salesforce, events platform and Google Sheets ingestion, exercised
against the mock APIs through FastAPI's TestClient (no network)."""
from datetime import datetime

import pandas as pd
from fastapi.testclient import TestClient

from mock_api.main import app
from pipeline.ingest import contact_preferences, events, salesforce_activities
from pipeline.schemas import CONTACT_PREFERENCES, ENGAGEMENTS, EVENT_ATTENDANCE, EVENTS

AUTH = {"Authorization": "Bearer local-dev-token"}
client = TestClient(app)


def test_requires_token():
    assert client.get("/events/v1/events").status_code == 401


# ----- Salesforce

def _sf_query(wm):
    return client.get("/services/data/v60.0/query", headers=AUTH,
                      params={"q": salesforce_activities.SOQL.format(wm=wm)}).json()


def _sf_walk(first):
    fetch_next = lambda url: client.get(url, headers=AUTH).json()  # noqa: E731
    return [r for p in salesforce_activities.paginate(first, fetch_next) for r in p["records"]]


def test_salesforce_pagination_and_flatten():
    first = _sf_query("2026-01-01T00:00:00Z")
    assert first["done"] is False and "nextRecordsUrl" in first
    records = _sf_walk(first)
    assert len(records) == first["totalSize"]
    df = salesforce_activities.to_frame(records)
    assert list(df.columns) == ENGAGEMENTS.data_columns
    assert df["activity_id"].is_unique and df["activity_id"].str.len().eq(18).all()
    assert (df["member_id"] == df["member_id"].str.strip().str.upper()).all()   # hand-typed ids cleaned
    assert df.loc[df["status"] == "Open", "notes"].isna().all()


def test_salesforce_watermark_is_incremental():
    records = _sf_walk(_sf_query("2026-01-01T00:00:00Z"))
    mid = sorted(r["LastModifiedDate"] for r in records)[len(records) // 2][:19] + "Z"
    newer = _sf_walk(_sf_query(mid))
    assert 0 < len(newer) < len(records)
    assert all(r["LastModifiedDate"][:19] > mid[:19] for r in newer)


# ----- Events platform

def _events_walk(updated_since):
    params = {"updated_since": updated_since, "page_size": 25}
    out = []
    while True:
        page = client.get("/events/v1/events", headers=AUTH, params=params).json()
        out += page["data"]
        if not page["next_cursor"]:
            return out
        params = {"cursor": page["next_cursor"], "page_size": 25}


def test_events_flatten_nested_attendees():
    evts = _events_walk("2026-01-01T00:00:00Z")
    events_df, att_df = events.to_frames(evts)
    assert list(events_df.columns) == EVENTS.data_columns
    assert list(att_df.columns) == EVENT_ATTENDANCE.data_columns
    assert events_df["event_id"].is_unique
    assert 0 < len(att_df) <= sum(len(e["attendees"]) for e in evts)
    assert not att_df.duplicated(["event_id", "member_id"]).any()
    assert set(att_df["event_id"]) <= set(events_df["event_id"])
    assert not att_df.merge(events_df.query("status != 'completed'"), on="event_id")["attended"].any()
    assert att_df.merge(events_df.query("status == 'completed'"), on="event_id")["attended"].any()


def test_events_cursor_pages_have_no_overlap():
    evts = _events_walk("2026-01-01T00:00:00Z")
    assert len({e["id"] for e in evts}) == len(evts)


def test_empty_frames_keep_contract_columns():
    e, a = events.to_frames([])
    assert list(e.columns) == EVENTS.data_columns and list(a.columns) == EVENT_ATTENDANCE.data_columns


# ----- Google Sheets do-not-contact list

def test_dnc_sheet_normalization():
    values = client.get("/v4/spreadsheets/mock-dnc-sheet/values/DNC!A:E", headers=AUTH).json()["values"]
    clean, rejects = contact_preferences.normalize(values)
    assert list(clean.columns) == CONTACT_PREFERENCES.data_columns
    assert clean["member_id"].str.fullmatch(r"MEM\d{5}").all()
    assert not clean.duplicated(["member_id", "channel"]).any()
    assert set(clean["channel"]) <= {"phone", "mail", "all"}
    assert clean["requested_date"].notna().all()                 # all three date styles parsed
    assert list(rejects["member_id"]) == ["MEM1OO12"]            # typo rejected, not guessed


def test_channel_normalization_is_conservative():
    n = contact_preferences.normalize_channel
    assert n("PHONE - calls only") == "phone"
    assert n("Mail only") == "mail"
    assert n("") == "all"
    assert n("text messages") == "all"          # unrecognised -> strictest
    assert n("phone and mail") == "all"


def test_short_rows_from_sheets_api_are_padded():
    values = [["Member ID", "Date Requested", "Channel"], ["MEM10001", "2026-09-01"]]
    clean, _ = contact_preferences.normalize(values)
    assert clean.iloc[0]["channel"] == "all"
