"""Mock external systems, so the pipeline integrates over real HTTP/JSON with
the same response shapes as the real services:

1. Salesforce REST   GET /services/data/v60.0/query?q=<SOQL>
                     GET /services/data/v60.0/query/<cursor>
   CHW activity (Task) with free-text notes. Supports the one filter the
   pipeline uses: LastModifiedDate > <ts>. records / done / nextRecordsUrl.

2. Events platform   GET /events/v1/events?updated_since=<ts>&page_size=N
                     GET /events/v1/events?cursor=<c>
   Community events with nested attendee lists, cursor-paginated.

3. Google Sheets     GET /v4/spreadsheets/<id>/values/<range>
   The member-services team's hand-maintained do-not-contact sheet, in the
   Sheets API v4 values.get shape ({"range", "majorDimension", "values"}).

Everything is generated deterministically from dates, so the same history
comes back on every call; "now" only decides what exists yet.

Run: uvicorn mock_api.main:app --port 9000
"""
import base64
import hashlib
import json
import os
import random
import re
from datetime import date, datetime, time, timedelta, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from pipeline.reference_data import (ACTIVITY_START, ACTIVITY_TYPES, EVENT_TYPES, VENUES, chw_names,
                                     member_ids, member_roster)

app = FastAPI(title="Mock external systems (Salesforce, events platform, Google Sheets)")

API_TOKEN = os.environ.get("MOCK_API_TOKEN", "local-dev-token")  # shared secret the pipeline must send
SF_PAGE_SIZE = 50                                                 # Salesforce-style page size


def require_token(authorization: str = Header(default="")):
    # Every endpoint (except /health) requires "Authorization: Bearer <token>",
    # like the real APIs do -- so the pipeline's auth handling is exercised.
    if authorization != f"Bearer {API_TOKEN}":
        raise HTTPException(status_code=401, detail="invalid or missing bearer token")


def _now() -> datetime:
    # "Now" decides which records exist yet (the history grows over time).
    return datetime.now(timezone.utc)


def _encode(state: dict) -> str:
    # Pagination cursor = base64 of {filter, offset}. Opaque to the client,
    # like real APIs' cursors.
    return base64.urlsafe_b64encode(json.dumps(state).encode()).decode()


def _decode(cursor: str) -> dict:
    # Reverse of _encode; a tampered/garbled cursor -> 400 Bad Request.
    try:
        return json.loads(base64.urlsafe_b64decode(cursor.encode()))
    except ValueError:
        raise HTTPException(status_code=400, detail="bad cursor")


def _hash_int(key: str) -> int:
    # Stable pseudo-random number from a string (same string -> same number).
    return int(hashlib.md5(key.encode()).hexdigest(), 16)


def propensity(member_id: str) -> float:
    """How likely a member is to engage, fixed per member. Skewed so a few
    members are very engaged, many occasionally, some almost never -- which is
    what makes engagement-rate KPIs meaningful instead of 100% everywhere."""
    # 0-0.99 from the member id, cubed to skew toward low values, +0.01 so nobody is exactly 0.
    return (_hash_int("propensity-" + member_id) % 100 / 100) ** 3 + 0.01


def weighted_sample(rng: random.Random, pool: list[str], k: int) -> list[str]:
    # Weighted sampling without replacement (Efraimidis-Spirakis keys):
    # each member gets key = random ** (1/weight); higher-propensity members
    # tend to get bigger keys, so they're picked more often. Take the top k.
    return sorted(pool, key=lambda m: rng.random() ** (1 / propensity(m)), reverse=True)[:k]


# ------------------------------------------------------------------ Salesforce

# CHW notes: the kind of free text the SDoH tagging has to deal with.
NOTES_COMPLETED = [
    "Member mentioned food runs out before the end of the month and she is skipping meals some days. Shared food pantry info.",
    "Member asked about SNAP benefits, says the fridge is nearly empty by the 20th.",
    "Member has no ride to his cardiology appointment next Tuesday. Set up the plan's transportation benefit.",
    "Missed PCP visit because the bus route changed. Needs a ride for the rescheduled visit.",
    "Member lives alone and says she feels lonely since her husband passed. Invited her to the Thursday neighborhood group.",
    "Hasn't left the house in two weeks, no family nearby. Will check in again Friday.",
    "Landlord is raising the rent and member is worried about eviction. Referred to housing counselor.",
    "Heat not working in apartment for a week, landlord not responding.",
    "Member is skipping doses of her insulin because she can't afford the copay. Flagged to pharmacy team.",
    "Rationing blood pressure pills to make them last until the next refill.",
    "Member doing well, attended the walking group last week. No needs identified.",
    "Completed annual check-in; member engaged and positive about the program.",
    "Reviewed upcoming events calendar with member, she plans to come to the health fair.",
    "Member asked us to stop calling. Please remove from call list.",
]
# Notes for calls that didn't connect -- should never be tagged with a need.
NOTES_NO_ANSWER = ["Left voicemail.", "No answer, will retry tomorrow.", "Number rang busy twice."]


def _sf_id(key: str) -> str:
    return "00T" + hashlib.md5(key.encode()).hexdigest()[:15].upper()  # Task ids: 00T..., 18 chars


def generate_activities(now: datetime) -> list[dict]:
    """CHW activity history from ACTIVITY_START to now. Activities are Open
    for 2 days, then become Completed or No Answer -- so the same record is
    modified again later, which is what the incremental upsert must handle."""
    chws, members = chw_names(), member_ids()
    today = now.date()
    records = []
    d = ACTIVITY_START
    while d <= today:                                    # one pass per calendar day
        rng = random.Random(d.toordinal())               # seeded by date -> same history every call
        for k in range(rng.randint(2, 5)):               # 2-5 activities per day
            closed = (today - d).days >= 2               # older than 2 days -> has an outcome
            status = ("No Answer" if rng.random() < 0.25 else "Completed") if closed else "Open"
            # Closing an activity modifies it again 2 days later -> it re-appears in incremental pulls.
            modified_day = d + timedelta(days=2) if closed else d
            modified = datetime.combine(modified_day, time(9 + k, rng.randint(0, 59)), tzinfo=timezone.utc)
            activity_type = rng.choice(ACTIVITY_TYPES)
            # Usually a real member (engaged ones more often); ~3% an id that's on no roster.
            member = (weighted_sample(rng, members, 1)[0] if rng.random() > 0.03
                      else "MEM99" + str(rng.randint(100, 999)))
            # Completed -> a real note; No Answer -> a voicemail note; Open -> no note yet.
            note = (rng.choice(NOTES_COMPLETED) if status == "Completed"
                    else rng.choice(NOTES_NO_ANSWER) if status == "No Answer" else None)
            # Same field names and shape as a real Salesforce Task record.
            records.append({
                "attributes": {"type": "Task", "url": f"/services/data/v60.0/sobjects/Task/{_sf_id(f'{d}-{k}')}"},
                "Id": _sf_id(f"{d}-{k}"),
                "Member_ID__c": member if rng.random() > 0.1 else f" {member.lower()} ",  # hand-typed
                "Type": activity_type,
                "Subject": f"{activity_type} - {d:%b %d}",
                "Status": status,
                "ActivityDate": d.isoformat(),
                "Description": note,
                "Owner": {"attributes": {"type": "User"}, "Name": rng.choice(chws)},
                "LastModifiedDate": modified.strftime("%Y-%m-%dT%H:%M:%S.000+0000"),
            })
        d += timedelta(days=1)
    # Only records that "exist" as of now, oldest change first (like ORDER BY LastModifiedDate).
    return sorted((r for r in records if _parse_sf_ts(r["LastModifiedDate"]) <= now),
                  key=lambda r: r["LastModifiedDate"])


def _parse_sf_ts(value: str) -> datetime:
    # Salesforce timestamp format, e.g. 2026-09-28T13:39:00.000+0000 -> aware datetime.
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.000+0000").replace(tzinfo=timezone.utc)


def _sf_page(watermark: str | None, offset: int) -> dict:
    # Build one page of a SOQL result, applying the LastModifiedDate > watermark filter.
    records = generate_activities(_now())
    if watermark:
        wm = datetime.strptime(watermark, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        records = [r for r in records if _parse_sf_ts(r["LastModifiedDate"]) > wm]
    chunk = records[offset:offset + SF_PAGE_SIZE]           # this page's records
    done = offset + SF_PAGE_SIZE >= len(records)            # true on the last page
    body = {"totalSize": len(records), "done": done, "records": chunk}
    if not done:
        # Where the client fetches the next page (Salesforce's "query more" link).
        body["nextRecordsUrl"] = f"/services/data/v60.0/query/{_encode({'wm': watermark, 'offset': offset + SF_PAGE_SIZE})}"
    return body


@app.get("/services/data/v60.0/query", dependencies=[Depends(require_token)])
def soql_query(q: str = Query(...)):
    if "FROM Task" not in q:
        raise HTTPException(status_code=400, detail="mock only supports the Task object")
    # Minimal SOQL "parser": pull out the timestamp after "LastModifiedDate >".
    marker = "LastModifiedDate >"
    wm = q.split(marker, 1)[1].split()[0] if marker in q else None
    return _sf_page(wm, 0)


@app.get("/services/data/v60.0/query/{cursor}", dependencies=[Depends(require_token)])
def soql_query_more(cursor: str):
    # Follow-up pages: the cursor carries the original filter and the next offset.
    state = _decode(cursor)
    return _sf_page(state["wm"], state["offset"])


# -------------------------------------------------------------- events platform


def generate_events(now: datetime) -> list[dict]:
    """~1 event a week per county. Past events are 'completed' with
    check-ins, recorded the day after they happen (updated_at moves);
    ~5% are cancelled."""
    # Members grouped by county -- events draw attendees from their own county.
    roster = member_roster()
    by_county = {}
    for m in roster:
        by_county.setdefault(m["county"], []).append(m["member_id"])
    chws = chw_names()
    events = []
    d = ACTIVITY_START
    while d <= now.date() + timedelta(days=14):  # includes scheduled upcoming events
        for county, venues in VENUES.items():
            rng = random.Random(_hash_int(f"{d}-{county}"))  # seeded per day+county
            if rng.random() > 0.15:                          # ~15% of days have an event here
                continue
            event_id = f"EVT-{d:%Y%m%d}-{county[:3].upper()}"
            starts = datetime.combine(d, time(rng.choice([10, 13, 15])), tzinfo=timezone.utc)
            created = starts - timedelta(days=14)        # events are posted 2 weeks ahead
            cancelled = rng.random() < 0.05
            happened = starts + timedelta(hours=2) <= now and not cancelled
            if cancelled:
                status, updated = "Cancelled", starts - timedelta(days=1)
            elif happened:
                status, updated = "Completed", starts + timedelta(days=1)
            else:
                status, updated = "Scheduled", created
            if updated > now:  # completion not recorded yet
                status, updated = "Scheduled", created
            if created > now:
                continue                                 # not announced yet

            # Registrants: 2-6 county members, weighted toward engaged ones.
            pool = by_county[county]
            registrants = weighted_sample(rng, pool, min(len(pool), rng.randint(2, 6)))
            if rng.random() < 0.15:
                registrants.append("MEM99" + str(rng.randint(100, 999)))  # walk-in not on any roster
            attendees = []
            for mid in registrants:
                att = {"member_id": mid, "registered_at": (created + timedelta(days=rng.randint(0, 10))).isoformat(),
                       "checked_in_at": None}
                # ~80% of registrants show up (only for events that actually happened).
                if status == "Completed" and rng.random() < 0.8:
                    att["checked_in_at"] = (starts + timedelta(minutes=rng.randint(-10, 20))).isoformat()
                attendees.append(att)

            etype = rng.choice(EVENT_TYPES)
            events.append({
                "id": event_id,
                "name": f"{etype} at {venues[0]}",
                "type": etype,
                "venue": {"name": rng.choice(venues), "county": county},
                "starts_at": starts.isoformat(),
                "host": rng.choice(chws),
                "status": status,
                "capacity": rng.choice([15, 20, 30]),
                "attendees": attendees,
                "updated_at": updated.isoformat(),
            })
        d += timedelta(days=1)
    # Stable order by change time -> cursor paging never skips or repeats an event.
    return sorted(events, key=lambda e: (e["updated_at"], e["id"]))


@app.get("/events/v1/events", dependencies=[Depends(require_token)])
def list_events(updated_since: str | None = None, cursor: str | None = None,
                page_size: int = Query(default=25, ge=1, le=100)):
    if cursor:
        # Later pages: restore the filter and position from the cursor.
        state = _decode(cursor)
        updated_since, offset = state["updated_since"], state["offset"]
    else:
        offset = 0                                   # first page
    events = generate_events(_now())
    if updated_since:
        since = datetime.fromisoformat(updated_since.replace("Z", "+00:00"))
        events = [e for e in events if datetime.fromisoformat(e["updated_at"]) > since]
    chunk = events[offset:offset + page_size]
    more = offset + page_size < len(events)          # anything left after this page?
    return {"data": chunk,
            "next_cursor": _encode({"updated_since": updated_since, "offset": offset + page_size}) if more else None}


# ---------------------------------------------------------------- Google Sheets

# The inconsistent ways people type dates, channels and sources into the sheet.
DATE_STYLES = ["{d.month}/{d.day}/{d.year}", "{d:%Y-%m-%d}", "{d:%b} {d.day}, {d.year}"]
CHANNEL_STYLES = ["Phone", "phone ", "PHONE - calls only", "All", "all contact", "Mail only", "mail", ""]
VIA = ["Call Center", "CHW", "Member Portal", "Letter"]


def dnc_sheet_values(today: date) -> list[list[str]]:
    """~12% of members have opted out at some point; the sheet grows as their
    request dates pass. Hand-entry mess included on purpose."""
    rows = [["Member ID", "Date Requested", "Channel", "Requested Via", "Notes"]]  # header row
    for m in member_roster():
        h = _hash_int("dnc-" + m["member_id"])
        if h % 8 != 0:
            continue                                     # ~1 in 8 members ever opts out
        requested = ACTIVITY_START + timedelta(days=h % 100)
        if requested > today:
            continue                                     # hasn't asked yet
        rng = random.Random(h)
        mid = m["member_id"]
        mid = f" {mid.lower()}" if rng.random() < 0.3 else mid  # sloppy typing
        rows.append([mid, rng.choice(DATE_STYLES).format(d=requested), rng.choice(CHANNEL_STYLES),
                     rng.choice(VIA), rng.choice(["", "per member request", "daughter called on her behalf"])])
        if rng.random() < 0.3:
            rows.append(rows[-1][:3])  # duplicate entry, trailing cells dropped like the Sheets API does
    rows.append(["", "", "", "", ""])                              # blank row
    rows.append(["MEM1OO12", "9/1/2026", "phone", "Call Center"])  # typo'd id (letter O)
    return rows


@app.get("/v4/spreadsheets/{sheet_id}/values/{sheet_range}", dependencies=[Depends(require_token)])
def sheet_values(sheet_id: str, sheet_range: str):
    # Same URL and response shape as the Google Sheets API v4 values.get call.
    if sheet_id != "mock-dnc-sheet":
        raise HTTPException(status_code=404, detail="Requested entity was not found.")
    return {"range": sheet_range, "majorDimension": "ROWS", "values": dnc_sheet_values(_now().date())}


@app.get("/health")
def health():
    # Unauthenticated liveness check (used to confirm the server is up).
    return {"status": "ok"}


# ------------------------------------------------- NYC Open Data (HPD violations)
# Same path and field names as the real Socrata dataset (wvxf-dwi5). Public
# data in reality, so -- like the real API -- no bearer token is required.

HPD_CODES = [
    ("C", True,  "§ 27-2029 ADM CODE PROVIDE AN ADEQUATE SUPPLY OF HEAT FOR THE APARTMENT IN THE ENTIRE APARTMENT"),
    ("C", True,  "§ 27-2031 ADMIN. CODE: PROVIDE HOT WATER AT ALL HOT WATER FIXTURES IN THE ENTIRE APARTMENT"),
    ("C", False, "§ 27-2017.4 ADM CODE ABATE THE INFESTATION CONSISTING OF MICE IN THE ENTIRE APARTMENT"),
    ("B", False, "§ 27-2017.3 HMC: TRACE AND CORRECT THE CONDITIONS CAUSING MOLD IN THE BATHROOM"),
    ("B", False, "§ 27-2005 ADM CODE REPAIR THE BROKEN OR DEFECTIVE PLASTERED SURFACES IN THE KITCHEN"),
    ("A", False, "§ 27-2046.1 HMC: REPAIR THE SMOKE DETECTOR IN THE HALLWAY"),
]
NYC_BOROS = {"Kings": "BROOKLYN", "Queens": "QUEENS", "Bronx": "BRONX"}


def hpd_violations(today: date) -> list[dict]:
    """Open violations for NYC ZIPs where members live. Some ZIPs are much
    worse than others (deterministic per ZIP), with the usual export mess."""
    rows = []
    zips = sorted({(m["zip"], m["county"]) for m in member_roster() if m["county"] in NYC_BOROS})
    for zip_code, county in zips:
        rng = random.Random(_hash_int("hpd-" + zip_code))
        for n in range(rng.choice([2, 5, 10, 25, 40])):            # bad buildings cluster by ZIP
            cls, _heat, desc = rng.choices(HPD_CODES, weights=[3, 2, 2, 3, 3, 1])[0]
            inspected = today - timedelta(days=rng.randint(5, 900))
            zip_out = rng.choice([zip_code] * 8 + [f"{zip_code}-{rng.randint(1000, 9999)}", ""])
            rows.append({
                "violationid": str(10_000_000 + _hash_int(f"{zip_code}-{n}") % 9_000_000),
                "zip": zip_out,                                     # ZIP+4 or blank sometimes
                "boro": NYC_BOROS[county],
                "class": cls.lower() if rng.random() < 0.05 else cls,
                "inspectiondate": inspected.strftime("%Y-%m-%dT00:00:00.000"),   # Socrata floating timestamp
                "novdescription": desc,
                "violationstatus": "Open",
                "currentstatus": rng.choice(["NOV SENT OUT", "FIRST NO ACCESS TO RE- INSPECT VIOLATION"]),
            })
    return sorted(rows, key=lambda r: r["violationid"])


@app.get("/resource/wvxf-dwi5.json")
def socrata_hpd(where: str | None = Query(default=None, alias="$where"),
                limit: int = Query(default=1000, alias="$limit"),
                offset: int = Query(default=0, alias="$offset")):
    rows = hpd_violations(_now().date())
    # Minimal SoQL support: honour "zip in ('11201', ...)" -- everything we return is Open.
    m = re.search(r"zip\s+in\s*\(([^)]*)\)", where or "", re.I)
    if m:
        wanted = {z.strip().strip("'\"") for z in m.group(1).split(",")}
        rows = [r for r in rows if r["zip"][:5] in wanted or r["zip"] == ""]
    return rows[offset:offset + limit]                              # Socrata returns a bare JSON list
