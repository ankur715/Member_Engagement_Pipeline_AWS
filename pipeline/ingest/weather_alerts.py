"""NOAA / National Weather Service alerts for New York -> core.weather_alerts.

Pulls api.weather.gov/alerts/active?area=NY (GeoJSON), classifies each alert
as a heat, cold or other hazard, and splits each alert's county list (6-digit
SAME codes, "036047") into 5-digit county FIPS ("36047") so alerts can reach
members through their county. Public data -- no PHI.

The API only returns alerts active *now*, so merging on the alert id builds
the history. In production this would run every hour or two; in this DAG it
runs with the daily batch.
"""
import json
from datetime import date, datetime, timezone

import pandas as pd

from pipeline import config, http, s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.schemas import WEATHER_ALERT_COUNTIES, WEATHER_ALERTS

SOURCE = "weather_alerts"

# NWS event names (including the 2024-25 renames: Excessive -> Extreme Heat,
# Wind Chill -> Extreme Cold / Cold Weather).
HEAT_EVENTS = {"Heat Advisory", "Excessive Heat Warning", "Excessive Heat Watch",
               "Extreme Heat Warning", "Extreme Heat Watch"}
COLD_EVENTS = {"Cold Weather Advisory", "Extreme Cold Warning", "Extreme Cold Watch",
               "Wind Chill Advisory", "Wind Chill Warning", "Wind Chill Watch"}


def hazard_for(event: str) -> str:
    if event in HEAT_EVENTS:
        return "heat"
    if event in COLD_EVENTS:
        return "cold"
    return "other"


def same_to_fips(code: str) -> str | None:
    # SAME = "0" + state FIPS (2) + county FIPS (3). Marine/zone codes start with 07x.
    code = (code or "").strip()
    return code[1:] if len(code) == 6 and code.startswith("0") and code[1:3] != "73" else None


def fetch() -> dict:
    s = http.session(auth=False)                       # public API: no bearer token
    s.headers["User-Agent"] = config.NWS_USER_AGENT    # required by api.weather.gov
    s.headers["Accept"] = "application/geo+json"
    return http.get_json(s, f"{config.NWS_API_URL.rstrip('/')}/alerts/active", {"area": "NY"})


def _utc(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce", utc=True)   # "-04:00" offsets -> UTC
    return ts.tz_localize(None) if not pd.isna(ts) else pd.NaT


def to_frames(payload: dict, seen_at: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    """GeoJSON FeatureCollection -> (alerts, alert_counties). Pure, unit tested."""
    alerts, counties = [], []
    for feature in payload.get("features", []):
        p = feature.get("properties", {})
        if not p.get("id") or not p.get("event"):
            continue
        alerts.append({
            "alert_id": p["id"],
            "event": p["event"],
            "hazard": hazard_for(p["event"]),
            "severity": p.get("severity"),
            "urgency": p.get("urgency"),
            "certainty": p.get("certainty"),
            "message_type": p.get("messageType"),
            "onset_at": _utc(p.get("onset") or p.get("effective")),
            "ends_at": _utc(p.get("ends") or p.get("expires")),   # "ends" is often null
            "headline": (p.get("headline") or "")[:500],
            "seen_at": seen_at,
        })
        for code in (p.get("geocode") or {}).get("SAME", []):
            fips = same_to_fips(code)
            if fips:
                counties.append({"alert_id": p["id"], "county_fips": fips})
    alerts_df = pd.DataFrame(alerts, columns=WEATHER_ALERTS.data_columns).drop_duplicates(subset=["alert_id"])
    counties_df = pd.DataFrame(counties, columns=WEATHER_ALERT_COUNTIES.data_columns).drop_duplicates()
    return alerts_df, counties_df


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    payload = fetch()
    s3_io.put_bytes(f"raw/{SOURCE}/dt={batch_date}/{now:%Y%m%dT%H%M%S}.json",
                    json.dumps(payload).encode(), "application/json")
    alerts, counties = to_frames(payload, now)
    # Zero active alerts is normal (most days) -- the load still runs so the
    # audit row records a successful, empty pull.
    stage_and_merge(load_id_for(SOURCE, batch_date), SOURCE,
                    [(WEATHER_ALERTS, alerts), (WEATHER_ALERT_COUNTIES, counties)],
                    "core.sp_merge_weather_alerts",
                    source_uri=f"{config.NWS_API_URL.rstrip('/')}/alerts/active?area=NY", rows_in=len(alerts))
    summary = {"alerts": len(alerts), "heat": int((alerts["hazard"] == "heat").sum()),
               "cold": int((alerts["hazard"] == "cold").sum()), "county_links": len(counties)}
    print(summary)
    return summary


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
