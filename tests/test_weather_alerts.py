from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from mock_api.main import app
from pipeline.ingest import weather_alerts as wa
from pipeline.schemas import WEATHER_ALERT_COUNTIES, WEATHER_ALERTS

client = TestClient(app)
NOW = datetime(2026, 7, 15, 12, 0)


def _payload(monkeypatch, scenario):
    monkeypatch.setenv("MOCK_WEATHER_SCENARIO", scenario)
    return client.get("/alerts/active", params={"area": "NY"}).json()


def test_public_endpoint_needs_no_token():
    assert client.get("/alerts/active", params={"area": "NY"}).status_code == 200


def test_geojson_shape(monkeypatch):
    p = _payload(monkeypatch, "heat")
    assert p["type"] == "FeatureCollection" and p["features"]
    props = p["features"][0]["properties"]
    assert {"id", "event", "severity", "onset", "ends", "expires", "messageType", "geocode"} <= props.keys()


@pytest.mark.parametrize("event,hazard", [("Heat Advisory", "heat"), ("Extreme Heat Warning", "heat"),
                                          ("Excessive Heat Warning", "heat"), ("Extreme Cold Warning", "cold"),
                                          ("Cold Weather Advisory", "cold"), ("Wind Chill Advisory", "cold"),
                                          ("Small Craft Advisory", "other"), ("Flood Watch", "other")])
def test_hazard_classification(event, hazard):
    assert wa.hazard_for(event) == hazard


@pytest.mark.parametrize("code,fips", [("036047", "36047"), ("036005", "36005"),
                                       ("073335", None), ("36047", None), ("", None)])
def test_same_code_to_county_fips(code, fips):
    assert wa.same_to_fips(code) == fips


def test_heat_scenario_frames(monkeypatch):
    alerts, counties = wa.to_frames(_payload(monkeypatch, "heat"), NOW)
    assert list(alerts.columns) == WEATHER_ALERTS.data_columns
    assert list(counties.columns) == WEATHER_ALERT_COUNTIES.data_columns
    assert set(alerts["hazard"]) == {"heat", "other"}
    assert alerts["ends_at"].notna().all()                       # "ends" null -> falls back to expires
    heat_ids = set(alerts.loc[alerts["hazard"] == "heat", "alert_id"])
    assert set(counties.loc[counties["alert_id"].isin(heat_ids), "county_fips"]) == {"36047", "36005", "36081"}
    assert "73335" not in set(counties["county_fips"])            # marine zone dropped
    assert (alerts["onset_at"] < alerts["ends_at"]).all()         # offsets converted to UTC consistently


def test_cold_scenario_covers_suburbs(monkeypatch):
    alerts, counties = wa.to_frames(_payload(monkeypatch, "cold"), NOW)
    assert set(alerts["hazard"]) == {"cold", "other"}
    assert {"36059", "36119"} <= set(counties["county_fips"])     # Nassau, Westchester


def test_no_alerts_scenario(monkeypatch):
    alerts, counties = wa.to_frames(_payload(monkeypatch, "none"), NOW)
    assert set(alerts["hazard"]) == {"other"} and counties.empty


def test_empty_payload():
    alerts, counties = wa.to_frames({"features": []}, NOW)
    assert alerts.empty and counties.empty and list(alerts.columns) == WEATHER_ALERTS.data_columns
