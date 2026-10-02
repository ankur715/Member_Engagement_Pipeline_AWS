from datetime import date

from fastapi.testclient import TestClient

from mock_api.main import app
from pipeline.ingest import housing_violations as hv
from pipeline.reference_data import member_roster
from pipeline.schemas import HOUSING_VIOLATIONS

client = TestClient(app)
NYC_ZIPS = sorted({m["zip"] for m in member_roster() if m["county"] in ("Kings", "Queens", "Bronx")})
ALL_ZIPS = sorted({m["zip"] for m in member_roster()})


def _pull(zips, page_size=25):
    rows, offset = [], 0
    while True:
        page = client.get("/resource/wvxf-dwi5.json", params={
            "$where": hv.soql_where(zips, date(2023, 10, 1)), "$limit": page_size, "$offset": offset}).json()
        rows += page
        if len(page) < page_size:
            return rows
        offset += page_size


def test_public_endpoint_needs_no_token():
    assert client.get("/resource/wvxf-dwi5.json").status_code == 200


def test_paging_returns_every_row_once():
    rows = _pull(ALL_ZIPS, page_size=7)
    ids = [r["violationid"] for r in rows]
    assert len(ids) == len(set(ids)) and len(ids) > 20


def test_only_nyc_zips_have_violations():
    rows = _pull(ALL_ZIPS)
    zips = {r["zip"][:5] for r in rows if r["zip"]}
    assert zips and zips <= set(NYC_ZIPS)            # Nassau / Westchester: outside HPD's coverage


def test_normalize_cleans_and_rejects():
    clean, rejects = hv.normalize(_pull(ALL_ZIPS))
    assert list(clean.columns) == HOUSING_VIOLATIONS.data_columns
    assert clean["zip"].str.fullmatch(r"\d{5}").all()                      # ZIP+4 trimmed
    assert set(clean["violation_class"]) <= {"A", "B", "C"}                 # lowercase fixed
    assert set(rejects["reject_reason"]) == {"invalid_zip;"}                # blank ZIPs can't join
    assert clean["violation_id"].is_unique


def test_heat_flag_matches_code_sections_in_both_wordings():
    rows = [{"violationid": "1", "zip": "11201", "class": "C",
             "novdescription": "§ 27-2029 ADM CODE PROVIDE AN ADEQUATE SUPPLY OF HEAT"},
            {"violationid": "2", "zip": "11201", "class": "C",
             "novdescription": "§ 27-2031 ADMIN. CODE: PROVIDE HOT WATER AT ALL HOT WATER FIXTURES"},
            {"violationid": "3", "zip": "11201", "class": "B",
             "novdescription": "§ 27-2017.3 HMC: TRACE AND CORRECT THE CONDITIONS CAUSING MOLD"}]
    clean, _ = hv.normalize(rows)
    assert clean["is_heat_hot_water"].tolist() == [True, True, False]


def test_missing_fields_are_tolerated():
    clean, rejects = hv.normalize([{"violationid": "9", "zip": "10451", "class": "B"}])  # Socrata omits nulls
    assert len(clean) == 1 and clean.iloc[0]["is_heat_hot_water"] == False  # noqa: E712


def test_soql_where_is_server_side_filter():
    w = hv.soql_where(["11201", "10451"], date(2023, 10, 1))
    assert "violationstatus='Open'" in w and "class in('B','C')" in w and "zip in('10451','11201')" in w
