import io
from datetime import date, datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import pytest

from conftest import FakeConnection
from pipeline import loaders, s3_io
from pipeline.schemas import ENGAGEMENTS, EVENTS


def _engagements():
    return pd.DataFrame([{
        "activity_id": "00T" + "A" * 15, "member_id": "MEM10001", "activity_type": "Wellness Call",
        "subject": "Wellness Call - Sep 28", "status": "Completed", "activity_date": date(2026, 9, 28),
        "owner_name": "Pat Doe", "notes": "Left voicemail.",
        "last_modified_at": pd.Timestamp("2026-09-28T10:15:00.123456789Z"),
    }, {
        "activity_id": "00T" + "B" * 15, "member_id": None, "activity_type": "Home Visit",
        "subject": None, "status": "Open", "activity_date": date(2026, 9, 28),
        "owner_name": None, "notes": pd.NA, "last_modified_at": datetime(2026, 9, 28, 11, tzinfo=timezone.utc),
    }])


def test_parquet_matches_contract_exactly():
    table = pq.read_table(io.BytesIO(loaders.to_parquet_bytes(_engagements().assign(load_id="x"), ENGAGEMENTS)))
    assert table.schema == ENGAGEMENTS.arrow_schema      # names, order and types -- COPY maps by position
    assert str(table.schema.field("last_modified_at").type) == "timestamp[us]"
    rows = table.to_pylist()
    assert rows[1]["member_id"] is None and rows[1]["notes"] is None


def test_int_columns_survive_pandas_nullable():
    df = pd.DataFrame([{"event_id": "E1", "event_name": "n", "event_type": "t", "venue_name": "v",
                        "county": "Kings", "starts_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
                        "host_chw": "h", "status": "completed", "capacity": 20.0,
                        "updated_at": datetime(2026, 9, 2, tzinfo=timezone.utc), "load_id": "x"}])
    table = pq.read_table(io.BytesIO(loaders.to_parquet_bytes(df, EVENTS)))
    assert table.to_pylist()[0]["capacity"] == 20


def test_missing_column_fails_loudly():
    with pytest.raises(ValueError, match="missing columns"):
        loaders.to_parquet_bytes(_engagements().drop(columns=["notes"]).assign(load_id="x"), ENGAGEMENTS)


def test_stage_and_merge_is_one_transaction(s3_bucket, monkeypatch):
    conn = FakeConnection()
    monkeypatch.setattr(loaders, "get_connection", lambda: conn)
    loaders.stage_and_merge("sf-2026-09-28", "salesforce_activities", [(ENGAGEMENTS, _engagements())],
                            "core.sp_merge_engagements",
                            extra_sql=[("DELETE FROM ops.watermarks WHERE source = %s;", ("sf",))])
    assert s3_io.list_keys("staged/engagements/") == ["staged/engagements/load_id=sf-2026-09-28/part-000.parquet"]
    assert [sql.split()[0] for sql, _ in conn.log] == ["DELETE", "COPY", "CALL", "DELETE", "INSERT"]
    assert conn.log[1][1] == ("s3://test-claims-lake/staged/engagements/load_id=sf-2026-09-28/",
                              "arn:aws:iam::123456789012:role/test-copy")
    assert conn.committed and not conn.rolled_back


def test_failure_rolls_back_and_is_audited(s3_bucket, monkeypatch):
    first, audit = FakeConnection(fail_on="CALL"), FakeConnection()
    conns = [first, audit]
    monkeypatch.setattr(loaders, "get_connection", lambda: conns.pop(0))
    with pytest.raises(RuntimeError, match="simulated"):
        loaders.stage_and_merge("sf-2026-09-28", "salesforce_activities", [(ENGAGEMENTS, _engagements())],
                                "core.sp_merge_engagements")
    assert first.rolled_back and not first.committed
    assert "'failed'" in audit.log[0][0] and audit.committed
