import pytest

from pipeline import migrate, phi
from pipeline.publish import plan_kpis
from pipeline.quality import data_quality


def test_member_token_is_keyed_and_stable():
    t = phi.member_token("MEM10001")
    assert t == phi.member_token("MEM10001")
    assert t != phi.member_token("MEM10001", key="another-key")
    assert "MEM10001" not in t


def test_member_token_refuses_empty_key():
    with pytest.raises(RuntimeError):
        phi.member_token("MEM10001", key="")


def test_redact_removes_identifiers():
    out = phi.redact("MEM10042 DOB 03/14/1948 phone (718) 555-0142 a@b.org")
    for secret in ("MEM10042", "03/14/1948", "555-0142", "a@b.org"):
        assert secret not in out


def test_migrations_are_well_formed_and_ordered():
    versions = [v for v, _, _ in migrate.discover()]
    assert versions == list(range(1, len(versions) + 1))  # contiguous, no gaps


def test_editing_an_applied_migration_is_rejected():
    found = migrate.discover()
    version, _, checksum = found[0]
    with pytest.raises(RuntimeError, match="modified after being applied"):
        migrate.pending({version: "0" * 64}, found)
    assert migrate.pending({version: checksum}, found) == found[1:]


def test_dq_checks_are_unique_and_evaluate():
    names = [c.name for c in data_quality.CHECKS]
    assert len(names) == len(set(names))
    check = next(c for c in data_quality.CHECKS if c.name == "dnc_list_present")
    assert not data_quality.evaluate(check, 0)["passed"]
    assert data_quality.evaluate(check, 4)["passed"]


def test_kpi_sheet_upsert_replaces_month_row():
    header = ["health_plan", "month", "x"]
    rows, replaced = plan_kpis.upsert_rows([], header, ["P", "2026-09-01", "1"])
    assert rows == [header, ["P", "2026-09-01", "1"]] and not replaced
    rows, replaced = plan_kpis.upsert_rows(rows, header, ["P", "2026-09-01", "2"])
    assert rows == [header, ["P", "2026-09-01", "2"]] and replaced
    rows, _ = plan_kpis.upsert_rows(rows, header, ["P", "2026-10-01", "3"])
    assert len(rows) == 3


def test_reject_rate_check_flags_a_spike_but_not_normal_noise():
    check = next(c for c in data_quality.CHECKS if c.name == "load_reject_rate_pct")
    assert check.severity == "error"
    assert data_quality.evaluate(check, 5.1)["passed"]          # normal: ~5% of a history file
    assert not data_quality.evaluate(check, 64.0)["passed"]     # a partner format change
    assert "rows_in >= 20" in check.sql                          # tiny daily files can't trip it
