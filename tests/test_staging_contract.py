"""COPY ... FORMAT AS PARQUET maps columns by position. If the Parquet
schema and the staging DDL drift apart, loads either fail or -- worse --
silently put values in the wrong columns. This test pins them together."""
import re
from pathlib import Path

import pytest

from pipeline.schemas import ALL_SPECS

# Staging tables can be added by any migration (V003 created the first ones),
# so read them all.
SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "redshift"
DDL = "\n".join(p.read_text() for p in sorted(SQL_DIR.glob("V*.sql")))


def ddl_columns(table: str) -> list[str]:
    m = re.search(rf"CREATE TABLE IF NOT EXISTS staging\.{table} \((.*?)\n\);", DDL, re.S)
    assert m, f"staging.{table} not found in any migration"
    body = re.sub(r"--[^\n]*", "", m.group(1))
    return [col.strip().split()[0] for col in re.split(r",\s*\n", body) if col.strip()]


@pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s.table)
def test_parquet_schema_matches_staging_ddl(spec):
    assert spec.columns == ddl_columns(spec.table)


def test_every_staging_table_has_a_spec():
    tables = set(re.findall(r"CREATE TABLE IF NOT EXISTS staging\.(\w+)", DDL))
    assert tables == {s.table for s in ALL_SPECS}
