"""High-water marks for incremental API pulls (ops.watermarks).

The new value is written by the SAME transaction that merges the data it
covers (via loaders.stage_and_merge(extra_sql=...)), so a failed load can
never advance the watermark past records that didn't land.
"""
from datetime import datetime

from pipeline.redshift import fetch_all

# Where a brand-new source starts pulling from on its very first run.
DEFAULT = datetime(2026, 1, 1)


def get(source: str) -> datetime:
    # Latest record timestamp already loaded for this source, e.g.
    # "salesforce_activities" -> 2026-09-28 13:39. Next pull asks for > this.
    rows = fetch_all("SELECT watermark_ts FROM ops.watermarks WHERE source = %s;", (source,))
    return rows[0][0] if rows else DEFAULT


def set_sql(source: str, value: datetime) -> list[tuple[str, tuple]]:
    # Returned as SQL (not executed here) so the caller can run it inside the
    # same transaction as the data merge. Delete + insert is a simple upsert
    # (Redshift doesn't enforce the PRIMARY KEY, so no ON CONFLICT).
    return [
        ("DELETE FROM ops.watermarks WHERE source = %s;", (source,)),
        ("INSERT INTO ops.watermarks (source, watermark_ts) VALUES (%s, %s);", (source, value)),
    ]
