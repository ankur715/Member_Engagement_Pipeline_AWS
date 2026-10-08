"""Read-only Redshift access for the triage agent.

  - Connects as TRIAGE_REDSHIFT_USER (triage_reader), which only has the
    triage_reader_ro role from V017: SELECT on the three ops objects in TABLES
    and nothing else -- no staging, core or care table, so no member PHI.
  - No free-form SQL: callers pass the NAME of one of the fixed queries in
    QUERIES plus parameters; there is no way to hand this module a SQL string.
  - One connection per triage run, opened on first use, so a paused
    Serverless workgroup wakes once; results are cached per (query, params),
    so asking for the same thing twice costs nothing.
"""
from datetime import date, datetime
from decimal import Decimal

from pipeline import config
from pipeline.redshift import get_connection

# Everything the triage user can read (V017 grants exactly these).
TABLES = ("ops.load_audit", "ops.dq_results", "ops.v_triage_staging_counts")

QUERIES = {
    # get_load_audit: the failed load plus every other load of the same batch date
    # (load ids end with, or contain, the batch date).
    "batch_loads": """
        SELECT load_id, entity, status, rows_in, rows_staged, rows_rejected,
               started_at, finished_at, details
        FROM ops.load_audit
        WHERE load_id = %(load_id)s OR POSITION(%(batch_date)s IN load_id) > 0
        ORDER BY started_at
        LIMIT 100;""",
    # get_dq_results: every check for the batch date, plus the previous 7 days
    # of any check that failed, in ONE query.
    "dq_results": """
        WITH failed AS (
            SELECT DISTINCT check_name FROM ops.dq_results
            WHERE batch_date = %(batch_date)s AND NOT passed
        )
        SELECT batch_date, check_name, severity, passed, observed_value
        FROM ops.dq_results
        WHERE batch_date = %(batch_date)s
           OR (check_name IN (SELECT check_name FROM failed)
               AND batch_date BETWEEN %(batch_date)s::DATE - 7 AND %(batch_date)s::DATE - 1)
        ORDER BY check_name, batch_date DESC;""",
    # get_staging_counts: rows staged per staging table for this batch's loads (counts only).
    "staging_counts": """
        SELECT staging_table, load_id, row_count
        FROM ops.v_triage_staging_counts
        WHERE POSITION(%(batch_date)s IN load_id) > 0
        ORDER BY staging_table, load_id
        LIMIT 200;""",
    # get_file_history: recent loads of the same source, newest first.
    "entity_history": """
        SELECT load_id, entity, status, rows_in, rows_staged, rows_rejected, started_at
        FROM ops.load_audit
        WHERE LEFT(entity, LEN(%(entity_prefix)s)) = %(entity_prefix)s
        ORDER BY started_at DESC
        LIMIT %(limit)s;""",
}


def _jsonable(v):
    # Redshift returns Decimal / date / datetime; the model gets JSON.
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


class ReadOnlySession:
    """One read-only connection + result cache for a single triage run."""

    def __init__(self):
        self._conn = None
        self._cache: dict[tuple, list[dict]] = {}
        self.queries_run = 0

    def query(self, name: str, **params) -> list[dict]:
        sql = QUERIES[name]          # KeyError for anything that isn't a fixed, named query
        key = (name, tuple(sorted(params.items())))
        if key not in self._cache:
            if self._conn is None:
                if not config.TRIAGE_REDSHIFT_PASSWORD:
                    raise RuntimeError("TRIAGE_REDSHIFT_PASSWORD is not set (run python -m pipeline.triage.setup_reader)")
                self._conn = get_connection(user=config.TRIAGE_REDSHIFT_USER,
                                            password=config.TRIAGE_REDSHIFT_PASSWORD)
            try:
                with self._conn.cursor() as cur:
                    cur.execute(sql, params)
                    cols = [d[0] for d in cur.description]
                    self._cache[key] = [{c: _jsonable(v) for c, v in zip(cols, row)} for row in cur.fetchall()]
            finally:
                self._conn.rollback()    # read-only: never leave a transaction open
            self.queries_run += 1
        return self._cache[key]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
