"""pandas DataFrame -> Parquet on S3 -> COPY into staging -> CALL merge proc.

The whole load for one load_id runs in a single Redshift transaction:
    DELETE staging rows for this load_id   (rerun-safe)
    COPY each staged Parquet file
    CALL core.sp_merge_<entity>(load_id)
    INSERT ops.load_audit row
    COMMIT
Any failure rolls back everything, so core never sees a half-loaded batch.
(TRUNCATE would commit implicitly in Redshift, which is why it's DELETE.)
"""
import io
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import config, s3_io
from pipeline.redshift import get_connection
from pipeline.schemas import StagingSpec


def _is_missing(v) -> bool:
    # pandas has several ways to say "no value" (None, NaT, NA, NaN) --
    # they all need to become a real NULL in Parquet/Redshift.
    return v is None or v is pd.NaT or v is pd.NA or (isinstance(v, float) and math.isnan(v))


def _coerce_column(values: pd.Series, arrow_type: pa.DataType) -> list:
    """Convert a pandas column into Python values Arrow will accept for the
    target type without surprises (floats -> exact Decimals, datetimes ->
    dates, NaN -> None)."""
    out = []
    for v in values.tolist():
        if _is_missing(v):
            out.append(None)
        elif pa.types.is_decimal(arrow_type):
            # Go through str() so 0.1 + 0.2 becomes Decimal("0.30"), not 0.2999...
            out.append(Decimal(str(v)).quantize(Decimal(1).scaleb(-arrow_type.scale)))
        elif pa.types.is_date32(arrow_type):
            # A full datetime in a DATE column -> keep just the date part.
            out.append(v.date() if isinstance(v, datetime) else v)
        elif pa.types.is_timestamp(arrow_type):
            ts = pd.Timestamp(v).floor("us")  # Redshift/Parquet: microsecond precision
            # Timezone-aware -> convert to UTC then drop the tz (Redshift TIMESTAMP is naive, stored as UTC).
            out.append((ts.tz_convert("UTC").tz_localize(None) if ts.tzinfo else ts).to_pydatetime())
        elif pa.types.is_integer(arrow_type):
            out.append(int(v))   # e.g. 20.0 (pandas float) -> 20
        elif pa.types.is_boolean(arrow_type):
            out.append(bool(v))
        else:
            out.append(str(v))   # strings: anything else is written as text
    return out


def to_parquet_bytes(df: pd.DataFrame, spec: StagingSpec) -> bytes:
    # Fail loudly if a column the staging table expects is missing --
    # better than COPY silently shifting columns (it maps by position).
    missing = set(spec.columns) - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame for staging.{spec.table} is missing columns: {sorted(missing)}")
    # Build one Arrow array per column, in the exact order and types of the contract.
    arrays = [pa.array(_coerce_column(df[name], typ), type=typ) for name, typ in spec.fields]
    table = pa.Table.from_arrays(arrays, schema=spec.arrow_schema)
    buf = io.BytesIO()                                   # write to memory, not disk
    pq.write_table(table, buf, compression="snappy")     # snappy: fast, Redshift-supported
    return buf.getvalue()


@dataclass
class LoadResult:
    # What a load reports back to its caller (and to Airflow's logs/XCom).
    load_id: str
    entity: str
    rows_staged: dict = field(default_factory=dict)  # {staging table: row count}


def stage_and_merge(
    load_id: str,
    entity: str,
    frames: list[tuple[StagingSpec, pd.DataFrame]],
    merge_proc: str,
    source_uri: str = "",
    rows_in: int | None = None,
    rows_rejected: int = 0,
    extra_sql: list[tuple[str, tuple]] | None = None,
) -> LoadResult:
    """Stage every (spec, frame) pair and run merge_proc, atomically.

    extra_sql runs inside the same transaction after the merge -- used e.g.
    to advance an API watermark only if the data it covers actually landed.
    """
    started = datetime.now(timezone.utc).replace(tzinfo=None)  # for ops.load_audit
    result = LoadResult(load_id=load_id, entity=entity)

    # 1. Write Parquet to S3 first (outside the transaction; overwriting the
    #    same key on a rerun is harmless).
    staged = []
    for spec, df in frames:
        df = df.assign(load_id=load_id)                     # tag every row with this load
        prefix = f"staged/{spec.table}/load_id={load_id}/"  # one folder per table per load
        if len(df):
            s3_io.put_bytes(prefix + "part-000.parquet", to_parquet_bytes(df, spec))
        staged.append((spec, prefix, len(df)))
        result.rows_staged[spec.table] = len(df)

    # 2. One transaction for staging refresh + merge + audit.
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for spec, prefix, n in staged:
                # Clear any rows a previous (failed or repeated) run left for this load_id.
                cur.execute(f"DELETE FROM staging.{spec.table} WHERE load_id = %s;", (load_id,))
                if n:
                    # Redshift reads the Parquet straight from S3 using its own IAM role.
                    cur.execute(
                        f"COPY staging.{spec.table} FROM %s IAM_ROLE %s FORMAT AS PARQUET;",
                        (s3_io.uri(prefix), config.REDSHIFT_IAM_ROLE_ARN),
                    )
            # The stored procedure moves this load's rows from staging into core.
            cur.execute(f"CALL {merge_proc}(%s);", (load_id,))
            # Anything that must commit together with the data (e.g. watermarks).
            for sql, params in extra_sql or []:
                cur.execute(sql, params)
            # Lineage/observability: where the data came from and how much landed.
            cur.execute(
                """
                INSERT INTO ops.load_audit (load_id, entity, source_uri, rows_in, rows_staged,
                                            rows_rejected, status, started_at, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, 'succeeded', %s, GETDATE());
                """,
                (load_id, entity, source_uri[:1000], rows_in, sum(result.rows_staged.values()),
                 rows_rejected, started),
            )
        conn.commit()  # everything above becomes visible at once
    except Exception as exc:
        conn.rollback()  # undo every statement in this load
        _record_failure(load_id, entity, source_uri, started, exc)
        raise            # re-raise so Airflow marks the task failed and retries
    finally:
        conn.close()
    return result


def _record_failure(load_id, entity, source_uri, started, exc):
    # Separate connection/transaction: the failed one was rolled back, and
    # we still want the failure visible in ops.load_audit.
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ops.load_audit (load_id, entity, source_uri, status, started_at, finished_at, details)
                VALUES (%s, %s, %s, 'failed', %s, GETDATE(), %s);
                """,
                (load_id, entity, source_uri[:1000], started, f"{type(exc).__name__}: {exc}"[:2000]),
            )
        conn.commit()
        conn.close()
    except Exception:
        pass  # never mask the original error


def mark_task_failed(task_id: str, entity_prefix: str | None, batch_date: str, since, error: str) -> str:
    """Make sure ops.load_audit has a 'failed' row for a task that failed in this
    DAG run, and return its load_id (used by the DAG failure callback).

    A failure inside stage_and_merge() already wrote that row (_record_failure);
    reuse the newest one for this task's entity since the run started. A task
    that failed before loading anything (missing file, API down, a data quality
    check) gets a new row keyed on the task and batch date.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if entity_prefix:
                cur.execute(
                    """SELECT load_id FROM ops.load_audit
                       WHERE status = 'failed' AND LEFT(entity, LEN(%s)) = %s AND started_at >= %s
                       ORDER BY started_at DESC LIMIT 1;""",
                    (entity_prefix, entity_prefix, since),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
            load_id = load_id_for(task_id, batch_date)[:64]
            cur.execute(
                """INSERT INTO ops.load_audit (load_id, entity, source_uri, status, started_at, finished_at, details)
                   VALUES (%s, %s, %s, 'failed', GETDATE(), GETDATE(), %s);""",
                (load_id, task_id[:50], f"airflow:{task_id}", error[:2000]),
            )
        conn.commit()
        return load_id
    finally:
        conn.close()


def load_id_for(source: str, batch_date: str | date) -> str:
    # e.g. ("member_file-evergreen", "2026-09-28") -> "member_file-evergreen-2026-09-28".
    # Same inputs always give the same id, which is what makes reruns replace, not duplicate.
    return f"{source}-{batch_date}"
