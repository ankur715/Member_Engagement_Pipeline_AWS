"""Redshift connection helpers. Redshift speaks the Postgres wire protocol,
so plain psycopg2 works; port 5439, SSL required.
"""
import time

import psycopg2

from pipeline import config


def get_connection(attempts: int = 4, user: str | None = None, password: str | None = None):
    # connect_timeout is generous because a paused Serverless workgroup takes
    # a few seconds to resume on the first query of the day. Transient network
    # drops are retried with backoff (5s, 10s, 20s) before giving up.
    # user/password default to the ETL user; the triage agent passes its read-only user.
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(
                host=config.REDSHIFT_HOST,
                port=config.REDSHIFT_PORT,
                dbname=config.REDSHIFT_DB,
                user=user or config.REDSHIFT_USER,
                password=password or config.REDSHIFT_PASSWORD,
                sslmode="require",   # encrypted in transit -- PHI never crosses the wire in clear text
                connect_timeout=30,  # seconds to wait for the TCP/SSL handshake
            )
        except psycopg2.OperationalError:
            if attempt == attempts:
                raise  # out of retries: surface the real error to Airflow
            time.sleep(5 * 2 ** (attempt - 1))  # 5s, 10s, 20s


def fetch_all(sql: str, params=None) -> list[tuple]:
    # Convenience for read-only queries: open a connection, run one query,
    # return all rows, and always close the connection (even on error).
    # params are passed separately so psycopg2 escapes them -- never
    # f-string user/data values into SQL.
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def current_member_ids() -> set[str]:
    # Everyone on a current health-plan roster (the SCD2 "is_current" row).
    return {r[0] for r in fetch_all("SELECT member_id FROM core.member_eligibility WHERE is_current;")}
