"""Create (or reset) the triage agent's read-only Redshift user and give it the
triage_reader_ro role from V017 (SELECT on ops.load_audit, ops.dq_results and
ops.v_triage_staging_counts -- nothing else). Run once, as the admin user,
after `python -m pipeline.migrate`:

    python -m pipeline.triage.setup_reader

The password comes from TRIAGE_REDSHIFT_PASSWORD in .env (Redshift requires
8+ characters with upper case, lower case and a digit). Writing the triage
note back to ops.load_audit is done by the ETL user, never by this one.
"""
from pipeline import config
from pipeline.redshift import get_connection

ROLE = "triage_reader_ro"


def main() -> None:
    user, pw = config.TRIAGE_REDSHIFT_USER, config.TRIAGE_REDSHIFT_PASSWORD
    if not pw:
        raise SystemExit("Set TRIAGE_REDSHIFT_PASSWORD in .env first")
    if not user.isidentifier():
        raise SystemExit(f"TRIAGE_REDSHIFT_USER {user!r} is not a plain identifier")
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_user WHERE usename = %s;", (user,))
            exists = cur.fetchone() is not None
            # The user name comes from config (checked above); the password goes through a parameter.
            cur.execute(f"{'ALTER' if exists else 'CREATE'} USER {user} PASSWORD %s;", (pw,))
            cur.execute(f"ALTER USER {user} SET statement_timeout TO 30000;")   # 30 s per query
            cur.execute(f"GRANT ROLE {ROLE} TO {user};")
        conn.commit()
    finally:
        conn.close()
    print(f"{user}: {'password reset' if exists else 'created'}, role {ROLE} (read-only, counts and IDs only)")


if __name__ == "__main__":
    main()
