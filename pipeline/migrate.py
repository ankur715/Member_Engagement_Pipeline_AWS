"""Versioned, checksummed migrations for Redshift (Flyway-style, no extra tool).

- Files are sql/redshift/V<NNN>__<name>.sql, applied in order.
- Each file runs in its own transaction together with its row in
  ops.schema_migrations, so a failed migration leaves no trace.
- Editing an already-applied file is an error (checksum mismatch): schema
  changes go in a NEW migration. That's what keeps table evolution safe.

Usage: python -m pipeline.migrate [--dry-run]
"""
import hashlib
import re
import sys
from pathlib import Path

from pipeline.redshift import get_connection

# <project>/sql/redshift, located relative to this file (works from any folder).
SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "redshift"
# Valid names look like V001__schemas.sql -> version 1.
FILENAME = re.compile(r"^V(\d{3})__(\w+)\.sql$")

# The migrations table has to exist before we can check what's been applied.
BOOTSTRAP = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.schema_migrations (
    version     INTEGER       NOT NULL,
    filename    VARCHAR(200)  NOT NULL,
    checksum    VARCHAR(64)   NOT NULL,
    applied_at  TIMESTAMP     NOT NULL DEFAULT GETDATE()
);
"""


def discover() -> list[tuple[int, Path, str]]:
    # Find every migration file, in name order, with a SHA-256 of its contents.
    found = []
    for path in sorted(SQL_DIR.glob("V*.sql")):
        m = FILENAME.match(path.name)
        if not m:
            raise ValueError(f"Bad migration filename: {path.name}")
        found.append((int(m.group(1)), path, hashlib.sha256(path.read_bytes()).hexdigest()))
    # Two files claiming the same version would make the order ambiguous.
    versions = [v for v, _, _ in found]
    if len(versions) != len(set(versions)):
        raise ValueError("Duplicate migration version numbers")
    return found


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into single statements. Redshift validates a
    multi-statement batch as a whole, so `CREATE SCHEMA x; CREATE TABLE x.t`
    sent together fails -- each statement must go on its own. Semicolons
    inside $$-quoted procedure bodies, 'strings' and -- comments don't count."""
    statements, buf, i = [], [], 0
    # Which kind of text we're currently inside (only one can be true at a time).
    in_dollar = in_quote = in_comment = False
    while i < len(sql):
        ch, two = sql[i], sql[i:i + 2]   # current char, and it plus the next one
        if in_comment:
            buf.append(ch)
            in_comment = ch != "\n"       # a -- comment ends at the end of the line
        elif in_dollar:
            if two == "$$":               # closing $$ of a procedure body
                buf.append(two)
                i += 1
                in_dollar = False
            else:
                buf.append(ch)            # anything (including ;) inside $$...$$ is body text
        elif in_quote:
            buf.append(ch)
            in_quote = ch != "'"          # closing quote ends the string
        elif two == "--":                 # start of a comment
            buf.append(two)
            i += 1
            in_comment = True
        elif two == "$$":                 # start of a procedure body
            buf.append(two)
            i += 1
            in_dollar = True
        elif ch == "'":                   # start of a string literal
            buf.append(ch)
            in_quote = True
        elif ch == ";":                   # a real statement terminator
            statements.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf).strip())  # whatever follows the last ;
    # Drop fragments that are only whitespace/comments.
    return [s for s in statements
            if any(line.strip() and not line.strip().startswith("--") for line in s.splitlines())]


def pending(applied: dict[int, str], migrations) -> list[tuple[int, Path, str]]:
    # applied = {version: checksum} from ops.schema_migrations.
    todo = []
    for version, path, checksum in migrations:
        if version in applied:
            # Already ran: its file must be byte-for-byte unchanged.
            if applied[version] != checksum:
                raise RuntimeError(
                    f"{path.name} was modified after being applied. "
                    "Never edit an applied migration -- add a new one."
                )
            continue
        todo.append((version, path, checksum))  # not applied yet -> run it
    return todo


def main(dry_run: bool = False) -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Make sure the bookkeeping table exists, then read what's applied.
            for statement in split_statements(BOOTSTRAP):
                cur.execute(statement)
            conn.commit()
            cur.execute("SELECT version, checksum FROM ops.schema_migrations;")
            applied = dict(cur.fetchall())

        todo = pending(applied, discover())
        for version, path, checksum in todo:
            print(f"{'Would apply' if dry_run else 'Applying'} {path.name}")
            if dry_run:
                continue
            with conn.cursor() as cur:
                # Run each statement of the file, then record the file as applied --
                # all in one transaction, so it's all-or-nothing.
                for statement in split_statements(path.read_text()):
                    cur.execute(statement)
                cur.execute(
                    "INSERT INTO ops.schema_migrations (version, filename, checksum) VALUES (%s, %s, %s);",
                    (version, path.name, checksum),
                )
            conn.commit()
        if not todo:
            print("Schema up to date.")
        return [p.name for _, p, _ in todo]
    except Exception:
        conn.rollback()  # a failed migration leaves nothing half-applied
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # `python -m pipeline.migrate --dry-run` lists what would run without running it.
    main(dry_run="--dry-run" in sys.argv)
