"""Run the triage agent on a failed load from the command line and watch it work:

    python -m pipeline.triage.run load_claims-2026-10-02 --task load_claims            # print the diagnosis
    python -m pipeline.triage.run load_claims-2026-10-02 --task load_claims --write    # ...and save it

The batch date is read from the load id; the error comes from the load's audit
row (read-only). Needs LLM_PROVIDER other than none, Bedrock model access, and
the triage_reader user (python -m pipeline.triage.setup_reader).
"""
import argparse
import logging
import re

from pipeline import config
from pipeline.triage import agent
from pipeline.triage.readonly import ReadOnlySession


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("load_id")
    ap.add_argument("--task", required=True, help="the DAG task that failed, e.g. load_claims or data_quality")
    ap.add_argument("--write", action="store_true", help="save the note to ops.load_audit")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.getLogger("botocore").setLevel(logging.WARNING)   # only the agent's steps, not AWS SDK chatter

    if not agent.enabled():
        raise SystemExit("LLM_PROVIDER=none: the triage agent is turned off")
    m = re.search(r"\d{4}-\d{2}-\d{2}", args.load_id)
    if not m:
        raise SystemExit(f"Can't find a batch date (YYYY-MM-DD) in {args.load_id!r}")
    batch_date = m.group(0)
    session = ReadOnlySession()
    try:
        rows = [r for r in session.query("batch_loads", load_id=args.load_id, batch_date=batch_date)
                if r["load_id"] == args.load_id]
    finally:
        session.close()
    if not rows:
        raise SystemExit(f"No audit row for {args.load_id}")
    audit = rows[-1]
    print(f"Triage {args.load_id} ({args.task}, status {audit['status']}) with {agent.model_id()}, "
          f"at most {config.TRIAGE_MAX_STEPS} steps / {config.TRIAGE_MAX_TOKENS} tokens\n", flush=True)

    run = agent.triage_failed_load if args.write else agent.triage
    result = run(args.load_id, args.task, batch_date, audit.get("details") or "unknown error")
    print("\n" + "=" * 72)
    print(result.note or f"(no note: {result.status})")
    print("=" * 72)
    print(f"status {result.status} | {result.steps} steps | {result.tokens} tokens | "
          f"{result.redshift_queries} Redshift queries | saved: {bool(args.write and result.note)}")


if __name__ == "__main__":
    main()
