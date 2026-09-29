#!/usr/bin/env bash
# BEFORE: the nightly cron job the DAG replaces.
#
# What's wrong with this shape (and where the DAG fixes it):
#   - One step fails -> everything after it silently doesn't run; the only
#     signal is a log file nobody reads.         (DAG: task state + email alert)
#   - No retries for transient failures (Redshift resuming, API 429/503).
#                                                (DAG: retries w/ exponential backoff)
#   - Strictly sequential even where steps are independent.
#                                                (DAG: sources load in parallel)
#   - Timing coupling: the Salesforce and SDoH jobs are scheduled 30/60 min
#     later and *hope* earlier jobs finished.    (DAG: explicit dependencies)
#   - "Today" is implicit, so re-running yesterday means editing the script.
#                                                (DAG: logical date `ds`, backfills)
#   - KPIs go out even if the data behind them is broken.
#                                                (DAG: publish only after DQ passes)
#   - No record of what ran, when, or on what.   (DAG: run history, ops.load_audit,
#                                                 ops.dq_results, ops.v_sla_status)
set -euo pipefail
cd /opt/engagement
source .venv/bin/activate

DS=$(date +%F)

python -m pipeline.migrate
python -m pipeline.ingest.member_files "$DS"
python -m pipeline.ingest.events "$DS"
python -m pipeline.ingest.contact_preferences "$DS"
python -m pipeline.quality.data_quality "$DS"
