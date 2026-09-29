"""Simulated upstream systems. Health plans send roster files (SFTP -> S3 in
real life); here they're generated so the pipeline has realistic,
deliberately messy input every day. API-based sources (Salesforce, events
platform, Google Sheets) are simulated by mock_api/ instead.

Generators are seeded from the batch date, so regenerating a date
reproduces byte-identical files -- backfills are deterministic.
"""
import hashlib
import random


def rng_for(namespace: str, batch_date: str) -> random.Random:
    # md5 of "namespace-date" -> a big number -> a valid random seed. Same
    # namespace + date always gives the same random sequence (deterministic files).
    seed = int(hashlib.md5(f"{namespace}-{batch_date}".encode()).hexdigest(), 16) % (2**32)
    return random.Random(seed)
