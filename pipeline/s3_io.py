"""Thin boto3 wrapper for the data lake bucket.

Key layout (Hive-style partitions so Athena/Spectrum could read it later):
    raw/<source>/dt=YYYY-MM-DD/...        partner drops and API payloads, as received
    staged/<table>/load_id=<id>/...       Parquet written for Redshift COPY
    rejects/<source>/dt=YYYY-MM-DD/...    rows that failed validation
"""
from functools import lru_cache

import boto3

from pipeline import config


@lru_cache(maxsize=1)
def _client():
    # One S3 client per process: lru_cache makes the first call build it and
    # every later call reuse it. Credentials come from AWS_PROFILE / ~/.aws.
    return boto3.client("s3", region_name=config.AWS_REGION)


def uri(key: str) -> str:
    # Turn an object key like "raw/x.csv" into "s3://bucket/raw/x.csv"
    # (the form Redshift COPY and the audit tables use).
    return f"s3://{config.S3_BUCKET}/{key}"


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    # Bucket default encryption (SSE-S3) applies; ServerSideEncryption is set
    # explicitly anyway so the intent is visible at the call site.
    _client().put_object(
        Bucket=config.S3_BUCKET, Key=key, Body=data,
        ContentType=content_type, ServerSideEncryption="AES256",
    )
    return uri(key)  # callers log/audit the full s3:// location


def get_bytes(key: str) -> bytes:
    # Download the whole object into memory -- files here are small (KBs),
    # so no temp files ever touch local disk.
    return _client().get_object(Bucket=config.S3_BUCKET, Key=key)["Body"].read()


def list_keys(prefix: str) -> list[str]:
    # list_objects_v2 returns at most 1000 keys per call -- paginate.
    paginator = _client().get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=config.S3_BUCKET, Prefix=prefix):
        # "Contents" is missing entirely when a page (or prefix) is empty.
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return sorted(keys)  # stable order -> deterministic processing and logs
