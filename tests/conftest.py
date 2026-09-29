import os
import sys

# Test config must be in place before pipeline.config is imported.
os.environ.update({
    "S3_BUCKET": "test-claims-lake",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "testing",
    "AWS_SECRET_ACCESS_KEY": "testing",
    "PHI_HASH_KEY": "unit-test-key",
    "REDSHIFT_IAM_ROLE_ARN": "arn:aws:iam::123456789012:role/test-copy",
    "MOCK_API_TOKEN": "local-dev-token",
})
os.environ.pop("AWS_PROFILE", None)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import boto3  # noqa: E402
import pytest  # noqa: E402
from moto import mock_aws  # noqa: E402


@pytest.fixture
def s3_bucket():
    from pipeline import s3_io
    with mock_aws():
        s3_io._client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-claims-lake")
        yield "test-claims-lake"
    s3_io._client.cache_clear()


class FakeCursor:
    def __init__(self, log, fail_on=None):
        self.log, self.fail_on = log, fail_on

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("simulated Redshift error")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConnection:
    """Records SQL so tests can assert on transaction shape without Redshift."""
    def __init__(self, fail_on=None):
        self.log, self.fail_on = [], fail_on
        self.committed = self.rolled_back = False

    def cursor(self):
        return FakeCursor(self.log, self.fail_on)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass
