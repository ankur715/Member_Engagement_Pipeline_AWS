"""All runtime configuration comes from environment variables (.env locally,
Airflow Variables/Connections or Secrets Manager in a real deployment).
Nothing secret is ever hardcoded or committed.
"""
import os

from dotenv import load_dotenv

# Load the project's .env file (one folder up from pipeline/) into os.environ,
# no matter which folder the script is launched from. Values already set in
# the real environment win over .env, so Airflow/CI can override anything.
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# --- AWS ---
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")   # region everything lives in
S3_BUCKET = os.environ.get("S3_BUCKET", "")              # the data lake bucket (terraform output)

# --- Redshift Serverless (Postgres-compatible, port 5439) ---
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST", "")                  # workgroup endpoint
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))         # env vars are strings -> int
REDSHIFT_DB = os.environ.get("REDSHIFT_DB", "engagement")            # database created by Terraform
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "admin")             # admin/ETL user
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD", "")          # never has a default
REDSHIFT_IAM_ROLE_ARN = os.environ.get("REDSHIFT_IAM_ROLE_ARN", "")  # role Redshift assumes to COPY from S3

# --- PHI: secret key for HMAC member tokens (see phi.py) ---
PHI_HASH_KEY = os.environ.get("PHI_HASH_KEY", "")

# --- Mock external APIs (mock_api/ -- stands in for Salesforce, events platform, Sheets) ---
MOCK_API_URL = os.environ.get("MOCK_API_URL", "http://localhost:9000")
MOCK_API_TOKEN = os.environ.get("MOCK_API_TOKEN", "local-dev-token")

# --- Public data APIs. Default to the mock (offline); set to the real hosts:
#     NYC_OPEN_DATA_URL=https://data.cityofnewyork.us   NWS_API_URL=https://api.weather.gov
NYC_OPEN_DATA_URL = os.environ.get("NYC_OPEN_DATA_URL", MOCK_API_URL)
NYC_OPEN_DATA_APP_TOKEN = os.environ.get("NYC_OPEN_DATA_APP_TOKEN", "")  # optional; raises rate limits
NWS_API_URL = os.environ.get("NWS_API_URL", MOCK_API_URL)
# api.weather.gov requires a User-Agent identifying the app and a contact.
NWS_USER_AGENT = os.environ.get("NWS_USER_AGENT", "member-engagement-pipeline (contact@example.com)")

# --- Real Salesforce (optional). If SF_USERNAME is blank, the mock is used. ---
SF_USERNAME = os.environ.get("SF_USERNAME", "")
SF_PASSWORD = os.environ.get("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.environ.get("SF_SECURITY_TOKEN", "")
SF_DOMAIN = os.environ.get("SF_DOMAIN", "login")   # "login" = production/dev org, "test" = sandbox

# --- Real Google Sheets (optional). If blank, the mock Sheets API is used. ---
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")  # path to key file
GOOGLE_DNC_SHEET_ID = os.environ.get("GOOGLE_DNC_SHEET_ID", "")   # input: do-not-contact sheet
GOOGLE_KPI_SHEET_ID = os.environ.get("GOOGLE_KPI_SHEET_ID", "")   # output: KPI report sheet
