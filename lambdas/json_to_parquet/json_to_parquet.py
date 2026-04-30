"""
Lambda: JSON Reference Data → Silver Layer (Parquet)
────────────────────────────────────────────────────
Triggered by S3 event when new JSON lands in the Bronze bucket
under the reference_data prefix. (.json suffix)

Improvements over original:
  - Data validation before writing
  - Deduplication of category records
  - Proper error handling with dead-letter alerting
  - Idempotent writes (overwrites partition, not append)
  - Structured logging
  - Dot columns renamed (snippet.title → title) for Athena compatibility
  - key variable scoped safely before try/except
  - GLUE_DB default corrected to match live environment

Environment Variables:
    S3_BUCKET_SILVER            — Target bucket for cleansed data
    S3_BUCKET_ATHENA_RESULTS    — Athena query results bucket (needed for Glue catalog registration)
    GLUE_DB_SILVER              — Glue catalog database name
    GLUE_TABLE_REFERENCE        — Glue catalog table name
    SNS_ALERT_TOPIC_ARN         — SNS topic for alerts
"""

import json
import os # to get environment variables from configuration
import logging # write messages to cloudwatch
from datetime import datetime, timezone 
from urllib.parse import unquote_plus # if necessary to decode spaces and non-ASCII characters in file names

import boto3
import awswrangler as wr 
# AWS SDK for pandas
# read and write files directly to/ from s3 - while updating the glue catalog
# Use Pandas functions in AWS
# Works with pandas layer

import pandas as pd

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────
SILVER_BUCKET   = os.environ["S3_BUCKET_SILVER"]
ATHENA_BUCKET   = os.environ.get("S3_BUCKET_ATHENA_RESULTS", "youtube-data-gr-lambda-athena-results")
GLUE_DB         = os.environ.get("GLUE_DB_SILVER", "youtube-data-gr-silver")   # fixed default
GLUE_TABLE      = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC       = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
# the .get version will return None or the default given instaed of a KeyError

SILVER_PATH     = f"s3://{SILVER_BUCKET}/youtube/reference_data/"
ATHENA_OUTPUT   = f"s3://{ATHENA_BUCKET}/query-results/"

s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")

logger.info(f"Silver path   : {SILVER_PATH}")
logger.info(f"Athena output : {ATHENA_OUTPUT}")
logger.info(f"Glue catalog  : {GLUE_DB}.{GLUE_TABLE}")


# ─────────────────────────────────────────────────────────────────────────────
def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read raw JSON from S3 using boto3 instead of awswrangler.
    wr.s3.read_json() fails on YouTube category JSON because it has
    mixed types (strings + nested arrays) which pandas can't parse
    directly into a DataFrame.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())


def extract_region_from_key(key: str) -> str:
    """Pull region value from Hive-style S3 key, e.g. region=ca → 'ca'."""
    for part in key.split("/"):
        if part.lower().startswith("region="):
            return part.split("=", 1)[1].lower().strip()
    return "unknown"


def flatten_category_items(raw_data: dict) -> pd.DataFrame:
    """
    Normalise the YouTube API category envelope into a flat DataFrame.

    Input shape:
        { "kind": "...", "etag": "...", "items": [
            { "kind": "...", "etag": "...", "id": "1",
              "snippet": { "title": "Film & Animation", "assignable": true, ... }
            }, ...
        ]}

    Output columns (dot-free, Athena-safe):
        id, title, assignable, channel_id, kind, etag
    """
    # Check if items exists and is a list
    if "items" not in raw_data or not isinstance(raw_data["items"], list):
        raise ValueError(f"Expected 'items' array in JSON, got keys: {list(raw_data.keys())}")

    df = pd.json_normalize(raw_data["items"]) 
    # this flattens nested dicts into flat columns (snippet.title) but not nested lists which may need explode 

    # Rename dot-notation columns produced by pd.json_normalize so Athena
    # can query them without backtick escaping or parse errors.
    # e.g. "snippet.title" → "title", "snippet.assignable" → "assignable"
    rename_map = {}
    for col in df.columns:
        if "." in col:
            # Take the last segment: "snippet.channelId" → "channel_id"
            last = col.rsplit(".", 1)[-1]
            # camelCase → snake_case
            snake = "".join(
                ["_" + c.lower() if c.isupper() else c for c in last]
            ).lstrip("_")
            rename_map[col] = snake

    if rename_map:
        logger.info(f"  Renaming columns: {rename_map}")
        df = df.rename(columns=rename_map)

    # Resolve any duplicate column names caused by renaming
    seen = {}
    deduped = []
    for col in df.columns:
        if col in seen:
            seen[col] += 1
            deduped.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            deduped.append(col)
    df.columns = deduped

    return df


def validate_category_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and clean the category reference DataFrame."""
    if df.empty:
        raise ValueError("Empty DataFrame — no category items found")

    # Deduplicate on id
    if "id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["id"], keep="last")
        removed = before - len(df)
        if removed:
            logger.info(f"  Removed {removed} duplicate category rows")

    # Warn on missing expected columns
    expected = {"id", "title"}
    missing = expected - set(df.columns) # Not subtraction, set 1- set 2 , what is in set 1 but not set 2 
    if missing:
        logger.warning(f"  Missing expected columns after normalisation: {missing}")

    return df


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


# ─────────────────────────────────────────────────────────────────────────────
def process_record(bucket: str, key: str) -> dict:
    """Process a single S3 object. Returns a result dict."""
    logger.info(f"Processing: s3://{bucket}/{key}")

    raw_data = read_json_from_s3(bucket, key)
    df = flatten_category_items(raw_data)
    logger.info(f"  Raw shape after flatten: {df.shape}")

    df = validate_category_data(df)

    # Metadata
    df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    df["_source_file"] = key
    df["region"] = extract_region_from_key(key)

    logger.info(f"  Final shape: {df.shape}, region: {df['region'].iloc[0]}")

    # Write to Silver — overwrite_partitions makes writes idempotent per region
    wr.s3.to_parquet(
        df=df,
        path=SILVER_PATH,
        dataset=True, # allow you to update glue catalog and query with Athena 
        database=GLUE_DB,
        table=GLUE_TABLE,
        partition_cols=["region"],
        mode="overwrite_partitions",
        schema_evolution=True,
        boto3_session=boto3.Session(), # allow lmabda to use the defualt IAM role for the operation
        s3_additional_kwargs={}, # Allows for my s3 settings
    )

    logger.info(f"  Written to Silver: {SILVER_PATH}")
    return {"key": key, "region": df["region"].iloc[0], "rows": len(df)}


# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Entry point. Supports:
      - Direct S3 event notifications  (Records[].s3)
      - EventBridge S3 events          (detail.bucket / detail.object)
      - Step Functions direct invoke   (pass bucket + key explicitly)
    """
    processed = []
    errors    = []

    # ── Normalise event shape into a list of (bucket, key) tuples ────────
    invocations = []

    # Standard S3 notification
    for record in event.get("Records", []):
        s3_info = record.get("s3", {})
        b = s3_info.get("bucket", {}).get("name")
        k = unquote_plus(s3_info.get("object", {}).get("key", "")) 
        # unquoute plus only needed for file names with spaces and non-ASCII characters 
        if b and k:
            invocations.append((b, k))

    # EventBridge S3 event
    if not invocations and "detail" in event:
        detail = event["detail"]
        b = detail.get("bucket", {}).get("name")
        k = unquote_plus(detail.get("object", {}).get("key", ""))
        if b and k:
            invocations.append((b, k))

    # Step Functions direct invoke: {"bucket": "...", "key": "..."}
    if not invocations and "bucket" in event and "key" in event:
        invocations.append((event["bucket"], unquote_plus(event["key"])))

    if not invocations:
        logger.warning("No S3 records found in event — nothing to process.")
        logger.warning(f"Event received: {json.dumps(event, default=str)}")
        return {"statusCode": 200, "processed": [], "errors": []}

    # ── Process each file ─────────────────────────────────────────────────
    for bucket, key in invocations:
        try:
            result = process_record(bucket, key)
            processed.append(result)
        except Exception as e:
            logger.error(f"Error processing s3://{bucket}/{key}: {e}", exc_info=True)
            errors.append({"bucket": bucket, "key": key, "error": str(e)})

    # ── Alert on any failures ─────────────────────────────────────────────
    if errors:
        send_alert(
            subject="[YT Pipeline] Silver reference transform failed",
            message=json.dumps(errors, indent=2), # converts from python object to json object on 2 lines
        )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors,
    }