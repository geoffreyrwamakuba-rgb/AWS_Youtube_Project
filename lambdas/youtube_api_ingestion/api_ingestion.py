"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────
Triggered by EventBridge on a schedule (e.g., every 6 hours).
Pulls trending videos from the YouTube Data API for each configured region
and writes raw JSON responses to the Bronze S3 bucket.

Improvements over v1:
  1. requests + urllib3 Retry replaces urllib + manual backoff
  2. Pagination — follows nextPageToken until exhausted
  3. Quota tracking — emits EstimatedQuotaUnitsUsed to CloudWatch
  4. Idempotency guard — skips duplicate writes if EventBridge fires twice

Environment Variables:
    YOUTUBE_API_KEY     — YouTube Data API v3 key
    S3_BUCKET_BRONZE    — Target S3 bucket for raw data
    YOUTUBE_REGIONS     — Comma-separated region codes (default: US,GB,CA,...)
    SNS_ALERT_TOPIC_ARN — SNS topic for failure alerts
"""

import json
import os
import logging
from datetime import datetime, timezone

import boto3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Structured JSON logging — queryable in CloudWatch Logs Insights, vs just string with logger.info()"""
def log(level: str, msg: str, **kwargs) -> None:
    logger.log(
        getattr(logging, level.upper()),
        json.dumps({"message": msg, **kwargs}),
    )


# ── AWS Clients ──────────────────────────────────────────────────────────────
s3_client  = boto3.client("s3")
sns_client = boto3.client("sns")
cw_client  = boto3.client("cloudwatch")

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY    = os.environ["YOUTUBE_API_KEY"]
BUCKET     = os.environ["S3_BUCKET_BRONZE"]
REGIONS    = os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,DE,FR,IN,JP,KR,MX,RU").split(",")
SNS_TOPIC  = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
API_BASE   = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50

# YouTube Data API v3 quota costs (units per call)
# https://developers.google.com/youtube/v3/determine_quota_cost
QUOTA_COST_VIDEOS     = 100   # videos.list with snippet+statistics+contentDetails
QUOTA_COST_CATEGORIES = 1     # videoCategories.list


# ── HTTP Session ─────────────────────────────────────────────────────────────
def build_session() -> requests.Session:
    """
    Build a requests Session with:
      - Connection pooling (reused across regions in the same invocation)
      - Automatic retry with exponential backoff for transient errors
      - 403 (quota exhausted) is NOT retried — fail fast and surface it
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,                        # waits: 1s, 2s, 4s
        status_forcelist=[429, 500, 502, 503, 504], # 429 and 500 not included by default 
        allowed_methods=["GET"],
        raise_on_status=False,                   # we call raise_for_status() ourselves
    )
    adapter = HTTPAdapter(max_retries=retry) 
    session.mount("https://", adapter) # this adds retry logic to your requests session
    return session


# Module-level session — reused across warm invocations (no rebuild cost)
SESSION = build_session()


# ── YouTube API Calls ────────────────────────────────────────────────────────
def fetch_trending_videos(region_code: str, api_key: str) -> tuple[list[dict], int]:
    """
    Fetch all trending videos for a region, following pagination.
    Returns (items, pages_fetched).

    Quota cost: QUOTA_COST_VIDEOS per page fetched.
    """
    items       = []
    page_token  = None
    pages       = 0

    while True:
        params = {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode":  region_code,
            "maxResults":  MAX_RESULTS,
            "key":         api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = SESSION.get(f"{API_BASE}/videos", params=params, timeout=30)

        # 403 = quota exhausted; surface it immediately, no retry
        if resp.status_code == 403:
            log("error", "quota exhausted or forbidden", region=region_code,
                status=resp.status_code, body=resp.text[:500])
            resp.raise_for_status()

        resp.raise_for_status()
        data = resp.json() # alternative to data = json.loads(resp.text)
        pages += 1

        items.extend(data.get("items", []))
        page_token = data.get("nextPageToken")

        if not page_token:
            break

    return items, pages


def fetch_video_categories(region_code: str, api_key: str) -> list[dict]:
    """
    Fetch the video category mapping for a region.
    Single page — the API does not paginate this endpoint.

    Quota cost: QUOTA_COST_CATEGORIES (1 unit).
    """
    params = {
        "part":       "snippet",
        "regionCode": region_code,
        "key":        api_key,
    }
    resp = SESSION.get(f"{API_BASE}/videoCategories", params=params, timeout=30)

    if resp.status_code == 403:
        log("error", "quota exhausted or forbidden", region=region_code,
            status=resp.status_code, body=resp.text[:500])
        resp.raise_for_status()

    resp.raise_for_status()
    return resp.json().get("items", [])


# ── Quota Tracking ───────────────────────────────────────────────────────────
def emit_quota_metric(units: int, ingestion_id: str) -> None:
    """
    Publish estimated quota units consumed to CloudWatch.
    Set an alarm at ~8,000 units/day (80% of the free 10,000 quota)
    to get early warning before the pipeline starts failing with 403s.
    """
    try:
        cw_client.put_metric_data(
            Namespace="YTPipeline",
            MetricData=[
                {
                    "MetricName": "EstimatedQuotaUnitsUsed",
                    "Value":      float(units),
                    "Unit":       "Count",
                    "Dimensions": [
                        {"Name": "IngestionId", "Value": ingestion_id}
                    ],
                }
            ],
        )
        log("info", "quota metric emitted", units=units, ingestion_id=ingestion_id)
    except Exception as e:
        # Non-fatal — don't let metric emission break the pipeline
        log("warning", "failed to emit quota metric", error=str(e))


# ── S3 Helpers ───────────────────────────────────────────────────────────────
def s3_key_exists(bucket: str, key: str) -> bool:
    """Return True if the S3 object already exists (idempotency guard)."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def write_to_s3(data: dict, bucket: str, key: str) -> None:
    """Write JSON data to S3."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"), 
        # Python dict → JSON string → UTF-8 bytes → stored in S3, S3's put_object requires bytes
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source":              "youtube_data_api_v3",
        },
    )


# ── Alerting ─────────────────────────────────────────────────────────────────
def send_alert(subject: str, message: str) -> None:
    if SNS_TOPIC:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )


# ── Main Handler ─────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Main handler. Iterates over regions, fetches trending videos
    and category mappings, writes everything to the Bronze layer.

    The ingestion_id is derived from the EventBridge scheduled event time
    (event["time"]) rather than datetime.now() so that if the Lambda retries
    or is invoked twice for the same schedule tick, the S3 keys are identical
    and the idempotency guard prevents duplicate writes.
    """
    # Prefer EventBridge event time for stable, idempotent partitioning
    event_time_str = event.get("time")
    if event_time_str:
        now = datetime.fromisoformat(event_time_str.replace("Z", "+00:00"))
    else:
        now = datetime.now(timezone.utc)

    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id   = now.strftime("%Y%m%d_%H%M%S")

    api_key       = API_KEY
    results       = {"success": [], "failed": []}
    total_quota   = 0

    for region in REGIONS:
        region_code = region.strip().lower()   # 
        log("info", "processing region", region=region_code, ingestion_id=ingestion_id)

        # ── Trending Videos ──────────────────────────────────────────────────
        try:
            s3_key = (
                f"youtube/raw_statistics/"
                f"region={region_code}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )

            if s3_key_exists(BUCKET, s3_key):
                # EventBridge fired twice for the same window — skip safely
                log("info", "skipping duplicate trending write",
                    region=region_code, s3_key=s3_key)
            else:
                items, pages = fetch_trending_videos(region_code, api_key)
                total_quota += pages * QUOTA_COST_VIDEOS

                payload = {
                    "items": items,
                    "_pipeline_metadata": {
                        "ingestion_id":        ingestion_id,
                        "region":              region_code,
                        "ingestion_timestamp": now.isoformat(),
                        "video_count":         len(items),
                        "pages_fetched":       pages,
                        "source":              "youtube_data_api_v3",
                    },
                }
                write_to_s3(payload, BUCKET, s3_key)
                log("info", "wrote trending videos",
                    region=region_code, video_count=len(items),
                    pages=pages, s3_key=s3_key)

        except requests.HTTPError as e:
            log("error", "HTTP error fetching trending",
                region=region_code, error=str(e))
            results["failed"].append(
                {"region": region_code, "type": "trending", "error": str(e)}
            )
            continue   # categories depend on a working API key — skip if trending fails
        except Exception as e:
            log("error", "unexpected error fetching trending",
                region=region_code, error=str(e))
            results["failed"].append(
                {"region": region_code, "type": "trending", "error": str(e)}
            )
            continue

        # ── Category Reference Data ──────────────────────────────────────────
        try:
            ref_key = (
                f"youtube/raw_statistics_reference_data/"
                f"region={region_code}/"
                f"date={date_partition}/"
                f"{region_code}_category_id.json"
            )

            if s3_key_exists(BUCKET, ref_key):
                log("info", "skipping duplicate category write",
                    region=region_code, s3_key=ref_key)
            else:
                category_items = fetch_video_categories(region_code, api_key)
                total_quota   += QUOTA_COST_CATEGORIES

                payload = {
                    "items": category_items,
                    "_pipeline_metadata": {
                        "ingestion_id":        ingestion_id,
                        "region":              region_code,
                        "ingestion_timestamp": now.isoformat(),
                        "source":              "youtube_data_api_v3",
                    },
                }
                write_to_s3(payload, BUCKET, ref_key)
                log("info", "wrote category reference",
                    region=region_code, category_count=len(category_items),
                    s3_key=ref_key)

        except requests.HTTPError as e:
            log("error", "HTTP error fetching categories",
                region=region_code, error=str(e))
            results["failed"].append(
                {"region": region_code, "type": "categories", "error": str(e)}
            )
            continue

        results["success"].append(region_code)

    # ── Quota metric ─────────────────────────────────────────────────────────
    emit_quota_metric(total_quota, ingestion_id)

    # ── Summary & alerting ───────────────────────────────────────────────────
    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}/{len(REGIONS)} regions. "
        f"Failed: {len(results['failed'])}. "
        f"Estimated quota used: {total_quota} units."
    )
    log("info", summary, results=results, quota_units=total_quota)

    if results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Partial failure — {ingestion_id}",
            message=json.dumps(results, indent=2),
        )

    return {
        "statusCode":    200,
        "ingestion_id":  ingestion_id,
        "quota_units":   total_quota,
        "results":       results,
    }