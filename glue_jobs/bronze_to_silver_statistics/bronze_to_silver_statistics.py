"""
Glue Job: Bronze → Silver (Statistics Data)
────────────────────────────────────────────
Reads BOTH:
  - Legacy Kaggle CSV data  (s3://bronze/youtube/raw_statistics/region=x/date=x/*.csv)
  - Live API JSON data      (s3://bronze/youtube/raw_statistics/region=x/date=x/hour=x/*.json)

Applies schema normalisation, cleansing, deduplication, then writes
unified Parquet to the Silver layer.

KEY GLUE CONCEPTS IN THIS JOB:
- GlueContext = Spark + AWS integrations (Catalog, S3, etc.)
- Glue Data Catalog = central metadata store (tables, schema, partitions)
- DynamicFrame = Helps read data in glue
- Sink = Helps write data in glue
- Bookmarking = incremental processing (handled via job + commit)
"""

import sys
from datetime import datetime

# ── Glue-specific imports ─────────────────────────────────────────────────────
# These are boilerplate and at the top of all glue scripts
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions # helps read parameters
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job # starts and ends the job, also has bookmarking to track which s3 files have been processed
from awsglue.dynamicframe import DynamicFrame # glue dataframe that deals with schema changes better

# ── Spark imports ─────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType, TimestampType
)

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
    "silver_database",
    "silver_table",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

# ── Config ───────────────────────────────────────────────────────────────────
BRONZE_BUCKET = args["bronze_bucket"]
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB     = args["silver_database"]
SILVER_TABLE  = args["silver_table"]

REGIONS     = ["ca", "gb", "us", "in"]
BRONZE_BASE = f"s3://{BRONZE_BUCKET}/youtube/raw_statistics/"
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

# Build explicit per-region paths.
# Using basePath=BRONZE_BASE tells Spark the root of the partition tree,
# so it injects `region` as a column automatically from the directory name.
CSV_PATHS  = [f"{BRONZE_BASE}region={r}/" for r in REGIONS]
JSON_PATHS = [f"{BRONZE_BASE}region={r}/" for r in REGIONS]

logger.info(f"Bronze base : {BRONZE_BASE}")
logger.info(f"Silver      : {SILVER_DB}.{SILVER_TABLE} → {SILVER_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# SHARED SILVER SCHEMA
# Both sources are normalised into this column set before union.
# ─────────────────────────────────────────────────────────────────────────────
SILVER_COLUMNS = [
    "video_id", "trending_date", "title", "channel_title", "category_id",
    "publish_time", "tags", "views", "likes", "dislikes", "comment_count",
    "thumbnail_link", "comments_disabled", "ratings_disabled",
    "video_error_or_removed", "description", "region",
]


# ─────────────────────────────────────────────────────────────────────────────
# Step 1a: Read CSV (legacy Kaggle data)
# - basePath tells Spark the partition root → injects `region` column
# - pathGlobFilter restricts to .csv only → JSON files are ignored
# - recursiveFileLookup scans all subdirectories (date=x/ etc.)
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Reading CSV data from Bronze...")

try:
    # recursiveFileLookup + basePath are mutually exclusive in Spark.
    # With recursiveFileLookup=True, partition discovery is disabled so
    # `region` is never injected. Fix: read each region path separately
    # and tag with F.lit(region).
    csv_frames = []
    for region in REGIONS:
        region_path = f"{BRONZE_BASE}region={region}/"
        try:
            df_r = (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("pathGlobFilter", "*.csv")
                .option("recursiveFileLookup", "true")
                .csv(region_path)
                .withColumn("region", F.lit(region)))
            cnt = df_r.count()
            logger.info(f"  CSV region={region}: {cnt} records")
            if cnt > 0:
                csv_frames.append(df_r)
        except Exception as re:
            logger.info(f"  No CSV for region={region}: {re}")

    if csv_frames:
        df_csv = csv_frames[0]
        for frame in csv_frames[1:]:
            df_csv = df_csv.unionByName(frame, allowMissingColumns=True)
        csv_count = df_csv.count()
    else:
        df_csv = spark.createDataFrame([], schema=StructType([]))
        csv_count = 0
    logger.info(f"CSV records read total: {csv_count}")
except Exception as e:
    logger.info(f"Error reading CSV data: {e}")
    df_csv = spark.createDataFrame([], schema=StructType([]))
    csv_count = 0


# ─────────────────────────────────────────────────────────────────────────────
# Step 1b: Read JSON (live API data)
# - basePath tells Spark the partition root → injects `region` column
# - pathGlobFilter restricts to .json only → CSV files are ignored
# - multiLine=true because each file is one JSON object (full API envelope)
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Reading JSON data from Bronze...")

try:
    json_frames = []
    for region in REGIONS:
        region_path = f"{BRONZE_BASE}region={region}/"
        try:
            df_r = (spark.read
                .option("multiLine", "true")
                .option("pathGlobFilter", "*.json")
                .option("recursiveFileLookup", "true")
                .json(region_path)
                .withColumn("region", F.lit(region)))
            df_r = df_r.filter(F.col("items").isNotNull())
            cnt = df_r.count()
            logger.info(f"  JSON region={region}: {cnt} records")
            if cnt > 0:
                json_frames.append(df_r)
        except Exception as re:
            logger.info(f"  No JSON for region={region}: {re}")

    if json_frames:
        df_json = json_frames[0]
        for frame in json_frames[1:]:
            df_json = df_json.unionByName(frame, allowMissingColumns=True)
        json_count = df_json.count()
    else:
        df_json = spark.createDataFrame([], schema=StructType([]))
        json_count = 0
    logger.info(f"JSON records read total: {json_count}")
except Exception as e:
    logger.info(f"Error reading JSON data: {e}")
    df_json = spark.createDataFrame([], schema=StructType([]))
    json_count = 0

if csv_count == 0 and json_count == 0:
    logger.info("No data found in Bronze. Committing empty job.")
    job.commit()
    sys.exit(0)


# ─────────────────────────────────────────────────────────────────────────────
# Step 2a: Normalise CSV → silver schema
# ─────────────────────────────────────────────────────────────────────────────
def normalise_csv(df):
    """Cast legacy Kaggle CSV columns to the silver schema."""
    logger.info("Normalising CSV format...")
    return df.select(
        F.col("video_id").cast(StringType()),
        F.col("trending_date").cast(StringType()),
        F.col("title").cast(StringType()),
        F.col("channel_title").cast(StringType()),
        F.col("category_id").cast(LongType()),
        F.col("publish_time").cast(StringType()),
        F.col("tags").cast(StringType()),
        F.col("views").cast(LongType()),
        F.col("likes").cast(LongType()),
        F.col("dislikes").cast(LongType()),
        F.col("comment_count").cast(LongType()),
        F.col("thumbnail_link").cast(StringType()),
        F.col("comments_disabled").cast(BooleanType()),
        F.col("ratings_disabled").cast(BooleanType()),
        F.col("video_error_or_removed").cast(BooleanType()),
        F.col("description").cast(StringType()),
        F.col("region").cast(StringType()),  # from basePath partition discovery
        F.lit("csv").alias("_source"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 2b: Normalise JSON (YouTube API) → silver schema
# Each file is a full API envelope: {"items": [...], "_pipeline_metadata": {...}}
# We explode items so each video becomes one row.
# `region` is already injected by basePath partition discovery.
# ─────────────────────────────────────────────────────────────────────────────
def normalise_json(df):
    """Flatten nested API JSON into the silver schema."""
    logger.info("Normalising JSON (API) format — exploding items array...")

    df = df.select(
        F.explode("items").alias("item"),
        F.col("region"),  # from basePath partition discovery
    )

    return df.select(
        F.col("item.id").alias("video_id"),
        F.lit(datetime.utcnow().strftime("%y.%d.%m")).alias("trending_date"),
        F.col("item.snippet.title").alias("title"),
        F.col("item.snippet.channelTitle").alias("channel_title"),
        F.col("item.snippet.categoryId").cast(LongType()).alias("category_id"),
        F.col("item.snippet.publishedAt").alias("publish_time"),
        F.concat_ws("|", F.col("item.snippet.tags")).alias("tags"),  # array → pipe-separated string to match CSV schema
        F.col("item.statistics.viewCount").cast(LongType()).alias("views"),
        F.col("item.statistics.likeCount").cast(LongType()).alias("likes"),
        F.lit(0).cast(LongType()).alias("dislikes"),  # removed from YouTube API Dec 2021
        F.col("item.statistics.commentCount").cast(LongType()).alias("comment_count"),
        F.col("item.snippet.thumbnails.default.url").alias("thumbnail_link"),
        F.lit(False).alias("comments_disabled"),
        F.lit(False).alias("ratings_disabled"),
        F.lit(False).alias("video_error_or_removed"),
        F.col("item.snippet.description").alias("description"),
        F.col("region"),
        F.lit("api").alias("_source"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Union both sources
# ─────────────────────────────────────────────────────────────────────────────
frames = []
if csv_count > 0:
    frames.append(normalise_csv(df_csv))
if json_count > 0:
    frames.append(normalise_json(df_json))

df = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])
logger.info(f"Combined record count before cleansing: {df.count()}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Cleansing
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Cleansing data...")

df = df.filter(F.col("video_id").isNotNull())
df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

# Parse inconsistent date formats (yy.dd.MM from CSV, ISO from API)
df = df.withColumn(
    "trending_date_parsed",
    F.when(
        F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
        F.to_date(F.col("trending_date"), "yy.dd.MM")
    ).otherwise(
        F.to_date(F.col("trending_date"))
    )
)

# Fill null numeric fields
for col_name in ["views", "likes", "dislikes", "comment_count"]:
    df = df.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

# Derived metrics
df = df.withColumn("like_ratio",
    F.when(F.col("views") > 0,
           F.round(F.col("likes") / F.col("views") * 100, 4)
    ).otherwise(0.0)
)
df = df.withColumn("engagement_rate",
    F.when(F.col("views") > 0,
           F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4)
    ).otherwise(0.0)
)

# Lineage metadata
df = df.withColumn("_processed_at", F.current_timestamp())
df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))
# CSV tags use pipe-separated values (e.g. "tag1|tag2"); API tags are arrays/null
# _source was stamped inside normalise_csv / normalise_json — no derivation needed here.


# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Deduplication
# Prefer API records over CSV for the same video+region+date.
# ─────────────────────────────────────────────────────────────────────────────
logger.info("Deduplicating (API records preferred over CSV)...")

window = Window.partitionBy("video_id", "region", "trending_date_parsed") \
    .orderBy(
        F.when(F.col("_source") == "api", 0).otherwise(1),  # api wins
        F.col("_processed_at").desc()
    )

df = df.withColumn("_row_num", F.row_number().over(window)) \
       .filter(F.col("_row_num") == 1) \
       .drop("_row_num")

logger.info(f"Records after deduplication: {df.count()}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Write to Silver
# ─────────────────────────────────────────────────────────────────────────────
logger.info(f"Writing to Silver: {SILVER_PATH}")

dynamic_frame = DynamicFrame.fromDF(df, glueContext, "silver_statistics")

sink = glueContext.getSink(
    connection_type="s3",
    path=SILVER_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink.setCatalogInfo(
    catalogDatabase=SILVER_DB,
    catalogTableName=SILVER_TABLE
)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(dynamic_frame)

logger.info("Silver write complete.")

job.commit()
