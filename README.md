# AWS Youtube Project

## Executive Summary

This project builds an end-to-end, scalable data pipeline to ingest, process, and analyse YouTube data for marketing insights. The solution leverages AWS services to automate data ingestion, transformation, and querying, enabling near real-time performance tracking and decision-making.

The pipeline is designed with modularity, scalability, and data quality in mind, following modern data engineering best practices.

## Data Flow
![ ](https://github.com/geoffreyrwamakuba-rgb/AWS_Youtube_Project/blob/4144f6a8b43428482cfd3e4ff1f74559755a1022/Images/AWS_Data_Flow.drawio)

## Project Overview
This project implements a cloud-native ETL pipeline using AWS to process YouTube trending and metadata datasets.

Tech Stack
- AWS S3 – Data lake (raw, processed, curated layers)
- AWS Lambda – Event-driven ingestion & processing
- AWS Glue – Data catalog + transformations
- AWS Athena – Serverless querying
- AWS Step Functions – Pipeline orchestration
- IAM – Secure access control
- Python (Pandas / boto3 / awswrangler) – Data processing
- AWS CLI (PowerShell) – Data ingestion & backfill

### 🏢 Business Scenario

### Context

A marketing client wants to understand:
- What content performs best across regions
- Trends in engagement (views, likes, comments)
- Category-level performance
- Opportunities for campaign optimisation

### ❌ The Problem
- Raw YouTube data is unstructured and fragmented
- Manual analysis is time-consuming and not scalable
- No automated pipeline for continuous ingestion and reporting

### ✅ The Solution
A fully automated pipeline that:
1. Ingests raw YouTube data into S3
2. Cleans and structures data using AWS Glue
3. Stores curated datasets for analytics
4. Enables querying via Athena
5. Orchestrates workflows using Step Functions (DAG-style)

### Pipeline Orchestration (Step Functions DAG)

**The pipeline follows a DAG structure:**

Ingestion Layer 
- Upload raw CSV/JSON data to S3 (Bronze layer)
Processing Layer 
- Lambda triggers transformation jobs
- Glue jobs clean, normalize, and enrich data
Storage Layer
- Data stored in partitioned format (Parquet)
Analytics Layer
- Athena queries for reporting and dashboards

![ ](https://github.com/geoffreyrwamakuba-rgb/AWS_Youtube_Project/blob/4144f6a8b43428482cfd3e4ff1f74559755a1022/Images/stepfunctions_graph.svg)
---

## 📂 Repository Structure
```
youtube-data-pipeline-2026/
│
├── lambdas/
│   ├── youtube_api_integstion/        # Ingestion Lambda
│   │   └── lambda_function.py         # Fetches trending videos & categories from YouTube API
│   └── json_to_parquet/               # Reference data transformation Lambda
│       └── lambda_function.py         # Converts JSON category mappings to Parquet
│
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py # PySpark job: raw data → cleansed statistics
│   └── silver_to_gold_analytics.py    # PySpark job: cleansed data → business aggregations
│
├── data_quality/
│   └── dq_lambda.py                   # Data quality validation Lambda
│
├── step_functions/
│   └── pipeline_orchestation.json     # Step Functions state machine definition
│
├── scripts/
│   ├── aws_copy.sh                    # Upload historical data to Bronze S3 bucket
│   └── information.md                 # AWS resource names & configuration reference
│
├── data/                              # Reference & historical data
│   ├── {region}videos.csv             # Kaggle trending video datasets (10 regions)
│   └── {region}_category_id.json      # YouTube category ID mappings (10 regions)
│
└── Images.png                         # Supporting diagrams
```
