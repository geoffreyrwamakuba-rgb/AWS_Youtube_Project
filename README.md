# AWS Youtube Project

## Executive Summary

This project builds an end-to-end, scalable data pipeline to ingest, process, and analyse YouTube data for marketing insights. The solution leverages AWS services to automate data ingestion, transformation, and querying, enabling near real-time performance tracking and decision-making.

The pipeline is designed with modularity, scalability, and data quality in mind, following modern data engineering best practices.

## Data Flow

![ ](https://github.com/geoffreyrwamakuba-rgb/Spark-Declarative-Pipeline-Project/blob/a00f521355dfc2e9d10525cd4e1c14ce22511619/SDP_data_model.png)
---

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

#### Context

A marketing client wants to understand:
- What content performs best across regions
- Trends in engagement (views, likes, comments)
- Category-level performance
- Opportunities for campaign optimisation

#### The Problem
- Raw YouTube data is unstructured and fragmented
- Manual analysis is time-consuming and not scalable
- No automated pipeline for continuous ingestion and reporting

#### The Solution
A fully automated pipeline that:
1. Ingests raw YouTube data into S3
2. Cleans and structures data using AWS Glue
3. Stores curated datasets for analytics
4. Enables querying via Athena
5. Orchestrates workflows using Step Functions (DAG-style)

### Pipeline Orchestration (Step Functions DAG)

The pipeline follows a DAG structure:

Ingestion Layer 
- Upload raw CSV/JSON data to S3 (Bronze layer)
Processing Layer 
- Lambda triggers transformation jobs
- Glue jobs clean, normalize, and enrich data
Storage Layer
- Data stored in partitioned format (Parquet)
Analytics Layer
- Athena queries for reporting and dashboards

![ ](https://github.com/geoffreyrwamakuba-rgb/Spark-Declarative-Pipeline-Project/blob/a00f521355dfc2e9d10525cd4e1c14ce22511619/SDP_data_model.png)
---

## 📂 Repository Structure

```bash
├── 01_project_setup/        # Initial setup of the Databricks environment - Unity Catalog schema
│
├── 02_bronze/
│   ├── city.py              # Batch ingestion of raw city data into Bronze Delta table from S3
│   ├── trips.py             # Streaming ingestion of trips data using Auto Loader
│
├── 03_silver/
│   ├── calendar.py          # Dynamically generates a date dimension table
│   ├── city.py              # Cleans and standardises city dimension data
│   ├── trips.py             # Applies data quality expectations + Standardises schema and column names
│   ├── trips2.py            # Implements CDC-based upsert into Silver trips table using SDP
│  
├── 04_gold/ 
│   ├── trips_gold.sql       # Builds final fact table for analytics
│   ├── city_views.sql       # Creates city-level aggregated views
│
├── 05_data/
│   ├── trips                # Raw trips CSV files (full + incremental loads)
│   └── city                 # Raw city dimension data
│
└── README.md               
