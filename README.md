# Yelp Analytics Data Platform (ETS Platform)

This repository implements a production-grade, end-to-end Data Engineering pipeline for processing, validating, and serving the Yelp Academic Dataset (~9 GB). The architecture is designed around the **Medallion Architecture** using **PySpark** for ETL processing, **MinIO** for raw object storage, **Apache Airflow** for workflow scheduling, and **PostgreSQL** as the Central Data Warehouse serving **FastAPI**, **React**, and **Power BI**.

---

## 🏗️ End-to-End Pipeline Architecture

The flow of data through the ETS Data Platform is structured as follows:

```mermaid
flowchart TD
    subgraph Source [1. Data Source]
        Yelp[(Yelp Dataset\n~9GB JSON)]
    end

    subgraph Ingest [2. Ingestion Layer]
        PyIngest[Ingestion Script\nPython / Boto3]
        MinIO[(MinIO Object Storage\nRaw Bucket)]
    end

    subgraph Orchestration [3. Orchestration]
        Airflow[Apache Airflow\nDAG Scheduler]
    end

    subgraph Processing [4. Processing Layer - PySpark Medallion]
        Bronze[Bronze ETL\nRaw Format & Schema]
        Silver[Silver ETL\nCleaned, Cast & Deduplicated]
        Gold[Gold ETL\nFacts & Dimensions]
    end

    subgraph Warehouse [5. Data Warehouse & Marts]
        DWH[(PostgreSQL\nData Warehouse)]
        MVs[Materialized Views\n& Analytical Marts]
    end

    subgraph Validation [6. Quality & Governance]
        DQ[Data Quality Validation\nGreat Expectations / Custom]
    end

    subgraph Serving [7. Serving & Presentation]
        API[FastAPI\nREST API]
        ReactUI[React App\nFrontend Dashboard]
        PBI[Power BI\nReports & Analytics]
    end

    %% Flow Connections
    Yelp -->|Upload| PyIngest
    PyIngest -->|Raw Upload| MinIO
    Airflow -->|Trigger & Orchestrate| Bronze
    Airflow -->|Schedule| Silver
    Airflow -->|Schedule| Gold

    MinIO -->|Read S3a JSON| Bronze
    Bronze -->|Save Parquet| Silver
    Silver -->|Save Delta/Parquet| Gold
    Gold -->|JDBC Write| DWH
    DWH -->|DDL/Materialization| MVs
    MVs -->|Validate Rules| DQ
    MVs -->|Serve| API
    DWH -->|Direct Import| PBI
    API -->|Fetch API| ReactUI

    %% Custom Styling
    style Yelp fill:#ff9999,stroke:#cc0000,stroke-width:2px,color:#333
    style PyIngest fill:#e6f2ff,stroke:#0066cc,stroke-width:2px,color:#333
    style MinIO fill:#ffe6cc,stroke:#ff6600,stroke-width:2px,color:#333
    style Airflow fill:#e2f0cb,stroke:#5c8a6f,stroke-width:2px,color:#333
    style Bronze fill:#f5d6eb,stroke:#c2185b,stroke-width:2px,color:#333
    style Silver fill:#d6e4ff,stroke:#1d39c4,stroke-width:2px,color:#333
    style Gold fill:#d9f7be,stroke:#389e0d,stroke-width:2px,color:#333
    style DWH fill:#efdbff,stroke:#531dab,stroke-width:2px,color:#333
    style MVs fill:#fff1b8,stroke:#d4b106,stroke-width:2px,color:#333
    style DQ fill:#ffccc7,stroke:#cf1322,stroke-width:2px,color:#333
    style API fill:#e6fffb,stroke:#08979c,stroke-width:2px,color:#333
    style ReactUI fill:#e6f7ff,stroke:#096dd9,stroke-width:2px,color:#333
    style PBI fill:#fffbe6,stroke:#d4b106,stroke-width:2px,color:#333
```

---

## 🗂️ Medallion Pipeline Stages

The platform utilizes a structured medallion model to transform the raw Yelp JSON data into refined, query-optimized datasets:

### 1. Ingestion Layer (Python $\rightarrow$ MinIO)
* **Goal**: Safely parse local raw JSON data and stream or bulk-upload files to MinIO buckets.
* **Technology**: Python, `boto3` SDK.
* **Storage**: MinIO serving as an S3-compatible raw landing zone.

### 2. Bronze Layer (Raw ETL)
* **Goal**: Establish a point-in-time snapshot of the raw landing zone.
* **Spark Operations**:
  - PySpark reads JSON files directly from MinIO using S3a endpoints.
  - Adds schema-relaxed definitions to ensure data ingestion is robust against minor schema shifts.
  - Appends audit columns (e.g., `_ingested_at`, `_source_file`).
  - Writes as compressed **Parquet** format.

### 3. Silver Layer (Cleaned & Standardized)
* **Goal**: Clean, cast, normalize, and de-duplicate the data to produce a queryable, high-integrity schema.
* **Spark Operations**:
  - Handles missing / null values (imputing default variables or dropping corrupted rows).
  - Explicitly enforces strong schemas (casting to `DoubleType`, `TimestampType`, etc.).
  - Deduplicates records on primary keys (e.g., `user_id`, `business_id`).
  - Standardizes text strings and category formats.

### 4. Gold Layer (Data Warehouse / Star Schema)
* **Goal**: Structure the data into fact and dimension tables tailored for analytical workloads.
* **Spark Operations**:
  - Dimensional modeling: Creates `dim_users`, `dim_businesses`, and `fact_reviews`.
  - Performs analytical joins and cohorts aggregations.
  - Writes data directly to the **PostgreSQL Data Warehouse** target database via JDBC.

---

## 🏛️ DWH, Data Marts & Quality Gates

### A. PostgreSQL Data Warehouse & Materialized Views
* **Storage Model**: Star schema structure in a PostgreSQL database optimized for read-heavy workloads.
* **Materialized Views**: Optimized aggregations (e.g., `mv_business_ratings_by_category`, `mv_user_activity_cohorts`) are created and indexed to provide sub-second query execution times for the serving layer.

### B. Data Quality & Validation
* **Framework**: **Great Expectations** (or custom script assertions) running validations after load.
* **Rules Executed**:
  - Primary keys are unique and non-null.
  - Value distributions lie within correct bounds (e.g., `stars` must reside between $1.0$ and $5.0$).
  - Schema consistency checks.
  - Row counts align with expectations (e.g., no sudden $50\%$ drop in review records).

---

## 🔗 Serving & Presentation Layer

Once the warehouse is populated and validated, the data is served via three endpoints:

1. **FastAPI REST API**: High-performance backend service exposing CRUD endpoints and analytical routes (e.g., `/api/v1/businesses/top`, `/api/v1/users/trends`).
2. **React Frontend Dashboard**: Visualizes top-performing businesses, user reviews distributions, check-in activity heatmaps, and network characteristics.
3. **Power BI**: Connects directly to the PostgreSQL warehouse and materialized views for advanced business intelligence reporting.

---

## 📂 Project Directory Structure

```directory
ets_platform/
├── configs/                   # Configuration files for all services
│   ├── airflow/
│   ├── spark/
│   ├── postgres/
│   ├── minio/
│   ├── logging/
│   └── application.yaml       # Global application-wide configuration
├── infrastructure/            # Infrastructure setup files
│   ├── docker/                # Dockerfiles for containers (Airflow, Spark, Postgres, MinIO, FastAPI)
│   ├── kubernetes/            # Kubernetes manifest files
│   └── terraform/             # Terraform infrastructure automation code
├── datasets/                  # Local and sample datasets
│   ├── raw/                   # Raw Yelp academic datasets (large JSONs)
│   ├── sample/                # Small sample subsets for fast testing
│   └── metadata/              # Extracted metadata & schemas
├── docs/                      # Documentation and images
│   ├── architecture/
│   ├── diagrams/
│   └── pipeline/              # Sequential execution plan
├── notebooks/                 # Jupyter notebooks for EDA
├── scripts/                   # Utility and helper shell scripts
├── sql/                       # PostgreSQL query scripts
│   ├── ddl/                   # Table definitions
│   ├── dml/                   # Data manipulation scripts
│   ├── views/                 # Materialized views
│   └── indexes/               # DB indexes for fast queries
├── tests/                     # Verification tests
│   ├── unit/                  # PyTest units
│   └── integration/           # E2E integration verification
├── logs/                      # Log directories
├── storage/                   # Local storage mounts for docker containers
│   ├── minio/                 # Persistent MinIO storage
│   └── postgres/              # Persistent PostgreSQL database files
├── src/                       # Main source code
│   ├── ingestion/             # Python scripts to upload JSON to MinIO
│   ├── etl/                   # PySpark ETL scripts (Bronze, Silver, Gold)
│   ├── api/                   # FastAPI server codebase
│   └── validation/            # Data quality verification scripts
├── docker-compose.yml         # Container configuration file
├── .env.example               # Template environment variables
├── pyproject.toml             # Poetry project config & dependencies
├── requirements.txt           # Main python dependency file
├── Makefile                   # Execution targets and shortcuts
└── LICENSE                    # Project LICENSE
```

---

## 🛠️ Step-by-Step Setup Guide

### 1. Start Infrastructure
Deploy the central components (MinIO, PostgreSQL, Airflow) using Docker Compose:
```bash
docker-compose up -d
```

### 2. Ingest Data to MinIO
Execute the Python ingestion script to upload the Yelp datasets from `datasets/raw/` to your MinIO raw landing bucket:
```bash
python src/ingestion/ingest.py
```

### 3. Trigger Airflow DAG
Log into the Airflow Web Server at `http://localhost:8080` and trigger the main workflow DAG:
1. `yelp_ingest_to_bronze` - Triggers Spark job to write to Bronze Parquet.
2. `yelp_bronze_to_silver` - Cleanses and standardizes the datasets.
3. `yelp_silver_to_gold` - Creates dimension/fact models and writes to PostgreSQL.
4. `yelp_refresh_mvs` - Refreshes Materialized Views in PostgreSQL.
5. `yelp_validate_data` - Triggers Great Expectations testing.

### 4. Start serving Layer
Run the FastAPI application locally:
```bash
make api
```

Run the React development server:
```bash
cd frontend
npm install
npm run dev
```

---

## 📝 Code Blueprints

### 1. Ingest to MinIO (`src/ingestion/ingest.py`)
```python
import os
import boto3
from botocore.client import Config

def upload_to_minio(bucket_name, file_path, object_name):
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Check if bucket exists, create if not
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)
        
    print(f"Uploading {file_path} to {bucket_name}/{object_name}...")
    s3.upload_file(file_path, bucket_name, object_name)
    print("Upload completed successfully!")

if __name__ == "__main__":
    upload_to_minio(
        bucket_name="yelp-raw",
        file_path="datasets/raw/yelp_academic_dataset_business.json",
        object_name="business/yelp_academic_dataset_business.json"
    )
```

### 2. Medallion PySpark Job Example (`src/etl/spark_medallion.py`)
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, split

def build_spark_session():
    return SparkSession.builder \
        .appName("Yelp_ETL_Pipeline") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.5.4") \
        .getOrCreate()

def run_etl():
    spark = build_spark_session()
    
    # --- BRONZE LAYER ---
    df_raw = spark.read.json("s3a://yelp-raw/business/*.json")
    df_bronze = df_raw.withColumn("_ingested_at", current_timestamp())
    df_bronze.write.mode("overwrite").parquet("s3a://yelp-bronze/business")
    
    # --- SILVER LAYER ---
    df_bronze_read = spark.read.parquet("s3a://yelp-bronze/business")
    df_silver = df_bronze_read \
        .dropDuplicates(["business_id"]) \
        .filter(col("business_id").isNotNull()) \
        .withColumn("stars", col("stars").cast("double")) \
        .withColumn("review_count", col("review_count").cast("long"))
    df_silver.write.mode("overwrite").parquet("s3a://yelp-silver/business")
    
    # --- GOLD LAYER (Loading Postgres DWH) ---
    df_silver_read = spark.read.parquet("s3a://yelp-silver/business")
    dim_business = df_silver_read.select("business_id", "name", "address", "city", "state", "stars", "review_count")
    
    dim_business.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/yelp_dwh") \
        .option("dbtable", "dim_business") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()
        
    print("ETL successfully written to PostgreSQL DWH!")
    spark.stop()

if __name__ == "__main__":
    run_etl()
```

### 3. FastAPI Endpoint (`src/api/main.py`)
```python
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/yelp_dwh"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI(title="Yelp Analytics API", version="1.0.0")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/v1/businesses/top")
def get_top_businesses(limit: int = 10, db = Depends(get_db)):
    # Query PostgreSQL Materialized View
    query = text("SELECT * FROM mv_business_ratings_by_category LIMIT :limit")
    result = db.execute(query, {"limit": limit})
    return [dict(row._mapping) for row in result]
```
