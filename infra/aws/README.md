# AWS Deployment Guide

## Goal

This document defines the AWS deployment structure for the Istanbul real-time
air quality pipeline. It is written as the shared contract for Engineer 1,
Engineer 2, and Engineer 3.

The first cloud deployment should be simple:

- S3 stores raw data, processed data, model artifacts, and reports.
- EC2 runs always-on services such as Kafka-compatible ingestion, Grafana, and
  MLflow.
- EMR or a Spark-capable EC2 host runs batch and streaming Spark jobs.
- CloudWatch collects logs and basic health metrics.

## Planned AWS Services

| Layer | AWS service | Purpose |
|---|---|---|
| Storage | S3 | Raw, processed, model, report, and dashboard export data |
| Compute | EC2 | Docker services, ingestion producers, Grafana, MLflow |
| Spark | EMR | Batch training, historical processing, optional streaming jobs |
| Monitoring | CloudWatch | Logs, alarms, service health metrics |
| Security | IAM | Least-privilege access to S3, CloudWatch, and EC2 |
| Network | VPC | Private subnets for internal services, restricted public access |

## Environment Names

Use these environment labels consistently:

| Environment | Suffix | Purpose |
|---|---|---|
| `dev` | `-dev` | Local/team testing |
| `staging` | `-staging` | Pre-demo validation |
| `prod` | `-prod` | Final project/demo deployment |

Current config uses `dev` bucket names.

## S3 Storage Layout

Bucket prefix:

```text
istanbul-air-quality-dev
```

Buckets:

| Bucket | Purpose |
|---|---|
| `istanbul-air-quality-dev-raw` | Immutable raw API and Kafka landing data |
| `istanbul-air-quality-dev-processed` | Cleaned, joined, feature-ready parquet outputs |
| `istanbul-air-quality-dev-models` | MLflow artifacts, Spark model exports, reports |

Folder convention:

```text
s3://istanbul-air-quality-dev-raw/
  ibb/year=YYYY/month=MM/day=DD/
  openaq/year=YYYY/month=MM/day=DD/
  weather/year=YYYY/month=MM/day=DD/

s3://istanbul-air-quality-dev-processed/
  enriched_air_quality/year=YYYY/month=MM/day=DD/
  features/year=YYYY/month=MM/day=DD/
  predictions/year=YYYY/month=MM/day=DD/
  metrics/year=YYYY/month=MM/day=DD/

s3://istanbul-air-quality-dev-models/
  mlflow-artifacts/
  spark-models/gbt_1h/
  spark-models/gbt_3h/
  spark-models/gbt_6h/
  reports/evaluation/
```

Partition rules:

| Dataset | Partition keys |
|---|---|
| Raw API data | `source`, `year`, `month`, `day` |
| Enriched streaming output | `year`, `month`, `day`, optional `hour` |
| Predictions | `year`, `month`, `day`, `horizon_h` |
| Metrics | `year`, `month`, `day`, `service` |

Retention rules:

| Data class | Retention |
|---|---|
| Raw data | Keep for full project duration |
| Processed data | Keep latest 12 months for active analysis |
| Predictions | Keep latest 6 months |
| System metrics | Keep latest 30 to 90 days |
| Model artifacts | Keep all promoted models; archive failed runs after review |

## IAM Setup

Create separate roles for compute and human users.

Recommended roles:

| Role | Used by | Access |
|---|---|---|
| `AirQualityEC2Role` | EC2 instance profile | Read/write project S3 buckets, write CloudWatch logs |
| `AirQualityEMRRole` | EMR service role | Create and manage EMR resources |
| `AirQualityEMREC2Role` | EMR EC2 profile | Read/write project S3 buckets, write CloudWatch logs |
| `AirQualityDeployUser` | Engineer 3 / deployment | Start/stop EC2, EMR, read logs, manage S3 objects |

Minimum S3 permissions for EC2/EMR jobs:

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::istanbul-air-quality-dev-raw",
    "arn:aws:s3:::istanbul-air-quality-dev-raw/*",
    "arn:aws:s3:::istanbul-air-quality-dev-processed",
    "arn:aws:s3:::istanbul-air-quality-dev-processed/*",
    "arn:aws:s3:::istanbul-air-quality-dev-models",
    "arn:aws:s3:::istanbul-air-quality-dev-models/*"
  ]
}
```

Secrets:

| Secret | Storage recommendation |
|---|---|
| `OPENAQ_API_KEY` | AWS Systems Manager Parameter Store or Secrets Manager |
| `OWM_API_KEY` | AWS Systems Manager Parameter Store or Secrets Manager |
| Grafana admin password | Secrets Manager |
| MLflow backend credentials, if external DB is added | Secrets Manager |

Do not commit API keys or cloud credentials to the repository.

## Network

Suggested VPC structure:

| Component | Placement |
|---|---|
| Grafana | Public subnet with restricted inbound access |
| MLflow | Private subnet if possible; otherwise restricted public access |
| Kafka / streaming services | Private subnet |
| EMR | Private subnet |
| S3 | Access through IAM; VPC endpoint recommended |

Security group rules:

| Port | Service | Source |
|---|---|---|
| `22` | SSH | Your IP only |
| `3000` | Grafana | Your IP or demo IP range only |
| `5001` or `5000` | MLflow | Team IP only, or private network only |
| `9092` | Kafka | Spark / producer security group only |
| `7077`, `8080`, `8081` | Spark | Team IP for UI, private network for workers |

For a class project/demo, a single EC2 security group with restricted inbound
access is acceptable. For production-like deployment, keep Kafka and MLflow
private.

## Compute Plan

### Option A: Simple EC2 Deployment

Use one EC2 instance for the first cloud demo.

Recommended instance:

```text
t3.large or t3.xlarge
Ubuntu 22.04 LTS
30-60 GB gp3 EBS
```

Runs:

- Docker Compose stack
- Kafka
- Grafana
- MLflow
- ingestion producers
- optional local Spark jobs for small test data

### Option B: EC2 + EMR

Use EC2 for services and EMR for Spark.

EC2 runs:

- Kafka-compatible message layer
- Grafana
- MLflow
- API producers

EMR runs:

- historical merge jobs
- feature engineering
- model training
- large batch inference
- optional Structured Streaming job

Suggested EMR baseline:

```text
EMR 7.x
Spark 3.x
1 primary node: m5.xlarge
2 core nodes: m5.xlarge
Auto-terminate for batch jobs
```

## Environment Variables

Shared environment variables:

| Variable | Local value | AWS value |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | `<ec2-private-dns>:9092` or MSK bootstrap |
| `SPARK_MASTER` | `local[2]` or `spark://localhost:7077` | EMR/Spark cluster master |
| `MLFLOW_TRACKING_URI` | `http://localhost:5001` | `http://<mlflow-host>:5000` |
| `OPENAQ_API_KEY` | local shell / `.env` | Parameter Store / Secrets Manager |
| `OWM_API_KEY` | local shell / `.env` | Parameter Store / Secrets Manager |
| `RAW_BUCKET` | local `data/raw` | `s3://istanbul-air-quality-dev-raw` |
| `PROCESSED_BUCKET` | local `data/processed` | `s3://istanbul-air-quality-dev-processed` |
| `MODEL_BUCKET` | local `data/models` | `s3://istanbul-air-quality-dev-models` |

## Deployment Flow

### 1. Prepare AWS resources

1. Create S3 buckets.
2. Create IAM roles and attach S3/CloudWatch permissions.
3. Create or select VPC, subnets, and security groups.
4. Create EC2 key pair or configure SSM Session Manager.

### 2. Start service host

1. Launch EC2.
2. Install Docker and Docker Compose.
3. Clone the repository.
4. Create `.env` with API keys and cloud settings.
5. Start services:

   ```bash
   cd infra
   docker compose up -d
   ```

6. Confirm:

   ```bash
   docker compose ps
   docker exec airquality-kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```

### 3. Start ingestion

Engineer 1 starts producers with AWS values:

```bash
export KAFKA_BOOTSTRAP="<kafka-host>:9092"
export OPENAQ_API_KEY="<from-secret-store>"
export OWM_API_KEY="<from-secret-store>"
python -m src.ingestion.producer_ibb
python -m src.ingestion.producer_openaq
python -m src.ingestion.producer_weather
```

### 4. Run Spark processing

For local/small cloud test:

```bash
export KAFKA_BOOTSTRAP="<kafka-host>:9092"
spark-submit src/streaming/structured_streaming_job.py
```

For EMR:

1. Upload code or package to S3.
2. Submit Spark step with the same environment variables.
3. Write enriched outputs to the processed S3 bucket.

### 5. Train and register models

Engineer 2 trains with MLflow:

```bash
export MLFLOW_TRACKING_URI="http://<mlflow-host>:5000"
python -m src.ml.train_gbt_model
```

Model artifacts should be copied or logged to:

```text
s3://istanbul-air-quality-dev-models/mlflow-artifacts/
```

### 6. Connect Grafana

Grafana should read from the selected prediction sink. For the first version,
use one of:

| Data source | Use case |
|---|---|
| S3/Athena | Historical dashboard and prediction quality panels |
| PostgreSQL or TimescaleDB | Real-time dashboard tables |
| Kafka plugin or bridge service | Live stream panels |

The first recommended project path is:

```text
Kafka predictions -> processed S3 parquet -> Athena -> Grafana
```

This is easier to explain and more stable for demos than direct Kafka panels.

## Monitoring Plan

CloudWatch log groups:

| Log group | Source |
|---|---|
| `/airquality/ingestion` | IBB, OpenAQ, weather producers |
| `/airquality/streaming` | Spark Structured Streaming |
| `/airquality/ml` | training and inference jobs |
| `/airquality/docker` | EC2 Docker service logs |
| `/airquality/grafana` | Grafana server logs |
| `/airquality/mlflow` | MLflow server logs |

Minimum alarms:

| Alarm | Condition |
|---|---|
| Ingestion stopped | No producer log heartbeat for 10 minutes |
| Kafka lag high | Consumer lag above agreed threshold |
| Spark job failed | Structured Streaming query terminated |
| Disk usage high | EC2 disk above 80 percent |
| API failures high | 5xx or request exceptions exceed threshold |
| No predictions | No new prediction output for 15 minutes |

## Dashboard / Reporting Contract

Prediction outputs should be available with this schema:

```text
station_id      string
district        string
latitude        double
longitude       double
timestamp       timestamp
predicted_at    timestamp
horizon_h       int
predicted_aqi   double
aqi_category    string
model_name      string
model_version   string
```

Grafana refresh policy:

| Panel type | Refresh |
|---|---|
| City overview | 30 seconds |
| District map | 30 seconds |
| Station trends | 1 minute |
| Prediction quality | 5 minutes |
| Pipeline health | 30 seconds |

## Open Decisions

These must be finalized before final deployment:

| Decision | Owner |
|---|---|
| Use EC2 Kafka or AWS MSK | Engineer 3 |
| Use direct Kafka dashboard or Athena-backed dashboard | Engineer 3 |
| Final district GeoJSON source | Engineer 3 |
| Exact EMR instance size | Engineer 2 + Engineer 3 |
| Production model version | Engineer 2 |
| Public demo access policy | Whole team |
