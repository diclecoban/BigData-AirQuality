# Real-Time Air Quality Monitoring and Prediction for Istanbul

This repository is the implementation skeleton for the Big Data Analytics project described in `Project_Proposal.pdf`.

The goal is to build a real-time air quality monitoring and prediction pipeline for Istanbul using:

- Apache Kafka for ingestion
- Apache Spark / PySpark for stream and batch processing
- Spark MLlib for model training and inference
- AWS for storage and deployment
- Grafana for real-time visualization

At this stage, the repository contains the main structure, file headers, and team ownership boundaries. The implementation details will be added incrementally by the team.

## Project Scope

The system is planned to:

- ingest near real-time air quality data from IBB and OpenAQ
- enrich records with weather data
- process streaming data with Spark Structured Streaming
- train batch ML models for AQI and pollutant forecasting
- serve prediction outputs for dashboards
- visualize district-level air quality on a Grafana map

## Team Structure

### Engineer 1: Data and Streaming Engineer

Owner of ingestion and raw-to-stream pipeline.

Responsibilities:

- integrate IBB API
- integrate OpenAQ API
- integrate weather API
- normalize incoming records
- publish data into Kafka topics
- define streaming schemas
- maintain stream ingestion reliability

Primary files:

- `src/ingestion/producer_ibb.py`
- `src/ingestion/producer_openaq.py`
- `src/ingestion/producer_weather.py`
- `src/ingestion/schema.py`
- `src/streaming/structured_streaming_job.py`

### Engineer 2: ML and Analytics Engineer

Owner of feature engineering, model training, evaluation, and prediction logic.

Responsibilities:

- prepare historical training data
- create lag, rolling, temporal, and spatial features
- train baseline models
- train and tune the primary GBT model
- evaluate 1h / 3h / 6h forecasts
- define prediction outputs for downstream consumers

Primary files:

- `src/processing/feature_engineering.py`
- `src/batch/historical_analysis.py`
- `src/ml/train_baseline_models.py`
- `src/ml/train_gbt_model.py`
- `src/ml/evaluate_models.py`
- `src/ml/inference.py`

### Engineer 3: Cloud and Visualization Engineer

Owner of deployment, storage, orchestration, observability, and dashboards.

Responsibilities:

- prepare Docker-based local environment
- manage AWS deployment structure
- define S3 data layout
- prepare MLflow and experiment tracking hooks
- build Grafana dashboard structure
- document environment variables and run flow

Primary files:

- `infra/docker-compose.yml`
- `infra/aws/README.md`
- `infra/mlflow/README.md`
- `dashboard/grafana_dashboard_plan.md`
- `config/topics.yaml`
- `config/app.yaml`

## Suggested Work Split

### Engineer 1 deliverables

- API clients
- Kafka producers
- data schema definitions
- streaming input contracts

### Engineer 2 deliverables

- feature pipeline
- training scripts
- evaluation scripts
- inference logic

### Engineer 3 deliverables

- local environment
- cloud deployment notes
- storage layout
- dashboard structure

## Repository Layout

```text
.
├── README.md
├── Project_Proposal.pdf
├── config
│   ├── app.yaml
│   └── topics.yaml
├── dashboard
│   └── grafana_dashboard_plan.md
├── docs
│   ├── architecture.md
│   ├── engineer_1_data_streaming.md
│   ├── engineer_2_ml_analytics.md
│   └── engineer_3_cloud_visualization.md
├── infra
│   ├── aws
│   │   └── README.md
│   ├── docker-compose.yml
│   └── mlflow
│       └── README.md
├── src
│   ├── batch
│   │   └── historical_analysis.py
│   ├── common
│   │   ├── config.py
│   │   └── logger.py
│   ├── ingestion
│   │   ├── producer_ibb.py
│   │   ├── producer_openaq.py
│   │   ├── producer_weather.py
│   │   └── schema.py
│   ├── ml
│   │   ├── evaluate_models.py
│   │   ├── inference.py
│   │   ├── train_baseline_models.py
│   │   └── train_gbt_model.py
│   └── processing
│       └── feature_engineering.py
└── tests
    ├── test_feature_engineering.py
    ├── test_ingestion_schema.py
    └── test_inference_contract.py
```

## How To Use This Skeleton

1. Each engineer starts with the files under their ownership.
2. Shared contracts should be aligned first:
   - input schema
   - Kafka topic names
   - output prediction schema
3. After contracts are fixed, each engineer can implement independently.
4. Integration should happen around:
   - Kafka message format
   - Spark DataFrame schema
   - prediction output schema
   - dashboard field naming

## Shared Conventions

- Use Python 3.10+
- Keep all topic names and service configs inside `config/`
- Store common helpers under `src/common/`
- Keep implementation notes in `docs/`
- Add tests whenever a shared contract changes

## Next Recommended Steps

1. Engineer 1 finalizes raw data schemas and topic names.
2. Engineer 2 defines the feature set and model target columns.
3. Engineer 3 prepares local Docker services and dashboard data contract.
4. The team then integrates the first end-to-end prototype.
