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

## Data Sources

### IBB + OpenAQ Merger

The project includes a data merger pipeline that fetches real air quality data
from two official sources and merges them into a single, feature-enriched dataset.

**Quick start — last 7 days:**
```bash
python scripts/merge_historical_data.py
```

**Custom date range (e.g. full 2024):**
```bash
python scripts/merge_historical_data.py \
    --start-date 2024-01-01 --end-date 2024-12-31
```

**IBB only (no API key needed):**
```bash
python scripts/merge_historical_data.py --source ibb
```

**Via generate_training_data.py:**
```bash
# Synthetic data (default, no internet needed)
python scripts/generate_training_data.py

# Real data from APIs
python scripts/generate_training_data.py --mode real \
    --start-date 2018-01-01 --end-date 2024-12-31
```

### Getting an OpenAQ API Key

1. Register at [https://explore.openaq.org/register](https://explore.openaq.org/register) (free)
2. Copy your API key
3. Set the environment variable:

```bash
# Linux / macOS
export OPENAQ_API_KEY="your_key_here"

# Windows PowerShell
$env:OPENAQ_API_KEY = "your_key_here"
```

OpenAQ is optional — the pipeline continues with IBB-only data if the key is missing.

### Data Source Comparison

| | IBB | OpenAQ |
|---|---|---|
| Auth | None | Free API key |
| Stations | ~37 official | Variable (community sensors) |
| Priority | **Higher** (calibrated instruments) | Lower |
| Station ID prefix | `IST-*` | `OAQ-*` |

For full documentation see [docs/data_merger_guide.md](docs/data_merger_guide.md).

---

## Engineer 2 — ML and Analytics (COMPLETED)

All Engineer 2 deliverables are implemented and tested with synthetic data.
The pipeline is ready to accept real data from Engineer 1 without code changes.

### Running the ML Pipeline

**Step 1 — Generate synthetic training data (skip when real data is available):**
```bash
python scripts/generate_training_data.py
```

**Step 2 — Train baseline models (Linear Regression + Random Forest):**
```bash
python -m src.ml.train_baseline_models
```

**Step 3 — Train primary GBT models (1h / 3h / 6h forecasts, ~20 min):**
```bash
python -m src.ml.train_gbt_model
```

**Step 4 — Evaluate all models:**
```bash
python -m src.ml.evaluate_models
# outputs: data/reports/evaluation_report.json
#          data/reports/evaluation_summary.csv
```

**Step 5 — Run inference on test split:**
```bash
python -m src.ml.inference
# outputs: data/reports/sample_predictions_1h/
```

**Alternative — sklearn pipeline (no Spark required):**
```bash
python scripts/train_sklearn_model.py --horizon 1 --target aqi
```

### Trained Model Artifacts

| Path | Description |
|------|-------------|
| `data/models/baseline_linear_regression/` | Spark MLlib Pipeline |
| `data/models/baseline_random_forest/` | Spark MLlib Pipeline |
| `data/models/gbt_1h/` | GBT, 1-hour AQI forecast |
| `data/models/gbt_3h/` | GBT, 3-hour AQI forecast |
| `data/models/gbt_6h/` | GBT, 6-hour AQI forecast |
| `data/mlruns/` | MLflow experiment tracking |

View experiment results:
```bash
mlflow ui --backend-store-uri data/mlruns
# open http://localhost:5000
```

### Feature Contracts (locked — do not change column names)

**Input schema expected by feature engineering:**

Air quality columns: `station_id`, `station_name`, `district`, `timestamp`, `pm10`, `pm25`, `no2`, `so2`, `co`, `o3`, `aqi`, `latitude`, `longitude`

Weather columns: `timestamp`, `temperature`, `humidity`, `wind_speed`, `wind_direction`, `pressure`, `precipitation`, `visibility`, `cloud_cover`

**Engineered features (70+ columns):** lag features at 1/3/6/12/24h, rolling mean/std at 3h/6h/24h, cyclical temporal encodings, district index, distance from city centre.

**Target columns:** `target_aqi_1h`, `target_aqi_3h`, `target_aqi_6h`, `target_pm25_1h`, `target_pm25_3h`, `target_pm25_6h`

### Prediction Output Schema (for Engineer 3 / Grafana)

```
station_id      string   — e.g. "IST-003"
district        string   — e.g. "Şişli"
latitude        double
longitude       double
timestamp       timestamp — observation time
predicted_at    timestamp — inference wall-clock time
horizon_h       int       — 1, 3, or 6
predicted_aqi   double    — model output
aqi_category    string    — Good / Moderate / Unhealthy for Sensitive Groups /
                            Unhealthy / Very Unhealthy / Hazardous
```

This schema is also the Kafka topic `airquality.predictions` payload format.

### Integration Point for Engineer 1 (Streaming Data)

When real Kafka data is available, call `score_new_data()` instead of the batch inference loop:

```python
from src.ml.inference import score_new_data

# aq_df  — raw air quality Spark DataFrame from Kafka
# wx_df  — raw weather Spark DataFrame from Kafka
predictions = score_new_data(spark, aq_df, wx_df, horizon_h=1)
predictions.show()
```

`score_new_data()` handles feature engineering and data validation internally.
Out-of-range sensor readings are automatically clamped to NULL and imputed — no pre-cleaning needed on Engineer 1's side.

### Data Quality Thresholds (auto-applied)

| Pollutant | Valid Range |
|-----------|-------------|
| PM2.5 | 0 – 500 µg/m³ |
| PM10 | 0 – 600 µg/m³ |
| NO2 | 0 – 2000 µg/m³ |
| SO2 | 0 – 2000 µg/m³ |
| CO | 0 – 50 mg/m³ |
| O3 | 0 – 1000 µg/m³ |
| AQI | 0 – 500 |

Values outside these ranges are treated as sensor errors and set to NULL automatically in `feature_engineering.validate_and_clean()`.

---

## Next Recommended Steps

1. Engineer 1 finalizes Kafka producers and starts publishing real IBB/OpenAQ data.
2. Engineer 1 calls `score_new_data()` from `src/ml/inference.py` to wire up live predictions.
3. Engineer 3 connects Grafana to the `airquality.predictions` Kafka topic using the output schema above.
4. Retrain GBT models with real data once Engineer 1 pipeline is stable.
