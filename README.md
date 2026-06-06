# Real-Time Air Quality Monitoring and Prediction for Istanbul

This repository is the implementation of the Big Data Analytics project described in `Project_Proposal.pdf`.

The goal is to build a real-time air quality monitoring and prediction pipeline for Istanbul using:

- Apache Kafka for ingestion
- Apache Spark / PySpark for stream and batch processing
- Spark MLlib for model training and inference
- MLflow for experiment tracking and model registry
- Grafana for real-time visualization

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Repository Layout](#repository-layout)
3. [Docker Stack — Local Infrastructure](#docker-stack--local-infrastructure)
4. [Running the Full Pipeline on the Spark Cluster](#running-the-full-pipeline-on-the-spark-cluster)
5. [Pipeline Results](#pipeline-results)
6. [Structured Streaming Job](#structured-streaming-job)
7. [Configuration Reference](#configuration-reference)
8. [Known Issues and Solutions](#known-issues-and-solutions)
9. [Team Structure](#team-structure)
10. [Data Sources](#data-sources)
11. [ML Feature Contracts](#ml-feature-contracts)
12. [Next Steps](#next-steps)

---

## Architecture Overview

```
IBB API ──┐                          ┌─── Spark Structured Streaming ──► Parquet (enriched)
          ├──► Kafka (raw topics) ───┤
OpenAQ ───┘                          └─── Batch ML Training ──► MLflow Registry
                                                    │
Weather API ──► Kafka (weather_normalized) ─────────┘
                                                    │
                                          Grafana Dashboard
```

The pipeline runs fully inside a Docker Compose stack. Training jobs and streaming jobs are submitted via `docker exec` into the Spark master container, which dispatches work to the Spark worker.

---

## Repository Layout

```
.
├── README.md
├── Project_Proposal.pdf
├── requirements.txt                  ← pyspark==3.5.8 pinned to match Docker image
├── config/
│   ├── app.yaml
│   └── topics.yaml
├── dashboard/
│   └── grafana_dashboard_plan.md
├── data/
│   ├── raw/                          ← CSV inputs (airquality_historical.csv, weather_historical.csv)
│   ├── processed/
│   ├── models/                       ← Saved Spark ML PipelineModels
│   │   ├── baseline_linear_regression/
│   │   ├── baseline_random_forest/
│   │   ├── gbt_1h/
│   │   ├── gbt_3h/
│   │   └── gbt_6h/
│   ├── reports/                      ← Evaluation JSON + CSV outputs
│   └── mlruns/                       ← Local MLflow fallback (primary: HTTP server)
├── docs/
├── infra/
│   ├── docker-compose.yml            ← Full local stack (Kafka, Spark, MLflow, Grafana)
│   └── ...
├── scripts/
│   ├── run_pipeline.py               ← Full pipeline runner (cluster mode)
│   └── ...
└── src/
    ├── common/
    │   └── config.py                 ← All shared constants and env-var overrides
    ├── ingestion/
    ├── batch/
    │   └── historical_analysis.py
    ├── ml/
    │   ├── train_baseline_models.py
    │   ├── train_gbt_model.py
    │   └── evaluate_models.py
    ├── processing/
    │   └── feature_engineering.py
    └── streaming/
        └── structured_streaming_job.py
```

---

## Docker Stack — Local Infrastructure

### Services

| Service | Container name | Host URL | Docker-internal URL |
|---|---|---|---|
| Kafka broker | `airquality-kafka` | `localhost:9092` | `kafka:29092` |
| Spark master | `airquality-spark-master` | `spark://localhost:7077` · UI: `http://localhost:8080` | `spark://spark-master:7077` |
| Spark worker | `airquality-spark-worker` | UI: `http://localhost:8081` | — |
| MLflow tracking | `airquality-mlflow` | `http://localhost:5001` | `http://mlflow:5000` |
| Grafana | `airquality-grafana` | `http://localhost:3000` (admin/admin) | — |

> **Important URL difference:** MLflow is exposed on port **5001** on the host (maps to container port 5000).
> Always use `http://localhost:5001` from your browser and `http://mlflow:5000` in code running inside Docker.

### Start / Stop

```powershell
cd infra

# Start all services
docker compose up -d

# Check status
docker compose ps

# Stop without deleting data volumes
docker compose down

# Stop and wipe all data (Kafka, MLflow, Grafana state)
docker compose down -v
```

### Kafka Topics

The `kafka-init` service creates all topics automatically on first start.

```bash
# Verify topics
docker exec airquality-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topics:

```
air_quality_normalized
weather_normalized
airquality.enriched
airquality.predictions
airquality.ibb.raw
airquality.openaq.raw
weather.istanbul.raw
airquality.system.metrics
```

If topics are missing, recreate the init service:

```bash
docker compose up -d --force-recreate kafka-init
```

### Key docker-compose.yml Changes (from original)

Two changes were made to the original `infra/docker-compose.yml` to make the pipeline work end-to-end:

**1. `user: root` on both Spark containers**

The Spark worker runs as a non-root user by default. On Windows, Docker mounts NTFS volumes in a way that `chmod` inside Linux containers has no effect — so the worker executor cannot write model files to `/opt/airquality/data/models/`. Running as root bypasses this.

```yaml
spark-master:
  image: spark:3.5.8-python3
  user: root          # ← added
  ...

spark-worker:
  image: spark:3.5.8-python3
  user: root          # ← added
  ...
```

**2. Automatic pip install on container startup**

Python dependencies are not bundled in the `spark:3.5.8-python3` image. They are now installed automatically when the container starts, so they survive container restarts without manual intervention:

```yaml
spark-master:
  entrypoint: ["/bin/bash", "-c"]
  command:
    - |
      pip3 install -q pyspark==3.5.8 mlflow python-dotenv requests 2>/dev/null
      exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0
```

The same pattern is applied to `spark-worker`.

---

## Running the Full Pipeline on the Spark Cluster

### Prerequisites

- Docker Desktop running with the stack up (`docker compose up -d`)
- `data/raw/airquality_historical.csv` and `data/raw/weather_historical.csv` present

### One-command pipeline run

All four steps (historical analysis → baseline training → GBT training → evaluation) run in sequence with a single command executed inside the Spark master container:

```powershell
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality &&
  SPARK_EXECUTOR_MEMORY=1g SPARK_DRIVER_MEMORY=2g MLFLOW_TRACKING_URI=http://mlflow:5000 \
  python3 -m scripts.run_pipeline
"
```

The pipeline prints progress for each step and writes outputs to `data/models/` and `data/reports/`.

### Environment variable overrides

The pipeline reads all runtime settings from environment variables with sensible defaults. Override any of them before the command above if needed:

| Variable | Default (in Docker) | Notes |
|---|---|---|
| `SPARK_MASTER` | `spark://localhost:7077` | Auto-detected as `spark://spark-master:7077` inside Docker |
| `SPARK_DRIVER_MEMORY` | `2g` | Keep at 2g — worker has 2G total |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Must be ≤ 1g with a single 2G worker |
| `SPARK_EXECUTOR_CORES` | `2` | |
| `KAFKA_BOOTSTRAP` | `kafka:29092` | Use `localhost:9092` when running from host machine |
| `MLFLOW_TRACKING_URI` | `http://localhost:5000` | Use `http://mlflow:5000` inside Docker |

### Running individual steps

```powershell
# Step 1 — Historical batch analysis
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality && python3 -m src.batch.historical_analysis"

# Step 2 — Baseline models
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality && python3 -m src.ml.train_baseline_models"

# Step 3 — GBT models + MLflow tracking
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality &&
  MLFLOW_TRACKING_URI=http://mlflow:5000 python3 -m src.ml.train_gbt_model"

# Step 4 — Evaluate all models
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality && python3 -m src.ml.evaluate_models"
```

### Running the Structured Streaming job

```powershell
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality &&
  KAFKA_BOOTSTRAP=kafka:29092 python3 -m src.streaming.structured_streaming_job"
```

---

## Pipeline Results

The pipeline was successfully run end-to-end on the Docker Spark cluster (`spark://spark-master:7077`) against a synthetic dataset of 263,520 rows covering Istanbul air quality and weather history.

### Step 1 — Historical Batch Analysis

- 263,520 rows processed
- District-level statistics computed for all pollutants
- Hourly, daily, weekly, and monthly trend CSVs generated
- Top-5 most polluted districts identified

Outputs in `data/reports/`:
```
district_statistics.csv
trend_hourly.csv
trend_weekly.csv
trend_monthly.csv
trend_day_of_week.csv
top5_polluted_districts.json
```

### Step 2 — Baseline Model Training

- Dataset split: **train 183,810 / val 39,390 / test 39,420** (temporal split)
- LinearRegression and RandomForest pipelines trained on AQI 1h forecast target
- Models saved to `data/models/baseline_linear_regression/` and `data/models/baseline_random_forest/`

### Step 3 — GBT Hyperparameter Tuning + Training

CrossValidator (3-fold) on 1h horizon found the following best parameters, which were reused for 3h and 6h models:

| Parameter | Best value |
|---|---|
| `maxDepth` | 4 |
| `maxIter` | 50 |
| `stepSize` | 0.05 |

All three GBT models registered in MLflow experiment `istanbul-aqi-gbt`:

| Model | MLflow run | Local path |
|---|---|---|
| `gbt_1h` | `istanbul-aqi-gbt-1h` v1 | `data/models/gbt_1h/` |
| `gbt_3h` | `istanbul-aqi-gbt-3h` v1 | `data/models/gbt_3h/` |
| `gbt_6h` | `istanbul-aqi-gbt-6h` v1 | `data/models/gbt_6h/` |

### Step 4 — Model Evaluation

All five models evaluated on the held-out test split (39,420 rows):

| Model | RMSE | MAE | R² | Accuracy (clf) | F1 (clf) |
|---|---|---|---|---|---|
| **gbt_1h** | **86.04** | 36.99 | 0.0635 | 0.539 | 0.519 |
| baseline_random_forest | 86.12 | 38.54 | 0.0618 | 0.447 | 0.394 |
| baseline_linear_regression | 86.23 | 35.12 | 0.0594 | 0.584 | 0.573 |
| gbt_3h | 87.19 | 43.05 | 0.0399 | 0.423 | 0.423 |
| gbt_6h | 90.88 | 46.64 | −0.043 | 0.460 | 0.437 |

> These values are synthetic-benchmark results, not evidence of operational
> accuracy on real Istanbul sensor data. GBT achieves the lowest RMSE, but the
> proposal's predictive-performance targets were not met in this run.

Full results in:
- `data/reports/evaluation_report.json`
- `data/reports/evaluation_summary.csv`

View runs in MLflow UI: **http://localhost:5001**

---

## Structured Streaming Job

`src/streaming/structured_streaming_job.py` is a PySpark Structured Streaming job that:

1. Reads from `air_quality_normalized` and `weather_normalized` Kafka topics
2. Parses JSON payloads using strict schemas
3. Validates and cleans sensor readings (negative values → NULL)
4. Performs a stream–stream join with a 35-minute watermark and ±30-minute range window
5. Applies feature engineering (wind UV decomposition, cyclical hour/month encodings, AQI category)
6. Writes enriched output to Parquet (partitioned by district and date) every 30 seconds

### Stream-stream join design

Spark 3.x requires at least one equality predicate in a stream–stream join. A `date_trunc("hour")` bucket column is used as the equality key; the ±30-minute range condition provides fine-grained matching within each bucket:

```python
joined = aq.join(
    weather_prep,
    (aq["aq_bucket"] == weather_prep["w_bucket"]) &
    (aq["event_time"] >= weather_prep["w_event_time"] - F.expr("INTERVAL 30 MINUTES")) &
    (aq["event_time"] <= weather_prep["w_event_time"] + F.expr("INTERVAL 30 MINUTES")),
    how="inner",
)
```

---

## Configuration Reference

All shared constants and tunable parameters live in `src/common/config.py`. Every value can be overridden with an environment variable.

### Spark

```python
SPARK_MASTER                 = os.getenv("SPARK_MASTER",          "spark://localhost:7077")
SPARK_DRIVER_MEMORY          = os.getenv("SPARK_DRIVER_MEMORY",   "2g")
SPARK_EXECUTOR_MEMORY        = os.getenv("SPARK_EXECUTOR_MEMORY", "1g")
SPARK_EXECUTOR_CORES         = os.getenv("SPARK_EXECUTOR_CORES",  "2")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
SPARK_KAFKA_PACKAGE          = os.getenv("SPARK_KAFKA_PACKAGE",
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8")
```

### Kafka

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
```

Use `localhost:9092` when running producers/consumers from the host machine outside Docker.

### MLflow

```python
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT   = "istanbul-aqi-gbt"
```

Use `http://mlflow:5000` inside Docker containers; `http://localhost:5001` from the host browser.

### Data quality thresholds

Values outside these ranges are treated as sensor errors and set to NULL automatically:

| Pollutant | Min | Max |
|---|---|---|
| PM2.5 | 0 | 500 µg/m³ |
| PM10 | 0 | 600 µg/m³ |
| NO2 | 0 | 2000 µg/m³ |
| SO2 | 0 | 2000 µg/m³ |
| CO | 0 | 50 mg/m³ |
| O3 | 0 | 1000 µg/m³ |
| AQI | 0 | 500 |

### Python dependency pinning

`requirements.txt` pins PySpark to exactly the version running inside the Docker image:

```
pyspark==3.5.8   # must match spark:3.5.8-python3 Docker image
```

**Never change this to `>=3.5.0`.** A version mismatch between the client PySpark and the cluster Spark causes a `serialVersionUID` incompatibility error at runtime, where jobs appear to submit but all tasks fail immediately.

---

## Known Issues and Solutions

This section documents every non-trivial problem encountered while getting the pipeline running on the Docker Spark cluster, along with the root cause and fix applied.

### 1. `InvalidClassException: serialVersionUID mismatch`

**Symptom:** Jobs submit successfully but all tasks fail with a Java serialization error referencing two different `serialVersionUID` values.

**Root cause:** The host machine had PySpark 4.x installed while the Docker cluster runs Spark 3.5.8. Even a minor mismatch between client and server versions causes binary incompatibility in Spark's RPC protocol.

**Fix:** Pin PySpark to exactly the cluster version:
```powershell
pip install pyspark==3.5.8
```

Also update `requirements.txt` to `pyspark==3.5.8` (not `>=3.5.0`).

### 2. "Initial job has not accepted any resources" when running from host

**Symptom:** A SparkSession connects to `spark://localhost:7077` from the Windows host, the master accepts the application, but no tasks ever execute — the log repeats "Initial job has not accepted any resources" indefinitely.

**Root cause:** In cluster mode, the Spark worker runs inside Docker and needs to connect back to the driver (running on the host). The worker cannot reach the Windows host's IP on the dynamic driver port that Spark opens. The connection is one-directional from host to cluster, but Spark requires bidirectional communication.

**Fix:** Run the pipeline driver inside the Docker network, where master, worker, and driver can all reach each other:
```powershell
docker exec -u root -it airquality-spark-master bash -c "
  cd /opt/airquality && python3 -m scripts.run_pipeline"
```

### 3. `ModuleNotFoundError: No module named 'pyspark'` inside Docker

**Symptom:** Running `python3 -m scripts.run_pipeline` inside the container fails immediately.

**Root cause:** The `spark:3.5.8-python3` image includes Spark binaries but not the PySpark Python package. After a container restart, any previously installed packages are lost.

**Fix (permanent):** The `entrypoint` in `docker-compose.yml` now runs `pip3 install` before starting Spark, so packages are reinstalled automatically on every container start:
```yaml
entrypoint: ["/bin/bash", "-c"]
command:
  - |
    pip3 install -q pyspark==3.5.8 mlflow python-dotenv requests 2>/dev/null
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master --host 0.0.0.0
```

**Fix (one-time, existing container):**
```powershell
docker exec -u root airquality-spark-master pip3 install pyspark==3.5.8 mlflow python-dotenv requests
```

### 4. `IOException: Mkdirs failed` when saving models

**Symptom:** Model training completes successfully, but saving the PipelineModel to `data/models/` raises:
```
IOException: Mkdirs failed to create
file:/opt/airquality/data/models/baseline_linear_regression/metadata/_temporary/...
```

**Root cause:** The Spark worker spawns executor JVMs as a non-root user. The `data/` directory is on a Windows NTFS volume mounted into Docker via `../:/opt/airquality`. Linux `chmod` commands do not actually change permissions on NTFS-backed mounts — the filesystem ownership is controlled by Windows, not Linux. The executor user therefore has no write access.

**Fix:** Add `user: root` to both `spark-master` and `spark-worker` in `docker-compose.yml`:
```yaml
spark-master:
  user: root
spark-worker:
  user: root
```
Then restart the containers:
```powershell
docker compose stop spark-master spark-worker
docker compose up -d spark-master spark-worker
```

### 5. `SPARK_EXECUTOR_MEMORY` too high — worker rejects tasks

**Symptom:** The Spark master log shows the worker has 2G memory, but executor requests for 4g are rejected. Tasks never start.

**Root cause:** The docker-compose.yml configures the worker with `--memory 2G`. An executor requesting more than the available worker memory is simply not scheduled.

**Fix:** Run with `SPARK_EXECUTOR_MEMORY=1g`. The default in `config.py` is now set to `1g`.

### 6. Wrong project path inside Docker

**Symptom:** `cd /opt/airquality/BigData-AirQuality` fails — directory not found.

**Root cause:** The volume mount in docker-compose.yml is `../:/opt/airquality`, where `..` resolves to the parent of `infra/`, which is the project root (`BigData-AirQuality/`). The project root is therefore mounted directly at `/opt/airquality/`, not at a subdirectory.

**Fix:** Always use `cd /opt/airquality` (not `cd /opt/airquality/BigData-AirQuality`).

---

## Team Structure

### Engineer 1 — Data and Streaming Engineer

Owner of ingestion and raw-to-stream pipeline.

Primary files:
- `src/ingestion/producer_ibb.py`
- `src/ingestion/producer_openaq.py`
- `src/ingestion/producer_weather.py`
- `src/ingestion/schema.py`
- `src/streaming/structured_streaming_job.py`

### Engineer 2 — ML and Analytics Engineer

Owner of feature engineering, model training, evaluation, and prediction logic.

Primary files:
- `src/processing/feature_engineering.py`
- `src/batch/historical_analysis.py`
- `src/ml/train_baseline_models.py`
- `src/ml/train_gbt_model.py`
- `src/ml/evaluate_models.py`
- `src/ml/inference.py`

### Engineer 3 — Cloud and Visualization Engineer

Owner of deployment, storage, orchestration, observability, and dashboards.

Primary files:
- `infra/docker-compose.yml`
- `config/topics.yaml`
- `config/app.yaml`
- `dashboard/grafana_dashboard_plan.md`

---

## Data Sources

### IBB + OpenAQ

```bash
# Last 7 days (default)
python scripts/merge_historical_data.py

# Custom date range
python scripts/merge_historical_data.py --start-date 2024-01-01 --end-date 2024-12-31

# IBB only (no API key needed)
python scripts/merge_historical_data.py --source ibb
```

### Synthetic data (no internet required)

```bash
python scripts/generate_training_data.py
```

### OpenAQ API key

Register at https://explore.openaq.org/register (free), then:

```powershell
$env:OPENAQ_API_KEY = "your_key_here"
```

OpenAQ is optional — the pipeline continues with IBB-only data if the key is missing.

---

## ML Feature Contracts

### Input schema

Air quality columns: `station_id`, `station_name`, `district`, `timestamp`, `pm10`, `pm25`, `no2`, `so2`, `co`, `o3`, `aqi`, `latitude`, `longitude`

Weather columns: `timestamp`, `temperature`, `humidity`, `wind_speed`, `wind_direction`, `pressure`, `precipitation`, `visibility`, `cloud_cover`

### Engineered features (70+ columns)

- Lag features at 1h / 2h / 3h / 6h / 24h for each pollutant
- Rolling mean and std at 3h / 6h / 24h windows
- Cyclical time encodings: `hour_sin`, `hour_cos`, `month_sin`, `month_cos`
- `is_weekend` flag
- Wind UV decomposition: `wind_u`, `wind_v`
- `district_index`, `dist_from_center`

### Target columns

`target_aqi_1h`, `target_aqi_3h`, `target_aqi_6h`, `target_pm25_1h`, `target_pm25_3h`, `target_pm25_6h`

### Prediction output schema (Kafka topic: `airquality.predictions`)

```
station_id      string
district        string
latitude        double
longitude       double
timestamp       timestamp   observation time
predicted_at    timestamp   inference wall-clock time
horizon_h       int         1, 3, or 6
predicted_aqi   double
aqi_category    string      Good / Moderate / Unhealthy for Sensitive Groups / Unhealthy / Very Unhealthy / Hazardous
```

### Live inference integration (for Engineer 1)

```python
from src.ml.inference import score_new_data

predictions = score_new_data(spark, aq_df, wx_df, horizon_h=1)
predictions.show()
```

`score_new_data()` handles feature engineering and data validation internally.

---

## Remaining Validation Work

1. Validate forecasting quality on an archived real IBB/OpenAQ data window.
2. Wire live inference output to the `airquality.predictions` topic and dashboard.
3. Replace placeholder contract tests with schema, feature, and inference tests.
4. Benchmark end-to-end latency, throughput, fault recovery, and executor scaling.
5. Store immutable evaluation reports and MLflow run identifiers with each result table.
