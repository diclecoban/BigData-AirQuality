# MLflow Integration Guide

## Purpose

MLflow is used to track model experiments, compare metrics, store model
artifacts, and keep a clear handoff between the ML pipeline and deployment.

In this project MLflow mainly supports Engineer 2's model training work and
Engineer 3's deployment / observability work.

## Local Tracking Server

The Docker environment starts MLflow with:

| Setting | Value |
|---|---|
| Browser URL | `http://localhost:5001` |
| Container URL | `http://mlflow:5000` |
| Container port | `5000` |
| Host port | `5001` |
| Backend store | `sqlite:////mlflow/mlflow.db` |
| Artifact root | `/mlflow/artifacts` |
| Docker volume | `infra_mlflow-data` |

Start the local stack from `infra/`:

```bash
docker compose up -d
```

Open the UI:

```text
http://localhost:5001
```

## Current Code Behavior

`src/ml/train_gbt_model.py` currently writes MLflow runs to the local file
store:

```text
data/mlruns/
```

That means GBT training works even if the Docker MLflow service is not running.
To view those file-based runs without Docker:

```bash
mlflow ui --backend-store-uri data/mlruns
```

If port `5000` is already in use, run:

```bash
mlflow ui --backend-store-uri data/mlruns --port 5002
```

Team convention for shared local development is to use the Docker tracking
server:

```bash
export MLFLOW_TRACKING_URI="http://localhost:5001"
```

Future ML training scripts should read `MLFLOW_TRACKING_URI` from the
environment before falling back to `data/mlruns`.

## Experiment Naming

Use stable experiment names so the dashboard and reports can compare runs
across machines.

| Model family | Experiment name | Run name examples |
|---|---|---|
| Linear Regression baseline | `istanbul-aqi-baselines` | `linear_regression_1h` |
| Random Forest baseline | `istanbul-aqi-baselines` | `random_forest_1h` |
| GBT AQI forecast | `istanbul-aqi-gbt` | `gbt_1h`, `gbt_3h`, `gbt_6h` |
| PM2.5 forecast | `istanbul-pm25-forecast` | `gbt_pm25_1h`, `gbt_pm25_3h`, `gbt_pm25_6h` |

Required run parameters:

| Parameter | Example |
|---|---|
| `model_family` | `gbt` |
| `target_col` | `target_aqi_1h` |
| `horizon_h` | `1` |
| `n_features` | `72` |
| `training_start_date` | `2024-01-01` |
| `training_end_date` | `2024-12-31` |
| `data_source` | `ibb+openaq+weather` |

Required metrics:

| Metric | Meaning |
|---|---|
| `rmse` | Root mean squared error on test split |
| `mae` | Mean absolute error on test split |
| `r2` | Regression R squared on test split |
| `accuracy` | AQI category accuracy, if classification report is produced |
| `f1` | Weighted F1, if classification report is produced |

## Registered Models

Use these model registry names:

| Forecast | Registered model name |
|---|---|
| AQI 1h | `istanbul-aqi-gbt-1h` |
| AQI 3h | `istanbul-aqi-gbt-3h` |
| AQI 6h | `istanbul-aqi-gbt-6h` |
| PM2.5 1h | `istanbul-pm25-gbt-1h` |
| PM2.5 3h | `istanbul-pm25-gbt-3h` |
| PM2.5 6h | `istanbul-pm25-gbt-6h` |

`src/ml/train_gbt_model.py` already logs AQI GBT models with these names:

```text
istanbul-aqi-gbt-1h
istanbul-aqi-gbt-3h
istanbul-aqi-gbt-6h
```

## Promotion Rules

Model stages should be promoted manually after reviewing metrics.

Suggested flow:

| Stage | Rule |
|---|---|
| `None` | Every training run starts here |
| `Staging` | Model beats current baseline RMSE and has no obvious data issue |
| `Production` | Model beats current production RMSE and passes dashboard sanity checks |
| `Archived` | Older production model or failed validation |

Minimum acceptance criteria:

| Check | Required |
|---|---|
| RMSE | Lower than matching baseline for same horizon |
| MAE | Not worse than baseline by more than 5 percent |
| R2 | Positive on the test split |
| Prediction range | Most predictions between AQI 0 and 500 |
| Artifacts | Local model saved under `data/models/` |

## Engineer Workflow

### Engineer 2: Training

1. Start Docker services if using the shared tracking server:

   ```bash
   cd infra
   docker compose up -d mlflow
   ```

2. Set the tracking URI:

   ```bash
   export MLFLOW_TRACKING_URI="http://localhost:5001"
   ```

3. Run training from the project root:

   ```bash
   python -m src.ml.train_gbt_model
   ```

4. Open MLflow and compare runs:

   ```text
   http://localhost:5001
   ```

### Engineer 3: Deployment / Monitoring

1. Keep MLflow available with the Docker stack.
2. Confirm the chosen model exists in the registry.
3. Record the selected model name and version in deployment notes.
4. Expose model version, last training date, RMSE, and MAE in Grafana's
   prediction quality section.

## Dashboard Fields

Grafana should display these MLflow-derived fields in the prediction quality
panel:

| Field | Source |
|---|---|
| `model_name` | MLflow registered model |
| `model_version` | MLflow model version |
| `run_id` | MLflow run ID |
| `horizon_h` | MLflow parameter |
| `rmse` | MLflow metric |
| `mae` | MLflow metric |
| `r2` | MLflow metric |
| `last_training_date` | MLflow tag or run start time |

## Notes

- Docker MLflow uses `localhost:5001` because port `5000` may already be used
  by macOS services or another local app.
- Keep experiment and registered model names stable; dashboards and deployment
  notes depend on them.
- Do not delete the `infra_mlflow-data` Docker volume unless you intentionally
  want to remove local MLflow history.
