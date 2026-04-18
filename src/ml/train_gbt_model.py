"""Primary GBT model training with MLflow experiment tracking.

Trains a Gradient Boosted Trees Regressor for 1 h / 3 h / 6 h AQI forecasts.
Hyperparameter search is done with CrossValidator on the 1 h horizon model;
winning params are then reused for the 3 h and 6 h models.

All runs are tracked in MLflow (local file-system store by default).

Run:
  python -m src.ml.train_gbt_model
"""

from pathlib import Path

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.processing.feature_engineering import (
    build_feature_dataset,
    get_feature_columns,
)

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parents[2]
RAW_DIR    = ROOT / "data" / "raw"
MODEL_DIR  = ROOT / "data" / "models"
MLFLOW_URI = str(ROOT / "data" / "mlruns")

FORECAST_HORIZONS = [1, 3, 6]   # hours
FEATURE_COLS      = get_feature_columns()

MLFLOW_EXPERIMENT = "istanbul-aqi-gbt"


# ---------------------------------------------------------------------------
# 1. Load training dataset
# ---------------------------------------------------------------------------

def load_training_dataset(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Load raw CSVs, engineer features, return (train, val, test)."""
    aq = (
        spark.read
        .option("header", "true").option("inferSchema", "true")
        .csv(str(RAW_DIR / "airquality_historical.csv"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )
    wx = (
        spark.read
        .option("header", "true").option("inferSchema", "true")
        .csv(str(RAW_DIR / "weather_historical.csv"))
        .withColumn("timestamp", F.to_timestamp("timestamp"))
    )

    aq_h = aq.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    wx_h = wx.withColumn("ts_hour", F.date_trunc("hour", "timestamp"))
    weather_cols = [c for c in wx.columns if c != "timestamp"]
    joined = aq_h.join(wx_h.select(["ts_hour"] + weather_cols), on="ts_hour", how="left")

    features_df = build_feature_dataset(joined, horizons_h=tuple(FORECAST_HORIZONS))

    # Temporal 70 / 15 / 15 split
    w = Window.orderBy("timestamp", "station_id")
    ranked = features_df.withColumn("_rn", F.row_number().over(w))
    total  = features_df.count()
    n_train = int(total * 0.70)
    n_val   = int(total * 0.15)

    train = ranked.filter(F.col("_rn") <= n_train).drop("_rn").cache()
    val   = ranked.filter((F.col("_rn") > n_train) & (F.col("_rn") <= n_train + n_val)).drop("_rn").cache()
    test  = ranked.filter(F.col("_rn") > n_train + n_val).drop("_rn").cache()

    print(f"  Split → train: {train.count():,}  val: {val.count():,}  test: {test.count():,}")
    return train, val, test


# ---------------------------------------------------------------------------
# 2. Build base pipeline stages
# ---------------------------------------------------------------------------

def _build_pipeline_stages(target_col: str) -> tuple[list, GBTRegressor]:
    """Return (stages_list, gbt_estimator) for the given target column."""
    imputer = Imputer(
        inputCols=FEATURE_COLS,
        outputCols=[f"{c}_imp" for c in FEATURE_COLS],
        strategy="median",
    )
    assembler = VectorAssembler(
        inputCols=[f"{c}_imp" for c in FEATURE_COLS],
        outputCol="features",
        handleInvalid="skip",
    )
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=target_col,
        predictionCol="prediction",
        seed=42,
    )
    return [imputer, assembler, gbt], gbt


# ---------------------------------------------------------------------------
# 3. Hyperparameter tuning (1 h horizon)
# ---------------------------------------------------------------------------

def tune_gbt_hyperparameters(train_df: DataFrame, val_df: DataFrame) -> dict:
    """Grid search on the 1 h horizon model.  Returns best param dict."""
    target_col = "target_aqi_1h"
    train_val  = train_df.union(val_df)     # CrossValidator does its own splits

    stages, gbt = _build_pipeline_stages(target_col)
    pipeline    = Pipeline(stages=stages)

    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth,         [4, 6])
        .addGrid(gbt.maxIter,          [20, 50])
        .addGrid(gbt.stepSize,         [0.1, 0.05])
        .build()
    )

    evaluator = RegressionEvaluator(
        labelCol=target_col,
        predictionCol="prediction",
        metricName="rmse",
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=42,
    )

    print("  Running CrossValidator (this will take several minutes)...")
    cv_model = cv.fit(train_val)

    best = cv_model.bestModel
    gbt_model = best.stages[-1]
    best_params = {
        "maxDepth": gbt_model.getOrDefault("maxDepth"),
        "maxIter":  gbt_model.getOrDefault("maxIter"),
        "stepSize": gbt_model.getOrDefault("stepSize"),
    }
    print(f"  Best params: {best_params}")
    return best_params


# ---------------------------------------------------------------------------
# 4. Train one GBT model per horizon
# ---------------------------------------------------------------------------

def train_gbt_regressor(
    train_df: DataFrame,
    horizon_h: int,
    best_params: dict,
) -> PipelineModel:
    """Fit a GBT pipeline for the given forecast horizon."""
    target_col = f"target_aqi_{horizon_h}h"
    df = train_df.filter(F.col(target_col).isNotNull())

    stages, gbt = _build_pipeline_stages(target_col)
    gbt.setMaxDepth(best_params.get("maxDepth", 5))
    gbt.setMaxIter(best_params.get("maxIter", 50))
    gbt.setStepSize(best_params.get("stepSize", 0.1))

    pipeline = Pipeline(stages=stages)
    print(f"  Fitting GBT for horizon {horizon_h}h (target={target_col})...")
    return pipeline.fit(df)


# ---------------------------------------------------------------------------
# 5. Register in MLflow
# ---------------------------------------------------------------------------

def register_best_model(
    model: PipelineModel,
    horizon_h: int,
    metrics: dict,
    best_params: dict,
) -> None:
    """Log model, params, and metrics to MLflow; save artifact locally."""
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    with mlflow.start_run(run_name=f"gbt_{horizon_h}h"):
        # Log hyper-params
        mlflow.log_params(best_params)
        mlflow.log_param("horizon_h", horizon_h)
        mlflow.log_param("target_col", f"target_aqi_{horizon_h}h")
        mlflow.log_param("n_features",  len(FEATURE_COLS))

        # Log metrics
        for metric_name, value in metrics.items():
            mlflow.log_metric(metric_name, value)

        # Log Spark ML model
        mlflow.spark.log_model(
            spark_model=model,
            artifact_path=f"gbt_{horizon_h}h_model",
            registered_model_name=f"istanbul-aqi-gbt-{horizon_h}h",
        )

        # Also save locally for inference without MLflow server
        local_path = str(MODEL_DIR / f"gbt_{horizon_h}h")
        model.write().overwrite().save(local_path)
        mlflow.log_param("local_model_path", local_path)

    print(f"  Registered gbt_{horizon_h}h in MLflow + saved to {local_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> dict:
    """Full GBT training pipeline.  Returns dict of trained PipelineModels."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading dataset...")
    train, val, test = load_training_dataset(spark)

    print("Tuning hyperparameters on 1h horizon...")
    best_params = tune_gbt_hyperparameters(train, val)

    from src.ml.evaluate_models import evaluate_regression

    trained_models = {}
    for h in FORECAST_HORIZONS:
        print(f"\n=== Training GBT — horizon {h}h ===")
        model = train_gbt_regressor(train, h, best_params)

        target_col = f"target_aqi_{h}h"
        test_h = test.filter(F.col(target_col).isNotNull())
        preds  = model.transform(test_h)
        metrics = evaluate_regression(preds, target_col)
        print(f"  Test metrics (horizon={h}h): {metrics}")

        register_best_model(model, h, metrics, best_params)
        trained_models[f"gbt_{h}h"] = model

    return trained_models, test


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("train-gbt-model")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
        print("\nGBT training complete. Check data/mlruns/ for experiment results.")
    finally:
        spark.stop()
