"""Model evaluation for regression (AQI forecasting) and AQI classification.

Provides:
  evaluate_regression()       — RMSE, MAE, R²
  evaluate_classification()   — accuracy, F1, confusion matrix
  compare_models()            — side-by-side baseline vs GBT table
  build_evaluation_report()   — writes JSON + CSV to data/reports/

Usage (standalone):
  python -m src.ml.evaluate_models
"""

import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    configure_windows_hadoop_env,
    SPARK_DRIVER_MEMORY,
    SPARK_MASTER,
    SPARK_SQL_SHUFFLE_PARTITIONS,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parents[2]
MODEL_DIR  = ROOT / "data" / "models"
REPORT_DIR = ROOT / "data" / "reports"

configure_windows_hadoop_env()


# ---------------------------------------------------------------------------
# AQI category helper (matches historical_analysis.py)
# ---------------------------------------------------------------------------

def _aqi_to_category_int(df: DataFrame, aqi_col: str, out_col: str) -> DataFrame:
    """Map AQI float → integer class label (0–5) for classification eval."""
    return df.withColumn(
        out_col,
        F.when(F.col(aqi_col) <= 50,  0)    # Good
         .when(F.col(aqi_col) <= 100, 1)    # Moderate
         .when(F.col(aqi_col) <= 150, 2)    # USG
         .when(F.col(aqi_col) <= 200, 3)    # Unhealthy
         .when(F.col(aqi_col) <= 300, 4)    # Very Unhealthy
         .otherwise(5)
         .cast("double"),                   # Hazardous / evaluator-compatible
    )


# ---------------------------------------------------------------------------
# 1. Regression evaluation
# ---------------------------------------------------------------------------

def evaluate_regression(predictions_df: DataFrame, label_col: str) -> dict:
    """Compute RMSE, MAE, and R² on a predictions DataFrame.

    Args:
        predictions_df: DataFrame with columns `prediction` and `label_col`.
        label_col:      Name of the ground-truth column (e.g. "target_aqi_1h").

    Returns:
        dict with keys rmse, mae, r2.
    """
    def _metric(name: str) -> float:
        return float(
            RegressionEvaluator(
                labelCol=label_col,
                predictionCol="prediction",
                metricName=name,
            ).evaluate(predictions_df)
        )

    metrics = {
        "rmse": round(_metric("rmse"), 4),
        "mae":  round(_metric("mae"),  4),
        "r2":   round(_metric("r2"),   4),
    }
    return metrics


# ---------------------------------------------------------------------------
# 2. Classification evaluation (AQI category)
# ---------------------------------------------------------------------------

def evaluate_classification(predictions_df: DataFrame, label_col: str) -> dict:
    """Compute accuracy, weighted F1, and a confusion matrix summary.

    Converts continuous AQI predictions and labels into 6-class categories
    before evaluating.

    Args:
        predictions_df: DataFrame with `prediction` and `label_col`.
        label_col:      Ground-truth AQI column name.

    Returns:
        dict with keys accuracy, f1, confusion_matrix (list of dicts).
    """
    df = _aqi_to_category_int(predictions_df, label_col,      "true_class")
    df = _aqi_to_category_int(df,             "prediction",   "pred_class")

    def _cls_metric(name: str) -> float:
        return float(
            MulticlassClassificationEvaluator(
                labelCol="true_class",
                predictionCol="pred_class",
                metricName=name,
            ).evaluate(df)
        )

    accuracy = round(_cls_metric("accuracy"), 4)
    f1       = round(_cls_metric("weightedFMeasure"), 4)

    # Confusion matrix as list of {true, pred, count}
    cm = (
        df
        .groupBy("true_class", "pred_class")
        .count()
        .orderBy("true_class", "pred_class")
        .collect()
    )
    cm = [
        {
            "true_class": row["true_class"],
            "pred_class": row["pred_class"],
            "count": row["count"],
        }
        for row in cm
    ]

    return {"accuracy": accuracy, "f1": f1, "confusion_matrix": cm}


# ---------------------------------------------------------------------------
# 3. Model comparison
# ---------------------------------------------------------------------------

def compare_models(results: dict) -> list[dict]:
    """Build a side-by-side comparison table sorted by RMSE.

    Args:
        results: {model_name: {"rmse": ..., "mae": ..., "r2": ...}}

    Returns:
        List of row dicts sorted by RMSE ascending.
    """
    rows = [
        {"model": name, **metrics}
        for name, metrics in results.items()
    ]
    return sorted(rows, key=lambda row: row.get("rmse", float("inf")))


# ---------------------------------------------------------------------------
# 4. Build and export evaluation report
# ---------------------------------------------------------------------------

def build_evaluation_report(results: dict, classification_results: dict = None) -> Path:
    """Export a readable evaluation summary to data/reports/.

    Args:
        results:                {model_name: regression_metrics_dict}
        classification_results: {model_name: classification_metrics_dict} (optional)

    Returns:
        Path to the written JSON report.
    """
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    report = {
        "regression":    results,
        "classification": classification_results or {},
    }

    # Human-readable winner
    if results:
        winner = min(results, key=lambda k: results[k].get("rmse", float("inf")))
        report["best_model_by_rmse"] = winner

    report_path = REPORT_DIR / "evaluation_report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"  Evaluation report saved -> {report_path}")

    # Also write a CSV table
    import csv
    csv_path = REPORT_DIR / "evaluation_summary.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["model", "rmse", "mae", "r2"])
        writer.writeheader()
        for model_name, m in results.items():
            writer.writerow({"model": model_name, **m})
    print(f"  CSV summary saved       -> {csv_path}")

    return report_path


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def _discover_model_targets() -> dict[str, str]:
    """Scan MODEL_DIR and return {model_name: target_col} for all saved models.

    Naming convention (auto-detected):
      baseline_*          → target_aqi_1h
      gbt_{N}h            → target_aqi_{N}h
      gbt_pm25_{N}h       → target_pm25_{N}h
      sklearn_*_aqi_{N}h  → ignored (joblib format, not PipelineModel)
    """
    mapping: dict[str, str] = {}
    if not MODEL_DIR.exists():
        return mapping

    for model_dir in sorted(MODEL_DIR.iterdir()):
        if not model_dir.is_dir():
            continue
        name = model_dir.name

        # Skip sklearn joblib dirs (no metadata subfolder)
        if not (model_dir / "metadata").exists():
            continue

        if name.startswith("baseline_"):
            mapping[name] = "target_aqi_1h"
        elif name.startswith("gbt_pm25_"):
            h = name.replace("gbt_pm25_", "").replace("h", "")
            mapping[name] = f"target_pm25_{h}h"
        elif name.startswith("gbt_"):
            h = name.replace("gbt_", "").replace("h", "")
            mapping[name] = f"target_aqi_{h}h"

    return mapping


def run(spark: SparkSession) -> None:
    """Load all saved PipelineModels, evaluate on test data, write report."""
    from src.ml.train_baseline_models import load_training_dataset

    print("Loading test split...")
    _, _, test = load_training_dataset(spark)

    results     = {}
    cls_results = {}

    model_targets = _discover_model_targets()
    if not model_targets:
        print("No trained models found in data/models/. Run training scripts first.")
        return

    print(f"Found {len(model_targets)} model(s): {list(model_targets.keys())}")

    for model_name, target_col in model_targets.items():
        model_path = MODEL_DIR / model_name
        print(f"\n  Evaluating {model_name} (target={target_col})...")

        if target_col not in test.columns:
            print(f"    Skipping — target column '{target_col}' not in dataset")
            continue

        try:
            model = PipelineModel.load(str(model_path))
        except Exception as exc:
            print(f"    Could not load model: {exc}")
            continue

        test_h = test.filter(F.col(target_col).isNotNull())
        preds  = model.transform(test_h).cache()
        preds.count()

        results[model_name]     = evaluate_regression(preds, target_col)
        cls_results[model_name] = evaluate_classification(preds, target_col)
        print(f"    regression    : {results[model_name]}")
        print(f"    classification: accuracy={cls_results[model_name]['accuracy']}  f1={cls_results[model_name]['f1']}")
        preds.unpersist()

    print("\nModel comparison (sorted by RMSE):")
    for row in compare_models(results):
        print(f"  {row['model']}: rmse={row['rmse']}  mae={row['mae']}  r2={row['r2']}")

    build_evaluation_report(results, cls_results)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("evaluate-models")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()
