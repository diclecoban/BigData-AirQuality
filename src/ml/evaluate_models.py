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

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT       = Path(__file__).resolve().parents[2]
MODEL_DIR  = ROOT / "data" / "models"
REPORT_DIR = ROOT / "data" / "reports"


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
         .otherwise(5),                      # Hazardous
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
        .toPandas()
        .to_dict(orient="records")
    )

    return {"accuracy": accuracy, "f1": f1, "confusion_matrix": cm}


# ---------------------------------------------------------------------------
# 3. Model comparison
# ---------------------------------------------------------------------------

def compare_models(results: dict) -> DataFrame:
    """Build a side-by-side comparison DataFrame.

    Args:
        results: {model_name: {"rmse": ..., "mae": ..., "r2": ...}}

    Returns:
        Spark DataFrame with one row per model.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    rows = [
        {"model": name, **metrics}
        for name, metrics in results.items()
    ]
    df = spark.createDataFrame(rows)
    return df.orderBy("rmse")


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
    print(f"  Evaluation report saved → {report_path}")

    # Also write a CSV table
    import csv
    csv_path = REPORT_DIR / "evaluation_summary.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["model", "rmse", "mae", "r2"])
        writer.writeheader()
        for model_name, m in results.items():
            writer.writerow({"model": model_name, **m})
    print(f"  CSV summary saved       → {csv_path}")

    return report_path


# ---------------------------------------------------------------------------
# Standalone runner
# ---------------------------------------------------------------------------

def run(spark: SparkSession) -> None:
    """Load saved models, evaluate on test data, write report."""
    from src.ml.train_baseline_models import load_training_dataset

    print("Loading test split...")
    _, _, test = load_training_dataset(spark)

    results     = {}
    cls_results = {}

    model_targets = {
        "baseline_linear_regression": "target_aqi_1h",
        "baseline_random_forest":     "target_aqi_1h",
        "gbt_1h":                     "target_aqi_1h",
        "gbt_3h":                     "target_aqi_3h",
        "gbt_6h":                     "target_aqi_6h",
    }

    for model_name, target_col in model_targets.items():
        model_path = MODEL_DIR / model_name
        if not model_path.exists():
            print(f"  Skipping {model_name} (not found at {model_path})")
            continue

        print(f"  Evaluating {model_name}...")
        model = PipelineModel.load(str(model_path))
        test_h = test.filter(F.col(target_col).isNotNull())
        preds  = model.transform(test_h)

        results[model_name]     = evaluate_regression(preds, target_col)
        cls_results[model_name] = evaluate_classification(preds, target_col)
        print(f"    regression  : {results[model_name]}")
        print(f"    classification: accuracy={cls_results[model_name]['accuracy']}  f1={cls_results[model_name]['f1']}")

    print("\nModel comparison:")
    compare_models(results).show(truncate=False)

    build_evaluation_report(results, cls_results)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("evaluate-models")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
    finally:
        spark.stop()
