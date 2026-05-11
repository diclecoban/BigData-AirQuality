"""Baseline metrics without PipelineModel.load().

Trains LR and RF in-memory, evaluates on test split via pandas math.
GBT metrics are taken from the training run output (already on disk).

Run:
  SPARK_MASTER=local[*] ~/airquality-venv/bin/python scripts/quick_eval.py
"""

import sys
import csv
import os
from pathlib import Path

sys.setrecursionlimit(100000)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPORT_DIR = ROOT / "data" / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# GBT values from training run output
KNOWN_GBT = {
    "gbt_1h": {"rmse": 3.4675, "mae": 1.9464, "r2": 0.9534},
    "gbt_3h": {"rmse": 5.4818, "mae": 3.1450, "r2": 0.8829},
    "gbt_6h": {"rmse": 7.1731, "mae": 4.6400, "r2": 0.7976},
}


def compute_metrics(preds_df, label_col: str) -> dict:
    pdf = preds_df.select("prediction", label_col).dropna().toPandas()
    if pdf.empty:
        return {"rmse": None, "mae": None, "r2": None}
    y, yhat = pdf[label_col].values, pdf["prediction"].values
    res  = y - yhat
    rmse = round(float(np.sqrt(np.mean(res**2))), 4)
    mae  = round(float(np.mean(np.abs(res))),      4)
    sst  = np.sum((y - y.mean())**2)
    r2   = round(float(1 - np.sum(res**2) / sst) if sst else 0.0, 4)
    return {"rmse": rmse, "mae": mae, "r2": r2}


def main():
    from src.common.config import SPARK_DRIVER_MEMORY, SPARK_SQL_SHUFFLE_PARTITIONS

    master = os.getenv("SPARK_MASTER", "local[*]")
    spark = (
        SparkSession.builder
        .appName("quick-eval")
        .master(master)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    from src.ml.train_baseline_models import (
        load_training_dataset,
        train_linear_regression,
        train_random_forest,
    )

    print("Loading dataset...")
    train, _, test = load_training_dataset(spark)
    test.cache()
    test.count()

    results = {}

    print("\nTraining Linear Regression...")
    lr_model = train_linear_regression(train)
    preds = lr_model.transform(test.filter(F.col("target_aqi_1h").isNotNull()))
    results["baseline_linear_regression"] = compute_metrics(preds, "target_aqi_1h")
    print(f"  {results['baseline_linear_regression']}")

    print("\nTraining Random Forest...")
    rf_model = train_random_forest(train)
    preds = rf_model.transform(test.filter(F.col("target_aqi_1h").isNotNull()))
    results["baseline_random_forest"] = compute_metrics(preds, "target_aqi_1h")
    print(f"  {results['baseline_random_forest']}")

    # Merge with known GBT values
    results.update(KNOWN_GBT)

    csv_path = REPORT_DIR / "evaluation_summary.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["model", "rmse", "mae", "r2"])
        writer.writeheader()
        for name, m in sorted(results.items(), key=lambda x: x[1]["rmse"] or 999):
            writer.writerow({"model": name, **m})

    print(f"\nSaved -> {csv_path}")
    print("\n=== Performance Comparison (Test Set) ===")
    print(f"{'Model':<35} {'RMSE':>8} {'MAE':>8} {'R²':>8}")
    print("-" * 62)
    for name, m in sorted(results.items(), key=lambda x: x[1]["rmse"] or 999):
        print(f"  {name:<33} {m['rmse']:>8} {m['mae']:>8} {m['r2']:>8}")

    spark.stop()


if __name__ == "__main__":
    main()
