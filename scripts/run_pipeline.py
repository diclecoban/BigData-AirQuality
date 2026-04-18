"""Full ML pipeline runner.

Runs steps in order:
  1. Historical batch analysis
  2. Baseline model training (LinearRegression + RandomForest)
  3. GBT model training + MLflow tracking
  4. Model evaluation + report

Run:
  python scripts/run_pipeline.py
"""

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Windows: PySpark needs HADOOP_HOME pointing to a directory with winutils.exe.
# Run scripts/setup_winutils.py ONCE to download winutils, then this block
# sets the variable automatically for every subsequent run.
# ---------------------------------------------------------------------------
_HADOOP_HOME = Path("C:/hadoop")
if sys.platform == "win32" and _HADOOP_HOME.exists():
    os.environ.setdefault("HADOOP_HOME", str(_HADOOP_HOME))
    os.environ.setdefault("hadoop.home.dir", str(_HADOOP_HOME))
    _bin = str(_HADOOP_HOME / "bin")
    if _bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _bin + ";" + os.environ.get("PATH", "")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("istanbul-aqi-pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


if __name__ == "__main__":
    print("=" * 60)
    print("Istanbul AQI Pipeline")
    print("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Step 1: Historical analysis
        print("\n[1/4] Historical Batch Analysis")
        from src.batch.historical_analysis import run as run_historical
        run_historical(spark)

        # Step 2: Baseline models
        print("\n[2/4] Baseline Model Training")
        from src.ml.train_baseline_models import run as run_baselines
        run_baselines(spark)

        # Step 3: GBT + MLflow
        print("\n[3/4] GBT Model Training + MLflow")
        from src.ml.train_gbt_model import run as run_gbt
        run_gbt(spark)

        # Step 4: Evaluation
        print("\n[4/4] Model Evaluation")
        from src.ml.evaluate_models import run as run_eval
        run_eval(spark)

        print("\n" + "=" * 60)
        print("Pipeline complete.")
        print("  Reports  → data/reports/")
        print("  Models   → data/models/")
        print("  MLflow   → data/mlruns/  (open with: mlflow ui --backend-store-uri data/mlruns)")
        print("=" * 60)

    finally:
        spark.stop()
