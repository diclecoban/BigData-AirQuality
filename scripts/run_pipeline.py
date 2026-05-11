"""Full ML pipeline runner — Docker Spark cluster modu.

Runs steps in order:
  1. Historical batch analysis
  2. Baseline model training (LinearRegression + RandomForest)
  3. GBT model training + MLflow tracking
  4. Model evaluation + report

Çalıştırma (cluster modu):
  python scripts/run_pipeline.py

Lokal test için:
  SPARK_MASTER=local[*] python scripts/run_pipeline.py

Gereksinimler:
  - Docker stack ayakta olmalı (Spark master: spark://localhost:7077)
  - MLflow server çalışıyor olmalı (http://localhost:5000)
  - data/raw/ dizininde CSV'ler mevcut olmalı
"""

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Windows: PySpark needs HADOOP_HOME pointing to a directory with winutils.exe.
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

from src.common.config import (
    SPARK_MASTER,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_EXECUTOR_CORES,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    SPARK_KAFKA_PACKAGE,
    MLFLOW_TRACKING_URI,
)


def get_spark() -> SparkSession:
    """Docker Spark cluster'ına bağlanan SparkSession oluşturur.

    Ortam değişkenleriyle override edilebilir:
      SPARK_MASTER=local[*]          → lokal test
      SPARK_MASTER=spark://host:7077 → cluster
    """
    builder = (
        SparkSession.builder
        .appName("istanbul-aqi-pipeline")
        .master(SPARK_MASTER)
        .config("spark.driver.memory",          SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory",         SPARK_EXECUTOR_MEMORY)
        .config("spark.executor.cores",          SPARK_EXECUTOR_CORES)
        .config("spark.sql.shuffle.partitions",  SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.jars.packages",           SPARK_KAFKA_PACKAGE)
        .config("spark.mlflow.trackingUri",      MLFLOW_TRACKING_URI)
        .config("spark.driver.bindAddress",      "0.0.0.0")
        .config("spark.driver.maxResultSize",    "2g")
        .config("spark.sql.adaptive.enabled",    "true")
    )

    if not SPARK_MASTER.startswith("local"):
        import socket
        driver_host = socket.gethostbyname(socket.gethostname())
        builder = builder.config("spark.driver.host", driver_host)

    return builder.getOrCreate()


if __name__ == "__main__":
    print("=" * 60)
    print("Istanbul AQI Pipeline")
    print(f"  Spark master : {SPARK_MASTER}")
    print(f"  MLflow URI   : {MLFLOW_TRACKING_URI}")
    print("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("\n[1/4] Historical Batch Analysis")
        from src.batch.historical_analysis import run as run_historical
        run_historical(spark)

        print("\n[2/4] Baseline Model Training  (LinearRegression + RandomForest)")
        from src.ml.train_baseline_models import run as run_baselines
        run_baselines(spark)

        print("\n[3/4] GBT Model Training + MLflow Tracking")
        from src.ml.train_gbt_model import run as run_gbt
        run_gbt(spark)

        print("\n[4/4] Model Evaluation")
        from src.ml.evaluate_models import run as run_eval
        run_eval(spark)

        print("\n" + "=" * 60)
        print("Pipeline tamamlandı.")
        print(f"  Raporlar  -> data/reports/")
        print(f"  Modeller  -> data/models/")
        print(f"  MLflow UI -> {MLFLOW_TRACKING_URI}")
        print("=" * 60)

    finally:
        spark.stop()
