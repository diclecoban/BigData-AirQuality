"""
PySpark Structured Streaming — İstanbul AQI gerçek zamanlı işleme.

Çalıştırma (Docker cluster üzerinden spark-submit):
  spark-submit \
    --master spark://localhost:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    structured_streaming_job.py

Ortam değişkenleri:
  KAFKA_BOOTSTRAP   = kafka:29092          (Docker ağı içi — varsayılan)
                      localhost:9092       (host makineden test)
  SPARK_MASTER      = spark://localhost:7077
  OUTPUT_PATH       = /tmp/enriched_aq
  CHECKPOINT_PATH   = /tmp/checkpoints/aq
  PG_HOST           = timescaledb
  PG_USER           = airquality
  PG_PASSWORD       = airquality
  PG_DB             = airquality
"""

from __future__ import annotations

import os
import math
import logging
import socket

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from schema import get_air_quality_spark_schema, get_weather_spark_schema

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
AQ_TOPIC        = "air_quality_normalized"
WEATHER_TOPIC   = "weather_normalized"
OUTPUT_PATH     = os.getenv("OUTPUT_PATH",     "/tmp/enriched_aq")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/tmp/checkpoints/aq")
WATERMARK_DELAY = "35 minutes"

PG_HOST     = os.getenv("PG_HOST",     "timescaledb")
PG_PORT     = os.getenv("PG_PORT",     "5432")
PG_DB       = os.getenv("PG_DB",       "airquality")
PG_USER     = os.getenv("PG_USER",     "airquality")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airquality")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [Streaming] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """Docker Spark cluster'ına bağlanan streaming SparkSession."""
    from src.common.config import (
        SPARK_MASTER,
        SPARK_DRIVER_MEMORY,
        SPARK_EXECUTOR_MEMORY,
        SPARK_EXECUTOR_CORES,
        SPARK_KAFKA_PACKAGE,
    )

    packages = f"{SPARK_KAFKA_PACKAGE},org.postgresql:postgresql:42.7.3"

    builder = (
        SparkSession.builder
        .appName("IstanbulAQI_StructuredStreaming")
        .master(SPARK_MASTER)
        .config("spark.driver.memory",                    SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory",                  SPARK_EXECUTOR_MEMORY)
        .config("spark.executor.cores",                   SPARK_EXECUTOR_CORES)
        .config("spark.sql.shuffle.partitions",           "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages",                    packages)
        .config("spark.driver.bindAddress",               "0.0.0.0")
    )

    if not SPARK_MASTER.startswith("local"):
        builder = builder.config(
            "spark.driver.host", socket.gethostbyname(socket.gethostname())
        )

    return builder.getOrCreate()


def build_input_streams(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    aq_schema      = get_air_quality_spark_schema()
    weather_schema = get_weather_spark_schema()

    raw_aq = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", AQ_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    raw_weather = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", WEATHER_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    aq_df = (
        raw_aq
        .select(F.from_json(F.col("value").cast("string"), aq_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )
    weather_df = (
        raw_weather
        .select(F.from_json(F.col("value").cast("string"), weather_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )
    return aq_df, weather_df


def clean_and_validate(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("station_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("source").isNotNull())
        .filter(F.col("aqi").isNull() | F.col("aqi").between(0, 500))
        .withColumn("pm10", F.when(F.col("pm10") < 0, None).otherwise(F.col("pm10")))
        .withColumn("pm25", F.when(F.col("pm25") < 0, None).otherwise(F.col("pm25")))
        .withColumn("no2",  F.when(F.col("no2")  < 0, None).otherwise(F.col("no2")))
        .withColumn("co",   F.when(F.col("co")   < 0, None).otherwise(F.col("co")))
        .withColumn("o3",   F.when(F.col("o3")   < 0, None).otherwise(F.col("o3")))
        .filter(
            F.col("latitude").isNull() |
            ~((F.col("latitude") == 0) & (F.col("longitude") == 0))
        )
    )


def enrich_with_weather(air_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    aq = air_df.withColumn(
        "aq_bucket",
        F.date_trunc("hour", F.col("event_time"))
    )

    weather_prep = weather_df.select(
        F.col("event_time").alias("w_event_time"),
        F.date_trunc("hour", F.col("event_time")).alias("w_bucket"),
        F.col("temperature").alias("w_temperature"),
        F.col("humidity").alias("w_humidity"),
        F.col("wind_speed").alias("w_wind_speed"),
        F.col("wind_direction").alias("w_wind_direction"),
        F.col("pressure").alias("w_pressure"),
        F.col("precipitation").alias("w_precipitation"),
        F.col("visibility").alias("w_visibility"),
        F.col("cloud_cover").alias("w_cloud_cover"),
    )

    joined = aq.join(
        weather_prep,
        (aq["aq_bucket"] == weather_prep["w_bucket"]) &
        (aq["event_time"] >= weather_prep["w_event_time"] - F.expr("INTERVAL 30 MINUTES")) &
        (aq["event_time"] <= weather_prep["w_event_time"] + F.expr("INTERVAL 30 MINUTES")),
        how="inner",
    )

    enriched = (
        joined
        .select(
            aq["station_id"], aq["station_name"], aq["district"], aq["source"],
            aq["timestamp"], aq["event_time"],
            aq["latitude"], aq["longitude"],
            aq["pm10"], aq["pm25"], aq["no2"], aq["so2"], aq["co"], aq["o3"], aq["aqi"],
            weather_prep["w_temperature"],
            weather_prep["w_humidity"],
            weather_prep["w_wind_speed"],
            weather_prep["w_wind_direction"],
            weather_prep["w_pressure"],
            weather_prep["w_precipitation"],
            weather_prep["w_visibility"],
            weather_prep["w_cloud_cover"],
        )
        .withColumn("wind_u",
            F.col("w_wind_speed") * F.cos(F.radians(F.col("w_wind_direction"))))
        .withColumn("wind_v",
            F.col("w_wind_speed") * F.sin(F.radians(F.col("w_wind_direction"))))
        .withColumn("hour",     F.hour("event_time"))
        .withColumn("hour_sin", F.sin(F.lit(2 * math.pi) * F.col("hour") / F.lit(24)))
        .withColumn("hour_cos", F.cos(F.lit(2 * math.pi) * F.col("hour") / F.lit(24)))
        .withColumn("month",     F.month("event_time"))
        .withColumn("month_sin", F.sin(F.lit(2 * math.pi) * F.col("month") / F.lit(12)))
        .withColumn("month_cos", F.cos(F.lit(2 * math.pi) * F.col("month") / F.lit(12)))
        .withColumn("is_weekend",
            (F.dayofweek("event_time").isin([1, 7])).cast(IntegerType()))
        .withColumn("aqi_category",
            F.when(F.col("aqi").isNull(), None)
             .when(F.col("aqi") <= 50,  0)
             .when(F.col("aqi") <= 100, 1)
             .when(F.col("aqi") <= 150, 2)
             .when(F.col("aqi") <= 200, 3)
             .otherwise(4))
    )
    return enriched


def _write_batch_to_pg(batch_df, batch_id):
    """Her micro-batch'i TimescaleDB'ye yaz (JDBC)."""
    pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    (
        batch_df
        .select(
            F.col("event_time").alias("time"),
            "station_id", "station_name", "district", "source",
            "pm10", "pm25", "no2", "so2", "co", "o3", "aqi",
            "aqi_category",
            F.col("w_temperature").alias("temperature"),
            F.col("w_humidity").alias("humidity"),
            F.col("w_wind_speed").alias("wind_speed"),
            F.col("w_wind_direction").alias("wind_direction"),
            F.col("w_pressure").alias("pressure"),
        )
        .write
        .format("jdbc")
        .option("url", pg_url)
        .option("dbtable", "air_quality_enriched")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_enriched_output(enriched_df: DataFrame):
    parquet_query = (
        enriched_df
        .withColumn("date", F.to_date("event_time"))
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet")
        .partitionBy("district", "date")
        .trigger(processingTime="30 seconds")
        .start()
    )
    pg_query = (
        enriched_df
        .writeStream
        .outputMode("append")
        .foreachBatch(_write_batch_to_pg)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/postgres")
        .trigger(processingTime="30 seconds")
        .start()
    )
    console_query = (
        enriched_df
        .select(
            "station_id", "station_name", "district", "source",
            "event_time", "pm25", "aqi", "aqi_category",
            "w_temperature", "w_wind_speed",
        )
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", "20")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/console")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return parquet_query, pg_query, console_query


def main():
    log.info("SparkSession başlatılıyor ...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Kafka stream'leri okunuyor ...")
    aq_stream, weather_stream = build_input_streams(spark)

    log.info("Temizlik ve doğrulama uygulanıyor ...")
    clean_aq = clean_and_validate(aq_stream)

    log.info("Weather join + feature engineering ...")
    enriched = enrich_with_weather(clean_aq, weather_stream)

    log.info("Çıktı yazıcıları başlatılıyor ...")
    parquet_q, pg_q, console_q = write_enriched_output(enriched)

    log.info("Pipeline aktif. Durdurmak için Ctrl+C ...")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        log.info("Pipeline kullanıcı tarafından durduruldu.")
    finally:
        parquet_q.stop()
        pg_q.stop()
        console_q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
