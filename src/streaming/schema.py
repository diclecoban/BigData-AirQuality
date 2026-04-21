"""
Shared schemas for raw and normalized ingestion records.

Canonical model: AirQualityRecord
Her producer (IBB, OpenAQ, Weather) kendi ham verisini bu modele normalize eder.
Kafka'ya her zaman bu modelin JSON'u gider → Spark tek schema ile okur.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Canonical Air Quality Record  (Kafka topic: air_quality_normalized)
# ---------------------------------------------------------------------------

@dataclass
class AirQualityRecord:
    """
    Ortak hava kalitesi kaydı. IBB ve OpenAQ producer'ları
    kendi ham verisini bu yapıya çevirir.

    Zorunlu alanlar: station_id, timestamp, source
    Opsiyonel alanlar: sensörden sensöre değişen ölçümler
    """

    # --- Kimlik ---
    station_id: str                        # IBB: StationID, OpenAQ: location_id
    station_name: str                      # İstasyon adı
    district: str                          # İlçe adı (örn. "Kadıköy")
    source: str                            # "ibb" | "openaq"

    # --- Zaman ---
    timestamp: str                         # ISO-8601, UTC  "2026-04-21T10:00:00Z"

    # --- Konum ---
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # --- Kirletici ölçümler (µg/m³ veya ppb, kaynağa göre) ---
    pm10: Optional[float] = None
    pm25: Optional[float] = None
    no2: Optional[float] = None
    so2: Optional[float] = None
    co: Optional[float] = None
    o3: Optional[float] = None

    # --- İndeks ---
    aqi: Optional[int] = None

    def to_json(self) -> str:
        """Kafka'ya publish edilecek JSON string."""
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(raw: str) -> "AirQualityRecord":
        """Kafka'dan consume edilirken kullanılır."""
        return AirQualityRecord(**json.loads(raw))

    def validate(self) -> list[str]:
        """Basit doğrulama; hata mesajlarını döner."""
        errors = []
        if not self.station_id:
            errors.append("station_id boş olamaz")
        if not self.timestamp:
            errors.append("timestamp boş olamaz")
        if self.aqi is not None and not (0 <= self.aqi <= 500):
            errors.append(f"AQI değeri geçersiz: {self.aqi}")
        return errors


# ---------------------------------------------------------------------------
# Canonical Weather Record  (Kafka topic: weather_normalized)
# ---------------------------------------------------------------------------

@dataclass
class WeatherRecord:
    """
    OpenWeatherMap verisinin normalize edilmiş formu.
    Spark tarafında AirQualityRecord ile timestamp üzerinden join edilir.
    """

    timestamp: str                         # ISO-8601, UTC
    city: str = "Istanbul"

    temperature: Optional[float] = None   # °C
    humidity: Optional[float] = None      # %
    wind_speed: Optional[float] = None    # m/s
    wind_direction: Optional[float] = None  # derece (0-360)
    pressure: Optional[float] = None      # hPa
    precipitation: Optional[float] = None # mm/saat
    visibility: Optional[float] = None    # metre
    cloud_cover: Optional[int] = None     # % (0-100)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(raw: str) -> "WeatherRecord":
        return WeatherRecord(**json.loads(raw))


# ---------------------------------------------------------------------------
# Spark için StructType tanımları (structured_streaming_job.py tarafından import edilir)
# ---------------------------------------------------------------------------

# Lazy import: PySpark her ortamda olmayabilir
def get_air_quality_spark_schema():
    """AirQualityRecord için PySpark StructType döner."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType, IntegerType
    )
    return StructType([
        StructField("station_id",   StringType(),  nullable=False),
        StructField("station_name", StringType(),  nullable=True),
        StructField("district",     StringType(),  nullable=True),
        StructField("source",       StringType(),  nullable=False),
        StructField("timestamp",    StringType(),  nullable=False),
        StructField("latitude",     FloatType(),   nullable=True),
        StructField("longitude",    FloatType(),   nullable=True),
        StructField("pm10",         FloatType(),   nullable=True),
        StructField("pm25",         FloatType(),   nullable=True),
        StructField("no2",          FloatType(),   nullable=True),
        StructField("so2",          FloatType(),   nullable=True),
        StructField("co",           FloatType(),   nullable=True),
        StructField("o3",           FloatType(),   nullable=True),
        StructField("aqi",          IntegerType(), nullable=True),
    ])


def get_weather_spark_schema():
    """WeatherRecord için PySpark StructType döner."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, FloatType, IntegerType
    )
    return StructType([
        StructField("timestamp",      StringType(), nullable=False),
        StructField("city",           StringType(), nullable=True),
        StructField("temperature",    FloatType(),  nullable=True),
        StructField("humidity",       FloatType(),  nullable=True),
        StructField("wind_speed",     FloatType(),  nullable=True),
        StructField("wind_direction", FloatType(),  nullable=True),
        StructField("pressure",       FloatType(),  nullable=True),
        StructField("precipitation",  FloatType(),  nullable=True),
        StructField("visibility",     FloatType(),  nullable=True),
        StructField("cloud_cover",    IntegerType(),nullable=True),
    ])


# ---------------------------------------------------------------------------
# Eski alan listeleri — geriye dönük uyumluluk için korundu
# ---------------------------------------------------------------------------

RAW_AIR_QUALITY_FIELDS = [f.name for f in get_air_quality_spark_schema().fields] \
    if False else [          # PySpark yoksa statik liste kullanılır
    "station_id", "station_name", "district", "timestamp",
    "pm10", "pm25", "no2", "so2", "co", "o3", "aqi",
    "latitude", "longitude", "source",
]

WEATHER_FIELDS = [
    "timestamp", "temperature", "humidity", "wind_speed",
    "wind_direction", "pressure", "precipitation", "visibility", "cloud_cover",
]
