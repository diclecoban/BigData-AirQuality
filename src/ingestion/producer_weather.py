"""OpenWeatherMap hava durumu verisini Kafka'ya gönderen producer.

NASIL ÇALIŞIR:
  1. OpenWeatherMap API'den İstanbul'un anlık hava durumunu çek
  2. schema.py'deki WEATHER_FIELDS formatına normalize et
  3. JSON'a çevir
  4. Kafka'daki 'weather.istanbul.raw' topic'ine gönder
  5. 15 dakikada bir tekrar et

Test etmek için:
  python src/ingestion/producer_weather.py --dry-run --once
"""

from __future__ import annotations

import json
import os
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import requests
import pandas as pd

from src.common.logger import get_logger
from src.ingestion.schema import WEATHER_FIELDS

logger = get_logger(__name__)

WEATHER_TOPIC = "weather.istanbul.raw"
POLL_INTERVAL_SECONDS = 900  # 15 dakika

# İstanbul koordinatları
ISTANBUL_LAT = 41.0082
ISTANBUL_LON = 29.0230

# OpenWeatherMap API
OWM_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
OWM_API_KEY  = os.getenv("OPENWEATHER_API_KEY", "")


# ---------------------------------------------------------------------------
# ADIM 1: Veri çekme
# ---------------------------------------------------------------------------

def fetch_weather_data() -> dict:
    """OpenWeatherMap API'den İstanbul anlık hava durumunu çek.

    Returns:
        Ham API response dict. Hata durumunda boş dict.
    """
    if not OWM_API_KEY:
        logger.error("OPENWEATHER_API_KEY set edilmemiş! export OPENWEATHER_API_KEY=...")
        return {}

    params = {
        "lat":   ISTANBUL_LAT,
        "lon":   ISTANBUL_LON,
        "appid": OWM_API_KEY,
        "units": "metric",  # Celsius
    }

    try:
        resp = requests.get(OWM_BASE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("OpenWeatherMap: veri alındı — sıcaklık=%.1f°C", data["main"]["temp"])
        return data
    except requests.RequestException as exc:
        logger.error("OpenWeatherMap API hatası: %s", exc)
        return {}


# ---------------------------------------------------------------------------
# ADIM 2: Normalize etme
# ---------------------------------------------------------------------------

def normalize_weather_record(raw: dict) -> dict:
    """Ham OWM verisini schema.py WEATHER_FIELDS formatına çevir.

    OWM şöyle döndürür:
      {"main": {"temp": 13.4, "humidity": 62}, "wind": {"speed": 3.1}, ...}

    Biz şöyle istiyoruz (WEATHER_FIELDS):
      {"timestamp": "...", "temperature": 13.4, "humidity": 62, ...}

    Args:
        raw: OpenWeatherMap API'den gelen ham dict.

    Returns:
        WEATHER_FIELDS kolonlarına sahip dict.
    """
    if not raw:
        return {}

    main  = raw.get("main", {})
    wind  = raw.get("wind", {})
    rain  = raw.get("rain", {})    # yağış varsa gelir
    clouds = raw.get("clouds", {})

    # Rüzgar yönünü derece → m/s bileşenlerine çevirme (ML için daha iyi)
    wind_deg = wind.get("deg", 0)

    record = {
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "temperature":    round(main.get("temp", 0), 2),
        "humidity":       main.get("humidity", 0),
        "wind_speed":     round(wind.get("speed", 0), 2),
        "wind_direction": wind_deg,
        "pressure":       main.get("pressure", 0),
        "precipitation":  round(rain.get("1h", 0), 2),  # son 1 saatteki yağış mm
        "visibility":     round(raw.get("visibility", 10000) / 1000, 2),  # m → km
        "cloud_cover":    clouds.get("all", 0),  # yüzde
    }

    # WEATHER_FIELDS'daki tüm kolonların var olduğunu garantile
    for field in WEATHER_FIELDS:
        if field not in record:
            record[field] = None

    logger.info(
        "Normalize: sıcaklık=%.1f°C nem=%d%% rüzgar=%.1fm/s yağış=%.1fmm",
        record["temperature"], record["humidity"],
        record["wind_speed"],  record["precipitation"],
    )
    return record


# ---------------------------------------------------------------------------
# ADIM 3: Kafka'ya gönderme
# ---------------------------------------------------------------------------

def publish_to_kafka(record: dict, producer=None, dry_run: bool = False) -> bool:
    """Normalize edilmiş hava durumu kaydını Kafka'ya gönder.

    Args:
        record:   normalize_weather_record() çıktısı.
        producer: KafkaProducer nesnesi (None ise dry-run).
        dry_run:  True ise sadece ekrana yaz.

    Returns:
        True başarılı, False hata var.
    """
    if not record:
        logger.warning("Gönderilecek kayıt yok")
        return False

    json_str = json.dumps(record, ensure_ascii=False)
    key = "istanbul"  # tek şehir olduğu için sabit key

    if dry_run or producer is None:
        print(f"  [DRY-RUN] topic={WEATHER_TOPIC}  key={key}")
        print(f"            {json_str}")
    else:
        producer.send(
            WEATHER_TOPIC,
            key=key.encode("utf-8"),
            value=json_str.encode("utf-8"),
        )
        producer.flush()

    logger.info("Hava durumu mesajı gönderildi → topic: %s", WEATHER_TOPIC)
    return True


# ---------------------------------------------------------------------------
# Kafka producer oluşturma
# ---------------------------------------------------------------------------

def _create_kafka_producer(bootstrap_servers: str):
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python kurulu değil! Kur: pip install kafka-python")
        return None

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        retries=3,
        batch_size=16384,
        linger_ms=10,
    )
    logger.info("Kafka producer bağlandı → %s", bootstrap_servers)
    return producer


# ---------------------------------------------------------------------------
# Ana döngü
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OpenWeatherMap Kafka Producer")
    parser.add_argument("--dry-run", action="store_true",
                        help="Kafka'ya gönderme, sadece ekrana yaz")
    parser.add_argument("--once", action="store_true",
                        help="Sadece bir kez çalış")
    parser.add_argument("--bootstrap", default="localhost:9092",
                        help="Kafka adresi (varsayılan: localhost:9092)")
    args = parser.parse_args()

    producer = None
    if not args.dry_run:
        producer = _create_kafka_producer(args.bootstrap)
        if producer is None:
            args.dry_run = True

    logger.info("Weather Producer başladı | mod=%s | interval=%ds",
                "dry-run" if args.dry_run else "kafka", POLL_INTERVAL_SECONDS)

    while True:
        start = time.time()
        try:
            raw    = fetch_weather_data()
            record = normalize_weather_record(raw)
            publish_to_kafka(record, producer=producer, dry_run=args.dry_run)
            logger.info("Döngü tamamlandı: %.1fs", time.time() - start)
        except KeyboardInterrupt:
            logger.info("Producer durduruldu (Ctrl+C)")
            break
        except Exception as exc:
            logger.error("Hata: %s — %ds sonra tekrar denenecek", exc, POLL_INTERVAL_SECONDS)

        if args.once:
            break

        time.sleep(POLL_INTERVAL_SECONDS)

    if producer:
        producer.close()


if __name__ == "__main__":
    main()
