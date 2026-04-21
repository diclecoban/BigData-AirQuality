"""
Kafka producer — OpenWeatherMap hava durumu verisi.

İstanbul için saatlik hava durumu verisi çeker, WeatherRecord'a
normalize eder ve 'weather_normalized' Kafka topic'ine publish eder.

Spark Streaming tarafında AirQualityRecord ile timestamp üzerinden join edilir.

Ortam değişkenleri:
    OWM_API_KEY       = <OpenWeatherMap API key>
    OWM_CITY_ID       = 745042   (İstanbul)
    KAFKA_BOOTSTRAP   = localhost:9092
    POLL_INTERVAL_SEC = 1800     (varsayılan: 30 dakika)
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer

from schema import WeatherRecord

# ---------------------------------------------------------------------------
# Yapılandırma
# ---------------------------------------------------------------------------

OWM_BASE_URL      = "https://api.openweathermap.org/data/2.5"
OWM_API_KEY       = os.getenv("OWM_API_KEY", "")
OWM_CITY_ID       = os.getenv("OWM_CITY_ID", "745042")   # İstanbul
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC       = "weather_normalized"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "1800"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [Weather] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Hava durumu verisi çek
# ---------------------------------------------------------------------------

def fetch_weather_data() -> dict:
    """
    OpenWeatherMap /weather endpoint'inden İstanbul anlık verisini çeker.

    OWM örnek yanıtı:
    {
      "dt": 1745225600,          # Unix timestamp
      "main": {
        "temp": 14.5,            # °C (units=metric)
        "humidity": 68,          # %
        "pressure": 1012         # hPa
      },
      "wind": {
        "speed": 5.2,            # m/s
        "deg": 230               # derece
      },
      "rain": {"1h": 0.0},       # mm/saat (opsiyonel)
      "visibility": 9000,         # metre
      "clouds": {"all": 40}      # %
    }
    """
    if not OWM_API_KEY:
        raise EnvironmentError("OWM_API_KEY ortam değişkeni tanımlı değil")

    url = f"{OWM_BASE_URL}/weather"
    params = {
        "id":    OWM_CITY_ID,
        "appid": OWM_API_KEY,
        "units": "metric",
    }
    log.info("OpenWeatherMap verisi çekiliyor (city_id=%s)", OWM_CITY_ID)
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_weather_forecast(cnt: int = 8) -> list[dict]:
    """
    OWM /forecast endpoint'inden 3 saatlik tahminleri çeker.
    cnt=8 → sonraki 24 saat (8 × 3h).

    Dönen liste her öğe için aynı yapıda OWM yanıtı içerir.
    Feature engineering için lag değerleri oluşturmak amacıyla kullanılabilir.
    """
    if not OWM_API_KEY:
        raise EnvironmentError("OWM_API_KEY ortam değişkeni tanımlı değil")

    url = f"{OWM_BASE_URL}/forecast"
    params = {
        "id":    OWM_CITY_ID,
        "appid": OWM_API_KEY,
        "units": "metric",
        "cnt":   cnt,
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("list", [])


# ---------------------------------------------------------------------------
# 2. Normalize — ham OWM yanıtı → WeatherRecord
# ---------------------------------------------------------------------------

def normalize_weather_record(raw_record: dict) -> Optional[WeatherRecord]:
    """
    OWM ham JSON'unu WeatherRecord'a çevirir.

    raw_record: fetch_weather_data() veya fetch_weather_forecast() öğesi.
    """
    try:
        # Unix timestamp → ISO-8601 UTC
        unix_ts = raw_record.get("dt")
        if unix_ts:
            iso_ts = datetime.fromtimestamp(unix_ts, tz=timezone.utc).isoformat()
        else:
            iso_ts = datetime.now(tz=timezone.utc).isoformat()

        main   = raw_record.get("main", {})
        wind   = raw_record.get("wind", {})
        rain   = raw_record.get("rain", {})
        clouds = raw_record.get("clouds", {})

        record = WeatherRecord(
            timestamp      = iso_ts,
            city           = "Istanbul",
            temperature    = _to_float(main.get("temp")),
            humidity       = _to_float(main.get("humidity")),
            wind_speed     = _to_float(wind.get("speed")),
            wind_direction = _to_float(wind.get("deg")),
            pressure       = _to_float(main.get("pressure")),
            precipitation  = _to_float(rain.get("1h", 0.0)),
            visibility     = _to_float(raw_record.get("visibility")),
            cloud_cover    = _to_int(clouds.get("all")),
        )
        return record

    except Exception as exc:
        log.error("normalize_weather_record hatası: %s", exc)
        return None


# ---------------------------------------------------------------------------
# 3. Kafka'ya publish
# ---------------------------------------------------------------------------

def publish_to_kafka(
    producer: KafkaProducer,
    records: list[WeatherRecord],
) -> int:
    """
    WeatherRecord listesini 'weather_normalized' topic'ine gönderir.
    Key: "istanbul" (sabit) — tüm weather mesajları aynı partition'a gider.
    """
    sent = 0
    for record in records:
        try:
            producer.send(
                KAFKA_TOPIC,
                key   = b"istanbul",
                value = record.to_json().encode("utf-8"),
            )
            sent += 1
        except Exception as exc:
            log.error("Kafka publish hatası: %s", exc)

    producer.flush()
    log.info("%d weather kaydı Kafka'ya gönderildi", sent)
    return sent


# ---------------------------------------------------------------------------
# 4. Ana döngü
# ---------------------------------------------------------------------------

def main():
    """
    Tam weather producer akışı:
      1. Kafka producer başlat
      2. Her POLL_INTERVAL_SEC saniyede bir:
         a. Anlık hava durumunu çek
         b. Normalize et
         c. Kafka'ya publish et
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("Kafka producer başlatıldı → %s", KAFKA_BOOTSTRAP)

    while True:
        try:
            raw = fetch_weather_data()
            record = normalize_weather_record(raw)
            if record:
                publish_to_kafka(producer, [record])
        except Exception as exc:
            log.error("Ana döngü hatası: %s", exc)

        log.info("%d saniye bekleniyor...", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

def _to_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _to_int(val) -> Optional[int]:
    try:
        return int(float(val)) if val is not None else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    main()
