"""
Kafka producer — OpenAQ hava kalitesi verisi (v3 API).

OpenAQ v3'te endpoint yapısı değişti:
  - /v3/measurements?city=...  → KALDIRILDI (404)
  - Doğru akış:
      1. /v3/locations?coordinates=lat,lon&radius=... → İstanbul istasyonlarını bul
      2. Her location için /v3/locations/{id}/latest  → son ölçümü al

Ortam değişkenleri:
    OPENAQ_API_KEY    = <API key>
    KAFKA_BOOTSTRAP   = localhost:9092
    POLL_INTERVAL_SEC = 300
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from kafka import KafkaProducer

from schema import AirQualityRecord

OPENAQ_BASE_URL   = "https://api.openaq.org/v3"
OPENAQ_API_KEY    = os.getenv("OPENAQ_API_KEY", "")
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC       = "air_quality_normalized"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "300"))

ISTANBUL_LAT    = 41.0082
ISTANBUL_LON    = 28.9784
ISTANBUL_RADIUS = 50000   # metre

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [OpenAQ] %(levelname)s %(message)s")
log = logging.getLogger(__name__)

_HEADERS = {"X-API-Key": OPENAQ_API_KEY} if OPENAQ_API_KEY else {}

PARAM_MAP = {"pm10": "pm10", "pm25": "pm25", "no2": "no2",
             "so2": "so2", "co": "co", "o3": "o3"}


def fetch_istanbul_locations() -> list[dict]:
    url = f"{OPENAQ_BASE_URL}/locations"
    params = {
        "bbox":  "26.0,40.8,29.9,41.5",   # İstanbul bounding box: minLon,minLat,maxLon,maxLat
        "limit": 100,
    }
    log.info("İstanbul lokasyonları çekiliyor ...")
    resp = requests.get(url, params=params, headers=_HEADERS, timeout=20)
    
    # Debug: tam URL'i logla
    log.info("İstek URL: %s", resp.url)
    
    resp.raise_for_status()
    results = resp.json().get("results", [])
    log.info("%d lokasyon bulundu", len(results))
    return results


def fetch_latest_for_location(location_id: int) -> list[dict]:
    url = f"{OPENAQ_BASE_URL}/locations/{location_id}/latest"
    for attempt in range(4):
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning("Rate limit for %s, %ss bekleniyor", location_id, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json().get("results", [])
    return []


def normalize_openaq_record(location: dict, latest_measurements: list[dict]) -> Optional[AirQualityRecord]:
    try:
        timestamps = [
            m.get("datetime", {}).get("utc", "")
            for m in latest_measurements
            if m.get("datetime", {}).get("utc")
        ]
        iso_ts = max(timestamps) if timestamps else datetime.now(timezone.utc).isoformat()
        coords = location.get("coordinates", {})
        name   = location.get("name", "")
        locality = location.get("locality", "Istanbul")

        pollutants: dict = {k: None for k in PARAM_MAP.values()}
        for m in latest_measurements:
            param_name = m.get("parameter", {}).get("name", "").lower()
            if param_name in PARAM_MAP:
                pollutants[PARAM_MAP[param_name]] = _to_float(m.get("value"))

        record = AirQualityRecord(
            station_id   = str(location.get("id", "")),
            station_name = name,
            district     = _infer_district(name, locality),
            source       = "openaq",
            timestamp    = iso_ts,
            latitude     = coords.get("latitude"),
            longitude    = coords.get("longitude"),
            pm10=pollutants["pm10"], pm25=pollutants["pm25"],
            no2=pollutants["no2"],  so2=pollutants["so2"],
            co=pollutants["co"],    o3=pollutants["o3"],
            aqi=None,
        )
        errors = record.validate()
        if errors:
            log.warning("Geçersiz kayıt [%s]: %s", name, errors)
            return None
        return record
    except Exception as exc:
        log.error("normalize hatası [%s]: %s", location.get("name","?"), exc)
        return None


def publish_to_kafka(producer: KafkaProducer, records: list[AirQualityRecord]) -> int:
    sent = 0
    for record in records:
        try:
            producer.send(KAFKA_TOPIC,
                          key=record.station_id.encode("utf-8"),
                          value=record.to_json().encode("utf-8"))
            sent += 1
        except Exception as exc:
            log.error("Kafka publish hatası [%s]: %s", record.station_id, exc)
    producer.flush()
    log.info("%d / %d kayıt Kafka'ya gönderildi", sent, len(records))
    return sent


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("Kafka producer başlatıldı → %s", KAFKA_BOOTSTRAP)

    locations = fetch_istanbul_locations()
    last_loc_refresh = time.time()

    while True:
        if time.time() - last_loc_refresh > 3600:
            locations = fetch_istanbul_locations()
            last_loc_refresh = time.time()

        records: list[AirQualityRecord] = []
        for loc in locations:
            try:
                latest = fetch_latest_for_location(loc.get("id"))
                if latest:
                    rec = normalize_openaq_record(loc, latest)
                    if rec:
                        records.append(rec)
            except Exception as exc:
                log.warning("Lokasyon %s atlandı: %s", loc.get("id"), exc)
            time.sleep(1.0)

        if records:
            publish_to_kafka(producer, records)
        else:
            log.warning("Bu turda publish edilecek kayıt yok")

        log.info("%d saniye bekleniyor...", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)


def _infer_district(name: str | None, locality: str | None) -> str:
    name = (name or "").strip()
    locality = (locality or "Istanbul").strip()

    if " - " in name:
        return name.split(" - ")[0].strip()
    if "," in name:
        return name.split(",")[0].strip()
    return locality

def _to_float(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    main()