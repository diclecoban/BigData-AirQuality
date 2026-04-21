"""
Kafka producer — IBB hava kalitesi verisi.

Gerçek IBB API endpoint'leri:
  İstasyon listesi:
    GET https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIStations
    Yanıt: [{"Id":"uuid","Name":"Maslak","Adress":"...","Location":"POINT (lon lat)"}]

  Ölçüm sonuçları:
    GET https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId
        ?StationId=<uuid>&StartDate=<DD.MM.YYYY>&EndDate=<DD.MM.YYYY>
    Yanıt: [{"DateTime":"...","PM10":42.3,"PM2.5":18.1,"NO2":35.0,...,"AQI":67}]

Ortam değişkenleri:
    IBB_STATION_URL   = https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIStations
    IBB_MEASURE_URL   = https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId
    KAFKA_BOOTSTRAP   = localhost:9092
    POLL_INTERVAL_SEC = 300
"""

from __future__ import annotations

import os
import re
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from kafka import KafkaProducer

from schema import AirQualityRecord

# ---------------------------------------------------------------------------
# Yapılandırma
# ---------------------------------------------------------------------------

IBB_STATION_URL = os.getenv(
    "IBB_STATION_URL",
    "https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIStations"
)
IBB_MEASURE_URL = os.getenv(
    "IBB_MEASURE_URL",
    "https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId"
)
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC       = "air_quality_normalized"
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "300"))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [IBB] %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. İstasyon metadata'sı
# ---------------------------------------------------------------------------

def fetch_ibb_station_metadata() -> dict[str, dict]:
    """
    IBB istasyon listesini çeker ve parse eder.

    IBB gerçek yanıtı:
    [
      {
        "Id": "6b7a9840-1e13-4045-a79d-0f881c4852ad",
        "Name": "Maslak",
        "Adress": "İstanbul / Sarıyer - Turkey",
        "Location": "POINT (29.024512004171349 41.100072371412381)"
      },
      ...
    ]

    Location alanı WKT formatında: "POINT (lon lat)"
    """
    log.info("İstasyon listesi çekiliyor: %s", IBB_STATION_URL)
    resp = requests.get(IBB_STATION_URL, timeout=15)
    resp.raise_for_status()

    stations: dict[str, dict] = {}
    for row in resp.json():
        sid = str(row.get("Id", ""))
        if not sid:
            continue

        # Adres'ten ilçeyi çıkar: "İstanbul / Sarıyer - Turkey" → "Sarıyer"
        district = _parse_district(row.get("Adress", ""))

        # WKT parse: "POINT (lon lat)"
        lon, lat = _parse_wkt_point(row.get("Location", ""))

        stations[sid] = {
            "name":     row.get("Name", ""),
            "district": district,
            "lat":      lat,
            "lon":      lon,
        }

    log.info("%d istasyon yüklendi", len(stations))
    return stations


# ---------------------------------------------------------------------------
# 2. Ölçüm verisi
# ---------------------------------------------------------------------------

def fetch_ibb_measurements(station_id: str) -> list[dict]:
    today     = datetime.now().strftime("%d.%m.%Y")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
    params = {
        "StationId": station_id,
        "StartDate": f"{yesterday} 00:00:00",
        "EndDate":   f"{today} 23:59:59",
    }
    resp = requests.get(IBB_MEASURE_URL, params=params, timeout=15)
    resp.raise_for_status()

    if not resp.text.strip():
        return []
    try:
        data = resp.json()
    except Exception:
        log.warning("JSON parse edilemedi [%s]: %s", station_id, resp.text[:100])
        return []

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


# ---------------------------------------------------------------------------
# 3. Normalize
# ---------------------------------------------------------------------------

def normalize_ibb_record(
    raw_record: dict,
    station_meta: dict,
    station_id: str,
) -> Optional[AirQualityRecord]:
    try:
        raw_ts = raw_record.get("ReadTime", "")
        if not raw_ts:
            return None

        if raw_record.get("Concentration") is None:
            return None

        dt = datetime.fromisoformat(raw_ts).replace(tzinfo=timezone.utc)
        iso_ts = dt.isoformat()

        concentration = raw_record.get("Concentration", {}) or {}
        aqi_block     = raw_record.get("AQI", {}) or {}

        record = AirQualityRecord(
            station_id   = station_id,
            station_name = station_meta.get("name", ""),
            district     = station_meta.get("district", ""),
            source       = "ibb",
            timestamp    = iso_ts,
            latitude     = station_meta.get("lat"),
            longitude    = station_meta.get("lon"),
            pm10 = _to_float(concentration.get("PM10")),
            pm25 = _to_float(concentration.get("PM2.5")),
            no2  = _to_float(concentration.get("NO2")),
            so2  = _to_float(concentration.get("SO2")),
            co   = _to_float(concentration.get("CO")),
            o3   = _to_float(concentration.get("O3")),
            aqi  = _to_int(aqi_block.get("AQIIndex")),
        )

        errors = record.validate()
        if errors:
            log.warning("Geçersiz kayıt %s: %s", station_id, errors)
            return None
        return record

    except Exception as exc:
        log.error("normalize_ibb_record hatası [%s]: %s", station_id, exc)
        return None

# ---------------------------------------------------------------------------
# 4. Kafka'ya publish
# ---------------------------------------------------------------------------

def publish_to_kafka(producer: KafkaProducer, records: list[AirQualityRecord]) -> int:
    sent = 0
    for record in records:
        try:
            producer.send(
                KAFKA_TOPIC,
                key   = record.station_id.encode("utf-8"),
                value = record.to_json().encode("utf-8"),
            )
            sent += 1
        except Exception as exc:
            log.error("Kafka publish hatası [%s]: %s", record.station_id, exc)
    producer.flush()
    log.info("%d / %d kayıt Kafka'ya gönderildi", sent, len(records))
    return sent


# ---------------------------------------------------------------------------
# 5. Ana döngü
# ---------------------------------------------------------------------------

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    log.info("Kafka producer başlatıldı → %s", KAFKA_BOOTSTRAP)

    stations = fetch_ibb_station_metadata()
    last_meta_refresh = time.time()

    while True:
        if time.time() - last_meta_refresh > 3600:
            stations = fetch_ibb_station_metadata()
            last_meta_refresh = time.time()

        all_records: list[AirQualityRecord] = []
        for sid, meta in stations.items():
            try:
                raw_list = fetch_ibb_measurements(sid)
                for raw in raw_list:
                    rec = normalize_ibb_record(raw, meta, sid)
                    if rec:
                        all_records.append(rec)
            except Exception as exc:
                log.warning("İstasyon %s (%s) atlandı: %s", sid, meta.get("name",""), exc)

        if all_records:
            publish_to_kafka(producer, all_records)
        else:
            log.warning("Bu turda publish edilecek kayıt yok")

        log.info("%d saniye bekleniyor...", POLL_INTERVAL_SEC)
        time.sleep(POLL_INTERVAL_SEC)


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

def _parse_district(address: str) -> str:
    """
    "İstanbul / Sarıyer - Turkey"  → "Sarıyer"
    "Bağcılar İstanbul"            → "Bağcılar"
    """
    if "/" in address:
        part = address.split("/")[-1]          # " Sarıyer - Turkey"
        return part.split("-")[0].strip()
    return address.split()[0] if address else ""


def _parse_wkt_point(wkt: str) -> tuple[Optional[float], Optional[float]]:
    """
    "POINT (29.024512 41.100072)" → (lon=29.02, lat=41.10)
    Dikkat: WKT sırası lon lat!
    """
    match = re.search(r"POINT\s*\(\s*([\d.]+)\s+([\d.]+)\s*\)", wkt)
    if match:
        lon = float(match.group(1))
        lat = float(match.group(2))
        return lon, lat
    return None, None


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