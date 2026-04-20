
from __future__ import annotations

import json
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pandas as pd

from src.batch.data_merger import IBBDataFetcher, normalize_ibb_schema
from src.common.config import IBB_REQUEST_TIMEOUT
from src.common.logger import get_logger

logger = get_logger(__name__)

IBB_TOPIC = "airquality.ibb.raw"
POLL_INTERVAL_SECONDS = 300


def fetch_ibb_station_metadata() -> pd.DataFrame:
    logger.info("IBB istasyon listesi çekiliyor...")
    fetcher = IBBDataFetcher(timeout=IBB_REQUEST_TIMEOUT)
    df = fetcher.fetch_stations()
    if df.empty:
        logger.warning("IBB API boş döndü")
    else:
        logger.info("%d istasyon alındı", len(df))
    return df


def fetch_ibb_measurements() -> pd.DataFrame:
    raw_df = fetch_ibb_station_metadata()
    if raw_df.empty:
        return pd.DataFrame()
    return normalize_ibb_record(raw_df)


def normalize_ibb_record(raw_df: pd.DataFrame) -> pd.DataFrame:
    normalized = normalize_ibb_schema(raw_df)
    if "timestamp" in normalized.columns:
        now = pd.Timestamp(datetime.now(timezone.utc))
        normalized["timestamp"] = normalized["timestamp"].fillna(now)
    logger.info("Normalize sonrası %d kayıt hazır", len(normalized))
    return normalized


def _record_to_json(row: dict) -> str:
    cleaned = {}
    for key, value in row.items():
        if not isinstance(value, (list, dict)) and pd.isna(value):
            cleaned[key] = None
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.isoformat()
        elif isinstance(value, float):
            cleaned[key] = round(value, 4)
        else:
            cleaned[key] = value
    return json.dumps(cleaned, ensure_ascii=False)


def publish_to_kafka(records: pd.DataFrame, producer=None, dry_run: bool = False) -> int:
    if records.empty:
        logger.warning("Gönderilecek kayıt yok")
        return 0

    sent = 0
    for _, row in records.iterrows():
        row_dict = row.to_dict()
        json_str = _record_to_json(row_dict)
        key = str(row_dict.get("station_id", "unknown"))

        if dry_run or producer is None:
            print(f"  [DRY-RUN] topic={IBB_TOPIC}  key={key}")
            print(f"            {json_str[:120]}...")
        else:
            producer.send(
                IBB_TOPIC,
                key=key.encode("utf-8"),
                value=json_str.encode("utf-8"),
            )
        sent += 1

    if not dry_run and producer is not None:
        producer.flush()

    logger.info("%d mesaj gönderildi → topic: %s", sent, IBB_TOPIC)
    return sent


def main():
    parser = argparse.ArgumentParser(description="IBB Kafka Producer")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--bootstrap", default="localhost:9092")
    args = parser.parse_args()

    producer = None
    if not args.dry_run:
        try:
            from kafka import KafkaProducer
            producer = KafkaProducer(bootstrap_servers=args.bootstrap, retries=3)
            logger.info("Kafka producer bağlandı → %s", args.bootstrap)
        except Exception as exc:
            logger.warning("Kafka bağlanamadı (%s), dry-run moduna geçiliyor", exc)
            args.dry_run = True

    logger.info("IBB Producer başladı | mod=%s", "dry-run" if args.dry_run else "kafka")

    while True:
        try:
            records = fetch_ibb_measurements()
            publish_to_kafka(records, producer=producer, dry_run=args.dry_run)
        except KeyboardInterrupt:
            logger.info("Producer durduruldu")
            break
        except Exception as exc:
            logger.error("Hata: %s", exc)

        if args.once:
            break

        time.sleep(POLL_INTERVAL_SECONDS)

    if producer:
        producer.close()


if __name__ == "__main__":
    main()
