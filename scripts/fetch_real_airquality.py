"""Fetch real IBB + OpenAQ air quality data for Istanbul and save as CSV.

Replaces data/raw/airquality_historical.csv with real measured data.
Also regenerates data/raw/weather_historical.csv for the same date range.

Usage:
    # 2024 only (matches existing synthetic weather range — fastest)
    python scripts/fetch_real_airquality.py --start-date 2024-01-01 --end-date 2024-12-31

    # Multi-year from OpenAQ only (IBB may not have older data)
    python scripts/fetch_real_airquality.py --source openaq --start-date 2022-01-01 --end-date 2024-12-31

    # Both sources, last 90 days (quick test)
    python scripts/fetch_real_airquality.py --start-date 2025-02-01 --end-date 2025-05-01
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.batch.data_merger import (
    IBBDataFetcher,
    OpenAQDataFetcher,
    apply_data_quality_filters,
    merge_and_deduplicate,
    normalize_ibb_schema,
    normalize_openaq_schema,
)
from src.common.logger import get_logger

logger = get_logger(__name__)

ROOT    = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"


def _parse_args() -> argparse.Namespace:
    today    = datetime.now(tz=timezone.utc).date()
    year_ago = today.replace(year=today.year - 1)
    parser   = argparse.ArgumentParser(
        description="Fetch real Istanbul AQ data and replace synthetic CSV files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--start-date", default=str(year_ago),
                        help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end-date",   default=str(today),
                        help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument("--source", choices=["ibb", "openaq", "both"], default="both",
                        help="Which API(s) to fetch from.")
    return parser.parse_args()


def fetch_airquality(start: datetime, end: datetime, source: str) -> pd.DataFrame:
    """Fetch, normalise, merge and quality-filter IBB and/or OpenAQ data."""
    ibb_df    = pd.DataFrame()
    openaq_df = pd.DataFrame()

    if source in ("ibb", "both"):
        logger.info("Fetching IBB data %s -> %s ...", start.date(), end.date())
        raw = IBBDataFetcher().fetch_all_measurements(start, end)
        if not raw.empty:
            ibb_df = normalize_ibb_schema(raw)
            logger.info("IBB normalised: %d rows", len(ibb_df))
        else:
            logger.warning("IBB returned no data.")

    if source in ("openaq", "both"):
        api_key = os.getenv("OPENAQ_API_KEY", "")
        if not api_key:
            logger.warning("OPENAQ_API_KEY not set — OpenAQ requests may be rate-limited.")
        logger.info("Fetching OpenAQ data %s -> %s ...", start.date(), end.date())
        raw = OpenAQDataFetcher(api_key=api_key).fetch_all_measurements(start, end)
        if not raw.empty:
            openaq_df = normalize_openaq_schema(raw)
            logger.info("OpenAQ normalised: %d rows", len(openaq_df))
        else:
            logger.warning("OpenAQ returned no data.")

    merged = merge_and_deduplicate(ibb_df, openaq_df)
    clean  = apply_data_quality_filters(merged)
    logger.info("Final dataset: %d rows", len(clean))
    return clean


def generate_weather(start: datetime, end: datetime) -> pd.DataFrame:
    """Regenerate synthetic Istanbul weather for the given date range.

    Uses the same statistical model as generate_training_data.py but
    accepts an arbitrary date range so it always matches the AQ data window.
    """
    rows = []
    rng  = np.random.default_rng(seed=999)
    ts   = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    while ts <= end_naive:
        month = ts.month
        hour  = ts.hour
        t_mean  = 5 + 15 * math.sin(math.pi * (month - 3) / 6)
        t_diur  = 3 * math.sin(math.pi * (hour - 6) / 12)
        temp    = round(t_mean + t_diur + rng.normal(0, 1.5), 1)
        rows.append({
            "timestamp":      ts.isoformat(),
            "temperature":    temp,
            "humidity":       round(min(99, max(20, 65 - 0.5 * temp + rng.normal(0, 8))), 1),
            "wind_speed":     round(max(0.0, 3.5 + rng.normal(0, 1.5)), 1),
            "wind_direction": round(float(rng.uniform(0, 360)), 1),
            "pressure":       round(1012 + rng.normal(0, 5), 1),
            "precipitation":  round(max(0.0, float(rng.exponential(0.2))
                                        if rng.random() < 0.15 else 0.0), 2),
            "visibility":     round(max(0.5, 15 + rng.normal(0, 2)), 1),
            "cloud_cover":    round(min(100, max(0, 40 + rng.normal(0, 25))), 1),
        })
        ts += timedelta(hours=1)

    return pd.DataFrame(rows)


def main() -> None:
    args  = _parse_args()
    start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end   = datetime.strptime(args.end_date,   "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"\nFetching air quality: {args.start_date} -> {args.end_date}  source={args.source}")
    aq_df = fetch_airquality(start, end, args.source)

    if aq_df.empty:
        print("\nERROR: No data fetched from any source.")
        print("  - Check OPENAQ_API_KEY in your .env file")
        print("  - Check internet connectivity")
        print("  - Try --source openaq to skip IBB")
        sys.exit(1)

    aq_path = RAW_DIR / "airquality_historical.csv"
    aq_df.to_csv(aq_path, index=False)
    print(f"Saved {len(aq_df):,} air quality rows -> {aq_path}")

    print(f"\nRegenerating weather: {args.start_date} -> {args.end_date}")
    wx_df = generate_weather(start, end)
    wx_path = RAW_DIR / "weather_historical.csv"
    wx_df.to_csv(wx_path, index=False)
    print(f"Saved {len(wx_df):,} weather rows -> {wx_path}")

    n_days    = (end.date() - start.date()).days + 1
    n_sources = aq_df["source"].value_counts().to_dict()
    print(f"\nSummary:")
    print(f"  Date range : {args.start_date} -> {args.end_date} ({n_days} days)")
    print(f"  Stations   : {aq_df['station_id'].nunique()}")
    print(f"  Sources    : {n_sources}")
    print(f"  AQI range  : {aq_df['aqi'].min():.0f} - {aq_df['aqi'].max():.0f}")
    print(f"\nNext step: retrain ML models")
    print("  $env:SPARK_MASTER = 'local[*]'")
    print("  python -m src.ml.train_baseline_models")
    print("  python -m src.ml.train_gbt_model")


if __name__ == "__main__":
    main()
