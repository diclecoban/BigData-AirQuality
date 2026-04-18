"""
Generate Istanbul air quality training data.

Two modes (controlled by --mode):

  synthetic (default)
    Generates realistic synthetic data locally — no API access needed.
    Produces:
      data/raw/airquality_historical.csv   (~262 k rows, 30 stations × 1 year hourly)
      data/raw/weather_historical.csv      (8 760 rows, Istanbul hourly weather)

  real
    Fetches real data from IBB + OpenAQ APIs via merge_historical_data.py.
    Produces:
      data/processed/merged_historical.parquet  (feature-enriched, partitioned by date)
    Requires:
      OPENAQ_API_KEY env var (get free key at https://explore.openaq.org/register)
      Internet access to IBB and OpenAQ endpoints

Run:
  pip install pandas numpy requests pyspark mlflow

  # Synthetic (default — always works offline)
  python scripts/generate_training_data.py

  # Real data — last 7 days
  python scripts/generate_training_data.py --mode real

  # Real data — custom date range
  python scripts/generate_training_data.py --mode real \\
      --start-date 2024-01-01 --end-date 2024-12-31
"""

import argparse
import math
import random
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

# ---------------------------------------------------------------------------
# Istanbul districts with approximate coordinates
# ---------------------------------------------------------------------------
DISTRICTS = [
    ("Kadıköy",         41.0082, 29.0230),
    ("Beşiktaş",        41.0422, 29.0076),
    ("Şişli",           41.0609, 28.9870),
    ("Fatih",           41.0184, 28.9395),
    ("Beyoğlu",         41.0338, 28.9746),
    ("Üsküdar",         41.0250, 29.0155),
    ("Maltepe",         40.9353, 29.1328),
    ("Pendik",          40.8745, 29.2317),
    ("Ümraniye",        41.0165, 29.1144),
    ("Kartal",          40.9126, 29.1907),
    ("Bağcılar",        41.0364, 28.8558),
    ("Bahçelievler",    41.0003, 28.8623),
    ("Başakşehir",      41.0924, 28.8022),
    ("Esenyurt",        41.0297, 28.6762),
    ("Büyükçekmece",    41.0189, 28.5886),
    ("Bakırköy",        40.9819, 28.8660),
    ("Zeytinburnu",     41.0016, 28.9012),
    ("Eyüpsultan",      41.0781, 28.9340),
    ("Güngören",        41.0178, 28.8726),
    ("Küçükçekmece",    41.0015, 28.7747),
    ("Sarıyer",         41.1676, 29.0534),
    ("Beykoz",          41.1320, 29.0966),
    ("Çekmeköy",        41.0419, 29.1786),
    ("Tuzla",           40.8130, 29.2990),
    ("Sultanbeyli",     40.9641, 29.2653),
    ("Sancaktepe",      41.0016, 29.2249),
    ("Ataşehir",        40.9823, 29.1241),
    ("Avcılar",         40.9793, 28.7216),
    ("Bayrampaşa",      41.0502, 28.9118),
    ("Gaziosmanpaşa",   41.0700, 28.9139),
]

# ---------------------------------------------------------------------------
# Pollution baseline per district (industrial vs residential)
# ---------------------------------------------------------------------------
INDUSTRIAL_DISTRICTS = {
    "Esenyurt", "Büyükçekmece", "Tuzla", "Pendik",
    "Başakşehir", "Bağcılar", "Avcılar",
}

START_DATE = datetime(2024, 1, 1, 0, 0, 0)
END_DATE   = datetime(2024, 12, 31, 23, 0, 0)


def _hourly_timestamps():
    ts = START_DATE
    while ts <= END_DATE:
        yield ts
        ts += timedelta(hours=1)


def _pm25_base(district: str) -> float:
    return 35.0 if district in INDUSTRIAL_DISTRICTS else 20.0


def _seasonal_factor(month: int) -> float:
    """Winter months have worse air quality in Istanbul."""
    # Dec-Feb high pollution, Jun-Aug low
    return 1.0 + 0.4 * math.cos(math.pi * (month - 1) / 6)


def _rush_hour_factor(hour: int) -> float:
    """Morning and evening rush hour peaks."""
    morning = math.exp(-0.5 * ((hour - 8) / 1.5) ** 2)
    evening = math.exp(-0.5 * ((hour - 18) / 1.5) ** 2)
    return 1.0 + 0.5 * max(morning, evening)


def compute_aqi(pm25: float, pm10: float, no2: float, o3: float) -> float:
    """Simplified AQI calculation (US EPA breakpoints, linearised)."""
    def _breakpoint(val, breakpoints):
        for i, (lo, hi, aqilo, aqihi) in enumerate(breakpoints):
            if lo <= val <= hi:
                return aqilo + (val - lo) / (hi - lo) * (aqihi - aqilo)
        return 500.0

    pm25_bp = [
        (0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300),
    ]
    pm10_bp = [
        (0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
        (255, 354, 151, 200), (355, 424, 201, 300),
    ]
    no2_bp = [
        (0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
        (361, 649, 151, 200), (650, 1249, 201, 300),
    ]
    return max(
        _breakpoint(pm25, pm25_bp),
        _breakpoint(pm10, pm10_bp),
        _breakpoint(no2, no2_bp),
    )


# ---------------------------------------------------------------------------
# Generate air quality data
# ---------------------------------------------------------------------------

def generate_air_quality() -> pd.DataFrame:
    timestamps = list(_hourly_timestamps())
    rows = []

    for station_idx, (district, lat, lon) in enumerate(DISTRICTS):
        station_id   = f"IST-{station_idx + 1:03d}"
        station_name = f"{district} Ölçüm İstasyonu"
        base_pm25    = _pm25_base(district)

        # Noise correlation seed per station
        rng = np.random.default_rng(seed=station_idx * 7 + 13)

        for ts in timestamps:
            sf  = _seasonal_factor(ts.month)
            rhf = _rush_hour_factor(ts.hour)
            noise = rng.normal(0, 1)

            pm25 = max(1.0, base_pm25 * sf * rhf + noise * 4)
            pm10 = max(1.0, pm25 * rng.uniform(1.6, 2.2) + rng.normal(0, 3))
            no2  = max(1.0, 30 * sf * rhf + rng.normal(0, 6))
            so2  = max(0.1, 8  * sf       + rng.normal(0, 2))
            co   = max(0.1, 1.2 * sf * rhf + rng.normal(0, 0.2))
            o3   = max(1.0, 40 * (1 + 0.3 * math.sin(math.pi * (ts.hour - 14) / 12))
                        + rng.normal(0, 5))
            aqi  = compute_aqi(pm25, pm10, no2, o3)

            rows.append({
                "station_id":   station_id,
                "station_name": station_name,
                "district":     district,
                "timestamp":    ts.isoformat(),
                "pm10":         round(pm10, 2),
                "pm25":         round(pm25, 2),
                "no2":          round(no2,  2),
                "so2":          round(so2,  2),
                "co":           round(co,   3),
                "o3":           round(o3,   2),
                "aqi":          round(aqi,  1),
                "latitude":     round(lat + rng.normal(0, 0.002), 5),
                "longitude":    round(lon + rng.normal(0, 0.002), 5),
                "source":       "synthetic",
            })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Generate weather data (Istanbul-wide, one record per hour)
# ---------------------------------------------------------------------------

def generate_weather() -> pd.DataFrame:
    timestamps = list(_hourly_timestamps())
    rows = []
    rng  = np.random.default_rng(seed=999)

    for ts in timestamps:
        month = ts.month
        hour  = ts.hour

        # Temperature: seasonal + diurnal pattern (Istanbul averages)
        t_mean  = 5 + 15 * math.sin(math.pi * (month - 3) / 6)
        t_diur  = 3 * math.sin(math.pi * (hour - 6) / 12)
        temp    = round(t_mean + t_diur + rng.normal(0, 1.5), 1)

        humidity = round(min(99, max(20, 65 - 0.5 * temp + rng.normal(0, 8))), 1)
        wind_spd = round(max(0, 3.5 + rng.normal(0, 1.5)), 1)
        wind_dir = round(rng.uniform(0, 360), 1)
        pressure = round(1012 + rng.normal(0, 5), 1)
        precip   = round(max(0, rng.exponential(0.2) if rng.random() < 0.15 else 0), 2)
        vis      = round(max(0.5, 15 - 5 * (precip > 0) + rng.normal(0, 2)), 1)
        clouds   = round(min(100, max(0, 40 + rng.normal(0, 25))), 1)

        rows.append({
            "timestamp":      ts.isoformat(),
            "temperature":    temp,
            "humidity":       humidity,
            "wind_speed":     wind_spd,
            "wind_direction": wind_dir,
            "pressure":       pressure,
            "precipitation":  precip,
            "visibility":     vis,
            "cloud_cover":    clouds,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    out_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating air quality data  (this may take ~30 s)...")
    aq_df = generate_air_quality()
    aq_path = out_dir / "airquality_historical.csv"
    aq_df.to_csv(aq_path, index=False)
    print(f"  Saved {len(aq_df):,} rows → {aq_path}")

    print("Generating weather data...")
    wx_df = generate_weather()
    wx_path = out_dir / "weather_historical.csv"
    wx_df.to_csv(wx_path, index=False)
    print(f"  Saved {len(wx_df):,} rows → {wx_path}")

    print("\nDone. Next step:")
    print("  python scripts/run_pipeline.py")


def _run_real_mode(start_date: str, end_date: str, source: str) -> None:
    """Delegate to merge_historical_data.py for real API data.

    Args:
        start_date: YYYY-MM-DD string.
        end_date:   YYYY-MM-DD string.
        source:     'ibb', 'openaq', or 'both'.
    """
    script = Path(__file__).resolve().parent / "merge_historical_data.py"
    cmd = [
        sys.executable, str(script),
        "--start-date", start_date,
        "--end-date",   end_date,
        "--source",     source,
    ]
    print(f"Running: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("\nReal-data fetch failed.  Falling back to synthetic mode...")
        main()


def _parse_args() -> argparse.Namespace:
    from datetime import timezone
    today    = datetime.now(tz=timezone.utc).date()
    week_ago = today - timedelta(days=7)

    parser = argparse.ArgumentParser(
        description="Generate Istanbul AQI training data (synthetic or real).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["synthetic", "real"],
        default="synthetic",
        help=(
            "synthetic: generate offline data (no API needed).  "
            "real: fetch from IBB + OpenAQ APIs."
        ),
    )
    parser.add_argument(
        "--start-date",
        default=str(week_ago),
        help="[real mode] Fetch start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=str(today),
        help="[real mode] Fetch end date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--source",
        choices=["ibb", "openaq", "both"],
        default="both",
        help="[real mode] Which API(s) to use.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.mode == "real":
        print("Mode: real — fetching from IBB + OpenAQ APIs")
        _run_real_mode(args.start_date, args.end_date, args.source)
    else:
        print("Mode: synthetic — generating offline training data")
        main()
