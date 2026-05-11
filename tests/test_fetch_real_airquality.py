"""Tests for scripts/fetch_real_airquality.py.

All API calls are mocked so the tests run offline.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.fetch_real_airquality import fetch_airquality, generate_weather


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_aq_row(source="ibb", station="IST-001", ts="2024-06-01T10:00:00"):
    return {
        "station_id": station, "station_name": "Test Station",
        "district": "Kadıköy", "timestamp": ts,
        "pm10": 50.0, "pm25": 25.0, "no2": 40.0, "so2": 10.0,
        "co": 1.2, "o3": 30.0, "aqi": 80.0,
        "latitude": 41.008, "longitude": 29.022, "source": source,
    }


# ---------------------------------------------------------------------------
# fetch_airquality tests
# ---------------------------------------------------------------------------

def test_fetch_airquality_both_sources_returns_merged():
    ibb_df    = pd.DataFrame([_make_aq_row(source="ibb",    station="IST-001", ts="2024-06-01T10:00:00")])
    openaq_df = pd.DataFrame([_make_aq_row(source="openaq", station="OAQ-099", ts="2024-06-01T11:00:00")])

    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 6, 1, 23, 59, 59, tzinfo=timezone.utc)

    with (
        patch("scripts.fetch_real_airquality.IBBDataFetcher") as MockIBB,
        patch("scripts.fetch_real_airquality.OpenAQDataFetcher") as MockOAQ,
        patch("scripts.fetch_real_airquality.normalize_ibb_schema",    return_value=ibb_df),
        patch("scripts.fetch_real_airquality.normalize_openaq_schema", return_value=openaq_df),
    ):
        MockIBB.return_value.fetch_all_measurements.return_value = ibb_df
        MockOAQ.return_value.fetch_all_measurements.return_value = openaq_df

        result = fetch_airquality(start, end, "both")

    assert len(result) == 2
    assert set(result["source"].unique()) <= {"ibb", "openaq"}


def test_fetch_airquality_ibb_only_skips_openaq():
    ibb_df = pd.DataFrame([_make_aq_row(source="ibb")])
    start  = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end    = datetime(2024, 6, 1, 23, 59, 59, tzinfo=timezone.utc)

    with (
        patch("scripts.fetch_real_airquality.IBBDataFetcher") as MockIBB,
        patch("scripts.fetch_real_airquality.OpenAQDataFetcher") as MockOAQ,
        patch("scripts.fetch_real_airquality.normalize_ibb_schema", return_value=ibb_df),
    ):
        MockIBB.return_value.fetch_all_measurements.return_value = ibb_df

        result = fetch_airquality(start, end, "ibb")

    MockOAQ.assert_not_called()
    assert len(result) == 1
    assert result.iloc[0]["source"] == "ibb"


def test_fetch_airquality_empty_both_returns_empty():
    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 6, 1, 23, 59, 59, tzinfo=timezone.utc)

    with (
        patch("scripts.fetch_real_airquality.IBBDataFetcher") as MockIBB,
        patch("scripts.fetch_real_airquality.OpenAQDataFetcher") as MockOAQ,
    ):
        MockIBB.return_value.fetch_all_measurements.return_value = pd.DataFrame()
        MockOAQ.return_value.fetch_all_measurements.return_value = pd.DataFrame()

        result = fetch_airquality(start, end, "both")

    assert result.empty


# ---------------------------------------------------------------------------
# generate_weather tests
# ---------------------------------------------------------------------------

def test_generate_weather_row_count_3_days():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 1, 3, 23, 59, 59, tzinfo=timezone.utc)
    df    = generate_weather(start, end)
    assert len(df) == 72  # 3 days × 24 hours


def test_generate_weather_row_count_single_day():
    start = datetime(2024, 6, 15, tzinfo=timezone.utc)
    end   = datetime(2024, 6, 15, 23, 59, 59, tzinfo=timezone.utc)
    df    = generate_weather(start, end)
    assert len(df) == 24


def test_generate_weather_required_columns():
    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 6, 1, 23, tzinfo=timezone.utc)
    df    = generate_weather(start, end)
    expected = {
        "timestamp", "temperature", "humidity", "wind_speed",
        "wind_direction", "pressure", "precipitation", "visibility", "cloud_cover",
    }
    assert expected.issubset(set(df.columns))


def test_generate_weather_value_ranges_full_year():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    df    = generate_weather(start, end)
    assert df["humidity"].between(0, 100).all(),       "humidity out of range"
    assert (df["wind_speed"] >= 0).all(),               "wind_speed negative"
    assert df["wind_direction"].between(0, 360).all(), "wind_direction out of range"
    assert (df["precipitation"] >= 0).all(),            "precipitation negative"
    assert df["visibility"].between(0, 100).all(),     "visibility out of range"
    assert df["cloud_cover"].between(0, 100).all(),    "cloud_cover out of range"


def test_generate_weather_timestamp_format():
    start = datetime(2024, 3, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 3, 1, 1, tzinfo=timezone.utc)
    df    = generate_weather(start, end)
    # Should be plain ISO without timezone suffix, e.g. "2024-03-01T00:00:00"
    ts = df.iloc[0]["timestamp"]
    assert "+" not in ts and "Z" not in ts, f"Unexpected timezone in timestamp: {ts}"
    assert len(ts) == 19, f"Unexpected timestamp length: {ts}"  # YYYY-MM-DDTHH:MM:SS


def test_generate_weather_deterministic():
    start = datetime(2024, 6, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 6, 1, 5, tzinfo=timezone.utc)
    df1 = generate_weather(start, end)
    df2 = generate_weather(start, end)
    pd.testing.assert_frame_equal(df1, df2)
