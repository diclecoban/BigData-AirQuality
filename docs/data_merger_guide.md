# Data Merger Guide: IBB + OpenAQ

This guide explains how the IBB and OpenAQ data merger works, how to configure it, and how to troubleshoot common issues.

## Overview

The merger pipeline fetches real air quality data from two official sources, normalises both to a unified schema, deduplicates overlapping records, applies data quality filters, and outputs a feature-enriched Parquet dataset ready for model training.

```
IBB API ──────────────┐
                       ├──► normalize ──► merge_and_deduplicate ──► apply_data_quality_filters
OpenAQ API ───────────┘                        (IBB priority)               │
                                                                             ▼
                                                               PySpark feature engineering
                                                                             │
                                                                             ▼
                                                          data/processed/merged_historical.parquet
```

## Data Sources

### IBB (Istanbul Metropolitan Municipality)

| Property | Value |
|---|---|
| Provider | Istanbul Metropolitan Municipality |
| Auth required | No |
| Stations | ~37 official monitoring stations |
| Coverage | Istanbul province |
| Frequency | Hourly |
| Stations URL | `https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIStations` |
| Measurements URL | `https://api.ibb.gov.tr/havakalitesi/OpenDataPortalHandler/GetAQIByStationId` |

IBB is the primary source. All IBB station IDs are prefixed with `IST-*` in the unified schema.

### OpenAQ v3

| Property | Value |
|---|---|
| Provider | OpenAQ (open-source, non-profit) |
| Auth required | Yes — free API key |
| Coverage | Global, including Istanbul |
| Frequency | Varies by sensor (usually hourly) |
| Base URL | `https://api.openaq.org/v3` |

OpenAQ fills gaps where IBB has no station. All OpenAQ station IDs are prefixed with `OAQ-*` in the unified schema.

## Getting an OpenAQ API Key

1. Go to [https://explore.openaq.org/register](https://explore.openaq.org/register)
2. Create a free account
3. Copy your API key from the dashboard
4. Set it in your environment:

```bash
# Linux / macOS
export OPENAQ_API_KEY="your_key_here"

# Windows (PowerShell)
$env:OPENAQ_API_KEY = "your_key_here"

# Or add to a .env file (never commit this file)
echo OPENAQ_API_KEY=your_key_here >> .env
```

The key is read in `src/common/config.py`:
```python
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
```

Without a key, OpenAQ allows a small number of unauthenticated requests before rate-limiting.

## Unified Schema

After normalisation, both sources produce a DataFrame with exactly these columns (defined in `src/ingestion/schema.py`):

| Column | Type | Description |
|---|---|---|
| `station_id` | string | Unique station identifier (`IST-*` or `OAQ-*`) |
| `station_name` | string | Human-readable station name |
| `district` | string | Istanbul district name |
| `timestamp` | datetime | Measurement time (snapped to hour, UTC) |
| `pm10` | float | PM10 concentration (µg/m³) |
| `pm25` | float | PM2.5 concentration (µg/m³) |
| `no2` | float | NO₂ concentration (µg/m³) |
| `so2` | float | SO₂ concentration (µg/m³) |
| `co` | float | CO concentration (mg/m³) |
| `o3` | float | O₃ concentration (µg/m³) |
| `aqi` | float | Air Quality Index (0–500, US EPA) |
| `latitude` | float | Station latitude (WGS84) |
| `longitude` | float | Station longitude (WGS84) |
| `source` | string | `"ibb"` or `"openaq"` |

## Deduplication Strategy

**Key:** `(station_id, timestamp)` truncated to the hour.

When the same key appears in both sources:
- **IBB record wins** — IBB uses calibrated government instruments; OpenAQ data for the same location may come from low-cost sensors with higher noise.
- OpenAQ records for locations with no matching IBB station are kept as-is.

Implementation in `src/batch/data_merger.py → merge_and_deduplicate()`:

```python
# Sort: IBB first (priority 0), OpenAQ second (priority 1)
combined["_priority"] = combined["source"].map({"ibb": 0, "openaq": 1})
combined = combined.sort_values(["station_id", "timestamp", "_priority"])
deduped  = combined.drop_duplicates(subset=["station_id", "_ts_hour"], keep="first")
```

## Data Quality Filters

Configured via `src/common/config.py`:

| Parameter | Threshold | Action on violation |
|---|---|---|
| PM2.5 | 0 – 500 µg/m³ | → NaN |
| PM10  | 0 – 600 µg/m³ | → NaN |
| NO₂   | 0 – 2000 µg/m³ | → NaN |
| SO₂   | 0 – 2000 µg/m³ | → NaN |
| CO    | 0 – 50 mg/m³ | → NaN |
| O₃    | 0 – 1000 µg/m³ | → NaN |
| AQI   | 0 – 500 | → NaN |

Rows where **both** `pm25` and `aqi` are NaN are dropped entirely (no usable signal). Rows with unparseable timestamps are also dropped.

NaN values for individual pollutants are **not** dropped — the MLlib `Imputer` in the training pipeline fills them with column medians.

## Usage Examples

```bash
# 1. Last 7 days, both sources (default)
python scripts/merge_historical_data.py

# 2. Custom date range
python scripts/merge_historical_data.py \
    --start-date 2024-01-01 --end-date 2024-12-31

# 3. IBB only (no OpenAQ API key needed)
python scripts/merge_historical_data.py \
    --start-date 2024-04-11 --end-date 2024-04-18 \
    --source ibb

# 4. OpenAQ only
python scripts/merge_historical_data.py \
    --start-date 2024-04-11 --end-date 2024-04-18 \
    --source openaq

# 5. Custom output path, skip feature engineering
python scripts/merge_historical_data.py \
    --start-date 2018-01-01 --end-date 2024-12-31 \
    --output-path data/processed/full_history.parquet \
    --skip-features

# 6. Via generate_training_data.py (real mode)
python scripts/generate_training_data.py --mode real \
    --start-date 2024-01-01 --end-date 2024-12-31
```

## Output

The script writes a Parquet dataset partitioned by date:

```
data/processed/merged_historical.parquet/
  date=2024-04-11/
    part-00000-....parquet
  date=2024-04-12/
    ...
```

Expected row count (7-day test run):
- 7 days × 24 hours × ~37 stations ≈ **6,216 rows** (raw)
- After lag/rolling features and dropna: slightly fewer (boundary rows removed)

Read the output with PySpark:
```python
df = spark.read.parquet("data/processed/merged_historical.parquet")
df.show()
```

## Troubleshooting

### IBB API returns empty / times out

IBB endpoints are occasionally slow or unavailable. The fetcher logs a warning and continues with OpenAQ only.

```
WARNING | IBB stations fetch failed: ... — returning empty DataFrame
```

**Fix:** Run again later, or use `--source openaq` to skip IBB.

### OpenAQ rate-limiting (429 Too Many Requests)

Without an API key (or with a free-tier key), OpenAQ allows ~10 req/min.

**Fix:** Set `OPENAQ_API_KEY` (see above). The fetcher sleeps 0.5 s between location requests to stay within limits.

### No data fetched from any source

```
ERROR | No data fetched from any source.
```

Both APIs failed. Check internet connectivity and API key validity.

**Fallback:** Use synthetic data instead:
```bash
python scripts/generate_training_data.py  # synthetic mode (default)
```

### Parquet write fails (HADOOP_HOME not set on Windows)

Run the winutils setup once:
```bash
python scripts/setup_winutils.py
```
Then restart the terminal and retry.

### Feature column count mismatch after load

If you add new features to `feature_engineering.py`, the saved model expects the old feature vector. Retrain:
```bash
python scripts/run_pipeline.py
```
