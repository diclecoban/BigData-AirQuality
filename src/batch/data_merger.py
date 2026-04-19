"""IBB + OpenAQ data merger for Istanbul air quality.

Provides API clients, schema normalizers, deduplication, and quality filters.
All public functions work with pandas DataFrames; PySpark conversion happens
in scripts/merge_historical_data.py after this pipeline finishes.

Design notes:
- IBB has priority over OpenAQ when the same station/hour exists in both.
- API failures are isolated: if IBB is down, we continue with OpenAQ only.
- Normalised output columns match schema.py RAW_AIR_QUALITY_FIELDS exactly.
"""

from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests

from src.common.config import (
    IBB_MEASUREMENTS_URL,
    IBB_REQUEST_TIMEOUT,
    IBB_STATIONS_URL,
    ISTANBUL_LAT,
    ISTANBUL_LON,
    ISTANBUL_RADIUS,
    MAX_AQI,
    MAX_CO,
    MAX_NO2,
    MAX_O3,
    MAX_PM10,
    MAX_PM25,
    MAX_SO2,
    MIN_AQI,
    MIN_CO,
    MIN_NO2,
    MIN_O3,
    MIN_PM10,
    MIN_PM25,
    MIN_SO2,
    OPENAQ_API_KEY,
    OPENAQ_BASE_URL,
    OPENAQ_PAGE_LIMIT,
    OPENAQ_REQUEST_TIMEOUT,
)
from src.common.logger import get_logger
from src.ingestion.schema import RAW_AIR_QUALITY_FIELDS

logger = get_logger(__name__)

# Unified schema column order (from schema.py)
_UNIFIED_COLS = RAW_AIR_QUALITY_FIELDS  # 14 columns


def _is_dns_resolution_error(exc: Exception) -> bool:
    """Return True when the exception indicates a DNS resolution failure."""
    text = str(exc).lower()
    patterns = (
        "nameresolutionerror",
        "failed to resolve",
        "getaddrinfo failed",
        "could not resolve host",
        "temporary failure in name resolution",
    )
    return any(pattern in text for pattern in patterns)


# ---------------------------------------------------------------------------
# Helper: compute approximate AQI from PM2.5 (US EPA breakpoints).
# Used when IBB does not return an AQI value directly.
# ---------------------------------------------------------------------------

def _pm25_to_aqi(pm25: float) -> float:
    """Convert PM2.5 µg/m³ to US EPA AQI (simplified linear interpolation).

    Args:
        pm25: PM2.5 concentration in µg/m³.

    Returns:
        Estimated AQI value (0–500).
    """
    breakpoints = [
        (0.0, 12.0,   0,  50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 500.4, 301, 500),
    ]
    for lo, hi, aqilo, aqihi in breakpoints:
        if lo <= pm25 <= hi:
            return aqilo + (pm25 - lo) / (hi - lo) * (aqihi - aqilo)
    return 500.0


# ===========================================================================
# IBB Data Fetcher
# ===========================================================================

class IBBDataFetcher:
    """Client for the Istanbul Metropolitan Municipality air quality API.

    Endpoints:
      GetAQIStations      — returns current readings for all active stations.
      GetAQIByStationId   — returns historical readings for a single station.

    Both endpoints require no authentication.

    Example:
        fetcher = IBBDataFetcher()
        stations = fetcher.fetch_stations()
        measurements = fetcher.fetch_measurements("12", start, end)
    """

    def __init__(self, timeout: int = IBB_REQUEST_TIMEOUT) -> None:
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._dns_resolution_failed = False

    @staticmethod
    def _format_api_datetime(value: datetime) -> str:
        """Format datetimes using the IBB history endpoint contract."""
        return value.strftime("%d.%m.%Y %H:%M:%S")

    @staticmethod
    def _rows_from_payload(
        payload,
        *,
        endpoint: str,
        station_id: Optional[str] = None,
    ) -> list[dict]:
        """Return a list of row dicts from an IBB API payload."""
        context = endpoint if station_id is None else f"{endpoint} station={station_id}"

        if payload is None:
            return []

        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]

        if isinstance(payload, dict):
            for key in ("results", "data", "items", "value"):
                rows = payload.get(key)
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
            logger.warning(
                "IBB %s returned object payload with keys=%s",
                context,
                list(payload.keys())[:10],
            )
            return []

        if isinstance(payload, str):
            message = payload.strip().replace("\r", " ").replace("\n", " ")
            logger.warning("IBB %s returned message payload: %s", context, message[:240])
            return []

        logger.warning(
            "IBB %s returned unexpected payload type=%s",
            context,
            type(payload).__name__,
        )
        return []

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def fetch_stations(self) -> pd.DataFrame:
        """Fetch active IBB station metadata (includes latest measurement).

        Returns:
            DataFrame with columns matching IBB response; empty on failure.
        """
        if self._dns_resolution_failed:
            return pd.DataFrame()
        logger.info("IBB: fetching station list from %s", IBB_STATIONS_URL)
        try:
            resp = self._session.get(IBB_STATIONS_URL, timeout=self._timeout)
            resp.raise_for_status()
            rows = self._rows_from_payload(resp.json(), endpoint="stations")
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            logger.info("IBB: received %d stations", len(df))
            return df
        except requests.RequestException as exc:
            if _is_dns_resolution_error(exc):
                self._dns_resolution_failed = True
                logger.error("IBB DNS resolution failed; skipping remaining IBB requests: %s", exc)
                return pd.DataFrame()
            logger.warning("IBB stations fetch failed: %s - returning empty DataFrame", exc)
            return pd.DataFrame()

    # IBB API silently fails (500) for date ranges longer than this many days.
    _MAX_CHUNK_DAYS: int = 30

    @staticmethod
    def _date_chunks(
        start: datetime, end: datetime, chunk_days: int
    ):
        """Yield (chunk_start, chunk_end) pairs that together cover [start, end].

        Args:
            start:      Range start.
            end:        Range end (inclusive).
            chunk_days: Maximum days per chunk.

        Yields:
            (chunk_start, chunk_end) datetime tuples.
        """
        from datetime import timedelta as _td
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + _td(days=chunk_days - 1), end)
            yield cursor, chunk_end
            cursor = chunk_end + _td(seconds=1)

    def _fetch_chunk(
        self,
        station_id: str,
        chunk_start: datetime,
        chunk_end: datetime,
    ) -> pd.DataFrame:
        """Fetch one date-range chunk for a single station.

        Returns empty DataFrame on any failure without raising.
        """
        if self._dns_resolution_failed:
            return pd.DataFrame()
        params = {
            "stationId": station_id,
            "startDate": self._format_api_datetime(chunk_start),
            "endDate":   self._format_api_datetime(chunk_end),
        }
        try:
            resp = self._session.get(
                IBB_MEASUREMENTS_URL, params=params, timeout=self._timeout
            )
            resp.raise_for_status()
            rows = self._rows_from_payload(
                resp.json(), endpoint="measurements", station_id=station_id
            )
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            df["station_id_src"] = str(station_id)
            return df
        except requests.RequestException as exc:
            if _is_dns_resolution_error(exc):
                self._dns_resolution_failed = True
                logger.error(
                    "IBB DNS resolution failed during chunk fetch; skipping remaining IBB requests: %s",
                    exc,
                )
                return pd.DataFrame()
            logger.warning(
                "IBB chunk fetch failed (station=%s %s->%s): %s",
                station_id,
                chunk_start.date(),
                chunk_end.date(),
                exc,
            )
            return pd.DataFrame()
        except ValueError as exc:
            logger.warning(
                "IBB chunk response not valid JSON (station=%s): %s", station_id, exc
            )
            return pd.DataFrame()

    def fetch_measurements(
        self,
        station_id: str,
        start_date: datetime,
        end_date: datetime,
        chunk_days: int = _MAX_CHUNK_DAYS,
    ) -> pd.DataFrame:
        """Fetch hourly historical measurements for one IBB station.

        Automatically splits long date ranges into ``chunk_days``-day windows
        to avoid IBB API 500 errors on large intervals.

        Args:
            station_id: IBB station GUID from GetAQIStations.
            start_date: Inclusive start (timezone-aware or naive UTC).
            end_date:   Inclusive end.
            chunk_days: Max days per API request (default 30).

        Returns:
            Concatenated DataFrame of all chunks; empty on total failure.
        """
        chunks = list(self._date_chunks(start_date, end_date, chunk_days))
        logger.debug(
            "IBB: station %s - %d chunk(s) of <=%d days",
            station_id, len(chunks), chunk_days,
        )

        frames: list[pd.DataFrame] = []
        for cs, ce in chunks:
            if self._dns_resolution_failed:
                break
            df = self._fetch_chunk(station_id, cs, ce)
            if not df.empty:
                frames.append(df)
            time.sleep(0.1)   # small pause between chunk requests

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _parse_location(loc_value) -> tuple[Optional[float], Optional[float]]:
        """Extract (latitude, longitude) from IBB Location field.

        Handles multiple formats observed in the wild:
          - WKT:  "POINT (lon lat)"
          - JSON: {"type":"Point","coordinates":[lon,lat]}
          - JSON string: '{"latitude":41.04,"longitude":29.00}'
          - Comma-separated: "41.04,29.00"

        Returns:
            (latitude, longitude) tuple, or (None, None) on failure.
        """
        import json as _json
        if loc_value is None or (isinstance(loc_value, float) and math.isnan(loc_value)):
            return None, None
        s = str(loc_value).strip()
        # WKT POINT
        if s.upper().startswith("POINT"):
            inner = s.upper().replace("POINT", "").replace("(", "").replace(")", "").strip()
            parts = inner.split()
            if len(parts) >= 2:
                try:
                    return float(parts[1]), float(parts[0])   # lat, lon
                except ValueError:
                    pass
        # JSON object string
        if s.startswith("{"):
            try:
                obj = _json.loads(s)
                coords = obj.get("coordinates", [])
                if isinstance(coords, list) and len(coords) >= 2:
                    return float(coords[1]), float(coords[0])  # lat, lon
                lat = obj.get("latitude") or obj.get("Latitude")
                lon = obj.get("longitude") or obj.get("Longitude")
                if lat is not None and lon is not None:
                    return float(lat), float(lon)
            except (ValueError, _json.JSONDecodeError):
                pass
        # Comma-separated "lat,lon"
        if "," in s:
            parts = s.split(",")
            if len(parts) == 2:
                try:
                    return float(parts[0].strip()), float(parts[1].strip())
                except ValueError:
                    pass
        logger.debug("IBB: could not parse Location value: %r", s)
        return None, None

    def fetch_all_measurements(
        self,
        start_date: datetime,
        end_date: datetime,
        delay_s: float = 0.3,
    ) -> pd.DataFrame:
        """Fetch measurements for all active stations over a date range.

        Tries GetAQIByStationId for historical data per station.
        Falls back to the GetAQIStations snapshot if the history endpoint
        returns nothing (useful when the date range includes today).

        Args:
            start_date: Range start.
            end_date:   Range end.
            delay_s:    Seconds to sleep between station requests (rate limiting).

        Returns:
            Concatenated DataFrame of all station measurements.
        """
        stations = self.fetch_stations()
        if stations.empty:
            logger.warning("IBB: no stations returned - skipping measurement fetch")
            return pd.DataFrame()

        logger.debug("IBB stations columns: %s", list(stations.columns))

        # Detect station ID column — actual API returns 'Id'
        id_col = next(
            (c for c in ("Id", "id", "ID", "SensorId", "sensorId", "sensor_id", "StationId")
             if c in stations.columns),
            None,
        )
        if id_col is None:
            logger.error("IBB: cannot find station ID column in %s", list(stations.columns))
            return pd.DataFrame()

        # Detect name / address / location columns
        name_col    = next((c for c in ("Name", "SensorName", "name") if c in stations.columns), None)
        address_col = next((c for c in ("Adress", "Address", "District", "district") if c in stations.columns), None)
        location_col = next((c for c in ("Location", "location", "Geometry") if c in stations.columns), None)

        frames: list[pd.DataFrame] = []
        for _, row in stations.iterrows():
            if self._dns_resolution_failed:
                break
            sid = str(row[id_col])
            df  = self.fetch_measurements(sid, start_date, end_date)

            if df.empty:
                time.sleep(delay_s)
                continue

            # Attach station metadata from stations row
            if name_col and "station_name" not in df.columns:
                df["station_name"] = row.get(name_col)
            if address_col and "district" not in df.columns:
                df["district"] = row.get(address_col)

            # Parse coordinates from Location field
            if location_col and "latitude" not in df.columns:
                lat, lon = self._parse_location(row.get(location_col))
                df["latitude"]  = lat
                df["longitude"] = lon

            df["station_id_src"] = sid
            frames.append(df)
            time.sleep(delay_s)

        if not frames:
            # Fall back: use the GetAQIStations snapshot itself as a single-point-in-time dataset
            logger.info("IBB: GetAQIByStationId returned nothing - using GetAQIStations snapshot")
            return stations.copy()

        return pd.concat(frames, ignore_index=True)


# ===========================================================================
# OpenAQ Data Fetcher
# ===========================================================================

class OpenAQDataFetcher:
    """Client for the OpenAQ v3 REST API.

    Requires an API key for sustained use.  Set OPENAQ_API_KEY in your
    environment.  Without a key, the API allows a small number of requests
    before rate-limiting.

    Example:
        fetcher = OpenAQDataFetcher()
        locations = fetcher.fetch_istanbul_locations()
        measurements = fetcher.fetch_measurements(location_id=12345, ...)
    """

    def __init__(
        self,
        api_key: str = OPENAQ_API_KEY,
        timeout: int = OPENAQ_REQUEST_TIMEOUT,
    ) -> None:
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({
            "Accept":    "application/json",
            "X-API-Key": api_key,
        })
        self._dns_resolution_failed = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict) -> dict:
        """GET request with error handling.

        Returns:
            Parsed JSON dict, or {"results": []} on failure.
        """
        if self._dns_resolution_failed:
            return {"results": []}
        url = f"{OPENAQ_BASE_URL}/{path.lstrip('/')}"
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if _is_dns_resolution_error(exc):
                self._dns_resolution_failed = True
                logger.error(
                    "OpenAQ DNS resolution failed; skipping remaining OpenAQ requests: %s",
                    exc,
                )
                return {"results": []}
            logger.warning("OpenAQ request failed (%s %s): %s", url, params, exc)
            return {"results": []}

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    # Istanbul bounding box (WGS84) used to filter country-level results.
    _IST_MIN_LAT, _IST_MAX_LAT = 40.78, 41.32
    _IST_MIN_LON, _IST_MAX_LON = 28.50, 29.50

    def _in_istanbul(self, lat: Optional[float], lon: Optional[float]) -> bool:
        """Return True if the coordinate falls within the Istanbul bounding box."""
        if lat is None or lon is None:
            return False
        return (
            self._IST_MIN_LAT <= lat <= self._IST_MAX_LAT
            and self._IST_MIN_LON <= lon <= self._IST_MAX_LON
        )

    def fetch_istanbul_locations(self) -> pd.DataFrame:
        """Fetch OpenAQ monitoring locations within Istanbul province.

        Uses country_id=TR (reliable) then filters by Istanbul bounding box,
        avoiding the coordinates+radius parameter which returns 422 in some
        OpenAQ v3 deployments.

        Returns:
            DataFrame with location id, name, coordinates, and sensor list.
        """
        logger.info("OpenAQ: fetching TR locations then filtering to Istanbul bbox")
        all_results: list[dict] = []
        page = 1

        while True:
            if self._dns_resolution_failed:
                break
            data = self._get(
                "/locations",
                {"country_id": "TR", "limit": OPENAQ_PAGE_LIMIT, "page": page},
            )
            batch = data.get("results", [])
            if not batch:
                break
            all_results.extend(batch)
            if len(batch) < OPENAQ_PAGE_LIMIT:
                break
            page += 1
            time.sleep(0.2)

        if not all_results:
            logger.warning("OpenAQ: no locations found for TR")
            return pd.DataFrame()

        rows = []
        for loc in all_results:
            coords = loc.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            # Filter to Istanbul bounding box
            if not self._in_istanbul(
                float(lat) if lat is not None else None,
                float(lon) if lon is not None else None,
            ):
                continue

            rows.append({
                "location_id":   loc.get("id"),
                "location_name": loc.get("name", ""),
                "latitude":      float(lat) if lat is not None else None,
                "longitude":     float(lon) if lon is not None else None,
                "locality":      loc.get("locality", "Istanbul"),
                "country":       (loc.get("country") or {}).get("id", "TR"),
                "datetime_first_utc": ((loc.get("datetimeFirst") or {}).get("utc")),
                "datetime_last_utc":  ((loc.get("datetimeLast") or {}).get("utc")),
                "sensor_ids":    [
                    sensor.get("id")
                    for sensor in loc.get("sensors", [])
                    if sensor.get("id") is not None
                ],
                "parameters":    [
                    s.get("parameter", {}).get("name", "")
                    for s in loc.get("sensors", [])
                ],
            })

        if not rows:
            logger.warning(
                "OpenAQ: TR returned %d locations but none inside Istanbul bbox",
                len(all_results),
            )
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        logger.info("OpenAQ: found %d Istanbul locations (from %d TR total)", len(df), len(all_results))
        return df

    def fetch_measurements(
        self,
        location_id: int,
        start_date: datetime,
        end_date: datetime,
        sensor_ids: Optional[list[int]] = None,
    ) -> pd.DataFrame:
        """Fetch all measurements for one location over a date range.

        Handles pagination automatically.

        Args:
            location_id: OpenAQ integer location ID.
            start_date:  Range start (UTC).
            end_date:    Range end (UTC).

        Returns:
            DataFrame with columns: location_id, parameter, value, datetime_utc.
        """
        logger.debug("OpenAQ: fetching measurements for location %d", location_id)
        sensor_ids = [int(sensor_id) for sensor_id in (sensor_ids or []) if sensor_id is not None]
        if not sensor_ids:
            return pd.DataFrame()

        all_rows: list[dict] = []

        for sensor_id in sensor_ids:
            if self._dns_resolution_failed:
                break
            page = 1

            while True:
                if self._dns_resolution_failed:
                    break
                data = self._get(
                    f"/sensors/{sensor_id}/measurements",
                    {
                        "datetime_from": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "datetime_to":   end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "limit":         OPENAQ_PAGE_LIMIT,
                        "page":          page,
                    },
                )
                results = data.get("results", [])
                if not results:
                    break

                for r in results:
                    period = r.get("period", {})
                    dt_from = (period.get("datetimeFrom") or {}).get("utc", "")
                    param = (r.get("parameter") or {}).get("name", "")
                    all_rows.append({
                        "location_id":  location_id,
                        "parameter":    param,
                        "value":        r.get("value"),
                        "datetime_utc": dt_from,
                    })

                if len(results) < OPENAQ_PAGE_LIMIT:
                    break
                page += 1
                time.sleep(0.2)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        logger.debug(
            "OpenAQ: location %d -> %d measurement rows", location_id, len(df)
        )
        return df

    def fetch_all_measurements(
        self,
        start_date: datetime,
        end_date: datetime,
        delay_s: float = 0.5,
    ) -> pd.DataFrame:
        """Fetch and pivot measurements for all Istanbul locations.

        Args:
            start_date: Range start.
            end_date:   Range end.
            delay_s:    Sleep between location requests.

        Returns:
            Wide DataFrame: one row per (location_id, hour) with pollutant columns.
        """
        locations = self.fetch_istanbul_locations()
        if locations.empty:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date)
        for _, loc in locations.iterrows():
            if self._dns_resolution_failed:
                break
            loc_start = pd.to_datetime(loc.get("datetime_first_utc"), utc=True, errors="coerce")
            loc_end = pd.to_datetime(loc.get("datetime_last_utc"), utc=True, errors="coerce")
            if pd.notna(loc_start) and loc_start > end_ts:
                continue
            if pd.notna(loc_end) and loc_end < start_ts:
                continue

            lid = int(loc["location_id"])
            df  = self.fetch_measurements(
                lid,
                start_date,
                end_date,
                sensor_ids=loc.get("sensor_ids"),
            )
            if df.empty:
                time.sleep(delay_s)
                continue
            # Attach location metadata
            df["location_name"] = loc["location_name"]
            df["latitude"]      = loc["latitude"]
            df["longitude"]     = loc["longitude"]
            df["locality"]      = loc["locality"]
            frames.append(df)
            time.sleep(delay_s)

        if not frames:
            return pd.DataFrame()

        raw = pd.concat(frames, ignore_index=True)
        return raw


# ===========================================================================
# Schema normalizers
# ===========================================================================

def normalize_ibb_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Map IBB raw response to the project's unified air quality schema.

    IBB field names vary slightly across API versions.  This function handles
    the known variants and fills missing columns with NaN.

    Args:
        df: Raw DataFrame from IBBDataFetcher.

    Returns:
        DataFrame with exactly the columns in RAW_AIR_QUALITY_FIELDS.
    """
    if df.empty:
        return pd.DataFrame(columns=_UNIFIED_COLS)

    df = df.copy()

    if "Concentration" in df.columns:
        concentration = pd.json_normalize(
            df["Concentration"].apply(lambda value: value if isinstance(value, dict) else {})
        ).rename(columns={
            "PM10":  "pm10",
            "Pm10":  "pm10",
            "PM25":  "pm25",
            "Pm25":  "pm25",
            "PM2.5": "pm25",
            "Pm2.5": "pm25",
            "PM2_5": "pm25",
            "Pm2_5": "pm25",
            "NO2":   "no2",
            "No2":   "no2",
            "SO2":   "so2",
            "So2":   "so2",
            "CO":    "co",
            "Co":    "co",
            "O3":    "o3",
        })
        for col in ("pm10", "pm25", "no2", "so2", "co", "o3"):
            if col in concentration.columns:
                df[col] = concentration[col]
        df = df.drop(columns=["Concentration"])

    for aqi_col in ("AQI", "Aqi"):
        if aqi_col not in df.columns:
            continue

        aqi_series = df[aqi_col]
        aqi_scalar = aqi_series.where(~aqi_series.apply(lambda value: isinstance(value, dict)))
        aqi_nested = pd.json_normalize(
            aqi_series.apply(lambda value: value if isinstance(value, dict) else {})
        )

        if "AQIIndex" in aqi_nested.columns:
            df["aqi"] = aqi_nested["AQIIndex"]
            if aqi_scalar.notna().any():
                df["aqi"] = df["aqi"].where(df["aqi"].notna(), aqi_scalar)
        elif aqi_scalar.notna().any():
            df["aqi"] = aqi_scalar

        df = df.drop(columns=[aqi_col])

    # Field-name aliases: IBB name → unified name
    # Covers multiple observed IBB API versions (fields vary by endpoint/version).
    rename_map = {
        # Station identity — actual GetAQIStations returns 'Id', 'Name'
        "Id":          "station_id",
        "id":          "station_id",
        "ID":          "station_id",
        "SensorId":    "station_id",
        "sensorId":    "station_id",
        "sensor_id":   "station_id",
        "StationId":   "station_id",
        "station_id_src": "station_id",
        "Name":        "station_name",
        "SensorName":  "station_name",
        "sensorName":  "station_name",
        # Location — actual API has 'Adress' (sic) and 'Location' (WKT/JSON)
        "Adress":      "district",    # IBB typo: 'Adress' instead of 'Address'
        "Address":     "district",
        "District":    "district",
        "Latitude":    "latitude",
        "Longitude":   "longitude",
        # Timestamp
        "DateTime":    "timestamp",
        "dateTime":    "timestamp",
        "date_time":   "timestamp",
        "Date":        "timestamp",
        "ReadTime":    "timestamp",
        "readTime":    "timestamp",
        # Pollutants — actual API uses Title-Case (Pm10, Pm25, etc.)
        "Pm10":  "pm10",
        "PM10":  "pm10",
        "Pm25":  "pm25",
        "PM25":  "pm25",
        "PM2_5": "pm25",
        "Pm2_5": "pm25",
        "No2":   "no2",
        "NO2":   "no2",
        "So2":   "so2",
        "SO2":   "so2",
        "Co":    "co",
        "CO":    "co",
        "O3":    "o3",
        "Aqi":   "aqi",
        "AQI":   "aqi",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Parse coordinates from 'Location' WKT/JSON field if lat/lon still missing
    if ("latitude" not in df.columns or df["latitude"].isna().all()) and "Location" in df.columns:
        coords = df["Location"].apply(IBBDataFetcher._parse_location)
        df["latitude"]  = coords.apply(lambda c: c[0])
        df["longitude"] = coords.apply(lambda c: c[1])

    # Ensure timestamp is datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["timestamp"] = df["timestamp"].dt.floor("h")   # snap to hour

    # Derive AQI from PM2.5 if missing
    if "aqi" not in df.columns and "pm25" in df.columns:
        df["aqi"] = df["pm25"].apply(
            lambda v: _pm25_to_aqi(float(v)) if pd.notna(v) else float("nan")
        )

    df["source"] = "ibb"

    # Add missing columns as NaN
    for col in _UNIFIED_COLS:
        if col not in df.columns:
            df[col] = float("nan")

    return df[_UNIFIED_COLS].copy()


def normalize_openaq_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Map OpenAQ raw long-format response to the unified air quality schema.

    OpenAQ returns one row per (location, parameter, datetime).  This function
    pivots the data into one row per (location, hour) with pollutant columns.

    Args:
        df: Raw long-format DataFrame from OpenAQDataFetcher.

    Returns:
        DataFrame with exactly the columns in RAW_AIR_QUALITY_FIELDS.
    """
    if df.empty:
        return pd.DataFrame(columns=_UNIFIED_COLS)

    # Normalise parameter names to unified column names
    param_map = {
        "pm25":  "pm25",
        "pm2.5": "pm25",
        "pm10":  "pm10",
        "no2":   "no2",
        "so2":   "so2",
        "co":    "co",
        "o3":    "o3",
        "aqi":   "aqi",
    }
    df = df.copy()
    df["parameter"] = df["parameter"].str.lower().map(param_map)
    df = df.dropna(subset=["parameter"])

    # Parse timestamp and snap to hour
    df["timestamp"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df["timestamp"] = df["timestamp"].dt.tz_localize(None).dt.floor("h")

    # Pivot: rows=(location_id, timestamp), cols=pollutant parameters
    id_vars = ["location_id", "location_name", "latitude", "longitude",
               "locality", "timestamp"]
    id_vars = [c for c in id_vars if c in df.columns]

    pivot = df.pivot_table(
        index=id_vars,
        columns="parameter",
        values="value",
        aggfunc="mean",
    ).reset_index()
    pivot.columns.name = None

    # Rename location columns → unified schema
    pivot = pivot.rename(columns={
        "location_id":   "station_id",
        "location_name": "station_name",
        "locality":      "district",
    })
    pivot["station_id"] = "OAQ-" + pivot["station_id"].astype(str)
    pivot["source"] = "openaq"

    # Derive AQI if not returned by API
    if "aqi" not in pivot.columns and "pm25" in pivot.columns:
        pivot["aqi"] = pivot["pm25"].apply(
            lambda v: _pm25_to_aqi(float(v)) if pd.notna(v) else float("nan")
        )

    for col in _UNIFIED_COLS:
        if col not in pivot.columns:
            pivot[col] = float("nan")

    return pivot[_UNIFIED_COLS].copy()


# ===========================================================================
# Merge & deduplicate
# ===========================================================================

def merge_and_deduplicate(
    ibb_df: pd.DataFrame,
    openaq_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge IBB and OpenAQ datasets with IBB taking priority.

    Deduplication key: (station_id, timestamp).
    When both sources have a record for the same key, the IBB record is kept
    because IBB stations are official government instruments.

    For non-overlapping records (different station_ids) both are kept.

    Args:
        ibb_df:    Normalised IBB DataFrame (RAW_AIR_QUALITY_FIELDS columns).
        openaq_df: Normalised OpenAQ DataFrame (RAW_AIR_QUALITY_FIELDS columns).

    Returns:
        Deduplicated DataFrame with source column indicating origin.
    """
    frames = []
    if not ibb_df.empty:
        frames.append(ibb_df)
    if not openaq_df.empty:
        frames.append(openaq_df)

    if not frames:
        logger.warning("Both IBB and OpenAQ DataFrames are empty - nothing to merge")
        return pd.DataFrame(columns=_UNIFIED_COLS)

    combined = pd.concat(frames, ignore_index=True)

    if combined.empty:
        return combined

    # Sort so IBB rows come first (they win in drop_duplicates keep='first')
    source_priority = {"ibb": 0, "openaq": 1}
    combined["_priority"] = combined["source"].map(source_priority).fillna(99)
    combined = combined.sort_values(["station_id", "timestamp", "_priority"])

    # Deduplicate on (station_id, truncated-hour timestamp)
    combined["_ts_hour"] = pd.to_datetime(combined["timestamp"]).dt.floor("h")
    deduped = combined.drop_duplicates(
        subset=["station_id", "_ts_hour"], keep="first"
    ).drop(columns=["_priority", "_ts_hour"])

    n_before = len(combined)
    n_after  = len(deduped)
    logger.info(
        "merge_and_deduplicate: %d rows -> %d rows (removed %d duplicates)",
        n_before, n_after, n_before - n_after,
    )
    return deduped.reset_index(drop=True)


# ===========================================================================
# Data quality filters
# ===========================================================================

def apply_data_quality_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove obvious sensor errors and handle missing values.

    Strategy:
    - Values outside physical range → NaN (not dropped; ML imputer handles them).
    - Rows where BOTH pm25 AND aqi are NaN → dropped (record is useless).
    - Timestamp NaN rows → dropped.

    Thresholds are read from src/common/config.py.

    Args:
        df: Merged DataFrame (RAW_AIR_QUALITY_FIELDS columns).

    Returns:
        Cleaned DataFrame.
    """
    if df.empty:
        return df

    df = df.copy()

    bounds: dict[str, tuple[float, float]] = {
        "pm25": (MIN_PM25, MAX_PM25),
        "pm10": (MIN_PM10, MAX_PM10),
        "no2":  (MIN_NO2,  MAX_NO2),
        "so2":  (MIN_SO2,  MAX_SO2),
        "co":   (MIN_CO,   MAX_CO),
        "o3":   (MIN_O3,   MAX_O3),
        "aqi":  (MIN_AQI,  MAX_AQI),
    }

    outlier_total = 0
    for col, (lo, hi) in bounds.items():
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        mask = (numeric < lo) | (numeric > hi)
        n_outliers = int(mask.sum())
        if n_outliers:
            logger.debug("Quality filter: %d outliers in %s set to NaN", n_outliers, col)
            outlier_total += n_outliers
        df[col] = numeric.where(~mask, other=float("nan"))

    logger.info("apply_data_quality_filters: %d outlier values set to NaN", outlier_total)

    # Drop rows with no usable air quality signal
    useless_mask = df["pm25"].isna() & df["aqi"].isna()
    n_useless = int(useless_mask.sum())
    if n_useless:
        logger.info("Dropping %d rows with no pm25 or aqi values", n_useless)
        df = df[~useless_mask]

    # Drop rows with unparseable timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    n_bad_ts = int(df["timestamp"].isna().sum())
    if n_bad_ts:
        logger.info("Dropping %d rows with invalid timestamps", n_bad_ts)
        df = df.dropna(subset=["timestamp"])

    return df.reset_index(drop=True)
