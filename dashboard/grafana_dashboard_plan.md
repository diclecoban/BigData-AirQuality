# Grafana Dashboard Plan

## Dashboard Goal

Display real-time and predicted Istanbul air quality at station and district
level. The dashboard should help users answer:

- What is the current air quality in Istanbul?
- Which district or station is currently worst?
- What does the model predict for the next 1, 3, and 6 hours?
- Is the data pipeline healthy enough to trust the predictions?
- Which model version produced the current predictions?

## Access

Local Docker URL:

```text
http://localhost:3000
```

Default local credentials:

```text
username: admin
password: admin
```

## Recommended Data Path

For the first stable project version, use:

```text
Kafka airquality.predictions
  -> Spark writes parquet predictions
  -> S3 processed bucket
  -> Athena table
  -> Grafana Athena data source
```

This is preferred for demos because Athena-backed panels are easier to
reproduce than direct Kafka panels.

Alternative real-time path:

```text
Kafka airquality.predictions
  -> bridge/sink job
  -> PostgreSQL or TimescaleDB
  -> Grafana SQL data source
```

Direct Kafka panels are allowed only if the plugin is available and stable in
the deployment environment.

## Data Sources

| Data source | Required | Purpose |
|---|---|---|
| Athena | Yes for AWS demo | Predictions, historical AQI, model quality tables |
| S3 | Yes | Parquet storage behind Athena |
| MLflow | Optional direct link | Model run and registry reference |
| CloudWatch | Optional for AWS | Pipeline health metrics and logs |
| PostgreSQL / TimescaleDB | Optional | Low-latency real-time panels |

## Core Tables

### `airquality_predictions`

Prediction records from the `airquality.predictions` Kafka topic.

```text
station_id      string
district        string
latitude        double
longitude       double
timestamp       timestamp
predicted_at    timestamp
horizon_h       int
predicted_aqi   double
aqi_category    string
model_name      string
model_version   string
```

Partitions:

```text
year
month
day
horizon_h
```

### `airquality_latest`

Latest enriched observation per station.

```text
station_id      string
station_name    string
district        string
latitude        double
longitude       double
timestamp       timestamp
aqi             double
pm25            double
pm10            double
no2             double
so2             double
co              double
o3              double
aqi_category    string
```

### `station_metadata`

Station reference table.

```text
station_id      string
station_name    string
district        string
latitude        double
longitude       double
source          string
is_active       boolean
```

### `district_geojson`

District boundaries for Istanbul map panels.

Required properties:

```text
district
geometry
```

Recommended source: Istanbul district boundary GeoJSON exported into the S3
processed bucket.

### `pipeline_metrics`

Service health table or CloudWatch-backed equivalent.

```text
service         string
metric_name     string
metric_value    double
timestamp       timestamp
status          string
```

### `model_quality`

MLflow-derived model quality snapshot.

```text
model_name          string
model_version       string
run_id              string
horizon_h           int
rmse                double
mae                 double
r2                  double
last_training_date  timestamp
stage               string
```

## Dashboard Variables

Create these Grafana variables:

| Variable | Type | Values |
|---|---|---|
| `district` | Query | All Istanbul districts, default `All` |
| `station_id` | Query | Stations filtered by district |
| `horizon_h` | Custom | `1`, `3`, `6`, default `1` |
| `model_version` | Query | Available model versions, default latest |
| `refresh_rate` | Custom | `30s`, `1m`, `5m` |

## AQI Categories

Use the same categories across all panels:

| AQI range | Category | Color |
|---|---|---|
| `0-50` | Good | Green |
| `51-100` | Moderate | Yellow |
| `101-150` | Unhealthy for Sensitive Groups | Orange |
| `151-200` | Unhealthy | Red |
| `201-300` | Very Unhealthy | Purple |
| `301+` | Hazardous | Dark red |

## Planned Panels

### 1. City Overview

Purpose: give a one-screen status of Istanbul air quality.

Panels:

| Panel | Type | Fields |
|---|---|---|
| Current average AQI | Stat | `avg(aqi)` from `airquality_latest` |
| Worst district | Stat | district with highest average `aqi` |
| Most polluted station | Stat | station with highest `aqi` |
| Last update | Stat | `max(timestamp)` |
| AQI category split | Bar chart | count of stations by `aqi_category` |

Refresh:

```text
30 seconds
```

### 2. District Map

Purpose: show spatial risk across Istanbul.

Panel type:

```text
Geomap
```

Layers:

| Layer | Source | Notes |
|---|---|---|
| District polygons | `district_geojson` | Join on `district` |
| Station points | `airquality_latest` | Latitude/longitude markers |
| Prediction overlay | `airquality_predictions` | Filter by `horizon_h` |

Tooltip fields:

```text
district
current_aqi
predicted_aqi
horizon_h
aqi_category
station_count
last_update
```

Refresh:

```text
30 seconds
```

### 3. Station Details

Purpose: inspect pollutants and trends for a selected station.

Panels:

| Panel | Type | Fields |
|---|---|---|
| Latest pollutant values | Table | `pm25`, `pm10`, `no2`, `so2`, `co`, `o3`, `aqi` |
| AQI trend | Time series | `aqi` over time |
| PM2.5 / PM10 trend | Time series | `pm25`, `pm10` |
| NO2 / O3 trend | Time series | `no2`, `o3` |
| Forecast comparison | Time series | current `aqi` + predicted AQI by horizon |

Filters:

```text
district
station_id
```

Refresh:

```text
1 minute
```

### 4. Prediction Quality

Purpose: show whether the current model is reliable.

Panels:

| Panel | Type | Fields |
|---|---|---|
| RMSE by horizon | Bar chart | `horizon_h`, `rmse` |
| MAE by horizon | Bar chart | `horizon_h`, `mae` |
| R2 by horizon | Stat or bar chart | `horizon_h`, `r2` |
| Active model version | Stat | `model_name`, `model_version`, `stage` |
| Last training date | Stat | `last_training_date` |

Source:

```text
model_quality
```

Refresh:

```text
5 minutes
```

### 5. Pipeline Health

Purpose: show whether ingestion, streaming, and prediction are alive.

Panels:

| Panel | Type | Fields |
|---|---|---|
| Kafka lag | Time series | consumer lag by group |
| Spark micro-batch duration | Time series | batch duration milliseconds |
| Ingestion rate | Time series | records per minute by source |
| Failed records | Time series or stat | failed count by service |
| Prediction freshness | Stat | minutes since latest `predicted_at` |
| Service status | Table | service, status, last heartbeat |

Sources:

```text
pipeline_metrics
CloudWatch
```

Refresh:

```text
30 seconds
```

## Example Athena Queries

### Latest City Average AQI

```sql
SELECT avg(aqi) AS current_average_aqi
FROM airquality_latest
WHERE timestamp = (
  SELECT max(timestamp)
  FROM airquality_latest
);
```

### Worst District

```sql
SELECT district, avg(aqi) AS avg_aqi
FROM airquality_latest
WHERE timestamp >= current_timestamp - interval '1' hour
GROUP BY district
ORDER BY avg_aqi DESC
LIMIT 1;
```

### Latest Prediction by Horizon

```sql
SELECT
  district,
  station_id,
  latitude,
  longitude,
  predicted_aqi,
  aqi_category,
  model_name,
  model_version,
  predicted_at
FROM airquality_predictions
WHERE horizon_h = ${horizon_h}
  AND predicted_at >= current_timestamp - interval '1' hour;
```

### Prediction Freshness

```sql
SELECT
  date_diff('minute', max(predicted_at), current_timestamp) AS minutes_since_prediction
FROM airquality_predictions;
```

### Model Quality

```sql
SELECT
  model_name,
  model_version,
  horizon_h,
  rmse,
  mae,
  r2,
  last_training_date
FROM model_quality
WHERE stage = 'Production'
ORDER BY horizon_h;
```

## Refresh Policy

| Panel group | Refresh |
|---|---|
| City overview | `30s` |
| District map | `30s` |
| Station details | `1m` |
| Prediction quality | `5m` |
| Pipeline health | `30s` |

Dashboard default:

```text
30 seconds
```

## Alert Rules

Create these alerts for the AWS deployment:

| Alert | Condition | Severity |
|---|---|---|
| No new predictions | `minutes_since_prediction > 15` | High |
| Average AQI unhealthy | city average AQI `> 150` | Medium |
| District hazardous | any district predicted AQI `> 300` | High |
| Kafka lag high | lag above agreed threshold for 5 minutes | High |
| Spark streaming stalled | no micro-batch update for 10 minutes | High |
| API ingestion failing | failed request count increasing for 10 minutes | Medium |

## Implementation Checklist

1. Confirm `airquality.predictions` records include the required fields.
2. Persist predictions to processed parquet partitioned by date and horizon.
3. Create Athena external tables for predictions and latest observations.
4. Add station metadata table.
5. Add district GeoJSON file and configure Grafana Geomap.
6. Add MLflow-derived `model_quality` table or export.
7. Add pipeline metrics table or CloudWatch data source.
8. Build the five dashboard sections.
9. Configure refresh intervals and alert rules.
10. Export dashboard JSON after final review.

## Open Decisions

| Decision | Current recommendation |
|---|---|
| Primary dashboard data source | Athena over processed S3 parquet |
| Real-time low-latency source | PostgreSQL/TimescaleDB if needed |
| Map boundary source | Istanbul district GeoJSON in processed S3 bucket |
| Prediction horizon default | `1` hour |
| Dashboard default refresh | `30s` |
| Public demo access | Restricted by IP or local screen share |
