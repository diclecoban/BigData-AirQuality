# Grafana Dashboard Plan

## Dashboard Goal

Display real-time and predicted Istanbul air quality at station and district level.

## Planned Panels

### 1. City Overview

- current average AQI
- worst district
- most polluted station
- last update timestamp

### 2. District Map

- Istanbul district polygons
- color-coded AQI categories
- current AQI
- 1h / 3h / 6h predictions

### 3. Station Details

- pollutant breakdown
- latest sensor readings
- station trend charts

### 4. Prediction Quality

- RMSE
- MAE
- model version
- last training date

### 5. Pipeline Health

- Kafka lag
- Spark micro-batch duration
- ingestion rate
- failed records

## Data Contracts To Finalize

- prediction table schema
- district GeoJSON source
- station metadata source
- refresh interval
