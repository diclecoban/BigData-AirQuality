CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS air_quality_enriched (
    time           TIMESTAMPTZ      NOT NULL,
    station_id     TEXT,
    station_name   TEXT,
    district       TEXT,
    source         TEXT,
    pm10           DOUBLE PRECISION,
    pm25           DOUBLE PRECISION,
    no2            DOUBLE PRECISION,
    so2            DOUBLE PRECISION,
    co             DOUBLE PRECISION,
    o3             DOUBLE PRECISION,
    aqi            DOUBLE PRECISION,
    aqi_category   INTEGER,
    temperature    DOUBLE PRECISION,
    humidity       DOUBLE PRECISION,
    wind_speed     DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    pressure       DOUBLE PRECISION
);

SELECT create_hypertable('air_quality_enriched', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_aq_station  ON air_quality_enriched (station_id,  time DESC);
CREATE INDEX IF NOT EXISTS idx_aq_district ON air_quality_enriched (district,    time DESC);
