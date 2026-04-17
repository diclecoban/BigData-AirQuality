# System Architecture Notes

## Goal

Describe the final end-to-end architecture for the Istanbul air quality pipeline.

## Main Layers

### 1. Ingestion Layer

- IBB API producer
- OpenAQ API producer
- Weather API producer
- Kafka topics for raw events

### 2. Processing Layer

- Spark Structured Streaming job
- data cleaning
- schema validation
- enrichment joins
- feature generation

### 3. ML Layer

- batch model training
- model comparison
- model registry integration
- real-time inference

### 4. Storage Layer

- raw data in S3 / local lake
- processed parquet outputs
- model artifacts
- metrics and logs

### 5. Visualization Layer

- Grafana dashboard
- AQI trend panels
- district map
- pipeline health metrics

## Open Design Decisions

- exact Kafka partition strategy
- streaming trigger interval
- prediction sink destination
- district mapping method
- MLflow deployment mode
