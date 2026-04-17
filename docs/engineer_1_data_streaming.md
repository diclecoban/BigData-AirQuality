# Engineer 1: Data and Streaming

## Role

Own the ingestion layer and the streaming entry points.

## Main Responsibilities

- connect to IBB API
- connect to OpenAQ API
- connect to weather API
- normalize raw responses
- publish valid events to Kafka
- define schema and field naming consistency

## Files To Implement

- `src/ingestion/producer_ibb.py`
- `src/ingestion/producer_openaq.py`
- `src/ingestion/producer_weather.py`
- `src/ingestion/schema.py`
- `src/streaming/structured_streaming_job.py`

## Output Contracts To Agree With Team

- raw Kafka message schema
- timestamp format
- station identifier format
- district naming convention
- null handling policy

## Deliverables

- working producers
- repeatable ingestion flow
- streaming-ready schemas
- sample Kafka messages
