"""Kafka producer skeleton for weather enrichment data."""


def fetch_weather_data():
    """TODO: Pull Istanbul weather data from the selected API."""
    raise NotImplementedError


def normalize_weather_record(raw_record):
    """TODO: Convert weather payloads into the shared schema."""
    raise NotImplementedError


def publish_to_kafka(records):
    """TODO: Publish normalized weather records to Kafka."""
    raise NotImplementedError


def main():
    """TODO: Wire the full weather producer flow."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
