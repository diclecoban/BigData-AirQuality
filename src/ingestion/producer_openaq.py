"""Kafka producer skeleton for OpenAQ data ingestion."""


def fetch_openaq_measurements():
    """TODO: Pull historical or near-real-time data from OpenAQ."""
    raise NotImplementedError


def normalize_openaq_record(raw_record):
    """TODO: Convert OpenAQ payloads into the shared schema."""
    raise NotImplementedError


def publish_to_kafka(records):
    """TODO: Publish normalized OpenAQ records to Kafka."""
    raise NotImplementedError


def main():
    """TODO: Wire the full OpenAQ producer flow."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
