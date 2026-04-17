"""Kafka producer skeleton for IBB air quality data."""


def fetch_ibb_station_metadata():
    """TODO: Pull active station metadata from the IBB API."""
    raise NotImplementedError


def fetch_ibb_measurements():
    """TODO: Pull latest station measurements from the IBB API."""
    raise NotImplementedError


def normalize_ibb_record(raw_record):
    """TODO: Convert IBB payloads into the shared schema."""
    raise NotImplementedError


def publish_to_kafka(records):
    """TODO: Publish normalized IBB records to Kafka."""
    raise NotImplementedError


def main():
    """TODO: Wire the full IBB producer flow."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
