"""Minimal logging entry point used across modules."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Create a consistent logger configuration for project modules."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger(name)
