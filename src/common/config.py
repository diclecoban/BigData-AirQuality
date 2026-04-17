"""Shared configuration helpers for the project."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"


def get_config_path(filename: str) -> Path:
    """Return an absolute path under the config directory."""
    return CONFIG_DIR / filename
