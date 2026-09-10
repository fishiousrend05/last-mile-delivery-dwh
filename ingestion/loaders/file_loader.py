"""
ingestion/loaders/file_loader.py — Local raw-file cache loader.

Responsibilities
----------------
- Save an extracted DataFrame to data/raw/{source}/{table}.csv.
- Read a previously cached raw CSV back into a DataFrame.
- Validate the file at the boundary before pandas reads it.

This loader is intentionally independent from PostgreSQL and the ingestion
audit log. The PostgreSQL loader is the official landing loader; this module
is only a lightweight disk cache used by extractors/generators when needed.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file

logger = get_logger(__name__)

_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"
)
DEFAULT_RAW_ROOT = Path("data/raw")


def get_raw_dir(source_name: str) -> Path:
    """
    Resolve the raw-cache directory for a source.

    Priority:
        1. sources.{source_name}.raw_dir in ingestion_config.yaml
        2. data/raw/{source_name}

    The fallback is useful for one-off/generated sources that do not need a
    dedicated config entry.
    """
    if not source_name or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")

    if _CONFIG_PATH.exists():
        try:
            with _CONFIG_PATH.open("r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except (OSError, yaml.YAMLError) as exc:
            logger.warning(
                "Could not read ingestion config '%s': %s. "
                "Falling back to default raw directory.",
                _CONFIG_PATH,
                exc,
            )
        else:
            raw_dir = (
                config.get("sources", {})
                .get(source_name, {})
                .get("raw_dir")
            )
            if raw_dir:
                return Path(raw_dir)

    return DEFAULT_RAW_ROOT / source_name


def save_to_raw_file(
    df: pd.DataFrame,
    source_name: str,
    table_name: str,
    raw_dir: str | Path | None = None,
) -> Path:
    """
    Save a DataFrame to the local raw cache.

    Existing files are intentionally overwritten. The cache represents the
    latest extracted version; it is not an append-only history.

    Returns:
        Path to the written CSV file.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df must be a pandas DataFrame, got {type(df).__name__}")
    if not source_name or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")
    if not table_name or not table_name.strip():
        raise ValueError("table_name must be a non-empty string")

    target_dir = Path(raw_dir) if raw_dir else get_raw_dir(source_name)
    target_dir.mkdir(parents=True, exist_ok=True)

    output_path = target_dir / f"{table_name}.csv"

    # UTF-8 with BOM makes the cache friendlier to Excel on Windows while
    # remaining readable by pandas.
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info(
        "Saved %s row(s) to raw cache: %s",
        f"{len(df):,}",
        output_path,
    )
    return output_path


def load_from_raw_file(
    source_name: str,
    table_name: str,
    raw_dir: str | Path | None = None,
) -> pd.DataFrame:
    """
    Load a previously cached raw CSV.

    The file is validated before pandas reads it so missing, empty, incorrectly
    formatted, or non-UTF-8 files fail with a project-specific error.
    """
    if not source_name or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")
    if not table_name or not table_name.strip():
        raise ValueError("table_name must be a non-empty string")

    target_dir = Path(raw_dir) if raw_dir else get_raw_dir(source_name)
    file_path = target_dir / f"{table_name}.csv"

    validated_path = validate_file(file_path, expected_extension=".csv")
    df = pd.read_csv(validated_path)

    logger.info(
        "Loaded %s row(s) from raw cache: %s",
        f"{len(df):,}",
        validated_path,
    )
    return df
