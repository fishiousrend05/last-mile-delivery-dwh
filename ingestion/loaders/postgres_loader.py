"""
ingestion/loaders/postgres_loader.py — Load validated DataFrames into PostgreSQL.

Responsibilities
----------------
    Extractor
        -> raw DataFrame
    Schema validator
        -> validated/coerced DataFrame
    This module
        -> PostgreSQL landing.{table_name}
        -> ingestion audit log

Supported load modes
--------------------
- full_refresh: replace the landing table contents atomically.
- append: append rows to the existing landing table.

The loader does NOT perform business transformations, deduplication, or dbt
logic. Those belong to the transformation layer.

The .env file is intentionally not included in the repository. Connection
settings are read by ingestion.utils.database.get_engine(), which is the
single connection factory used by the ingestion project.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from ingestion.utils.database import get_engine
from ingestion.utils.logger import get_logger
from ingestion.utils.metadata import (
    end_run,
    get_last_run_status,
    init_metadata_table,
    start_run,
)

logger = get_logger(__name__)

_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"
)

DEFAULT_LANDING_SCHEMA = "landing"
DEFAULT_BATCH_SIZE = 5_000
SUPPORTED_LOAD_MODES = {"full_refresh", "append"}

# Internal table/schema names are not user-provided SQL fragments. Still,
# validate them before handing them to pandas/SQLAlchemy to catch accidental
# malformed identifiers early.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PostgresLoaderError(RuntimeError):
    """Base exception for PostgreSQL loader failures."""


class ConcurrentLoadError(PostgresLoaderError):
    """Raised when the same source/table already has a running ingestion."""


def _validate_identifier(value: str, field_name: str) -> str:
    """Validate a PostgreSQL identifier used by the ingestion pipeline."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")

    value = value.strip()
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"Invalid {field_name} '{value}'. "
            "Expected only letters, digits, and underscores, "
            "starting with a letter or underscore."
        )
    return value


def _load_postgres_config() -> dict[str, Any]:
    """
    Read PostgreSQL loader settings from ingestion_config.yaml.

    Supported keys:
        postgres.schema_landing
        postgres.batch_size
    """
    defaults = {
        "schema_landing": DEFAULT_LANDING_SCHEMA,
        "batch_size": DEFAULT_BATCH_SIZE,
    }

    if not _CONFIG_PATH.exists():
        return defaults

    try:
        with _CONFIG_PATH.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as exc:
        logger.warning(
            "Could not read ingestion config '%s': %s. "
            "Using PostgreSQL loader defaults.",
            _CONFIG_PATH,
            exc,
        )
        return defaults

    postgres_config = config.get("postgres", {}) or {}
    schema_landing = postgres_config.get(
        "schema_landing", DEFAULT_LANDING_SCHEMA
    )
    batch_size = postgres_config.get("batch_size", DEFAULT_BATCH_SIZE)

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(
            f"postgres.batch_size must be a positive integer, got {batch_size!r}"
        )

    return {
        "schema_landing": schema_landing,
        "batch_size": batch_size,
    }


def get_landing_schema() -> str:
    """Return the configured PostgreSQL landing schema."""
    config = _load_postgres_config()
    return _validate_identifier(
        str(config["schema_landing"]),
        "landing schema",
    )


def get_batch_size() -> int:
    """Return the configured pandas/to_sql batch size."""
    return int(_load_postgres_config()["batch_size"])


def _ensure_landing_schema(schema: str) -> None:
    """Create the landing schema if it does not exist."""
    engine = get_engine()
    with engine.begin() as conn:
        # schema has already passed strict identifier validation.
        conn.exec_driver_sql(
            f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
        )


def _normalise_load_mode(load_mode: str) -> str:
    """Validate and normalise the loader's write strategy."""
    if not isinstance(load_mode, str):
        raise ValueError("load_mode must be a string")

    mode = load_mode.strip().lower()
    if mode not in SUPPORTED_LOAD_MODES:
        raise ValueError(
            f"Unsupported load_mode '{load_mode}'. "
            f"Expected one of: {sorted(SUPPORTED_LOAD_MODES)}"
        )
    return mode


def load_dataframe(
    df: pd.DataFrame,
    source_name: str,
    table_name: str,
    load_mode: str,
    *,
    schema: str | None = None,
    batch_size: int | None = None,
    prevent_concurrent_load: bool = True,
) -> int:
    """
    Load one validated DataFrame into PostgreSQL.

    Args:
        df:
            DataFrame that has already passed schema validation. This function
            deliberately does not call Pandera again.
        source_name:
            Logical ingestion source, e.g. ``olist`` or ``weather``.
        table_name:
            Landing table name, e.g. ``olist_orders``.
        load_mode:
            ``full_refresh`` or ``append``.
        schema:
            PostgreSQL target schema. Defaults to config ``schema_landing``.
        batch_size:
            Number of rows per INSERT batch. Defaults to config.
        prevent_concurrent_load:
            If True, refuse to start when the latest audit run for the same
            source/table is still ``running``.

    Returns:
        Number of rows loaded.

    Raises:
        ConcurrentLoadError:
            Another ingestion run for this source/table is still running.
        ValueError:
            Invalid arguments or an empty DataFrame.
        PostgresLoaderError:
            PostgreSQL write/audit operation failed.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df must be a pandas DataFrame, got {type(df).__name__}")

    if not source_name or not source_name.strip():
        raise ValueError("source_name must be a non-empty string")

    source_name = source_name.strip()
    table_name = _validate_identifier(table_name, "table_name")
    mode = _normalise_load_mode(load_mode)

    target_schema = (
        _validate_identifier(schema, "schema")
        if schema is not None
        else get_landing_schema()
    )

    effective_batch_size = batch_size if batch_size is not None else get_batch_size()
    if not isinstance(effective_batch_size, int) or effective_batch_size <= 0:
        raise ValueError(
            f"batch_size must be a positive integer, got {effective_batch_size!r}"
        )

    if df.empty:
        raise ValueError(
            f"Refusing to load empty DataFrame into "
            f"{target_schema}.{table_name}"
        )

    # Metadata is part of the ingestion contract. Initialise the audit table
    # before checking/running the actual load.
    init_metadata_table()

    if prevent_concurrent_load:
        previous_status = get_last_run_status(source_name, table_name)
        if previous_status == "running":
            raise ConcurrentLoadError(
                f"A load for {source_name}.{table_name} is already running."
            )

    run_id = start_run(source_name, table_name, mode)

    try:
        _ensure_landing_schema(target_schema)

        engine = get_engine()

        # Passing an explicit SQLAlchemy Connection inside an engine.begin()
        # makes the pandas to_sql operation part of one DB transaction.
        # Therefore a failed full_refresh does not leave a half-written table.
        with engine.begin() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                schema=target_schema,
                if_exists="replace" if mode == "full_refresh" else "append",
                index=False,
                chunksize=effective_batch_size,
                method="multi",
            )

        row_count = len(df)
        end_run(run_id, "success", row_count=row_count)

        logger.info(
            "[%s] Loaded %s row(s) into %s.%s (%s)",
            run_id,
            f"{row_count:,}",
            target_schema,
            table_name,
            mode,
        )
        return row_count

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"

        # Audit failure should not hide the original PostgreSQL error. If the
        # audit update itself fails, log it and re-raise the original exception.
        try:
            end_run(
                run_id,
                "failed",
                row_count=None,
                error_message=error_message,
            )
        except Exception as audit_exc:
            logger.exception(
                "[%s] Could not update failed audit run: %s",
                run_id,
                audit_exc,
            )

        logger.exception(
            "[%s] Failed loading %s.%s",
            run_id,
            target_schema,
            table_name,
        )
        raise PostgresLoaderError(
            f"Failed to load {source_name}.{table_name} "
            f"into {target_schema}.{table_name}: {exc}"
        ) from exc


def load_extraction_result(
    extraction_result: Any,
    *,
    source_name: str | None = None,
    load_mode: str | None = None,
    schema: str | None = None,
    batch_size: int | None = None,
    prevent_concurrent_load: bool = True,
) -> int:
    """
    Convenience wrapper for the project's ExtractionResult contract.

    It accepts an ``ExtractionResult``-like object with:
        .table_name
        .df
        .metadata

    ``source_name`` and ``load_mode`` can be supplied explicitly. If omitted,
    the loader attempts to read them from ``extraction_result.metadata``.

    This keeps the loader compatible with the current extractor contract while
    avoiding a hard dependency on the concrete dataclass.
    """
    if not hasattr(extraction_result, "df") or not hasattr(
        extraction_result, "table_name"
    ):
        raise TypeError(
            "extraction_result must provide '.df' and '.table_name' attributes"
        )

    metadata = getattr(extraction_result, "metadata", {}) or {}

    resolved_source = source_name or metadata.get("source_name")
    resolved_mode = load_mode or metadata.get("load_mode")

    if not resolved_source:
        raise ValueError(
            "source_name was not provided and is missing from "
            "extraction_result.metadata"
        )

    if not resolved_mode:
        raise ValueError(
            "load_mode was not provided and is missing from "
            "extraction_result.metadata"
        )

    return load_dataframe(
        df=extraction_result.df,
        source_name=resolved_source,
        table_name=extraction_result.table_name,
        load_mode=resolved_mode,
        schema=schema,
        batch_size=batch_size,
        prevent_concurrent_load=prevent_concurrent_load,
    )
