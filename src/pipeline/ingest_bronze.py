from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.config.settings import settings
from src.helpers.delta_writer import write_delta
from src.utils.connection import duckdb_connection
from src.utils.schema.schema_loader import load_schema_registry

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY = load_schema_registry()
DEFAULT_PATH_NAME = "data"
DEFAULT_MAX_WORKERS = 4


def validate_bronze_settings() -> None:
    """Validate mandatory configuration for bronze ingestion."""

    settings.validate_required(
        "landing_container",
        "bronze_container",
        "account_name",
        "access_key",
        "root",
    )


def build_query(table: str, path_name: str, batch_id: str, source_file: str) -> str:
    """Build the bronze extraction SQL for a table and source file."""

    if table not in SCHEMA_REGISTRY:
        raise KeyError(f"Schema not found for table: {table}")

    schema = SCHEMA_REGISTRY[table]
    landing_container = settings.get_required("landing_container")

    dq_rules = schema.get("data_quality", {})
    dq_conditions = " AND ".join([
        f"{col} {rule}"
        for col, rule in dq_rules.items()
    ]) or "1=1"

    query = f"""
        WITH source AS (
            SELECT *,
                md5(concat_ws('|', COLUMNS(*))) AS _row_hash
            FROM read_csv_auto('az://{landing_container}/{path_name}/{source_file}')
        )

        SELECT
            *,
            CURRENT_TIMESTAMP AS _ingestion_timestamp,
            '{batch_id}' AS _batch_id,
            '{source_file}' AS _source_file
        FROM source
        WHERE {dq_conditions}
    """

    return query


def build_bronze_path(table: str, root: str) -> str:
    """Build the destination bronze Delta path for a table."""

    bronze_container = settings.get_required("bronze_container")
    account_name = settings.get_required("account_name")
    return f"abfs://{bronze_container}@{account_name}.dfs.core.windows.net/{root}/{table}"


def bronze_load(conn: Any, table: str, path_name: str, root: str) -> None:
    """Load one table from landing CSV into the bronze layer."""

    batch_id = uuid.uuid4().hex
    source_file = f"{table}.csv"

    bronze_path = build_bronze_path(table, root)

    logger.info("Reading %s from landing", source_file)

    query = build_query(table, path_name, batch_id, source_file)

    arrow_table = conn.sql(query).arrow().read_all()

    partition_by = SCHEMA_REGISTRY[table].get("partition_by")

    write_delta(
        bronze_path,
        arrow_table,
        partition_by=partition_by,
        target_file_size=134217728,
        settings=settings
    )

    logger.info("%s rows ingested to bronze table %s", arrow_table.num_rows, table)


def process_table(table: str, path_name: str, root: str) -> None:
    """Process one bronze table inside an isolated DuckDB connection."""

    try:
        logger.info("[%s] Starting", table)

        with duckdb_connection() as conn:
            bronze_load(conn, table, path_name, root)

        logger.info("[%s] Finished", table)

    except Exception:
        logger.exception("[%s] Error", table)
        raise


def ingest_bronze(path_name: str = DEFAULT_PATH_NAME, max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """Run bronze ingestion for all configured tables in parallel."""

    logger.info("Starting bronze ingestion")

    validate_bronze_settings()
    root = settings.get_required("root")
    tables = list(SCHEMA_REGISTRY.keys())

    if not tables:
        logger.warning("No tables found for bronze ingestion")
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_table, table, path_name, root)
            for table in tables
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.exception("Error in parallel execution")
                raise

    logger.info("Bronze ingestion finished")

