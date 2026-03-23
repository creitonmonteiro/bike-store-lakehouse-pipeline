from __future__ import annotations

import logging
from typing import Any

from src.config.queries.aggregate import AGG_CONFIGS
from src.config.settings import settings
from src.helpers.delta_writer import write_delta
from src.utils.azure import create_azure_secret
from src.utils.connection import duckdb_connection

logger = logging.getLogger(__name__)


def validate_analytics_settings() -> tuple[str, str, str]:
    """Validate settings required to run analytics outputs."""

    settings.validate_required("account_name", "gold_container", "root")

    account_name = settings.get_required("account_name")
    gold_container = settings.get_required("gold_container")
    root = settings.get_required("root")

    return account_name, gold_container, root


def run_analytics(conn: Any) -> None:
    """Execute and persist configured aggregate analytics tables."""

    try:
        
        account_name, gold_container, root = validate_analytics_settings()
        gold_path = f"abfs://{gold_container}@{account_name}.dfs.core.windows.net/{root}"

        for agg in AGG_CONFIGS:

            query = agg.query.format(gold_path=gold_path)

            logger.info("Processing: %s", agg.table_name)
            df = conn.execute(query).df()

            if df.empty:
                logger.info("%s: no data", agg.table_name)
                continue

            path_name = f"{gold_path}/{agg.table_name}"

            write_delta(
                path_name,
                df,
                mode="overwrite",
                settings=settings
            )
        
    except Exception as exc:
        logger.exception("Error running analytics | error=%s", exc)
        raise


def ingest_analytics() -> None:
    """Entry point for analytics ingestion using DuckDB and Azure secret."""

    with duckdb_connection() as conn:
        create_azure_secret(conn)
        run_analytics(conn)
