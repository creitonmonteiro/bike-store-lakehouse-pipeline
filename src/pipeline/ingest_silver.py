from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from src.config.settings import settings
from src.helpers.delta_writer import write_delta
from src.utils.azure import create_azure_secret
from src.utils.connection import duckdb_connection
from src.utils.schema.schema_loader import load_schema_registry

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY = load_schema_registry()
DEFAULT_MAX_WORKERS = 4
MIN_WATERMARK = "1900-01-01"


def validate_silver_settings() -> tuple[str, dict[str, str], str, str, str]:
    """Validate mandatory settings and return normalized silver runtime config."""

    settings.validate_required(
        "account_name",
        "access_key",
        "root",
        "bronze_container",
        "silver_container",
    )

    account_name = settings.get_required("account_name")
    access_key = settings.get_required("access_key")
    root = settings.get_required("root")
    bronze_container = settings.get_required("bronze_container")
    silver_container = settings.get_required("silver_container")

    storage_options = {
        "account_name": account_name,
        "access_key": access_key,
    }

    return account_name, storage_options, root, bronze_container, silver_container


def get_partition(table: str, registry: dict[str, dict[str, Any]]) -> list[str]:
    """Return the partition configuration for a table."""

    if table not in registry:
        raise KeyError(f"Partition is not defined for table: {table}")

    return registry[table]["partition_by"]

def build_silver_transform(table: str, registry: dict[str, dict[str, Any]]) -> str:
    """Build cast and cleansing expressions for silver columns."""
    
    columns = registry[table]["columns"]

    exprs = []

    for col, dtype in columns.items():

        base = f"TRY_CAST(b.{col} AS {dtype})"

        if dtype.lower() == "varchar":

            expr = f"""
                LOWER(
                    TRIM(
                        REGEXP_REPLACE(
                            TRANSLATE(
                                NULLIF(
                                    CASE
                                        WHEN LOWER(TRIM({base})) IN ('','na','n/a','null','-')
                                        THEN NULL
                                        ELSE {base}
                                    END,
                                ''),
                                'ÁÀÂÃÄÅáàâãäåÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
                                'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'
                            ),
                            '\\s+',
                            ' '
                        )
                    )
                ) AS {col}
                """
        else:
            expr = f"{base} AS {col}"

        exprs.append(expr)

    return ",\n".join(exprs)


def build_row_hash(table: str, registry: dict[str, dict[str, Any]]) -> str:
    """Build a deterministic row hash expression for change detection."""

    columns = registry[table]["columns"].keys()

    cols = [f"COALESCE(CAST({c} AS VARCHAR), '<<NULL>>')" for c in columns]

    return f"""
    md5(
        concat_ws(
            '||',
            {', '.join(cols)}
        )
    ) AS row_hash
    """


def build_data_quality_rule(table: str, registry: dict[str, dict[str, Any]]) -> str:
    """Build the SQL WHERE clause fragment from configured quality rules."""

    if "data_quality" not in registry[table]:
        return ""
    
    rules = registry[table]["data_quality"]

    conditions = []

    for col, rule in rules.items():
        conditions.append(f"{col} {rule}")

    if not conditions:
        return ""
    
    return "AND " + " AND ".join(conditions)


def get_query_columns_dataset(row_hash: str) -> str:
    """Build the final SELECT list for the silver dataset."""
    
    select_parts = ["*"]

    select_parts.append(row_hash)
    select_parts.append("EXTRACT(YEAR FROM _ingestion_timestamp) AS year")
    select_parts.append("EXTRACT(MONTH FROM _ingestion_timestamp) AS month")

    return ",\n".join(select_parts)


def create_watermark(watermark_path: str) -> None:
    """Create an empty watermark table when it does not exist."""

    try:        
        schema = pa.schema([
            ("table_name", pa.string()),
            ("last_watermark", pa.timestamp("us"))
        ])

        empty_table = pa.Table.from_arrays(
            [pa.array([], type=pa.string()),
            pa.array([], type=pa.timestamp("us"))],
            schema=schema
        )

        write_delta(
            watermark_path, 
            empty_table, 
            mode="overwrite",
            settings=settings
        )
    except Exception as exc:
        logger.exception("Error creating watermark at %s | error=%s", watermark_path, exc)
        raise


def get_watermark(conn: Any, table: str, watermark_path: str) -> str:
    """Get the latest processed watermark for a table."""

    try:
        result = conn.sql(f"""
            SELECT last_watermark
            FROM delta_scan('{watermark_path}')
            WHERE table_name = '{table}'
        """).fetchall()

        if len(result) == 0:
            return MIN_WATERMARK

        return result[0][0]
    
    except Exception as exc:
        logger.exception("Error selecting watermark for %s | error=%s", table, exc)
        raise


def update_watermark(table: str, arrow_table: pa.Table, watermark_path: str) -> None:
    """Persist the newest ingestion timestamp for a processed table."""

    try:
        if arrow_table.num_rows == 0:
            return

        max_ts = max(
            arrow_table.column("_ingestion_timestamp").to_pylist()
        )

        watermark_table = pa.Table.from_pydict({
            "table_name": [table],
            "last_watermark": [max_ts]
        })

        write_delta(
            watermark_path, 
            watermark_table, 
            mode="append",
            settings=settings
        )

    except Exception as exc:
        logger.exception("Error updating watermark for %s | error=%s", table, exc)
        raise


def delta_exists(path: str, storage_options: dict[str, str] | None = None) -> bool:
    """Return True when a Delta table exists at the given path."""

    try:
        DeltaTable(path, storage_options=storage_options)
        return True
    except Exception:
        return False


def merge_delta(
    arrow_table: pa.Table,
    silver_path: str,
    pk: list[str],
    storage_options: dict[str, str],
) -> None:
    """Merge incoming silver rows into an existing Delta table."""

    if arrow_table.num_rows == 0:
        logger.info("No new data for merge")
        return

    dt = DeltaTable(
        silver_path,
        storage_options=storage_options,
    )

    predicate = " AND ".join(
        [f"t.{c} = s.{c}" for c in pk]
    )

    try:
        (
            dt.merge(
                source=arrow_table,
                predicate=predicate,
                source_alias="s",
                target_alias="t"
            )
            .when_matched_update_all(
                predicate="t.row_hash <> s.row_hash"
            )
            .when_not_matched_insert_all()
            .execute()
        )

    except Exception as exc:
        logger.exception("Error merging table at %s | error=%s", silver_path, exc)
        raise


def detect_pk(table: str, registry: dict[str, dict[str, Any]]) -> list[str]:
    """Return the configured primary key columns for a table."""

    if table not in registry:
        raise KeyError(f"PK is not defined for table: {table}")

    return registry[table]["primary_key"]


def mount_silver_dataset(
    conn: Any,
    bronze_path: str,
    watermark: str,
    pk_partition: str,
    silver_transform: str,
    query_columns: str,
    dq_rules: str,
) -> pa.Table:
    """Build and materialize the incremental silver dataset from bronze."""

    return conn.sql(f"""
        WITH bronze AS (
            SELECT *
            FROM delta_scan('{bronze_path}')
            WHERE _ingestion_timestamp > TIMESTAMP '{watermark}'
        ),
        bronze_dedup AS (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER(
                        PARTITION BY {pk_partition}
                        ORDER BY _ingestion_timestamp DESC
                    ) AS rn
                FROM bronze
            )
            WHERE rn = 1
        ),
        silver_base AS (
            SELECT
                {silver_transform},
                b._ingestion_timestamp
            FROM bronze_dedup b
        )
        SELECT
            {query_columns}
        FROM silver_base
        WHERE 1=1
        {dq_rules}
    """).arrow().read_all()


def silver_merge(
    conn: Any,
    table: str,
    root: str,
    registry: dict[str, dict[str, Any]],
    account_name: str,
    storage_options: dict[str, str],
    watermark_path: str,
    bronze_container: str,
    silver_container: str,
) -> None:
    """Run the full silver merge flow for a single table."""

    pk = detect_pk(table, registry)
    pk_partition = ", ".join(pk)

    bronze_path = (
        f"abfs://{bronze_container}@{account_name}.dfs.core.windows.net/{root}/{table}"
    )
    silver_path = (
        f"abfs://{silver_container}@{account_name}.dfs.core.windows.net/{root}/{table}"
    )

    if not delta_exists(watermark_path, storage_options):
        create_watermark(watermark_path)

    watermark = get_watermark(conn, table, watermark_path)

    silver_transform = build_silver_transform(table, registry)

    row_hash = build_row_hash(table, registry)

    dq_rules = build_data_quality_rule(table, registry)

    query_columns = get_query_columns_dataset(row_hash)

    arrow_dataset = mount_silver_dataset(
        conn,
        bronze_path,
        watermark,
        pk_partition,
        silver_transform,
        query_columns,
        dq_rules
    )

    if arrow_dataset.num_rows == 0:
        logger.info("No new data for table %s", table)
        return

    if not delta_exists(silver_path, storage_options):

        write_delta(
            silver_path, arrow_dataset, partition_by=get_partition(table, registry)
        )

        logger.info("Silver table created for %s", table)

    else:

        merge_delta(
            arrow_dataset,
            silver_path,
            pk,
            storage_options,
        )

    update_watermark(table, arrow_dataset, watermark_path)

    logger.info("%s rows ingested to silver table %s", arrow_dataset.num_rows, table)


def process_table(
    table: str,
    root: str,
    registry: dict[str, dict[str, Any]],
    account_name: str,
    storage_options: dict[str, str],
    watermark_path: str,
    bronze_container: str,
    silver_container: str,
) -> None:
    """Process one silver table with isolated connection and Azure secret."""

    try:
        logger.info("[%s] Starting", table)

        with duckdb_connection() as conn:
            create_azure_secret(conn)
            silver_merge(
                conn,
                table,
                root,
                registry,
                account_name,
                storage_options,
                watermark_path,
                bronze_container,
                silver_container,
            )

        logger.info("[%s] Finished", table)

    except Exception:
        logger.exception("[%s] Error", table)
        raise


def ingest_silver(max_workers: int = DEFAULT_MAX_WORKERS) -> None:
    """Run silver ingestion for all configured tables in parallel."""

    logger.info("Starting silver ingestion")

    account_name, storage_options, root, bronze_container, silver_container = validate_silver_settings()
    tables = list(SCHEMA_REGISTRY.keys())
    watermark_path = (
        f"abfs://{silver_container}@{account_name}.dfs.core.windows.net/metadata/pipeline_watermark"
    )

    if not tables:
        logger.warning("No tables found for silver ingestion")
        return
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_table,
                table,
                root,
                SCHEMA_REGISTRY,
                account_name,
                storage_options,
                watermark_path,
                bronze_container,
                silver_container,
            )
            for table in tables
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                logger.exception("Error in parallel execution")
                raise

    logger.info("Silver ingestion finished")
    