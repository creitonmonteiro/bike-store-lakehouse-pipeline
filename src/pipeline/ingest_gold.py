from __future__ import annotations

import logging
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from typing import Any

import pyarrow as pa
from deltalake import DeltaTable

from src.config.queries.gold import GoldQueryConfig
from src.config.settings import settings
from src.helpers.delta_writer import write_delta
from src.utils.azure import (
    create_azure_secret,
    ensure_watermark_table,
    get_last_processed_date,
    register_table,
)
from src.utils.connection import duckdb_connection
from src.utils.duckedb import ensure_schemas
from src.utils.schema.schema_loader import load_schema_registry

logger = logging.getLogger(__name__)
DEFAULT_DIMENSION_WORKERS = 4


def qualify_expression_columns(expression: str, columns: list[str], alias: str) -> str:
    """Qualify column names inside a SQL expression using a table alias."""

    qualified_expression = expression

    for column_name in sorted(columns, key=len, reverse=True):
        qualified_expression = re.sub(
            rf"\b{re.escape(column_name)}\b",
            f"{alias}.{column_name}",
            qualified_expression,
        )

    qualified_expression = re.sub(
        rf"NULLIF\(\s*({alias}\.\w+)\s*,\s*'NULL'\s*\)",
        r"\1",
        qualified_expression,
    )

    return qualified_expression


def build_orders_derived_columns_select(schema_registry: dict[str, Any]) -> str:
    """Build the SELECT fragment for orders derived columns from schema metadata."""

    orders_schema = schema_registry.get("orders", {})
    derived_columns = orders_schema.get("derived_columns", {})

    if not derived_columns:
        return ""

    order_columns = list(orders_schema.get("columns", {}).keys())
    if not order_columns:
        return ""

    rendered_columns: list[str] = []
    for derived_name, derived_expression in derived_columns.items():
        qualified_expression = qualify_expression_columns(
            str(derived_expression),
            order_columns,
            "o",
        )
        rendered_columns.append(f"            {qualified_expression} AS {derived_name}")

    return ",\n" + ",\n".join(rendered_columns)


def validate_gold_settings() -> tuple[str, dict[str, str], str, str]:
    """Validate mandatory settings and return storage options for gold layer."""

    settings.validate_required("account_name", "access_key", "gold_container", "root")

    account_name = settings.get_required("account_name")
    access_key = settings.get_required("access_key")
    gold_container = settings.get_required("gold_container")
    root = settings.get_required("root")

    storage_options = {
        "account_name": account_name,
        "access_key": access_key,
    }

    return account_name, storage_options, gold_container, root


def upsert_watermark(conn: Any, table_name: str, processed_date: date) -> None:
    """Replace the watermark value for a given table name."""

    conn.execute(
        f"""
        DELETE FROM gold.control_watermark
        WHERE table_name = '{table_name}'
        """
    )
    conn.execute(
        f"""
        INSERT INTO gold.control_watermark VALUES
        ('{table_name}', DATE '{processed_date}')
        """
    )


def normalize_tables(
    conn: Any,
    gold_path: str,
    silver_table: str,
    gold_table: str,
    query: str,
    storage_options: dict[str, str],
) -> None:
    """Upsert one gold dimension table from its silver source query."""

    dim_path = f"{gold_path}/{gold_table}"

    df = conn.execute(query).df()

    if df.empty:
        logger.info("[%s]: no data", silver_table)
        return
    
    df = df.drop_duplicates(subset=[f"{silver_table}_id"])
    
    df[f"{silver_table}_key"] = df[f"{silver_table}_id"]

    table_df = pa.Table.from_pandas(df)

    try:
        dt = DeltaTable(dim_path, storage_options=storage_options)
    except Exception:
        write_delta(
            dim_path,
            table_df,
            mode="overwrite",
            settings=settings,
        )

        logger.info("[%s] created", gold_table)
        return

    dt.merge(
        source=table_df,
        predicate=f"target.{silver_table}_id = source.{silver_table}_id",
        source_alias="source",
        target_alias="target"
    ).when_matched_update_all() \
     .when_not_matched_insert_all() \
     .execute()

    logger.info("[%s] updated", gold_table)


def normalize_dim_date(conn: Any, gold_path: str, storage_options: dict[str, str]) -> None:
    """Build and upsert the dim_date table incrementally."""

    table_name = "dim_date"
    dim_path = f"{gold_path}/{table_name}"

    ensure_watermark_table(conn)

    last_date = get_last_processed_date(conn, table_name)

    df = conn.execute(f"""
        SELECT DISTINCT
            order_date AS full_date
        FROM silver.orders
        WHERE order_date > DATE '{last_date}' - INTERVAL '7 days'
    """).df()

    if df.empty:
        logger.info(f"{table_name} no new data")
        return

    df["date_key"] = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["year"] = df["full_date"].dt.year
    df["month"] = df["full_date"].dt.month
    df["day"] = df["full_date"].dt.day
    df["quarter"] = df["full_date"].dt.quarter
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6])
    df["month_name"] = df["full_date"].dt.month_name()
    df["day_name"] = df["full_date"].dt.day_name()
    df["week_of_year"] = df["full_date"].dt.isocalendar().week
    df["is_month_start"] = df["full_date"].dt.is_month_start
    df["is_month_end"] = df["full_date"].dt.is_month_end

    table = pa.Table.from_pandas(df)

    try:
        dt = DeltaTable(dim_path, storage_options=storage_options)
    except Exception:

        write_delta(
            dim_path,
            table,
            mode="overwrite",
            settings=settings,
        )

        max_date = df["full_date"].max().date()
        upsert_watermark(conn, table_name, max_date)
        logger.info(f"[{table_name}] created and watermark updated")
        return

    dt.merge(
        source=table,
        predicate="target.date_key = source.date_key",
        source_alias="source",
        target_alias="target"
    ).when_matched_update_all() \
     .when_not_matched_insert_all() \
     .execute()

    max_date = df["full_date"].max().date()
    upsert_watermark(conn, table_name, max_date)
    logger.info(f"[{table_name}] updated to {max_date}")


def normalize_fact_sales(
    conn: Any,
    gold_path: str,
    fact_sales_query: str,
    storage_options: dict[str, str],
) -> None:
    """Build and upsert fact_sales with dynamic derived columns from orders."""

    table_name = "fact_sales"

    fact_path = f"{gold_path}/{table_name}"

    df = conn.execute(fact_sales_query).df()

    if df.empty:
        logger.info(f"{table_name}: no data")
        return
    
    df["customer_key"] = df["customer_key"].astype(str)
    df["store_key"] = df["store_key"].astype(str)
    
    df = df.drop_duplicates(
        subset=["order_id", "product_id", "customer_key", "store_key"]
    )
    
    table = pa.Table.from_pandas(df)

    try:
        dt = DeltaTable(fact_path, storage_options=storage_options)
    except Exception:

        write_delta(
            fact_path,
            table,
            mode="overwrite",
            settings=settings,
        )

        logger.info(f"[{table_name}] created")
        return

    dt.merge(
        source=table,
        predicate="""
            target.order_id = source.order_id
            AND target.product_id = source.product_id
            AND target.customer_key = source.customer_key
            AND target.store_key = source.store_key
        """,
        source_alias="source",
        target_alias="target"
    ).when_matched_update_all() \
     .when_not_matched_insert_all() \
     .execute()
    
    max_date_key = int(df["date_key"].max())
    max_date = datetime.strptime(str(max_date_key), "%Y%m%d").date()
    upsert_watermark(conn, table_name, max_date)

    logger.info(f"[{table_name}] updated")


def normalize_dimension_table_task(
    silver_table: str,
    gold_table: str,
    query: str,
    gold_path: str,
    storage_options: dict[str, str],
    silver_tables: list[str],
) -> None:
    """Execute one dimension normalization task using an isolated connection."""

    logger.info("[%s] Dimension normalization started", gold_table)

    with duckdb_connection() as conn:
        create_azure_secret(conn)
        ensure_schemas(conn)

        for table in silver_tables:
            register_table(conn, "silver", table)

        normalize_tables(
            conn,
            gold_path,
            silver_table,
            gold_table,
            query,
            storage_options,
        )

    logger.info("[%s] Dimension normalization finished", gold_table)


def ingest_gold(max_workers: int = DEFAULT_DIMENSION_WORKERS) -> None:
    """Run the full gold ingestion pipeline with parallel dimension processing."""

    account_name, storage_options, gold_container, root = validate_gold_settings()
    gold_path = f"abfs://{gold_container}@{account_name}.dfs.core.windows.net/{root}"

    queries = GoldQueryConfig()
    schema_registry = load_schema_registry()
    orders_derived_columns = build_orders_derived_columns_select(schema_registry)

    silver_tables = [
        "orders",
        "customers",
        "staffs",
        "stores",
        "categories",
        "brands",
        "products",
        "order_items",
    ]
    gold_tables = ["dim_date", "dim_customer", "dim_store", "dim_staff"]

    normalizing_tables = ["customer", "staff", "store", "product"]
    dimension_specs = [
        (
            dim_table,
            f"dim_{dim_table}",
            getattr(queries, f"dim_{dim_table}"),
        )
        for dim_table in normalizing_tables
    ]

    with duckdb_connection() as conn:

        create_azure_secret(conn)
        ensure_schemas(conn)

        for s_table in silver_tables:
            register_table(conn, "silver", s_table)
            logger.info("[%s] Registered", s_table)

        normalize_dim_date(conn, gold_path, storage_options)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    normalize_dimension_table_task,
                    silver_table,
                    gold_table,
                    query,
                    gold_path,
                    storage_options,
                    silver_tables,
                )
                for silver_table, gold_table, query in dimension_specs
            ]

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    logger.exception("Error in parallel dimension normalization")
                    raise

        for g_table in gold_tables:
            register_table(conn, "gold", g_table)
            logger.info("[%s] Registered", g_table)

        last_date = get_last_processed_date(conn, "fact_sales")

        query_sales = queries.fact_sales.format(
            last_date=str(last_date)[:10],
            orders_derived_columns=orders_derived_columns,
        )

        normalize_fact_sales(conn, gold_path, query_sales, storage_options)

    logger.info("Gold ingestion finished")
