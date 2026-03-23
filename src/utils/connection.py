import duckdb
import logging
from contextlib import contextmanager

from src.config.settings import settings

logger = logging.getLogger(__name__)


def get_duckdb_connection():
    logger.info("Creating DuckDB connection")

    try:
        settings.validate_required("connection_string")
        connection_string = settings.get_required("connection_string")

        conn = duckdb.connect()

        conn.sql("INSTALL azure; LOAD azure;")
        conn.sql("INSTALL delta; LOAD delta;")
        
        conn.sql(f"""
            SET azure_storage_connection_string='{connection_string}';
        """)

        logger.info("DuckDB connection created successfully")
        return conn

    except Exception:
        logger.exception("Failed to create DuckDB connection")
        raise


@contextmanager
def duckdb_connection():
    conn = get_duckdb_connection()
    try:
        yield conn
    finally:
        try:
            conn.close()
            logger.info("DuckDB connection closed")
        except Exception:
            logger.warning("Failed to close DuckDB connection")