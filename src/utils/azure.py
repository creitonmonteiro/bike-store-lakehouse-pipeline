import logging
from src.config.settings import settings

logger = logging.getLogger(__name__)


def create_azure_secret(conn):
    settings.validate_required("connection_string")
    connection_string = settings.get_required("connection_string")

    try:
        conn.sql("""
            CREATE OR REPLACE SECRET azure_secret (
                TYPE AZURE,
                PROVIDER CONFIG,
                CONNECTION_STRING '{}'
            );
        """.format(connection_string))
    except Exception as e:
        logger.exception(f"Error creating Azure secret: {e}")
        raise

def register_table(conn, container, table):
    settings.validate_required("account_name")
    account_name = settings.get_required("account_name")

    path = f"abfs://{container}@{account_name}.dfs.core.windows.net/sales/{table}"

    conn.execute(f"""
        CREATE OR REPLACE VIEW {container}.{table} AS
        SELECT * FROM delta_scan('{path}')
    """)


def ensure_watermark_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gold.control_watermark (
            table_name VARCHAR,
            last_processed_date DATE
        )
    """)


def get_last_processed_date(conn, table_name):

    result = conn.execute(f"""
        SELECT MAX(last_processed_date)
        FROM gold.control_watermark
        WHERE table_name = '{table_name}'
    """).fetchone()[0]

    return result if result else "1900-01-01"
