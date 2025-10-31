# snowflake_connector.py

import logging
from typing import Dict, Any, Optional

import snowflake.connector
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeError

LOGGER_NAME = "[SnowflakeConnection]"

def get_snowflake_connection(
    user: str,
    password: str,
    account: str,
    database: str,
    schema: str,
    role: Optional[str] = None,
    warehouse: Optional[str] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Establishes a connection to Snowflake, with type hints for clarity.
    """
    log = logger or logging.getLogger(LOGGER_NAME)

    try:
        log.info(f"Attempting to establish connection for user '{user}'...")
        conn: SnowflakeConnection = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            database=database,
            schema=schema,
            role=role,
            warehouse=warehouse
        )
        # MODIFIED LINE: Create a more detailed success log message
        log.info(
            f" Connection successful. "
            f"User: {conn.user}, Role: {conn.role}, "
            f"Database: {conn.database}, Schema: {conn.schema}"
        )
        return {"status": "success", "connection": conn, "error": None}
    except SnowflakeError as e:
        log.exception("Error connecting to Snowflake.")
        return {"status": "failure", "connection": None, "error": e}