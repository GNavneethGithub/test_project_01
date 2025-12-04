# utils/db_utils.py

from typing import Optional
from snowflake.connector import SnowflakeCursor
from snowflake.connector import SnowflakeConnection
from utils.custom_logging import CustomChainLogger

def safe_close_cursor_and_conn(
    cursor: Optional[SnowflakeCursor],
    conn: Optional[SnowflakeConnection],
    log: CustomChainLogger,
    log_key: str
) -> None:
    """
    Safely close cursor and connection without raising.

    - cursor: may be None or a Snowflake cursor
    - conn: may be None or a Snowflake connection
    - log: CustomChainLogger (use logger.new_frame(...) by caller)
    - log_key: the immutable logging keyword for this operation (e.g. "PIPELINE_RUN_TRACKING_TABLE_CREATION")

    This will attempt to close cursor first, then connection. Any exception during close is caught
    and logged as an error with the provided log_key. No exception is propagated.
    """
    # Close cursor first
    if cursor is not None:
        try:
            cursor.close()
            log.info(log_key, "Cursor closed.")
        except Exception as e:
            # Never raise; log the error
            try:
                log.error(log_key, "Error closing cursor.", error_message=str(e))
            except Exception:
                # last-resort: swallow silently so cleanup never fails
                pass

    # Then close connection
    if conn is not None:
        try:
            conn.close()
            log.info(log_key, "Connection closed.")
        except Exception as e:
            try:
                log.error(log_key, "Error closing connection.", error_message=str(e))
            except Exception:
                pass






