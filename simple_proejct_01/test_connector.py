# test_connector.py

import unittest
import os
import logging
import sys
import time
from dotenv import load_dotenv
from snowflake.connector.errors import Error as SnowflakeError

from snowflake_connector import get_snowflake_connection

# 1. DEFINE A CUSTOM FORMATTER
# This class formats the timestamp to include both Local and UTC time.
class DualTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # Get the default local time string
        local_time_str = logging.Formatter.formatTime(self, record, datefmt)
        # Create a UTC time string
        utc_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(record.created))
        utc_time_str += f",{int(record.msecs):03d}"
        return f"{local_time_str} (Local) | {utc_time_str} (UTC)"

class TestSnowflakeConnection(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Load env variables and set up root logging to show all messages."""
        load_dotenv()

        # --- New Root Logger Configuration ---
        # 2. GET THE ROOT LOGGER
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        # Prevent adding duplicate handlers if tests are run multiple times
        if not root_logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            
            # 3. USE THE CUSTOM FORMATTER
            # Note the simpler format string, as the time is handled by the class now.
            formatter = DualTimeFormatter(
                '%(asctime)s - %(levelname)s:%(name)s:%(message)s'
            )
            handler.setFormatter(formatter)
            root_logger.addHandler(handler)
        
        # Optional: If the Snowflake library is too noisy, you can quiet it down
        # logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
        # --- End of New Configuration ---

        cls.creds = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
        if not all(cls.creds.values()):
            raise ValueError("Snowflake credentials not found in environment variables.")

    def test_successful_connection(self):
        """
        Tests that a valid connection can be established with correct credentials.
        """
        result = get_snowflake_connection(**self.creds)

        if result["status"] == "failure":
            self.fail(f"Connection failed with real credentials: {result['error']}")

        self.assertEqual(result["status"], "success")
        self.assertIsNone(result["error"])
        self.assertIsNotNone(result["connection"])

        conn = result["connection"]
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                self.assertEqual(cursor.fetchone()[0], 1)
        finally:
            conn.close()

    # # Make sure this test is also present in your file
    # def test_failed_connection_wrong_password(self):
    #     """
    #     Tests that the function handles a connection failure gracefully.
    #     """
    #     invalid_creds = self.creds.copy()
    #     invalid_creds["password"] = "this_is_a_wrong_password"

    #     result = get_snowflake_connection(**invalid_creds)

    #     self.assertEqual(result["status"], "failure")
    #     self.assertIsNone(result["connection"])
    #     self.assertIsNotNone(result["error"])
    #     self.assertIsInstance(result["error"], SnowflakeError)

if __name__ == '__main__':
    unittest.main()