# my_main_project_folder/framework/snowflake_connector.py

import snowflake.connector
import traceback
from utils.custom_logging import CustomChainLogger, setup_logger


def get_snowflake_connection(sf_con_parms: dict, logger: CustomChainLogger):
    """
    Connects to Snowflake and returns a connection, cursor, and status.

    Args:
        sf_con_parms (dict): Dict with 'username', 'password', 'account'.
        logger (CustomChainLogger): Our custom logger for logging.

    Returns:
        dict: A dictionary containing 'conn', 'cursor', 
              'continue_dag_run', and 'error_message'.
    """
    
    log = logger.new_frame("get_snowflake_connection")

    return_object = {
        'conn': None,
        'cursor': None,
        'continue_dag_run': False,
        'error_message': None
    }

    try:
        log.info(
            "SNOWFLAKE_CONNECTION",
            "Attempting to connect to Snowflake...",
            account=sf_con_parms.get("account"),
            user=sf_con_parms.get("username")
        )

        conn = snowflake.connector.connect(
            user=sf_con_parms.get("username"),
            password=sf_con_parms.get("password"),
            account=sf_con_parms.get("account"),
            login_timeout=30  # Timeout after 30 seconds if connection fails
        )
        cursor = conn.cursor()

        log.info(
            "SNOWFLAKE_CONNECTION",
            "Successfully connected to Snowflake and created cursor."
        )

        return_object['conn'] = conn
        return_object['cursor'] = cursor
        return_object['continue_dag_run'] = True
        
        return return_object


    except snowflake.connector.Error as e:
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Snowflake Connection Failed (SnowflakeError)!\n\n"
            f"Error Location: get_snowflake_connection\n"
            f"Error Type: {type(e).__name__}\n\n"
            f"--- Snowflake-Specific Details ---\n"
            f"Message: {e.msg}\n"
            f"Snowflake Error No: {e.errno}\n"
            f"SQLState: {e.sqlstate}\n"
            f"Snowflake Query ID (SFQID): {e.sfqid}\n\n"
            f"--- Full Traceback ---\n{error_details}"
        )
        

        log.error(
            "SNOWFLAKE_CONNECTION",
            "Snowflake connection failed.",
            exc_info=True, 
            error_message=e.msg,
            error_number=e.errno,
            sql_state=e.sqlstate,
            sfqid=e.sfqid
        )

        return_object['error_message'] = error_msg_for_email
        return return_object

    except Exception as e:
        # This is a catch-all for any *other* non-Snowflake error
        
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Snowflake Connection Failed (Unexpected Error)!\n\n"
            f"Error Location: get_snowflake_connection\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"Full Traceback:\n{error_details}"
        )
        
        log.error(
            "SNOWFLAKE_CONNECTION",
            "An unexpected error occurred during connection.",
            exc_info=True,
            error_message=str(e)
        )
        
        return_object['error_message'] = error_msg_for_email
        return return_object


# --- Example of how to use this new file ---

if __name__ == "__main__":
    
    base_logger = setup_logger()
    log_frame = CustomChainLogger(base_logger).new_frame("main_test_script")

    log_frame.info("MAIN_TEST", "--- STARTING FAILED CONNECTION TEST ---")
    
    bad_sf_params = {
        "username": "BAD_USER",
        "password": "BAD_PASSWORD",
        "account": "xy12345"  
    }
    
    result_fail = get_snowflake_connection(bad_sf_params, log_frame)
    
    print("\n--- RESULT OF FAILED TEST ---")
    print(result_fail)
    if not result_fail['continue_dag_run']:
        print("\n--- EMAIL MESSAGE WOULD BE ---")
        print(result_fail['error_message'])
    print("-----------------------------\n")