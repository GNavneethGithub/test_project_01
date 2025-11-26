# my_main_project_folder/framework/monitoring_scripts/snowflake_connector.py

import snowflake.connector
import traceback
from typing import Optional
from utils.custom_logging import CustomChainLogger, setup_logger


def get_snowflake_connection(sf_con_parms: dict, logger: CustomChainLogger, QUERY_TAG: str = None):
    """
    Connects to Snowflake and returns a connection, cursor, and status.

    Args:
        sf_con_parms (dict): Dict with connection parameters:
            Required:
            - 'username': Snowflake username
            - 'password': Snowflake password
            - 'account': Snowflake account identifier
            
            Optional (Context):
            - 'role': Snowflake role
            - 'warehouse': Snowflake warehouse
            - 'database': Snowflake database
            - 'schema': Snowflake schema
            
            Optional (Timeouts):
            - 'login_timeout': Login timeout in seconds (default: 30)
            - 'network_timeout': Network timeout in seconds (default: 300)
            
        logger (CustomChainLogger): Our custom logger for logging.
        
        QUERY_TAG (str, optional): Query tag for tracking queries in Snowflake.
            If None, no QUERY_TAG will be set.
            If provided, session_parameters={'QUERY_TAG': QUERY_TAG} will be created internally.
            Example: 'PIPELINE_STATIC_CONFIG_TABLE_CREATION'

    Returns:
        dict: A dictionary containing:
            - 'conn': Snowflake connection object or None
            - 'cursor': Snowflake cursor object or None
            - 'continue_dag_run': True if connection successful, False otherwise
            - 'error_message': Error message if failed, None if successful
    
    Example:
        >>> sf_params = {
        ...     "username": "my_user",
        ...     "password": "my_pass",
        ...     "account": "my_account",
        ...     "role": "ETL_ROLE",
        ...     "warehouse": "ETL_WH",
        ...     "database": "PROD_DB",
        ...     "schema": "CONFIG"
        ... }
        >>> result = get_snowflake_connection(sf_params, logger, QUERY_TAG='MY_PIPELINE')
    """
    
    log = logger.new_frame("get_snowflake_connection")

    return_object = {
        'conn': None,
        'cursor': None,
        'continue_dag_run': False,
        'error_message': None
    }

    try:
        # Extract required connection parameters
        username = sf_con_parms.get("username")
        password = sf_con_parms.get("password")
        account = sf_con_parms.get("account")
        
        # Extract optional context parameters
        role = sf_con_parms.get("role")
        warehouse = sf_con_parms.get("warehouse")
        database = sf_con_parms.get("database")
        schema = sf_con_parms.get("schema")
        
        # Extract timeout parameters
        login_timeout = sf_con_parms.get("login_timeout", 30)
        network_timeout = sf_con_parms.get("network_timeout", 300)
        
        # Create session_parameters dictionary based on QUERY_TAG
        session_parameters = {}
        if QUERY_TAG is not None:
            session_parameters['QUERY_TAG'] = QUERY_TAG
        
        log.info(
            "SNOWFLAKE_CONNECTION",
            "Attempting to connect to Snowflake...",
            account=account,
            user=username,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            query_tag=QUERY_TAG,
            login_timeout=login_timeout,
            network_timeout=network_timeout
        )

        # Build connection parameters
        conn_params = {
            'user': username,
            'password': password,
            'account': account,
            'login_timeout': login_timeout,
            'network_timeout': network_timeout
        }
        
        # Add optional context parameters if provided
        if role:
            conn_params['role'] = role
        if warehouse:
            conn_params['warehouse'] = warehouse
        if database:
            conn_params['database'] = database
        if schema:
            conn_params['schema'] = schema
        
        # Add session_parameters only if QUERY_TAG is provided
        if session_parameters:  # Only non-empty dict
            conn_params['session_parameters'] = session_parameters

        # Connect to Snowflake
        log.info(
            "SNOWFLAKE_CONNECTION",
            "Initiating connection with all parameters..."
        )
        
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()

        log.info(
            "SNOWFLAKE_CONNECTION",
            "Successfully connected to Snowflake and created cursor.",
            session_id=conn.session_id,
            connection_id=conn.connection_id,
            query_tag_applied=QUERY_TAG
        )

        return_object['conn'] = conn
        return_object['cursor'] = cursor
        return_object['continue_dag_run'] = True
        
        return return_object


    except snowflake.connector.errors.DatabaseError as e:
        # This catches timeout errors and other database errors
        error_details = traceback.format_exc()
        
        # Check if it's a timeout error
        is_timeout = False
        timeout_type = "Unknown"
        
        if "timeout" in str(e).lower() or "timed out" in str(e).lower():
            is_timeout = True
            if "login" in str(e).lower() or "connect" in str(e).lower():
                timeout_type = "Login Timeout"
            else:
                timeout_type = "Network Timeout"
        
        error_msg_for_email = (
            f"Snowflake Connection Failed (DatabaseError)!\n\n"
            f"Error Location: get_snowflake_connection\n"
            f"Error Type: {type(e).__name__}\n"
            f"Is Timeout: {is_timeout}\n"
            f"Timeout Type: {timeout_type}\n\n"
            f"--- Connection Details ---\n"
            f"Account: {account}\n"
            f"User: {username}\n"
            f"Role: {role}\n"
            f"Warehouse: {warehouse}\n"
            f"Database: {database}\n"
            f"Schema: {schema}\n"
            f"Query Tag: {QUERY_TAG}\n"
            f"Login Timeout: {login_timeout} seconds\n"
            f"Network Timeout: {network_timeout} seconds\n"
            f"Session Parameters: {session_parameters}\n\n"
            f"--- Snowflake-Specific Details ---\n"
            f"Message: {e.msg if hasattr(e, 'msg') else str(e)}\n"
            f"Error Code: {e.errno if hasattr(e, 'errno') else 'N/A'}\n"
            f"SQLState: {e.sqlstate if hasattr(e, 'sqlstate') else 'N/A'}\n\n"
            f"--- Full Traceback ---\n{error_details}\n\n"
            f"--- Troubleshooting ---\n"
        )
        
        if is_timeout:
            if timeout_type == "Login Timeout":
                error_msg_for_email += (
                    f"Login timeout ({login_timeout}s) was exceeded.\n"
                    f"Possible causes:\n"
                    f"- Network connectivity issues\n"
                    f"- Snowflake service availability\n"
                    f"- Firewall blocking connection\n"
                    f"- Incorrect account identifier\n"
                    f"Recommendation: Increase login_timeout or check network/credentials.\n"
                )
            else:
                error_msg_for_email += (
                    f"Network timeout ({network_timeout}s) was exceeded.\n"
                    f"Possible causes:\n"
                    f"- Query taking too long\n"
                    f"- Network issues during operation\n"
                    f"- Large data transfer\n"
                    f"Recommendation: Increase network_timeout or optimize query.\n"
                )
        
        log.error(
            "SNOWFLAKE_CONNECTION",
            "Snowflake connection failed (DatabaseError).",
            exc_info=True,
            is_timeout=is_timeout,
            timeout_type=timeout_type,
            error_message=e.msg if hasattr(e, 'msg') else str(e),
            error_code=e.errno if hasattr(e, 'errno') else None,
            sql_state=e.sqlstate if hasattr(e, 'sqlstate') else None,
            login_timeout=login_timeout,
            network_timeout=network_timeout
        )

        return_object['error_message'] = error_msg_for_email
        return_object['continue_dag_run'] = False
        return return_object

    except snowflake.connector.Error as e:
        # This catches other Snowflake-specific errors
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Snowflake Connection Failed (SnowflakeError)!\n\n"
            f"Error Location: get_snowflake_connection\n"
            f"Error Type: {type(e).__name__}\n\n"
            f"--- Connection Details ---\n"
            f"Account: {account}\n"
            f"User: {username}\n"
            f"Role: {role}\n"
            f"Warehouse: {warehouse}\n"
            f"Database: {database}\n"
            f"Schema: {schema}\n"
            f"Query Tag: {QUERY_TAG}\n\n"
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
        return_object['continue_dag_run'] = False
        return return_object

    except Exception as e:
        # This is a catch-all for any *other* non-Snowflake error
        error_details = traceback.format_exc()
        error_msg_for_email = (
            f"Snowflake Connection Failed (Unexpected Error)!\n\n"
            f"Error Location: get_snowflake_connection\n"
            f"Error Type: {type(e).__name__}\n"
            f"Message: {str(e)}\n\n"
            f"--- Connection Details ---\n"
            f"Account: {account}\n"
            f"User: {username}\n"
            f"Role: {role}\n"
            f"Warehouse: {warehouse}\n"
            f"Database: {database}\n"
            f"Schema: {schema}\n"
            f"Query Tag: {QUERY_TAG}\n\n"
            f"--- Full Traceback ---\n{error_details}"
        )
        
        log.error(
            "SNOWFLAKE_CONNECTION",
            "An unexpected error occurred during connection.",
            exc_info=True,
            error_message=str(e)
        )
        
        return_object['error_message'] = error_msg_for_email
        return_object['continue_dag_run'] = False
        return return_object


# --- Example Usage ---

if __name__ == "__main__":
    
    base_logger = setup_logger()
    log_frame = CustomChainLogger(base_logger).new_frame("main_test_script")

    log_frame.info("MAIN_TEST", "--- STARTING CONNECTION TESTS ---")
    
    # Example 1: Full configuration with QUERY_TAG
    print("\n=== Test 1: Full configuration with QUERY_TAG ===")
    sf_params_full = {
        "username": "MY_USER",
        "password": "MY_PASSWORD",
        "account": "xy12345",
        "role": "ETL_ROLE",
        "warehouse": "ETL_WH",
        "database": "PROD_DB",
        "schema": "CONFIG",
        "login_timeout": 60,
        "network_timeout": 300
    }
    
    result1 = get_snowflake_connection(sf_params_full, log_frame, QUERY_TAG='MY_PIPELINE_TAG')
    print(f"Continue: {result1['continue_dag_run']}")
    if not result1['continue_dag_run']:
        print(f"Error snippet: {result1['error_message'][:200]}...")
    
    # Example 2: With QUERY_TAG = None (no query tag set)
    print("\n=== Test 2: With QUERY_TAG = None ===")
    sf_params_minimal = {
        "username": "MY_USER",
        "password": "MY_PASSWORD",
        "account": "xy12345"
    }
    
    result2 = get_snowflake_connection(sf_params_minimal, log_frame, QUERY_TAG=None)
    print(f"Continue: {result2['continue_dag_run']}")
    if not result2['continue_dag_run']:
        print(f"Error snippet: {result2['error_message'][:200]}...")
    
    # Example 3: Without passing QUERY_TAG (defaults to None)
    print("\n=== Test 3: Without passing QUERY_TAG parameter ===")
    sf_params_context = {
        "username": "MY_USER",
        "password": "MY_PASSWORD",
        "account": "xy12345",
        "role": "ETL_ROLE",
        "warehouse": "ETL_WH",
        "database": "PROD_DB",
        "schema": "CONFIG"
    }
    
    result3 = get_snowflake_connection(sf_params_context, log_frame)
    print(f"Continue: {result3['continue_dag_run']}")
    if not result3['continue_dag_run']:
        print(f"Error snippet: {result3['error_message'][:200]}...")
    
    # Example 4: With just QUERY_TAG (no additional session parameters needed)
    print("\n=== Test 4: With QUERY_TAG ===")
    sf_params_query_tag = {
        "username": "MY_USER",
        "password": "MY_PASSWORD",
        "account": "xy12345",
        "role": "ETL_ROLE",
        "warehouse": "ETL_WH"
    }
    
    result4 = get_snowflake_connection(sf_params_query_tag, log_frame, QUERY_TAG='ANOTHER_PIPELINE')
    print(f"Continue: {result4['continue_dag_run']}")
    if not result4['continue_dag_run']:
        print(f"Error snippet: {result4['error_message'][:200]}...")
    
    print("\n=== All Tests Complete ===")