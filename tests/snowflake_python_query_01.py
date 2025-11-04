import snowflake.connector
from snowflake.connector import DictCursor 
from typing import List, Tuple, Dict, Any
import json
import os



def build_single_interval_query_date_column(start_timestamp_iso: str, end_timestamp_iso: str) -> str:
    """
    Builds a Snowflake SQL query fragment for a single interval, 
    guaranteeing a row_count of 0 if no records are found within the specific interval.
    """
    query = f"""
    SELECT
        '{start_timestamp_iso}'::TIMESTAMP_TZ::DATE AS target_day,
        COALESCE(
            (
                SELECT
                    COUNT(*) AS row_count
                FROM
                    tb1
                WHERE
                    c_ts >= '{start_timestamp_iso}'::TIMESTAMP_TZ
                    AND c_ts < '{end_timestamp_iso}'::TIMESTAMP_TZ
            ), 
            0
        ) AS row_count
    """
    return query

def build_union_all_query_from_intervals(intervals: List[Tuple[str, str]]) -> str:
    """
    Generates a full SQL query by creating a fragment for each interval 
    and combining them with UNION ALL.
    """
    if not intervals:
        return "" 

    all_query_fragments = []
    for start_ts, end_ts in intervals:
        fragment = build_single_interval_query_date_column(start_ts, end_ts)
        all_query_fragments.append(fragment)
        
    final_query = "\nUNION ALL\n".join(all_query_fragments)
    final_query += "\nORDER BY target_day;"
    
    return final_query

# --- Snowflake Connection and Execution Functions ---

def get_snowflake_connection(user, password, account, warehouse, database, schema):
    """Establishes and returns a connection object to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        print("Snowflake connection established successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise

def execute_query_and_fetch_dicts(conn, sql_query: str) -> List[Dict[str, Any]]:
    """
    Executes the given SQL query on the connection using a DictCursor 
    and returns the results as a list of dictionaries.
    Handles cursor and connection closure within this scope.
    """
    results = []
    # Use a try-finally block for cursor management within this function
    try:
        cur = conn.cursor(DictCursor) 
        print(f"Executing query...")
        cur.execute(sql_query)
        
        results = cur.fetchall()
        print(f"Query executed. Fetched {len(results)} rows.")
        
    except Exception as e:
        print(f"An error occurred during query execution: {e}")
        raise
    finally:
        if 'cur' in locals() and cur:
            cur.close()
            
    # Note: We do *not* close the main `conn` object here so it can be closed externally.
    return results

# --- Main Execution Flow ---

if __name__ == '__main__':
    # 1. Define the inputs
    user_input_intervals: List[Tuple[str, str]] = [
        ('2023-10-25T00:00:00-07:00', '2023-10-26T00:00:00-07:00'), 
        ('2023-10-26T00:00:00-07:00', '2023-10-27T00:00:00-07:00'), 
        ('2023-10-27T00:00:00-07:00', '2023-10-28T00:00:00-07:00')  
    ]

    # 2. Generate the dynamic SQL string
    final_sql_query = build_union_all_query_from_intervals(user_input_intervals)

    # 3. Use the connection and execution functions
    conn = None
    try:
        # Pass your credentials to the connection function
        conn = get_snowflake_connection(
            user='your_user',
            password='your_password',
            account='your_account_identifier', 
            warehouse='your_warehouse',
            database='your_database',
            schema='your_schema'
        )

        # Execute the query and get the final list of dictionaries
        final_results = execute_query_and_fetch_dicts(conn, final_sql_query)
        
        # Print the final output
        print("\n--- Final Output (Python List of Dictionaries) ---")
        print(json.dumps(final_results, indent=4, default=str))

    except Exception as e:
        print(f"Script execution failed: {e}")

    finally:
        if conn:
            conn.close()
            print("\nConnection closed.")
