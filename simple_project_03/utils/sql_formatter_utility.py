# my_main_project_folder/utils/sql_formatter_utility.py

from typing import Dict, Any


def format_and_print_sql_query(sql_template: str, query_params: Dict[str, Any]) -> str:
    """
    Formats a SQL query template with parameters and prints it for display purposes.
    
    This function takes a SQL template with %(parameter_name)s placeholders and
    actual parameter values, formats the query with all parameters replaced with
    their actual values, prints it in a readable format, and returns the formatted
    query. This formatted query can be easily copy-pasted into Snowflake for manual testing.
    
    Args:
        sql_template (str): SQL query template with %(parameter_name)s placeholders.
                           Example: "SELECT * FROM %(table_name)s WHERE id = %(id)s"
        query_params (Dict[str, Any]): Dictionary of parameter names and their values.
                                       Example: {"table_name": "my_table", "id": 123}
    
    Returns:
        str: Formatted SQL query with all parameters replaced by their actual values.
             Example: "SELECT * FROM my_table WHERE id = 123"
    
    Example:
        >>> sql = "SELECT * FROM %(table)s WHERE name = '%(name)s' AND age > %(age)s"
        >>> params = {"table": "users", "name": "John", "age": 30}
        >>> formatted = format_and_print_sql_query(sql, params)
        # This will print:
        # ================================================================================
        # SQL QUERY FOR MANUAL TESTING
        # ================================================================================
        # SELECT * FROM users WHERE name = 'John' AND age > 30
        # ================================================================================
    """
    
    formatted_query = sql_template
    
    # Iterate through all parameters and replace them in the query
    for param_name, param_value in query_params.items():
        placeholder = f"%({param_name})s"
        
        # Handle different data types appropriately
        if isinstance(param_value, str):
            # For strings, wrap in quotes
            replacement = f"'{param_value}'"
        elif param_value is None:
            # For None values, use SQL NULL
            replacement = "NULL"
        else:
            # For numbers, booleans, etc., use as-is
            replacement = str(param_value)
        
        # Replace all occurrences of the placeholder
        formatted_query = formatted_query.replace(placeholder, replacement)
    
    # Print the query in a readable format (copyable for testing)
    print("\n" + "=" * 80)
    print("SQL QUERY FOR MANUAL TESTING")
    print("=" * 80)
    print(formatted_query)
    print("=" * 80 + "\n")
    
    return formatted_query