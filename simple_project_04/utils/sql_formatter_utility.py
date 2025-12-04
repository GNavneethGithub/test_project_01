
# utils/sql_formatter_utility.py
from typing import Dict, Any, Optional


def format_and_print_sql_query(sql_template: str, query_params: Dict[str, Any], logger: Optional[Any] = None) -> str:
    """
    Formats a SQL query template with parameters and prints it for display purposes.

    If a logger (CustomChainLogger) is provided, emits a debug log containing the
    formatted SQL and parameters instead of printing to stdout. Otherwise, keeps
    the previous print behavior for manual testing.

    Args:
        sql_template (str): SQL query template with %(parameter_name)s placeholders.
        query_params (Dict[str, Any]): Dictionary of parameter names and their values.
        logger (Optional[Any]): Optional logger (must implement debug(keyword, msg, **kwargs))

    Returns:
        str: Formatted SQL query with all parameters replaced by their actual values.
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

    # Emit debug log if logger provided; otherwise print for manual testing
    if logger is not None:
        try:
            # Use a fixed log keyword for this utility (keeps logs searchable)
            log_keyword = "SQL_FORMATTER_UTILITY"
            logger.debug(log_keyword, "Formatted SQL query for preview.", sql=formatted_query, params=query_params)
        except Exception:
            # Fallback to printing if logger fails for any reason
            print("\n" + "=" * 80)
            print("SQL QUERY FOR MANUAL TESTING")
            print("=" * 80)
            print(formatted_query)
            print("=" * 80 + "\n")
    else:
        print("\n" + "=" * 80)
        print("SQL QUERY FOR MANUAL TESTING")
        print("=" * 80)
        print(formatted_query)
        print("=" * 80 + "\n")

    return formatted_query






