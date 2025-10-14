from typing import Any, Dict, List
import re
import textwrap
from snowflake.connector.cursor import DictCursor

def _render_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int, float)):
        return str(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"

def render_and_print_query(query_template: str, params: Dict[str, Any],
                           header: str = "-- Debug SQL (copy-paste into Snowflake)") -> str:
    template = textwrap.dedent(query_template).strip()
    def _repl(m):
        name = m.group(1)
        return _render_literal(params[name]) if name in params else m.group(0)
    rendered = __import__("re").sub(r"%\(([^)]+)\)s", _repl, template)
    printable = f"{header}\n{rendered}"
    print(printable)
    return rendered

def fetch_sf_rows_as_dicts(sf_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Minimal error handling. Prints debug SQL. Raises RuntimeError with context on connection or execution failure.
    Requires get_sf_connection(sf_config) in scope.
    Expected keys: "table_name", "c1_value", "c2_value". Optional "limit_n".
    """
    fn = "fetch_sf_rows_as_dicts"
    table = sf_config.get("table_name")
    if not isinstance(table, str) or not re.match(r'^[A-Za-z0-9_.]+$', table):
        raise ValueError(f"{fn}: invalid table_name")

    params = {"c1_value": sf_config.get("c1_value"),
              "c2_value": sf_config.get("c2_value")}
    limit_n = int(sf_config.get("limit_n", 1000))

    exec_query = textwrap.dedent(f"""
        SELECT *
        FROM {table}
        WHERE c1 = %(c1_value)s
          AND c2 = %(c2_value)s
        ORDER BY c3
        LIMIT {limit_n}
    """).strip()

    printable = render_and_print_query(exec_query, params)

    # minimal: only catch connection errors to add context
    try:
        conn = get_sf_connection(sf_config)
    except Exception as e:
        raise RuntimeError(f"{fn}: failed to obtain Snowflake connection for table={table}") from e

    cur = conn.cursor(DictCursor)
    try:
        try:
            cur.execute(exec_query, params)
        except Exception as e:
            preview = printable 
            raise RuntimeError(f"""{fn}: query execution failed for table={table}; preview:\n{preview}""") from e

        rows = cur.fetchall()
        print(f"{fn}: fetched {len(rows)} rows")  # debug print
        return rows
    finally:
        cur.close()
        conn.close()
