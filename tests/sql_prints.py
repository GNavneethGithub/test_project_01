#!/usr/bin/env python3
# save as render_query_test.py and run: python render_query_test.py

from typing import Any, Dict
import textwrap
import re

def _render_literal(val: Any) -> str:
    """Render a Python value as a SQL literal for copy-paste debug output."""
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
    """
    Replace %(name)s placeholders with SQL literals from params.
    Prints the rendered SQL (dedented) and returns it as a string.
    Missing params leave the original placeholder untouched.
    """
    template = textwrap.dedent(query_template).strip()

    def _repl(m):
        name = m.group(1)
        return _render_literal(params[name]) if name in params else m.group(0)

    rendered = re.sub(r"%\(([^)]+)\)s", _repl, template)
    printable = f"{header}\n{rendered}"
    print(printable)
    return rendered

# Quick runnable tests/examples
if __name__ == "__main__":
    # Test 1: string with single quote and None
    q1 = """
        SELECT *
        FROM db.sch.tab
        WHERE c1 = %(c1_value)s
          AND c2 = %(c2_value)s
    """
    params1 = {"c1_value": "O'Reilly", "c2_value": None}
    out1 = render_and_print_query(q1, params1)
    assert "O''Reilly" in out1 and "NULL" in out1
    print("\nTest 1 passed.\n")

    # Test 2: missing param leaves placeholder
    q2 = "SELECT %(x)s, %(y)s"
    params2 = {"x": 1}
    out2 = render_and_print_query(q2, params2)
    assert "%(y)s" in out2
    print("Test 2 passed.\n")

    # Test 3: numbers and booleans
    q3 = """
        SELECT *
        FROM my.table
        WHERE n = %(num)s
          AND flag = %(flag)s
    """
    params3 = {"num": 42, "flag": True}
    out3 = render_and_print_query(q3, params3)
    assert "42" in out3 and "TRUE" in out3
    print("Test 3 passed.\n")

    print("All tests succeeded.")


# import textwrap

# # dedent for SQL templates (removes extra indentation)
# sql = textwrap.dedent("""
#     SELECT *
#     FROM my_db.my_schema.my_table
#     WHERE col = %(val)s
#     ORDER BY ts
# """).strip()
# print(sql)