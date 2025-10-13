from typing import Dict
import re
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_VALID_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_ident(name: str):
    if not _VALID_IDENT.match(name):
        raise ValueError(f"invalid identifier: {name!r}")


def count_rows_for_range(
    mysql_config: Dict,
    start_ts: str,
    end_ts: str,
    table_name: str,
    ts_col: str,
    *,
    connect_timeout: int = 10,
) -> int:
    """
    Return the number of rows in `table_name` whose `ts_col` is BETWEEN start_ts AND end_ts.

    Inputs:
      - mysql_config: dict with keys:
          host, user, password, database (or db). Optional: port, driver ('pymysql' default).
      - start_ts, end_ts: ISO timezone-aware strings.
      - table_name: table name (no schema). Only alnum + underscore allowed.
      - ts_col: timestamp column name. Only alnum + underscore allowed.
      - connect_timeout: optional DB connect timeout in seconds.

    Raises ValueError on invalid identifiers. Raises DB-related exceptions on connection/query errors.
    """
    # validate idents to avoid injection
    _validate_ident(table_name)
    _validate_ident(ts_col)

    driver = mysql_config.get("driver", "pymysql")
    user = mysql_config["user"]
    pw = mysql_config["password"]
    host = mysql_config["host"]
    port = mysql_config.get("port", 3306)
    database = mysql_config.get("database") or mysql_config.get("db") or None

    # build engine URL
    engine_url = f"mysql+{driver}://{user}:{pw}@{host}:{port}"
    if database:
        engine_url = engine_url + f"/{database}"

    engine: Engine = create_engine(
        engine_url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": connect_timeout},
    )

    # fully qualified table if database provided
    if database:
        table_ident = f"`{database}`.`{table_name}`"
    else:
        table_ident = f"`{table_name}`"

    sql = text(f"SELECT COUNT(*) AS cnt FROM {table_ident} WHERE `{ts_col}` BETWEEN :start_ts AND :end_ts")

    with engine.connect() as conn:
        result = conn.execute(sql, {"start_ts": start_ts, "end_ts": end_ts})
        # scalar() returns first column from first row
        cnt = result.scalar()
        # ensure integer
        return int(cnt or 0)
