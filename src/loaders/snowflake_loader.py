"""Snowflake bulk loader with MERGE-based upsert semantics."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def _quote_identifier(value: str) -> str:
    value = value.upper()
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(f"Invalid Snowflake identifier: {value!r}")
    return f'"{value}"'


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str | None = None


class SnowflakeLoader:
    """Thin wrapper around snowflake-connector-python for idempotent loads."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self.connection = None

    @classmethod
    def from_env(cls) -> "SnowflakeLoader":
        required = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "SECURITY_MASTER"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }
        missing = [key for key in ("account", "user", "password") if not required[key]]
        if missing:
            raise ValueError(
                f"Missing Snowflake environment variables: {', '.join(missing)}"
            )
        return cls(SnowflakeConfig(**required))

    def __enter__(self) -> "SnowflakeLoader":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def connect(self) -> None:
        if self.connection is not None:
            return

        import snowflake.connector

        kwargs = {
            "account": self.config.account,
            "user": self.config.user,
            "password": self.config.password,
            "warehouse": self.config.warehouse,
            "database": self.config.database,
            "schema": self.config.schema,
        }
        if self.config.role:
            kwargs["role"] = self.config.role

        self.connection = snowflake.connector.connect(**kwargs)
        logger.info(
            "Connected to Snowflake database=%s schema=%s",
            self.config.database,
            self.config.schema,
        )

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def upsert_dataframe(
        self, dataframe: pd.DataFrame, table: str, merge_keys: Iterable[str]
    ) -> None:
        """Write a dataframe to a temporary stage table, then MERGE into target."""
        if dataframe.empty:
            logger.info("Skipping empty upsert into %s", table)
            return
        if self.connection is None:
            self.connect()

        from snowflake.connector.pandas_tools import write_pandas

        df = dataframe.copy()
        df.columns = [column.upper() for column in df.columns]
        merge_keys = [key.upper() for key in merge_keys]
        table_name = table.upper()
        temp_table = f"{table_name}_STAGE"

        columns = list(df.columns)
        col_defs = ", ".join(f"{_quote_identifier(col)} VARCHAR" for col in columns)
        update_cols = [col for col in columns if col not in merge_keys]

        with self.connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TEMP TABLE {_quote_identifier(temp_table)} ({col_defs})"
            )
            write_pandas(self.connection, df.astype(str), temp_table)

            on_clause = " AND ".join(
                f"target.{_quote_identifier(key)} = source.{_quote_identifier(key)}"
                for key in merge_keys
            )
            update_clause = ", ".join(
                f"target.{_quote_identifier(col)} = source.{_quote_identifier(col)}"
                for col in update_cols
            )
            insert_cols = ", ".join(_quote_identifier(col) for col in columns)
            insert_vals = ", ".join(
                f"source.{_quote_identifier(col)}" for col in columns
            )

            sql = f"""
                MERGE INTO {_quote_identifier(table_name)} AS target
                USING {_quote_identifier(temp_table)} AS source
                ON {on_clause}
                WHEN MATCHED THEN UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            cursor.execute(sql)
            logger.info("Upserted %d rows into %s", len(df), table_name)
