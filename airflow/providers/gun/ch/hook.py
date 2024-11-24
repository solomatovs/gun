from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Optional, Union

from clickhouse_driver.dbapi.connection import Connection as DbApiConnection
from clickhouse_driver.dbapi.cursor import Cursor as ClickhouseCursor
from clickhouse_driver.dbapi.extras import DictCursor, NamedTupleCursor
from sqlalchemy.engine import URL

from airflow.exceptions import (
    AirflowProviderDeprecationWarning,
    AirflowNotFoundException,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.models.connection import Connection as AirflowConnection

CursorType = Union[DictCursor, NamedTupleCursor]


class ClickhouseNativeHook(DbApiHook):
    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_native_default"
    conn_type = "clickhouse_native"
    hook_name = "ClickhouseNative"
    supports_autocommit = True
    supports_executemany = True

    def __init__(self, *args, options: str | None = None, **kwargs) -> None:
        if "schema" in kwargs:
            warnings.warn(
                'The "schema" arg has been renamed to "database" as it contained the database name.'
                'Please use "database" to set the database name.',
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["database"] = kwargs["schema"]
        super().__init__(*args, **kwargs)
        self.connection: AirflowConnection | None = kwargs.pop("connection", None)
        self.conn: Optional[DbApiConnection] = None
        self.database: str | None = kwargs.pop("database", None)
        self.options = options

        self.connection_factory = ClickhouseNativeHook.default_connection_factory
        self.cursor_factory = ClickhouseCursor

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        return URL.create(
            drivername="clickhouse",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=self.database,
        )

    @staticmethod
    def default_connection_factory(**conn):
        conn = DbApiConnection(**conn)
        return conn

    def get_conn(self) -> DbApiConnection:
        conn_id = getattr(self, self.conn_name_attr)
        conn = deepcopy(self.connection or self.get_connection(conn_id))
        conn_args = self.get_conn_args_from_airflow(conn)
        self.conn = self.connection_factory(**conn_args)

        return self.conn

    def get_cursor(self) -> ClickhouseCursor:
        conn = self.get_conn()
        return conn.cursor(self.cursor_factory)

    def get_airflow_conn(self, conn_id):
        try:
            return AirflowConnection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException as e:
            print(f"conn: {conn_id} not found. Please make sure it exists")
            raise e
        except Exception as e:
            raise e

    def get_conn_args_from_airflow(self, conn: AirflowConnection):
        """
        Преобразование Airflow connection в аргументы коннекшина для clickhouse_driver
        """
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "database": conn.schema,
            "port": conn.port,
        }

        # любые опциональные параметры можно передать через блок extra в виде json
        if conn.extra_dejson:
            for name, val in conn.extra_dejson.items():
                conn_args[name] = val

        return conn_args

    def get_conn_attrs(self, conn_id: str):
        conn_airflow = self.get_airflow_conn(conn_id)
        conn_airflow = self.get_conn_args_from_airflow(conn_airflow)

        cursor_factory = conn_airflow.get("cursor_factory")
        if cursor_factory:
            self.cursor_factory = cursor_factory

        return conn_airflow

    def get_uri(self) -> str:
        """Extract the URI from the connection.

        :return: the extracted URI in Sqlalchemy URI format.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)
