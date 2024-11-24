import os
import sys
import io
import gzip
import threading
from typing import Any, Callable, Iterable, Mapping, Optional, TextIO, Type, Union, Dict

from pathlib import Path
from contextlib import closing

import oracledb

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage

__all__ = [
    "oc_execute",
    "oc_execute_and_commit",
    "oc_execute_file",
    "oc_execute_file_and_commit",
    "oc_fetch_to_stdout",
    "oc_commit",
]

oc_cur_key_default = "oc_cur"

class OracleAuthAirflowConnection(PipeTask):
    def __init__(
        self,
        context_key: str,
        stack_key: str,
        template_render: Callable,
        conn_id: Optional[str] = None,
        dsn: Optional[str] = None,
        connection_factory: Optional[Callable] = None,
        cursoc_name: Optional[str] = None,
        cur_key: str = oc_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "conn_id",
                "dsn",
                "cursoc_name",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.context_key = context_key
        self.stack_key = stack_key
        self.conn_id = conn_id
        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursoc_name = cursoc_name

    @staticmethod
    def default_connection_factory(**conn):
        conn = oracledb.connect(**conn)
        return conn

    def get_cur_with_stack(self, stack):
        if not self.connection_factory:
            self.connection_factory = (
                OracleAuthAirflowConnection.default_connection_factory
            )

        conn = self.get_conn()
        conn = self.connection_factory(**conn)
        conn = stack.enter_context(closing(conn))
        cur = self.get_cur(conn)
        cur = stack.enter_context(closing(cur))

        return cur

    def get_airflow_conn(self, conn_id):
        try:
            return Connection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException as e:
            print(f"conn: {conn_id} not found. Please make sure it exists")
            raise e
        except Exception as e:
            raise e

    def get_conn_args_from_airflow(self, conn: Connection):
        conn_args = {
            "user": conn.login,
            "password": conn.password,
            "dsn": conn.host,
        }

        if conn.extra_dejson:
            for name, val in conn.extra_dejson.items():
                conn_args[name] = val

        return conn_args

    def get_conn(self):
        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        if self.dsn:
            conn_user = {"dsn": self.dsn}
        else:
            conn_user = {}

        conn_args = {**conn_airflow, **conn_user}

        return conn_args

    def get_cur(self, conn: oracledb.Connection):
        return conn.cursor()

    def __call__(self, context):
        self.render_template_fields(context)

        stack = context[self.stack_key]
        oc_cur = self.get_cur_with_stack(stack)
        share = context[self.context_key]
        share[self.cur_key] = oc_cur

def oc_auth_airflow_conn(
    conn_id: str,
    dsn: Optional[str] = None,
    conn_factory: Optional[Any] = None,
    cursoc_name: Optional[str] = None,
    cur_key: str = oc_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            OracleAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id=conn_id,
                dsn=dsn,
                connection_factory=conn_factory,
                cursoc_name=cursoc_name,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper

class OracleExecuteModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql,
        params,
        cur_key: str = oc_cur_key_default,
    ):
        super().__init__(context_key)
        self.set_template_fields(["sql", "params"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql = sql
        self.params = params

    def __call__(self, context):
        self.render_template_fields(context)

        oc_cur: oracledb.Cursor = context[self.context_key][self.cur_key]
        oc_cur.execute(self.sql, self.params)

def oc_execute(
    sql: str,
    params=None,
    cur_key: str = oc_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            OracleExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

class OracleExecuteAndCommitModule(OracleExecuteModule):
    def __call__(self, context):
        super().__call__(context)

        oc_cur: oracledb.Cursor = context[self.context_key][self.cur_key]
        oc_cur.connection.commit()

def oc_execute_and_commit(
    sql: str,
    params=None,
    cur_key: str = oc_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            OracleExecuteAndCommitModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

class OracleFetchToStdoutModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        cur_key: str = oc_cur_key_default,
    ):
        super().__init__(context_key)

        self.cur_key = cur_key

    def __call__(self, context):
        self.render_template_fields(context)

        oc_cur = context[self.context_key][self.cur_key]
        try:
            for record in oc_cur:
                print(record)
        except oracledb.DatabaseError as e:
            print(f"---- {e} -----")

def oc_fetch_to_stdout(
    cur_key: str = oc_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            OracleFetchToStdoutModule(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

class OracleCommitModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        cur_key: str = oc_cur_key_default,
    ):
        super().__init__(context_key)

        self.cur_key = cur_key

    def __call__(self, context):
        oc_cur = context[self.context_key][self.cur_key]
        oc_cur.connection.commit()

def oc_commit(
    cur_key: str = oc_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            OracleCommitModule(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper

# Additional functions for copying data, registering types, and saving to XCom would follow a similar pattern.