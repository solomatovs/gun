import os
import io
import gzip
import csv

from typing import (
    Any,
    Callable,
    Optional,
    Type,
    Dict,
)

from pathlib import Path
from contextlib import closing

from clickhouse_driver import Client as ClickhouseClient
from clickhouse_driver.util.escape import escape_params
from clickhouse_driver.connection import Connection as ClickhouseConnection
from clickhouse_driver.dbapi.connection import Connection as DbApiConnection
from clickhouse_driver.dbapi.cursor import Cursor as ClickhouseCursor
from clickhouse_driver.block import ColumnOrientedBlock

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection as AirflowConnection
from airflow.utils.xcom import XCOM_RETURN_KEY

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage

__all__ = [
    "ch_auth_airflow_conn",
    "ch_disconnect",
    "ch_execute",
    "ch_execute_iter",
    "ch_execute_file_query",
    "ch_execute_file_queries",
    "ch_execute_and_save_to_context",
    "ch_execute_and_fetchone_to_context",
    "ch_execute_and_fetchall_to_context",
    "ch_execute_and_save_to_xcom",
    "ch_execute_and_fetchone_to_xcom",
    "ch_execute_and_fetchall_to_xcom",
    "ch_send_query",
    "ch_send_queries",
    "ch_send_file_query",
    "ch_send_file_queries",
    "ch_recv_blocks_to_stdout",
    "ch_recv_rows",
    "ch_save_to_csv",
    "ch_save_to_gzip_csv",
    "ch_save_to_xcom",
    "ch_save_to_context",
    "ch_fetchone_to_context",
    "ch_fetchall_to_context",
    "ch_fetchone_to_xcom",
    "ch_fetchall_to_xcom",
    "ch_check_table_exist",
    "ch_check_column_exist",
]


ch_cur_key_default = "ch_cur"


def mogrify(query, params, context):
    if not isinstance(params, dict):
        raise ValueError("Parameters are expected in dict form")

    escaped = escape_params(params, context)
    return query % escaped


class ClickhouseAuthAirflowConnection(PipeTask):
    def __init__(
        self,
        context_key: str,
        stack_key: str,
        template_render: Callable,
        conn_id: Optional[str],
        settings: Dict[str, Any],
        connection_factory: Optional[Type[DbApiConnection]],
        cursor_factory: Optional[Type[ClickhouseCursor]],
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "conn_id",
                "settings",
            )
        )
        super().set_template_render(template_render)
        self.cur_key = cur_key
        self.cur_key_deprecated = "cur"
        self.stack_key = stack_key
        self.conn_id = conn_id
        self.settings = settings
        self.connection_factory = connection_factory
        self.cursor_factory = cursor_factory

    @staticmethod
    def default_connection_factory(**conn):
        conn = DbApiConnection(**conn)
        return conn

    def get_cur_with_stack(self, stack):
        if not self.connection_factory:
            self.connection_factory = (
                ClickhouseAuthAirflowConnection.default_connection_factory
            )

        conn = self.get_conn()
        conn = self.connection_factory(**conn)
        conn = stack.enter_context(closing(conn))
        cur = self.get_cur(conn)
        cur = stack.enter_context(closing(cur))

        return cur

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

    def get_conn(self):
        if self.conn_id:
            conn_airflow = self.get_airflow_conn(self.conn_id)
            conn_airflow = self.get_conn_args_from_airflow(conn_airflow)
        else:
            conn_airflow = {}

        if self.settings and isinstance(self.settings, Dict):
            conn_settings = self.settings
        else:
            conn_settings = {}

        conn_args = conn_airflow | conn_settings

        return conn_args

    def get_cur(self, conn: DbApiConnection):
        """
        Создаёт cursor
        """
        cursor_factory = self.cursor_factory

        if cursor_factory:
            cur = conn.cursor(
                cursor_factory=cursor_factory,
            )
        else:
            cur = conn.cursor()

        return cur

    def __call__(self, context):
        self.render_template_fields(context)

        stack = context[self.stack_key]
        cur = self.get_cur_with_stack(stack)
        share = context[self.context_key]
        share[self.cur_key] = cur
        share[self.cur_key_deprecated] = cur


def ch_auth_airflow_conn(
    conn_id: str,
    settings: Optional[Dict[str, Any]] = None,
    conn_factory: Optional[Type[DbApiConnection]] = None,
    cursor_factory: Optional[Type[ClickhouseCursor]] = None,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id=conn_id,
                settings=settings or {},
                connection_factory=conn_factory,
                cursor_factory=cursor_factory,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseDisconnectModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)

        self.cur_key = cur_key

    def __call__(self, context):
        cur = context[self.context_key][self.cur_key]
        cur.connection.disconnect()


def ch_disconnect(
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseDisconnectModule(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSendQueryModule(PipeTask):
    """
    Отправляет sql запрос в clickhouse
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        query: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        end_query: bool,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "query",
                "query_id",
                "params",
                "settings",
                "client_settings",
                "end_query",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.query = query
        self.params = params
        self.query_id = query_id
        self.settings = settings
        self.client_settings = client_settings
        self.end_query = end_query
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        params = self.params
        query_id = self.query_id
        settings = self.settings
        client_settings = self.client_settings
        end_query = self.end_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            if not end_query:
                conn.send_query(query, query_id, params)
                conn.send_data(ColumnOrientedBlock())
            else:
                cur.execute(query, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_send_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSendQueryModule(
                builder.context_key,
                builder.template_render,
                query,
                params,
                settings or {},
                client_settings or {},
                query_id,
                end_query,
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSendQueryMultiStatementsModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        query: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        sep: str,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "query",
                "query_id",
                "params",
                "settings",
                "client_settings",
                "sep",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.params = params
        self.query_id = query_id
        self.query: str = query
        self.settings = settings
        self.client_settings = client_settings
        self.sep: str = sep
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        settings = self.settings
        client_settings = self.client_settings
        params = self.params
        query_id = self.query_id
        sep = self.sep

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            for query_part in query.split(sep):
                if not query_part:
                    continue

                cur.execute(query_part, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_send_queries(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    sql_file - может содержать несколько запросов, разделенных через ;
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSendQueryMultiStatementsModule(
                builder.context_key,
                builder.template_render,
                query,
                params,
                settings or {},
                client_settings or {},
                query_id,
                ";",
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSendQueryFileModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        end_query: bool,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "client_settings",
                "query_id",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.client_settings = client_settings
        self.query_id = query_id
        self.end_query = end_query
        self.query: str = ""
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = os.path.expandvars(self.sql_file)
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        end_query = self.end_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            print(f"try rendering file: {sql_file}")
            query = (
                Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
            )
            query = self.template_render(query, context)
            print(f"rendering success: {sql_file}")

            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            if not end_query:
                conn.send_query(query, query_id, params)
                conn.send_data(ColumnOrientedBlock())
            else:
                cur.execute(query, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_send_file_query(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSendQueryFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings or {},
                client_settings or {},
                query_id,
                end_query,
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSendQueryFileMultiStatementsModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        sep: str,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "client_settings",
                "query_id",
                "sep",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.client_settings = client_settings
        self.query_id = query_id
        self.sep = sep
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = self.sql_file
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        sep = self.sep

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            sql_file = os.path.expandvars(sql_file)
            sql_file = Path(sql_file)
            sql_file = sql_file.absolute()

            query = sql_file.read_text(encoding="utf-8", errors="ignore")
            query = self.template_render(query, context)

            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            for query_part in query.split(sep):
                if not query_part:
                    continue

                cur.execute(query_part, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_send_file_queries(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    sql_file - может содержать несколько запросов, разделенных через ;
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSendQueryFileMultiStatementsModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings or {},
                client_settings or {},
                query_id,
                ";",
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseResultRowToStdoutModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.cur_key = cur_key

    def __call__(self, context):
        self.render_template_fields(context)
        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        i = 0
        first = True
        for row in cur:
            if first:
                first = False
                print(cur.columns_with_types)
            print(row)
            i += 1

        if i != 0:
            cur._rowcount = i


def ch_recv_blocks_to_stdout(
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseResultRowToStdoutModule(
                builder.context_key,
                builder.template_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseReceiveAllRowsModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        block_processing: Callable,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.row_processing = block_processing

    def __call__(self, context):
        self.render_template_fields(context)
        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        i = 0
        for row in cur:
            self.row_processing(row, context)
            i += 1

        if i != 0:
            cur._rowcount = i


def ch_recv_rows(
    row_processing: Callable,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseReceiveAllRowsModule(
                builder.context_key,
                builder.template_render,
                row_processing,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSaveToCsvModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        file_path: str,
        delimiter: str,
        quotechar: Optional[str],
        escapechar: Optional[str],
        doublequote: bool,
        skipinitialspace: bool,
        lineterminator: str,
        quoting: int,
        strict: bool,
        encoding: str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "file_path",
                "delimiter",
                "quotechar",
                "escapechar",
                "doublequote",
                "skipinitialspace",
                "quoting",
                "strict",
                "encoding",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.file_path = file_path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.strict = strict
        self.encoding = encoding

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        self.file_path = os.path.expandvars(self.file_path)
        file = Path(self.file_path).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"clickhouse result copy to: {file} ...")

        with open(file, mode="w", newline="") as f:
            wrt = csv.writer(
                f,
                delimiter=self.delimiter,
                quotechar=self.quotechar,
                escapechar=self.escapechar,
                doublequote=self.doublequote,
                skipinitialspace=self.skipinitialspace,
                lineterminator=self.lineterminator,
                quoting=self.quoting,
                strict=self.strict,
            )

            i = 0
            first = True
            for row in cur:
                if first:
                    first = False
                    if cur.columns_with_types is not None:
                        csv_header = map(lambda x: x[0], cur.columns_with_types)
                        wrt.writerow(csv_header)

                wrt.writerow(row)
                i += 1

            if i != 0:
                cur._rowcount = i


def ch_save_to_csv(
    to_file: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    quoting: int = csv.QUOTE_MINIMAL,
    strict: bool = False,
    encoding: str = "utf-8",
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """ """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToCsvModule(
                builder.context_key,
                builder.template_render,
                to_file,
                delimiter,
                quotechar,
                escapechar,
                doublequote,
                skipinitialspace,
                lineterminator,
                quoting,
                strict,
                encoding,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSaveToGZipCsvModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        file_path: str,
        delimiter: str,
        quotechar: Optional[str],
        escapechar: Optional[str],
        doublequote: bool,
        skipinitialspace: bool,
        lineterminator: str,
        quoting: int,
        strict: bool,
        encoding: str,
        compresslevel: int,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "file_path",
                "delimiter",
                "quotechar",
                "escapechar",
                "doublequote",
                "skipinitialspace",
                "quoting",
                "strict",
                "encoding",
                "compresslevel",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.file_path = file_path
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.escapechar = escapechar
        self.doublequote = doublequote
        self.skipinitialspace = skipinitialspace
        self.lineterminator = lineterminator
        self.quoting = quoting
        self.strict = strict
        self.encoding = encoding
        self.compresslevel = compresslevel

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        self.file_path = os.path.expandvars(self.file_path)
        file = Path(self.file_path).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"clickhouse result copy to: {file} ...")

        with gzip.open(file, mode="wb", compresslevel=self.compresslevel) as f:
            with io.TextIOWrapper(f, encoding=self.encoding, newline="") as wrapper:
                wrt = csv.writer(
                    wrapper,
                    delimiter=self.delimiter,
                    quotechar=self.quotechar,
                    escapechar=self.escapechar,
                    doublequote=self.doublequote,
                    skipinitialspace=self.skipinitialspace,
                    lineterminator=self.lineterminator,
                    quoting=self.quoting,
                    strict=self.strict,
                )

                i = 0
                first = True
                for row in cur:
                    if first:
                        first = False
                    if cur.columns_with_types is not None:
                        csv_header = map(lambda x: x[0], cur.columns_with_types)
                        wrt.writerow(csv_header)

                        wrt.writerow(row)
                        i += 1

                if i != 0:
                    cur._rowcount = i


def ch_save_to_gzip_csv(
    to_file: str,
    delimiter: str = ",",
    quotechar: str | None = '"',
    escapechar: str | None = None,
    doublequote: bool = True,
    skipinitialspace: bool = False,
    lineterminator: str = "\r\n",
    quoting: int = csv.QUOTE_MINIMAL,
    strict: bool = False,
    encoding: str = "utf-8",
    compresslevel: int = 9,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToGZipCsvModule(
                builder.context_key,
                builder.template_render,
                to_file,
                delimiter,
                quotechar,
                escapechar,
                doublequote,
                skipinitialspace,
                lineterminator,
                quoting,
                strict,
                encoding,
                compresslevel,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSaveToXComModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to: str,
        save_builder: Callable[[ClickhouseCursor], Any],
        save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str,
        jinja_render: bool,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.ti_key = "ti"
        self.save_to = save_to
        self.save_builder = save_builder
        self.save_if = save_if
        self.jinja_render = jinja_render

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        res = self.save_builder(cur)
        # выполняем рендер jinja, если нужно
        if self.jinja_render and res is not None:
            res = self.template_render(res, context)

        if self.save_if_eval(context, res, cur):
            ti = context[self.ti_key]
            ti.xcom_push(key=self.save_to, value=res)

    def save_if_eval(self, context, res, cur):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if(context, res, cur)

        return save_if


def ch_save_to_xcom(
    save_builder: Callable[[ClickhouseCursor], Any],
    save_to: str = XCOM_RETURN_KEY,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования

    Args:
        save_to: это имя xcom ключа
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в xcom
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_to_xcom("my_context_key", lambda cur: {
                'target_row': cur.rowcount,
                'source_row': {{ params.source_row }},
                'error_row': 0,
            })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=save_builder,
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class ClickhouseSaveToContextModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        save_to: str,
        save_builder: Callable[[ClickhouseCursor], Any],
        save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str,
        jinja_render: bool,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.ti_key = "ti"
        self.save_to = save_to
        self.save_builder = save_builder
        self.save_if = save_if
        self.jinja_render = jinja_render

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        # выполняю проверку нужно ли сохранять результат в контекст
        self.render_template_fields(context)

        res = self.save_builder(cur)
        # выполняем рендер jinja, если нужно
        if self.jinja_render and res is not None:
            res = self.template_render(res, context)

        if self.save_if_eval(context, res, cur):
            context[self.save_to] = res

    def save_if_eval(self, context, res, cur):
        match self.save_if:
            case bool():
                save_if = self.save_if
            case str():
                save_if = self.template_render(self.save_if, context)
            case _:
                save_if = self.save_if(context, res, cur)

        return save_if


def ch_if_rows_exist(context, res, cur: ClickhouseCursor):
    # rowcount содержит кол-во строк которые были затронуты в последнем execute запросе
    # логика может быть не совсем верной, например когда выполнен update или delete
    # но это позволяет не начинать чтение из курсора если результат пустой для select
    # в остальных случаях просто не нужно использовать сохранение результата для update и delete операторов
    if cur.rowcount > 0 and res is not None:
        return True

    return False


def ch_save_to_context(
    save_to: str,
    save_builder: Callable[[ClickhouseCursor], Any],
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Args:
        save_to: это имя context ключа.
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в context
        save_if: это функция, которая будет использована для проверки, нужно ли сохранять результат в context
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_to_context("my_context_key", lambda cur: {
                'target_row': cur.rowcount,
                'source_row': {{ params.source_row }},
                'error_row': 0,
            })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=save_builder,
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class ClickhouseExecuteModule(PipeTask):
    """
    Отправляет sql запрос в clickhouse
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        params: Optional[Dict[str, Any]],
        external_tables: Optional[Dict],
        query_id: Optional[str],
        settings: Dict[str, Any],
        types_check: bool,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql",
                "query_id",
                "params",
                "settings",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql = sql
        self.params = params
        self.external_tables = external_tables
        self.query_id = query_id
        self.settings = settings
        self.types_check = types_check
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        sql = self.sql
        params = self.params
        external_tables = self.external_tables
        query_id = self.query_id
        settings = self.settings
        types_check = self.types_check

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            if query_id:
                cur.set_query_id(query_id)

            if settings:
                cur.set_settings(settings)

            if types_check:
                cur.set_types_check(types_check)

            if external_tables:
                cur.set_external_table(
                    name=external_tables["name"],
                    structure=external_tables["structure"],
                    data=external_tables["data"],
                )

            cur.execute(sql, params)

    def execute_if_eval(self, context, ch_cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, ch_cur)

        return execute_if


def ch_execute(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                query,
                params,
                external_tables,
                query_id,
                settings or {},
                types_check,
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseExecuteModuleIter(PipeTask):
    """
    Отправляет sql запрос в clickhouse
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        query: str,
        params: Optional[Dict[str, Any]],
        external_tables: Optional[Dict],
        query_id: Optional[str],
        settings: Dict[str, Any],
        types_check: bool,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "query",
                "query_id",
                "params",
                "settings",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.query = query
        self.params = params
        self.external_tables = external_tables
        self.query_id = query_id
        self.settings = settings
        self.types_check = types_check
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        params = self.params
        external_tables = self.external_tables
        query_id = self.query_id
        settings = self.settings
        types_check = self.types_check

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):

            if query_id:
                cur.set_query_id(query_id)

            if settings:
                cur.set_settings(settings)

            if types_check:
                cur.set_types_check(types_check)

            if external_tables:
                cur.set_external_table(
                    name=external_tables["name"],
                    structure=external_tables["structure"],
                    data=external_tables["data"],
                )

            # clickjhouse driver вызывает execite_iter только если передан cur.set_stream_results
            cur.set_stream_results(True, settings.get("max_block_size") or 65536)

            cur.execute(query, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_execute_iter(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModuleIter(
                builder.context_key,
                builder.template_render,
                query,
                params,
                external_tables,
                query_id,
                settings or {},
                types_check,
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseExecuteQueryFileModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "client_settings",
                "query_id",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.client_settings = client_settings
        self.query_id = query_id
        self.query: str = ""
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = os.path.expandvars(self.sql_file)
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):
            print(f"try rendering file: {sql_file}")
            query = (
                Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
            )
            query = self.template_render(query, context)
            print(f"rendering success: {sql_file}")

            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            cur.execute(query, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_execute_file_query(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteQueryFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings or {},
                client_settings or {},
                query_id,
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseExecuteFileMultiStatementsModule(PipeTask):
    """Выполняет sql запрос из переданного файла"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params: Optional[Dict[str, Any]],
        settings: Dict[str, Any],
        client_settings: Dict[str, Any],
        query_id: Optional[str],
        sep: str,
        execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str,
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "sql_file",
                "params",
                "settings",
                "client_settings",
                "query_id",
                "sep",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.params = params
        self.settings = settings
        self.client_settings = client_settings
        self.query_id = query_id
        self.sep = sep
        self.execute_if = execute_if

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = self.sql_file
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        sep = self.sep

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        if self.execute_if_eval(context, cur):

            sql_file = os.path.expandvars(sql_file)
            query = (
                Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
            )
            query = self.template_render(query, context)

            client: ClickhouseClient = cur._client
            conn: ClickhouseConnection = client.get_connection()

            if settings:
                cur.set_settings(settings)

            if query_id:
                cur.set_query_id(query_id)

            cur.set_types_check(False)

            if client_settings:
                conn.context.client_settings |= client_settings

            for query_part in query.split(sep):
                if not query_part:
                    continue

                cur.execute(query_part, params)

    def execute_if_eval(self, context, cur):
        match self.execute_if:
            case bool():
                execute_if = self.execute_if
            case str():
                execute_if = self.template_render(self.execute_if, context)
            case _:
                execute_if = self.execute_if(context, cur)

        return execute_if


def ch_execute_file_queries(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    sql_file - может содержать несколько запросов, разделенных через ;
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteFileMultiStatementsModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                settings or {},
                client_settings or {},
                query_id,
                ";",
                execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def ch_check_table_exist(
    schema: str,
    table: str,
    save_to: str,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql="""select
    true
from
    information_schema.tables
where 1=1
    and table_schema in (%(ch_schema)s)
    and table_name in (%(ch_table)s)
""",
                params={
                    "ch_schema": schema,
                    "ch_table": table,
                },
                external_tables=None,
                query_id=None,
                settings={},
                types_check=False,
                execute_if=True,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        def fetch_true_or_false(cur):
            res = cur.fetchone()

            if res is None:
                return False

            common_error = f"""ch_check_column_exist
unexpected result. Expected true, false or None
however result type is: {type(res)}
value: {res}"""

            if not isinstance(res, tuple):
                raise RuntimeError(common_error)

            if len(res) != 1 or len(res) > 1:
                raise RuntimeError(common_error)

            res = res[0]

            if not isinstance(res, bool):
                raise RuntimeError(common_error)

            return res

        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=fetch_true_or_false,
                save_if=save_if,
                jinja_render=False,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_check_column_exist(
    schema: str,
    table: str,
    column: str,
    save_to: str,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql="""select
    true
from
    information_schema.columns
where 1=1
    and table_schema in (%(ch_schema)s)
    and table_name in (%(ch_table)s)
    and column_name in (%(ch_column)s)
""",
                params={
                    "ch_schema": schema,
                    "ch_table": table,
                    "ch_column": column,
                },
                external_tables=None,
                query_id=None,
                settings={},
                types_check=False,
                execute_if=True,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        def fetch_true_or_false(cur):
            res = cur.fetchone()

            if res is None:
                return False

            common_error = f"""ch_check_column_exist
unexpected result. Expected true, false or None
however result type is: {type(res)}
value: {res}"""

            if not isinstance(res, tuple):
                raise RuntimeError(common_error)

            if len(res) != 1 or len(res) > 1:
                raise RuntimeError(common_error)

            res = res[0]

            if not isinstance(res, bool):
                raise RuntimeError(common_error)

            return res

        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=fetch_true_or_false,
                save_if=save_if,
                jinja_render=False,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_fetchone_to_context(
    save_to: str,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Args:
        save_to: это имя context ключа.
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_result_to_context("my_context_key")
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchone(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_fetchall_to_context(
    save_to: str,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Args:
        save_to: это имя context ключа.
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_result_to_context("my_context_key")
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchall(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_fetchone_to_xcom(
    save_to: str = XCOM_RETURN_KEY,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования

    Args:
        save_to: это имя xcom ключа
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_fetchone_to_xcom()
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchone(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_fetchall_to_xcom(
    save_to: str = XCOM_RETURN_KEY,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования

    Args:
        save_to: это имя xcom ключа
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_result_to_xcom()
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchall(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_save_to_context(
    sql: str,
    save_to: str,
    save_builder: Callable[[ClickhouseCursor], Any],
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет выполнить sql запрос и сохранить результат в Airflow Context для последующего использования

    Args:
        sql: это sql запрос, который будет выполнен
        save_to: это имя context ключа.
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в context
        params: это параметры, которые будут переданы в sql запрос (если запрос содержит параметры, см примеры ниже)
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_execute_and_save_to_context(
                sql="select %(date)s", params={"date": pendulum.now().date()},
                save_to="my_context_key",
                save_builder=lambda cur: {
                    'target_row': cur.rowcount,
                    'source_row': {{ params.source_row }},
                    'error_row': 0,
                },
            )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=save_builder,
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_fetchone_to_context(
    sql: str,
    save_to: str,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет выполнить sql запрос и сохранить результат в Airflow Context для последующего использования

    Args:
        sql: это sql запрос, который будет выполнен
        save_to: это имя context ключа.
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в context
        params: это параметры, которые будут переданы в sql запрос (если запрос содержит параметры, см примеры ниже)
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_execute_and_save_to_context(
                sql="select %(date)s", params={"date": pendulum.now().date()},
                save_to="my_context_key",
                save_builder=lambda cur: {
                    'target_row': cur.rowcount,
                    'source_row': {{ params.source_row }},
                    'error_row': 0,
                },
            )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchone(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_fetchall_to_context(
    sql: str,
    save_to: str,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет выполнить sql запрос и сохранить результат в Airflow Context для последующего использования

    Args:
        sql: это sql запрос, который будет выполнен
        save_to: это имя context ключа.
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в context
        params: это параметры, которые будут переданы в sql запрос (если запрос содержит параметры, см примеры ниже)
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_execute_and_save_to_context(
                sql="select %(date)s", params={"date": pendulum.now().date()},
                save_to="my_context_key",
                save_builder=lambda cur: {
                    'target_row': cur.rowcount,
                    'source_row': {{ params.source_row }},
                    'error_row': 0,
                },
            )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchall(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_save_to_xcom(
    sql: str,
    save_builder: Callable[[ClickhouseCursor], Any],
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    save_to: str = XCOM_RETURN_KEY,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет выполнить sql запрос и сохранить результат в Airflow Xcom для последующего использования

    Args:
        sql: это sql запрос, который будет выполнен
        save_to: это имя xcom ключа.
        save_builder: это функция, которая будет использована для генерации значения, которое будет добавлено в context
        params: это параметры, которые будут переданы в sql запрос (если запрос содержит параметры, см примеры ниже)
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_execute_and_save_to_xcom(
                sql="select %(date)s", params={"date": pendulum.now()},
                save_to="my_context_key",
                save_builder=lambda cur: {
                    'target_row': cur.rowcount,
                    'source_row': {{ params.source_row }},
                    'error_row': 0,
                },
            )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=save_builder,
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_fetchone_to_xcom(
    sql: str,
    save_to: str = XCOM_RETURN_KEY,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет выполнить sql запрос и сохранить результат в Airflow Xcom для последующего использования

    Args:
        sql: это sql запрос, который будет выполнен
        save_to: это имя xcom ключа, по умолчанию
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_result_to_xcom("my_context_key")
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchone(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def ch_execute_and_fetchall_to_xcom(
    sql: str,
    save_to: str = XCOM_RETURN_KEY,
    execute_if: Callable[[Any, ClickhouseCursor], bool] | bool | str = True,
    save_if: Callable[[Any, Any, ClickhouseCursor], bool] | bool | str = True,
    params: Optional[Any] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    jinja_render: bool = True,
    cur_key: str = ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования

    Args:
        save_to: это имя xcom ключа
        jinja_render: если True, то значение будет передано в шаблонизатор jinja2

    Examples:
        Например можно сохранить кол-во строк, которые вернул clickhouse:
        >>> @ch_save_result_to_xcom("my_context_key")
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                external_tables=external_tables,
                query_id=query_id,
                settings=settings or {},
                types_check=types_check,
                execute_if=execute_if,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                save_to=save_to,
                save_builder=lambda cur: cur.fetchall(),
                save_if=save_if,
                jinja_render=jinja_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper
