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

from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage

__all__ = [
    "ch_auth_airflow_conn",
    "ch_disconnect",
    "ch_execute",
    "ch_execute_iter",
    "ch_execute_file_query",
    "ch_execute_file_queries",
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


class ClickhouseDisconnect(PipeTask):
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
            ClickhouseDisconnect(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseSendQuery(PipeTask):
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        params = self.params
        query_id = self.query_id
        settings = self.settings
        client_settings = self.client_settings
        end_query = self.end_query
        print_query = self.print_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
        client: ClickhouseClient = cur._client
        conn: ClickhouseConnection = client.get_connection()

        if settings:
            cur.set_settings(settings)

        if query_id:
            cur.set_query_id(query_id)

        cur.set_types_check(False)

        if client_settings:
            conn.context.client_settings |= client_settings

        if print_query:
            print(query)

        if not end_query:
            conn.send_query(query, query_id, params)
            conn.send_data(ColumnOrientedBlock())
        else:
            cur.execute(query, params)

        if print_query:
            print(f"recv rows: {cur.rowcount}")


def ch_send_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
    print_query=False,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSendQuery(
                builder.context_key,
                builder.template_render,
                query,
                params,
                settings or {},
                client_settings or {},
                query_id,
                end_query,
                print_query,
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        settings = self.settings
        client_settings = self.client_settings
        params = self.params
        query_id = self.query_id
        sep = self.sep
        print_query = self.print_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
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

            if print_query:
                print(query_part)

            cur.execute(query_part, params)

            if print_query:
                print(f"recv rows: {cur.rowcount}")


def ch_send_queries(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    print_query: bool = False,
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
                print_query,
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = os.path.expandvars(self.sql_file)
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        end_query = self.end_query
        print_query = self.print_query

        print(f"try rendering file: {sql_file}")
        query = Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        query = self.template_render(query, context)
        print(f"rendering success: {sql_file}")

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
        client: ClickhouseClient = cur._client
        conn: ClickhouseConnection = client.get_connection()

        if settings:
            cur.set_settings(settings)

        if query_id:
            cur.set_query_id(query_id)

        cur.set_types_check(False)

        if client_settings:
            conn.context.client_settings |= client_settings

        if print_query:
            print(query)

        if not end_query:
            conn.send_query(query, query_id, params)
            conn.send_data(ColumnOrientedBlock())
        else:
            cur.execute(query, params)


def ch_send_file_query(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    end_query=True,
    print_query=False,
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
                print_query,
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = self.sql_file
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        sep = self.sep
        print_query = self.print_query

        sql_file = os.path.expandvars(sql_file)
        sql_file = Path(sql_file)
        sql_file = sql_file.absolute()

        query = sql_file.read_text(encoding="utf-8", errors="ignore")
        query = self.template_render(query, context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
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

            if print_query:
                print(query_part)

            cur.execute(query_part, params)

            if print_query:
                print(f"recv rows: {cur.rowcount}")


def ch_send_file_queries(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    print_query: bool = False,
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
                print_query,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


# class ClickhouseSendExternalTablesModule(PipeTask):
#     def __init__(
#         self,
#         context_key: str,
#         template_render: Callable,
#         external_tables,
#         types_check: bool,
#         end_query: bool,
#     ):
#         super().__init__(context_key)
#         super().set_template_render(template_render)

#         self.external_tables = external_tables
#         self.types_check = types_check
#         self.end_query = end_query

#     def __call__(self, context):
#         self.render_template_fields(context)
#         external_tables = self.external_tables
#         types_check = self.types_check
#         end_query = self.end_query

#         cur: ClickhouseCursor = context[self.context_key]["cur"]
#         client: ClickhouseClient = cur._client
#         conn: ClickhouseConnection = client.get_connection()

#         conn.send_external_tables(external_tables, types_check)

#         if end_query:
#             conn.send_data(RowOrientedBlock())


# def ch_send_external_tables(
#     external_tables: List[Dict[str, Any]],
#     types_check: bool = False,
#     end_query=True,
# ):
#     def wrapper(builder: PipeTaskBuilder):
#         builder.before_modules.append(
#             ClickhouseSendExternalTablesModule(
#                 builder.context_key,
#                 builder.template_render,
#                 external_tables,
#                 types_check,
#                 end_query,
#             )
#         )

#         return builder

#     return wrapper


# class ClickhouseEndQueryModule(PipeTask):
#     def __init__(
#         self,
#         context_key: str,
#         template_render: Callable,
#     ):
#         super().__init__(context_key)
#         super().set_template_render(template_render)

#     def __call__(self, context):
#         self.render_template_fields(context)
#         cur: ClickhouseCursor = context[self.context_key]["cur"]
#         client: ClickhouseClient = cur._client
#         conn: ClickhouseConnection = client.get_connection()

#         conn.send_data(RowOrientedBlock())


# def ch_end_query():
#     def wrapper(builder: PipeTaskBuilder):
#         builder.before_modules.append(
#             ClickhouseEndQueryModule(
#                 builder.context_key,
#                 builder.template_render,
#             )
#         )

#         return builder

#     return wrapper


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
    """ """

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
        name: str,
        func: Callable[[ClickhouseCursor, Any], Any],
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.ti_key = "ti"
        self.name = name
        self.func = func

    def __call__(self, context):
        self.render_template_fields(context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        res = self.func(cur, context)
        res = self.template_render(res, context)
        ti = context[self.ti_key]

        ti.xcom_push(key=self.name, value=res)


def ch_save_to_xcom(
    xcom_name: str,
    xcom_gen: Callable[[ClickhouseCursor, Any], Any],
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования
    Для сохранения информации в xcom нужно передать:
    - xcom_name - это имя xcom записи. Напомню, что по умолчанию имя в xcom используется "return_value".
        Необходимо использовать любое другое имя, отличное от этого для передачи касомного xcom между задачами
    - xcom_gen - это функция, которая будет использована для генерации значения.
    xcom_gen принимает 1 параметр: ClickhouseCursor - текущий clickhouse cursor
    xcom_gen должна возвращать то значение, которое можно сериализовать в xcom

    @ch_save_to_xcom("myxcom", lambda cur, context: {
        'source_row': 0,
        'error_row': 0,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToXComModule(
                builder.context_key,
                builder.template_render,
                xcom_name,
                xcom_gen,
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
        name: str,
        func: Callable[[ClickhouseCursor, Any], Any],
        cur_key: str = ch_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["name"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.ti_key = "ti"
        self.name = name
        self.func = func

    def __call__(self, context):
        self.render_template_fields(context)
        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

        res = self.func(cur, context)
        res = self.template_render(res, context)
        context[self.name] = res


def ch_save_to_context(
    context_name: str,
    context_gen: Callable[[ClickhouseCursor, Any], Any],
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Для сохранения информации в context нужно передать:
    - context_name - это имя context ключа.
    - context_gen - это функция, которая будет использована для генерации значения, которое будет добавлено в context
    context_gen принимает 1 параметра: context
    context_gen должна возвращать то значение, которое унжно сохранить в context

    @ch_save_to_context("my_context_key", lambda cur, context: {
        'target_row': "{{ params.target_row }}",
        'source_row': 0,
        'error_row': 0,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseSaveToContextModule(
                builder.context_key,
                builder.template_render,
                context_name,
                context_gen,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


# class ClickhouseSendDataFromCsvModule(PipeTask):
#     def __init__(
#         self,
#         context_key: str,
#         template_render: Callable,
#         file_path: str,
#         delimiter: str,
#         quotechar: Optional[str],
#         escapechar: Optional[str],
#         doublequote: bool,
#         skipinitialspace: bool,
#         lineterminator: str,
#         quoting: int,
#         strict: bool,
#         encoding: str,
#     ):
#         super().__init__(context_key)
#         super().set_template_fields(
#             (
#                 "file_path",
#                 "delimiter",
#                 "quotechar",
#                 "escapechar",
#                 "doublequote",
#                 "skipinitialspace",
#                 "quoting",
#                 "strict",
#                 "encoding",
#             )
#         )
#         super().set_template_render(template_render)

#         self.file_path = file_path
#         self.delimiter = delimiter
#         self.quotechar = quotechar
#         self.escapechar = escapechar
#         self.doublequote = doublequote
#         self.skipinitialspace = skipinitialspace
#         self.lineterminator = lineterminator
#         self.quoting = quoting
#         self.strict = strict
#         self.encoding = encoding

#     def receive_sample_block(self, conn: ClickhouseConnection):
#         while True:
#             packet = conn.receive_packet()

#             if packet.type == ServerPacketTypes.DATA:
#                 return packet.block

#             elif packet.type == ServerPacketTypes.EXCEPTION:
#                 raise packet.exception

#             elif packet.type == ServerPacketTypes.LOG:
#                 pass
#                 # log_block(packet.block)

#             elif packet.type == ServerPacketTypes.TABLE_COLUMNS:
#                 pass

#             else:
#                 message = conn.unexpected_packet_message(
#                     "Data, Exception, Log or TableColumns", packet.type
#                 )
#                 raise UnexpectedPacketFromServerError(message)

#     def __call__(self, context):
#         self.render_template_fields(context)

#         cur: ClickhouseCursor = context[self.context_key]["cur"]
#         client: ClickhouseClient = cur._client
#         conn: ClickhouseConnection = client.get_connection()

#         self.file_path = os.path.expandvars(self.file_path)
#         file = Path(self.file_path).absolute()

#         print(f"clickhouse read csv: {file} ...")

#         with open(file, mode="r", encoding=self.encoding) as f:
#             # Empty block, end of data transfer.
#             # conn.send_data(RowOrientedBlock())

#             sample_block = client.receive_sample_block()

#             if sample_block:
#                 for row in f.readlines():
#                     if row != '\n':
#                         block = RowOrientedBlock(
#                             sample_block.columns_with_types, row, types_check=False
#                         )

#                         conn.send_data(block)
#                         # client.send_data(sample_block, row, types_check=False, columnar=False)

#             client.receive_end_of_insert_query()


# def ch_send_data_from_csv(
#     from_file: str,
#     delimiter: str = ",",
#     quotechar: str | None = '"',
#     escapechar: str | None = None,
#     doublequote: bool = True,
#     skipinitialspace: bool = False,
#     lineterminator: str = "\r\n",
#     quoting: int = csv.QUOTE_MINIMAL,
#     strict: bool = False,
#     encoding: str = "utf-8",
# ):
#     """ """

#     def wrapper(builder: PipeTaskBuilder):
#         builder.before_modules.append(
#             ClickhouseSendDataFromCsvModule(
#                 builder.context_key,
#                 builder.template_render,
#                 from_file,
#                 delimiter,
#                 quotechar,
#                 escapechar,
#                 doublequote,
#                 skipinitialspace,
#                 lineterminator,
#                 quoting,
#                 strict,
#                 encoding,
#             )
#         )

#         return builder

#     return wrapper


class ClickhouseExecute(PipeTask):
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        params = self.params
        external_tables = self.external_tables
        query_id = self.query_id
        settings = self.settings
        types_check = self.types_check
        print_query = self.print_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

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

        if print_query:
            print(query)

        cur.execute(query, params)


def ch_execute(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    print_query=False,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecute(
                builder.context_key,
                builder.template_render,
                query,
                params,
                external_tables,
                query_id,
                settings or {},
                types_check,
                print_query,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class ClickhouseExecuteIter(PipeTask):
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        query = self.query
        params = self.params
        external_tables = self.external_tables
        query_id = self.query_id
        settings = self.settings
        types_check = self.types_check
        print_query = self.print_query

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]

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

        if print_query:
            print(query)

        cur.execute(query, params)


def ch_execute_iter(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    external_tables: Optional[Dict] = None,
    query_id: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    types_check=False,
    print_query=False,
    cur_key=ch_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Отправить sql запрос в clickhouse

    - settings: https://clickhouse.com/docs/en/operations/settings/settings
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            ClickhouseExecuteIter(
                builder.context_key,
                builder.template_render,
                query,
                params,
                external_tables,
                query_id,
                settings or {},
                types_check,
                print_query,
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = os.path.expandvars(self.sql_file)
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        print_query = self.print_query

        print(f"try rendering file: {sql_file}")
        query = Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        query = self.template_render(query, context)
        print(f"rendering success: {sql_file}")

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
        client: ClickhouseClient = cur._client
        conn: ClickhouseConnection = client.get_connection()

        if settings:
            cur.set_settings(settings)

        if query_id:
            cur.set_query_id(query_id)

        cur.set_types_check(False)

        if client_settings:
            conn.context.client_settings |= client_settings

        if print_query:
            print(query)

        cur.execute(query, params)


def ch_execute_file_query(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    print_query=False,
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
                print_query,
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
        print_query: bool,
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
        self.print_query = print_query

    def __call__(self, context):
        self.render_template_fields(context)

        sql_file = self.sql_file
        params = self.params
        settings = self.settings
        client_settings = self.client_settings
        query_id = self.query_id
        sep = self.sep
        print_query = self.print_query

        sql_file = os.path.expandvars(sql_file)
        query = Path(sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        query = self.template_render(query, context)

        cur: ClickhouseCursor = context[self.context_key][self.cur_key]
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

            if print_query:
                print(query_part)

            cur.execute(query_part, params)

            if print_query:
                print(f"recv rows: {cur.rowcount}")


def ch_execute_file_queries(
    sql_file: str,
    params: Optional[Dict[str, Any]] = None,
    settings: Optional[Dict[str, Any]] = None,
    client_settings: Optional[Dict[str, Any]] = None,
    query_id: Optional[str] = None,
    print_query: bool = False,
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
                print_query,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
