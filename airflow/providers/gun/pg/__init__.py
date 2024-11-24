import os
import sys
import io
import gzip
import threading
from typing import (
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    TextIO,
    Type,
    Union,
    Dict,
)

from pathlib import Path
from contextlib import closing

import psycopg2
import psycopg2.extras
import psycopg2.sql

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.providers.gun.pipe import PipeTask, PipeTaskBuilder, PipeStage


__all__ = [
    "pg_execute",
    "pg_execute_and_commit",
    "pg_execute_file",
    "pg_execute_file_and_commit",
    "pg_fetch_to_stdout",
    "pg_commit",
    "pg_module",
    "pg_copy_to_pg",
    "pg_copy_to_handle",
    "pg_copy_to_stdout",
    "pg_copy_to_file",
    "pg_copy_to_gzip",
    "pg_copy_to_file_use_sql_file",
    "pg_copy_to_gzip_use_sql_file",
    "pg_copy_from_file",
    "pg_copy_from_gzip",
    "pg_copy_from_file_use_sql_file",
    "pg_copy_from_gzip_use_sql_file",
    "pg_register_uuid",
    "pg_register_json",
    "pg_register_inet",
    "pg_register_hstore",
    "pg_register_default_json",
    "pg_register_default_jsonb",
    "pg_register_composite",
    "pg_register_ipaddress",
    "pg_register_range",
    "pg_save_to_xcom",
    "pg_res_execute_and_commit",
    "pg_res_fetchall_to_xcom",
]

pg_cur_key_default = "pg_cur"


class StringIteratorIO(io.TextIOBase):
    """на будущее"""

    def __init__(self, iterator):
        self._iter = iterator
        self._buff = ""

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret) :]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return "".join(line)


class PrintSqlCursor(psycopg2.extensions.cursor):
    """
    Курсор который принтует запросы в stdout
    """

    def execute(self, query, vars=None):
        try:
            print("---- query ----")
            print(self.connection.dsn)
            print(query)
            print(f"parameters: {vars}")
            res = super().execute(query, vars)
            print("---- query success ----")
            return res
        except Exception as e:
            print(f"---- query error: {e}", file=sys.stderr)
            raise

    def executemany(self, query, vars_list):
        try:
            print("---- query ----")
            print(self.connection.dsn)
            print(query)
            res = super().executemany(query, vars_list)
            print("---- query success ----")
            return res
        except Exception as e:
            print(f"---- query error: {e}", file=sys.stderr)
            raise

    def callproc(self, procname, vars=None):
        try:
            print(f"---- call {procname} ({vars}) ----")
            res = super().callproc(procname, vars)
            print("---- call success ----")
            return res
        except Exception as e:
            print(f"---- call error: {e}", file=sys.stderr)
            raise

    def copy_expert(self, sql, file, **kwargs):
        try:
            print(f"---- copy ----")
            print(self.connection.dsn)
            print(sql)
            res = super().copy_expert(sql, file, **kwargs)
            print("---- copy success ----")
            return res
        except Exception as e:
            print(f"---- copy error: {e}", file=sys.stderr)
            raise

    def copy_from(
        self,
        file,
        table: str,
        sep: str = "\t",
        null: str = "\\N",
        size: int = 8192,
        columns: Iterable[str] | None = None,
    ) -> None:
        try:
            print(f"---- copy from ----")
            print(self.connection.dsn)
            print(f"table: {table}")
            print(f"sep: {sep}")
            print(f"null: {null}")
            print(f"size: {size}")
            print(f"file: {file}")
            if columns:
                print(f"columns: {columns}")

            res = super().copy_from(file, table, sep, null, size, columns)
            print("---- copy from success ----")
            return res
        except Exception as e:
            print(f"---- copy from error: {e}", file=sys.stderr)
            raise

    def copy_to(
        self,
        file,
        table: str,
        sep: str = "\t",
        null: str = "\\N",
        columns: Iterable[str] | None = None,
    ):
        try:
            print(f"---- copy to ----")
            print(self.connection.dsn)
            print(f"table: {table}")
            print(f"file: {file}")
            if columns:
                print(f"columns: {columns}")

            res = super().copy_to(file, table, sep, null, columns)
            print("---- copy to success ----")
            return res
        except Exception as e:
            print(f"---- copy to error: {e}", file=sys.stderr)
            raise


class PostgresAuthAirflowConnection(PipeTask):
    def __init__(
        self,
        context_key: str,
        stack_key: str,
        template_render: Callable,
        conn_id: Optional[str] = None,
        dsn: Optional[str] = None,
        connection_factory: Optional[Callable] = None,
        cursor_name: Optional[str] = None,
        cursor_factory: Optional[Type[psycopg2.extensions.cursor]] = None,
        cursor_withhold: Optional[bool] = None,
        cursor_scrollable: Optional[bool] = None,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "conn_id",
                "dsn",
                "cursor_name",
                "cursor_withhold",
                "cursor_scrollable",
            )
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.cur_key_deprecated = "cur"
        self.context_key = context_key
        self.stack_key = stack_key
        self.conn_id = conn_id
        self.dsn = dsn
        self.connection_factory = connection_factory
        self.cursor_name = cursor_name
        self.cursor_factory = cursor_factory
        self.cursor_withhold = cursor_withhold
        self.cursor_scrollable = cursor_scrollable

    @staticmethod
    def default_connection_factory(**conn):
        conn = psycopg2.connect(**conn)
        return conn

    def get_cur_with_stack(self, stack):
        if not self.connection_factory:
            self.connection_factory = (
                PostgresAuthAirflowConnection.default_connection_factory
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
        """Преобразование Airflow connection в аргументы коннекшина для psycopg2
        драйвер psycopg2 принимает следующие параметры: https://github.com/psycopg/psycopg2/blob/master/psycopg/conninfo_type.c
        """
        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": conn.schema,
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

        if self.dsn:
            conn_user = psycopg2.extensions.parse_dsn(self.dsn)
        else:
            conn_user = {}

        conn_args = conn_airflow | conn_user

        return conn_args

    def get_cur(self, conn: psycopg2.extensions.connection):
        """
        Создаёт cursor
        """
        cursor_name = self.cursor_name
        cursor_factory = self.cursor_factory
        withhold = self.cursor_withhold or False
        scrollable = self.cursor_scrollable

        if cursor_factory:
            cur = conn.cursor(
                name=cursor_name,
                cursor_factory=cursor_factory,
                withhold=withhold,
                scrollable=scrollable,
            )
        else:
            cur = conn.cursor(
                name=cursor_name,
                withhold=withhold,
                scrollable=scrollable,
            )

        return cur

    def __call__(self, context):
        self.render_template_fields(context)

        stack = context[self.stack_key]
        pg_cur = self.get_cur_with_stack(stack)
        share = context[self.context_key]
        share[self.cur_key] = pg_cur
        share[self.cur_key_deprecated] = pg_cur


def pg_auth_airflow_conn(
    conn_id: str,
    dsn: Optional[str] = None,
    conn_factory: Optional[Any] = None,
    cursor_factory: Optional[Any] = None,
    cursor_name: Optional[str] = None,
    cursor_withhold: Optional[bool] = None,
    cursor_scrollable: Optional[bool] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Использовать указанный Airflow connection для подключения к postgres
    postgres_conn_id - может быть указано в виде jinja шаблона

    Airflow connection парсится на словарь атрибутов:
    host     - host в psycopg2
    schema   - dbname в psycopg2
    login    - user в psycopg2
    password - password в psycopg2
    port     - port  в psycopg2
    extra    - сюда можно передать остальные аргументы подключения в виде json словаря. данный словарь объедениться с первичными аргументами

    Из указанных атрибутов будет сформирована dsn строка подключения, например:
    {'password': 'secret', 'user': 'postgres', 'dbname': 'test'}
    'dbname=test user=postgres password=secret'

    {'host': 'example.com', 'user': 'someone', 'dbname': 'somedb', 'connect_timeout': '10'}
    "postgresql://someone@example.com/somedb?connect_timeout=10"

    Все dsn атрибуты можно посмотреть тут: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.ConnectionInfo

    pg_auth_conn_id и pg_auth_dsn можно использовать совместно
    если указаны оба варианта, атрибуты сливаются в единный словарь
    поверх pg_auth_conn_id накладываются атрибуты pg_auth_dsn и переопределяют их
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                conn_id=conn_id,
                dsn=dsn,
                connection_factory=conn_factory,
                cursor_factory=cursor_factory,
                cursor_name=cursor_name,
                cursor_scrollable=cursor_scrollable,
                cursor_withhold=cursor_withhold,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def pg_auth_dsn(
    dsn: str,
    conn_factory: Optional[Any] = None,
    cursor_factory: Optional[Any] = None,
    cursor_name: Optional[str] = None,
    cursor_withhold: Optional[bool] = None,
    cursor_scrollable: Optional[bool] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Указать dsn строку подключения
    Все dsn атрибуты можно посмотреть тут: https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.ConnectionInfo

    pg_auth_conn_id и pg_auth_dsn можно использовать совместно
    если указаны оба варианта, атрибуты сливаются в единный словарь
    поверх pg_auth_conn_id накладываются атрибуты pg_auth_dsn и переопределяют их
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresAuthAirflowConnection(
                builder.context_key,
                builder.stack_key,
                builder.template_render,
                dsn=dsn,
                connection_factory=conn_factory,
                cursor_factory=cursor_factory,
                cursor_name=cursor_name,
                cursor_scrollable=cursor_scrollable,
                cursor_withhold=cursor_withhold,
                cur_key=cur_key,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class PostgresExecuteModule(PipeTask):
    """Выполняет sql запрос без выполнения commit"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql,
        params,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        self.set_template_fields(["sql", "params"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql = sql
        self.params = params

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]
        pg_cur.execute(self.sql, self.params)


def pg_execute(
    sql: str,
    params=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить sql запрос
    commit не выполняется, только execute
    Для выполнения commit можно использовать
        @pg_commit
        @pg_execute_and_commit
    sql запрос может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresExecuteModule(
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


class PostgresExecuteAndCommitModule(PostgresExecuteModule):
    """Выполняет sql запрос и выполняет commit"""

    def __call__(self, context):
        super().__call__(context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_execute_and_commit(
    sql: str,
    params=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос и выполнить commit
    sql запрос может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresExecuteAndCommitModule(
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


class PostgresExecuteFileModule(PipeTask):
    """Выполняет sql запрос из переданного файла без выполнения commit"""

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        params,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["sql_file", "params"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.params = params

    def __call__(self, context):
        self.render_template_fields(context)
        self.sql_file = os.path.expandvars(self.sql_file)

        sql = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )

        print(f"try rendering file: {self.sql_file}")
        sql = self.template_render(sql, context)
        print(f"rendering success: {self.sql_file}")

        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.execute(sql, self.params)


def pg_execute_file(
    sql_file: str,
    params=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла
    sql_file - может быть передан в как jinja шаблон
    slq_file - контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresExecuteFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresExecuteFileAndCommitModule(PostgresExecuteFileModule):
    """Выполнить sql запрос и после этого commit"""

    def __call__(self, context):
        super().__call__(context)

        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_execute_file_and_commit(
    sql_file: str,
    params=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить запрос из указанного sql файла и выполнить commit
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresExecuteFileAndCommitModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                params,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresFetchToStdoutModule(PipeTask):
    """Считывает курсор и принтует данные в stdout"""

    def __init__(
        self,
        context_key: str,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)

        self.cur_key = cur_key

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]
        try:
            for record in pg_cur:
                print(record)
        except psycopg2.ProgrammingError as e:
            print(f"---- {e} -----")


def pg_fetch_to_stdout(
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Прочитать данные построчно и вывести их в stdout
    Данные выводятся с помощью print в том виде, в котором их принял драйвер psycopg
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresFetchToStdoutModule(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCommitModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)

        self.cur_key = cur_key

    def __call__(self, context):
        pg_cur = context[self.context_key][self.cur_key]
        pg_cur.connection.commit()


def pg_commit(
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить commit
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCommitModule(
                builder.context_key,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_module(
    module: Callable[[Any], None],
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет кастомный модуль обработки в конвейр PostgresBuilder
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            module,
            pipe_stage,
        )
        return builder

    return wrapper


# Copy From Thread
class CopyFromPipeThread(threading.Thread):
    def __init__(
        self,
        read_f,
        tgt_cursor,
        tgt_schema: str,
        tgt_table: str,
        tgt_truncate: bool,
        tgt_commit: bool,
        tgt_commit_after_truncate: bool,
        sep: str,
        null: str,
        size: int,
        columns: Iterable[str] | None,
        group: None = None,
        target: Callable[..., object] | None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.read_f = read_f
        self.tgt_cursor: psycopg2.extensions.cursor = tgt_cursor
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.tgt_truncate = tgt_truncate
        self.tgt_commit = tgt_commit
        self.tgt_commit_after_truncate = tgt_commit_after_truncate
        self.sep = sep
        self.null = null
        self.size = size
        self.columns = columns

    def _set_search_path(self):
        stmp = psycopg2.sql.SQL("set search_path = {}").format(
            psycopg2.sql.Identifier(self.tgt_schema),
        ).as_string(self.tgt_cursor)
        
        self.tgt_cursor.execute(stmp)

    def _truncate_table_if_needed(self):
        if self.tgt_truncate:
            stmp = psycopg2.sql.SQL("truncate table {}.{}").format(
                psycopg2.sql.Identifier(self.tgt_schema),
                psycopg2.sql.Identifier(self.tgt_table),
            ).as_string(self.tgt_cursor)
            print(stmp)
            self.tgt_cursor.execute(stmp)

            if self.tgt_commit_after_truncate:
                print("tgt commit;")
                self.tgt_cursor.connection.commit()

    def before_copy(self):
        self._set_search_path()
        self._truncate_table_if_needed()

    def copy_from(self):
        print(f"starting: {self.name} ...")
        self.tgt_cursor.copy_from(
            self.read_f,
            self.tgt_table,
            self.sep,
            self.null,
            self.size,
            self.columns,
        )
        print_query = (
            self.tgt_cursor.query.decode("utf-8", "ignore")
            if isinstance(self.tgt_cursor.query, bytes)
            else self.tgt_cursor.query
        )
        print(f"tgt: {print_query}")

        if self.tgt_commit:
            print("tgt commit;")
            self.tgt_cursor.connection.commit()

    def run(self):
        self.exc = None
        try:
            self.copy_from()
        except BaseException as e:
            self.exc = e
        finally:
            self.read_f.close()

    def join(self):
        threading.Thread.join(self)

        if self.exc:
            raise self.exc


# Copy To Thread
class CopyToPipeThread(threading.Thread):
    def __init__(
        self,
        write_f,
        src_cursor,
        src_schema: str,
        src_table: str,
        sep: str,
        null: str,
        columns: Iterable[str] | None,
        group: None = None,
        target: Callable[..., object] | None = None,
        name: str | None = None,
        args: Iterable[Any] = (),
        kwargs: Mapping[str, Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.write_f = write_f
        self.src_cursor: psycopg2.extensions.cursor = src_cursor
        self.src_schema = src_schema
        self.src_table = src_table
        self.sep = sep
        self.null = null
        self.columns = columns
        self._reset_isolation_level = None

    def _set_search_path(self):
        stmp = psycopg2.sql.SQL("set search_path = {}").format(
            psycopg2.sql.Identifier(self.src_schema),
        ).as_string(self.src_cursor)
        
        self.src_cursor.execute(stmp)

    def before_copy(self):
        self._set_search_path()

    def copy_to(self):
        print(f"starting: {self.name} ...")
        self.src_cursor.copy_to(
            self.write_f,
            self.src_table,
            self.sep,
            self.null,
            self.columns,
        )
        print_query = (
            self.src_cursor.query.decode("utf-8", "ignore")
            if isinstance(self.src_cursor.query, bytes)
            else self.src_cursor.query
        )
        print(f"src: {print_query}")

    def run(self):
        self.exc = None
        try:
            self.copy_to()
        except BaseException as e:
            self.exc = e
        finally:
            self.write_f.close()

    def join(self):
        threading.Thread.join(self)

        if self.exc:
            raise self.exc


class PostgresCopyModule(PipeTask):
    """Выполняет copy из одного postgres в другой
    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        src_cur_key: str,
        src_schema: str,
        src_table: str,
        tgt_cur_key: str,
        tgt_schema: str,
        tgt_table: str,
        tgt_truncate: bool,
        tgt_commit: bool,
        sep: str,
        null: str,
        size: int,
        columns: Iterable[str] | None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            [
                "src_cur_key",
                "src_schema",
                "src_table",
                "tgt_cur_key",
                "tgt_schema",
                "tgt_table",
                "tgt_truncate",
                "sep",
                "null",
                "size",
                "columns",
            ]
        )
        super().set_template_render(template_render)

        self.src_cur_key = src_cur_key
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_cur_key = tgt_cur_key
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.tgt_truncate = tgt_truncate
        self.tgt_commit = tgt_commit
        self.sep = sep
        self.null = null
        self.size = size
        self.columns = columns

    def __call__(self, context):
        self.render_template_fields(context)

        src_cursor: psycopg2.extensions.cursor = context[self.context_key].get(
            self.src_cur_key
        )
        tgt_cursor: psycopg2.extensions.cursor = context[self.context_key].get(
            self.tgt_cur_key
        )

        if src_cursor is None:
            raise RuntimeError(
                f"""Unable to find postgres cursor context[{self.context_key}][{self.src_cur_key}] in Airflow context
To get the cursor, call pg_auth_airflow_conn method before this function"""
            )

        if tgt_cursor is None:
            raise RuntimeError(
                f"""Unable to find postgres cursor context[{self.context_key}][{self.src_cur_key}] in Airflow context
To get the cursor, call pg_auth_airflow_conn method before this function"""
            )

        print(f"src: {src_cursor.connection.dsn}")
        print(f"tgt: {tgt_cursor.connection.dsn}")
        print(
            f"pg copy {self.src_schema}.{self.src_table} -> {self.tgt_schema}.{self.tgt_table} ..."
        )

        r_fd, w_fd = os.pipe()

        # определяю, является ли таблица указанная в src и tgt одной и той же
        # дело в том, что если пользователь указал одну и ту же таблицу на копирование (т.е. сделать копирование таблицы саму в себя - продублировать данные в таблице)
        # и одновременно с этим указал что в tgt нужно выполнить truncate перед выполнением копирования, то это приведет к ожиданию блокировки, т.к
        # truncate table получает эксклюзивную блокировку на таблицу, а последующий запуск copy to stdout будет ожидать, когда эту блокировку отпустят
        # данная ситуация возможна только если работа ведется в той же таблице, на том же сервере, в разных подключениях
        # поэтому я решил, эту ситуацию необходимо обработать как отсутствие запуска команды copy, ведь по логике
        # пользователь ожидает что таблица будет отчищена, а потом скопированы те-же самые данные что и были
        # есть вероятность что эта логика неверна, но хрен знает...
        tgt_without_copy = False
        tgt_commit_after_truncate = False

        if (
            src_cursor.connection.info.host == tgt_cursor.connection.info.host
            and src_cursor.connection.info.dbname == tgt_cursor.connection.info.dbname
            and self.src_schema == self.tgt_schema
            and self.src_table == self.tgt_table
            and self.tgt_truncate == True
        ):
            print(
                f"""trying copy table into itself with truncate: {self.tgt_schema}.{self.tgt_table}"""
            )
            print(f"""This means that no copying is required""")
            print(
                f"""If this logic does not suit you, write your own implementation of copy"""
            )
            tgt_without_copy = True
            tgt_commit_after_truncate = True

        if not tgt_without_copy:
            with os.fdopen(r_fd, "r") as read_f:
                with os.fdopen(w_fd, "w") as write_f:
                    # добавляю асинхронную задачу на копирование pipe -> tgt_schema.tgt_table
                    copy_from_pipe = CopyFromPipeThread(
                        read_f,
                        tgt_cursor,
                        self.tgt_schema,
                        self.tgt_table,
                        self.tgt_truncate,
                        self.tgt_commit,
                        tgt_commit_after_truncate,
                        self.sep,
                        self.null,
                        self.size,
                        self.columns,
                        name=f"copy_from_pipe: pipe({read_f.name}) -> {self.tgt_schema}.{self.tgt_table}",
                    )
                    # добавляю асинхронную задачу на копирование src_schema.src_table -> pipe
                    copy_to_pipe = CopyToPipeThread(
                        write_f,
                        src_cursor,
                        self.src_schema,
                        self.src_table,
                        self.sep,
                        self.null,
                        self.columns,
                        name=f"copy_to_pipe: {self.src_schema}.{self.src_table} -> pipe({write_f.name})",
                    )

                    # выполняю truncate table на tgt и установку set_search_path на обоих клиентах
                    copy_to_pipe.before_copy()
                    copy_from_pipe.before_copy()

                    # стартую задачи в асинхронном режиме
                    copy_from_pipe.start()
                    copy_to_pipe.start()

                    # сначала ожидаю выполнения copy_to и соответственно закрытия read_f и только в такой последовательности (не меняй последовательность)
                    # copy_to копирует данные в pipe, и когда copy_to завершится, то будет автоматически закрыт write_f сторона pipe
                    copy_to_pipe.join()
                    copy_from_pipe.join()

                    print(f"pg copy success: {tgt_cursor.rowcount} rows")


def pg_copy_to_pg(
    src_cur_key: str,
    src_schema: str,
    src_table: str,
    tgt_cur_key: str,
    tgt_schema: str,
    tgt_table: str,
    tgt_truncate: bool = False,
    tgt_commit: bool = True,
    sep: str = "\t",
    null: str = "\\N",
    size: int = 8192,
    columns: Iterable[str] | None = None,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy таблицы из одного postgres в другой

    документация по copy: https://www.psycopg.org/docs/cursor.html#cursor.copy_from
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyModule(
                builder.context_key,
                builder.template_render,
                src_cur_key=src_cur_key,
                src_schema=src_schema,
                src_table=src_table,
                tgt_cur_key=tgt_cur_key,
                tgt_schema=tgt_schema,
                tgt_table=tgt_table,
                tgt_truncate=tgt_truncate,
                tgt_commit=tgt_commit,
                sep=sep,
                null=null,
                size=size,
                columns=columns,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertModule(PipeTask):
    """Выполняет copy_expert в указанный handler, например sys.stdout
    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy
    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        handler: TextIO,
        params=None,
        size_limit: Optional[int] = None,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["sql", "params", "size_limit"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql = sql
        self.handler = handler
        self.params = params
        self.size_limit = size_limit

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        print(f"copy expert and handler type: {type(self.handler)}")
        if self.size_limit:
            pg_cur.copy_expert(self.sql, self.handler, self.size_limit)
        else:
            pg_cur.copy_expert(self.sql, self.handler)


def pg_copy_to_handle(
    sql: str,
    handler: TextIO,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в указанный handle (open() от файла или stdout)
    используя переданный sql запрос (принтует результат выполнения запроса)
    sql запрос может содержать jinja шаблоны

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertModule(
                builder.context_key,
                builder.template_render,
                sql,
                handler,
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlToStdoutModule(PostgresCopyExpertModule):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        params=None,
        size_limit: Optional[int] = None,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(
            context_key,
            template_render,
            sql,
            sys.stdout,
            params,
            size_limit,
            cur_key=cur_key,
        )


def pg_copy_to_stdout(
    sql: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в stdout используя переданный sql запрос (принтует результат выполнения запроса)
    sql запрос может содержать jinja шаблоны

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlToStdoutModule(
                builder.context_key,
                builder.template_render,
                sql,
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlAndFileModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql: str,
        data_file: str,
        mode: str,
        params=None,
        size_limit: Optional[int] = None,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ["sql", "data_file", "mode", "params", "size_limit"]
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql = sql
        self.data_file = data_file
        self.params = params
        self.size_limit = size_limit
        self.mode = mode

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with open(file, mode=self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(self.sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(self.sql, f)

        print(f"postgres copy success")


def pg_copy_to_file(
    sql: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в файл используя переданный sql запрос
    sql запрос может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlAndFileModule(
                builder.context_key,
                builder.template_render,
                sql,
                to_file,
                "wb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_copy_from_file(
    sql: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy из файла используя переданный sql запрос
    Файл будет прочитан как как поток байт и отправлен в postgres
    sql может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlAndFileModule(
                builder.context_key,
                builder.template_render,
                sql,
                from_file,
                "rb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlAndGZipModule(PostgresCopyExpertSqlAndFileModule):
    """
    Модуль аналогичен PostgresCopyExpertSqlToFileModule
    Однако сохраняет данные в GZip формате
    """

    def __call__(self, context):
        super().__call__(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)

        if self.params:
            self.sql = pg_cur.mogrify(self.sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)

        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with gzip.open(file, mode=self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(self.sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(self.sql, f)

        print(f"postgres copy success")


def pg_copy_to_gzip(
    sql: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в файл (архивируя поток в формате gzip) используя переданный sql запрос
    sql запрос может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql,
                to_file,
                "wb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_copy_from_gzip(
    sql: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy из заархивированного файла (в формате gzip) используя переданный sql запрос
    Внутри архива gzip может быть любой фалй, он будет прчитан как поток байт и отправлен в postgres
    sql запрос может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql,
                from_file,
                "rb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlFileAndFileModule(PipeTask):
    """Выполняет copy_expert команду из указанного sql файла
    путь к sql файлу можно указать в виде jinja шаблона
    sql стейт в файле может содержать jinja шаблоны
    В запросе необходимо передать TO STDOUT обязательно (запрос не должен содержать FROM STDIN)
    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        sql_file: str,
        data_file: str,
        mode: str,
        params=None,
        size_limit: Union[int, None] = None,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            ["sql_file", "data_file", "mode", "params", "size_limit"]
        )
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.sql_file = sql_file
        self.data_file = data_file
        self.mode = mode
        self.params = params
        self.size_limit = size_limit

    def _open(self, file, mode):
        return open(file, mode=mode)

    def __call__(self, context):
        self.render_template_fields(context)

        pg_cur = context[self.context_key][self.cur_key]

        self.data_file = os.path.expandvars(self.data_file)
        self.sql_file = os.path.expandvars(self.sql_file)

        sql = (
            Path(self.sql_file).absolute().read_text(encoding="utf-8", errors="ignore")
        )
        print(f"try rendering file: {self.sql_file}")
        sql = self.template_render(sql, context)
        print(f"rendering success: {self.sql_file}")

        if self.params:
            sql = pg_cur.mogrify(sql, self.params)

        file = Path(self.data_file).absolute()
        file.parent.mkdir(parents=True, exist_ok=True)
        print(f"postgres copy use file (mode: {self.mode}): {file} ...")

        with self._open(file, self.mode) as f:
            if self.size_limit:
                pg_cur.copy_expert(sql, f, self.size_limit)
            else:
                pg_cur.copy_expert(sql, f)

        print(f"postgres copy success")


def pg_copy_to_file_use_sql_file(
    sql_file: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в файл используя переданный sql file
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlFileAndFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                to_file,
                "wb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresCopyExpertSqlFileAndGZipModule(PostgresCopyExpertSqlFileAndFileModule):
    """
    Модуль аналогичен PostgresCopyExpertSqlToFileModule
    Однако сохраняет данные в GZip формате
    """

    def _open(self, file, mode):
        return gzip.open(file, mode=mode)


def pg_copy_from_file_use_sql_file(
    sql_file: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy из файла используя переданный sql file
    Файл будет прочитан как как поток байт и отправлен в postgres
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlFileAndFileModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                from_file,
                "rb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_copy_to_gzip_use_sql_file(
    sql_file: str,
    to_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy в файл (архивируя поток в формате gzip) используя переданный sql file
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    to_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy (
        select * from public.pgbench_history where tid > 4
    )
    to stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlFileAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                to_file,
                "wb",
                params,
                size_limit,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_copy_from_gzip_use_sql_file(
    sql_file: str,
    from_file: str,
    params=None,
    size_limit: Optional[int] = None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполняет copy из заархивированного файла (в формате gzip) используя переданный sql file
    Внутри архива gzip может быть любой файл, он будет прчитан как поток байт и отправлен в postgres
    sql_file - может быть передан в как jinja шаблон, контент в файле также может содержать jinja шаблоны
    from_file - путь к файлу может быть передан как jinja шаблон

    документация по copy: https://postgrespro.ru/docs/postgresql/14/sql-copy

    пример copy запроса:
    copy public.pgbench_history (tid, aid)
    from stdout with (
        format csv,
        header,
        delimiter ';',
        quote '"',
        escape '"'
    )
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresCopyExpertSqlFileAndGZipModule(
                builder.context_key,
                builder.template_render,
                sql_file,
                from_file,
                "rb",
                params,
                size_limit,
                cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresSaveToXComModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        name: str,
        func: Callable[[psycopg2.extensions.cursor, Any], Any],
        cur_key: str = pg_cur_key_default,
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

        pg_cur = context[self.context_key][self.cur_key]
        res = self.func(pg_cur, context)
        res = self.template_render(res, context)
        ti = context[self.ti_key]

        ti.xcom_push(key=self.name, value=res)


def pg_save_to_xcom(
    xcom_name: str,
    xcom_gen: Callable[[psycopg2.extensions.cursor, Any], Any],
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow XCom для последующего использования
    Для сохранения информации в xcom нужно передать:
    - xcom_name - это имя xcom записи. Напомню, что по умолчанию имя в xcom используется "return_value".
        Необходимо использовать любое другое имя, отличное от этого для передачи касомного xcom между задачами
    - xcom_gen - это функция, которая будет использована для генерации значения.
    xcom_gen принимает 1 параметр: psycopg2.extensions.cursor - текущий postgres cursor
    xcom_gen должна возвращать то значение, которое можно сериализовать в xcom

    Например можно сохранить кол-во строк, которые вернул postgres:
    @pg_save_to_xcom("myxcom", lambda cur, context: {
        'target_row': pg_cur.rownumber,
        'source_row': 0,
        'error_row': 0,
        "query": cur.query,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresSaveToXComModule(
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


class PostgresSaveToContextModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        name: str,
        func: Callable[[psycopg2.extensions.cursor, Any], Any],
        cur_key: str = pg_cur_key_default,
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
        pg_cur = context[self.context_key][self.cur_key]
        res = self.func(pg_cur, context)
        res = self.template_render(res, context)
        context[self.name] = res


def pg_save_to_context(
    context_name: str,
    context_gen: Callable[[psycopg2.extensions.cursor, Any], Any],
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить любую информацию в Airflow Context для последующего использования
    Для сохранения информации в context нужно передать:
    - context_name - это имя context ключа.
    - context_gen - это функция, которая будет использована для генерации значения, которое будет добавлено в context
    context_gen принимает 1 параметра: context
    context_gen должна возвращать то значение, которое унжно сохранить в context

    Например можно сохранить кол-во строк, которые вернул postgres:
    @pg_save_to_context("my_context_key", lambda cur, context: {
        'target_row': "{{ params.target_row }}",
        'source_row': 0,
        'error_row': 0,
    })
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresSaveToContextModule(
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


class PostgresInsertDictionaryModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        schema_name: str,
        table_name: str,
        payload: Dict,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_fields(["schema_name", "table_name", "payload"])
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.context_key = context_key
        self.template_render = template_render
        self.schema_name = schema_name
        self.table_name = table_name
        self.payload = payload

    def _sql_parts(self, record: Dict[str, Any]):
        values = map(lambda x: "%s", record.keys())
        values = ", ".join(values)

        names = map(lambda x: psycopg2.sql.Identifier(x), record.keys())
        names = psycopg2.sql.SQL(", ").join(names)

        params = map(lambda x: x, record.values())
        params = tuple(params)

        return (names, values, params)

    def __call__(self, context):
        self.render_template_fields(context)
        pg_cur = context[self.context_key][self.cur_key]

        # необходимо для обработки dict
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        names_stmp, values_stmp, params = self._sql_parts(self.payload)
        names_stmp = names_stmp.as_string(pg_cur)
        stmp = (
            "insert into {schema_name}.{table_name} ("
            + names_stmp
            + ") values ("
            + values_stmp
            + ")"
        )
        stmp = psycopg2.sql.SQL(stmp)
        stmp = stmp.format(
            schema_name=psycopg2.sql.Identifier(self.schema_name),
            table_name=psycopg2.sql.Identifier(self.table_name),
        )
        stmp = stmp.as_string(pg_cur)

        pg_cur.execute(stmp, params)
        pg_cur.connection.commit()


def pg_insert_dict(
    schema_name: str,
    table_name: str,
    payload: Dict,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresInsertDictionaryModule(
                builder.context_key,
                builder.template_render,
                schema_name,
                table_name,
                payload,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_uuid(
    oids=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку uuid типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_uuid(
                oids=oids,
                conn_or_curs=pg_cur,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_json(
    globally=False,
    loads=None,
    oid=None,
    array_oid=None,
    name="json",
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку json типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_json(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
                oid=oid,
                array_oid=array_oid,
                name=name,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_inet(
    oid=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку inet типов данных
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_inet(
                oid=oid,
                conn_or_curs=pg_cur,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_hstore(
    globally=False,
    unicode=False,
    oid=None,
    array_oid=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_hstore(
                conn_or_curs=pg_cur,
                globally=globally,
                unicode=unicode,
                oid=oid,
                array_oid=array_oid,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_default_json(
    globally=False,
    loads=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_default_json(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_default_jsonb(
    globally=False,
    loads=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку hstore хранения
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_default_jsonb(
                conn_or_curs=pg_cur,
                globally=globally,
                loads=loads,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_composite(
    name,
    globally=False,
    factory=None,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку composite
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_composite(
                name=name,
                conn_or_curs=pg_cur,
                globally=globally,
                factory=factory,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_ipaddress(
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку ipaddress
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_ipaddress(
                conn_or_curs=pg_cur,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


def pg_register_range(
    pgrange,
    pyrange,
    globally=False,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Добавляет поддержку ipaddress
    """

    def wrapper(builder: PipeTaskBuilder):
        def mod(context):
            pg_cur = context[builder.context_key][cur_key]
            psycopg2.extras.register_range(
                pgrange=pgrange,
                pyrange=pyrange,
                conn_or_curs=pg_cur,
                globally=globally,
            )

        builder.add_module(
            mod,
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresExecuteAndCommitResultAfterModule(PipeTask):
    """
    Выполняет sql запрос полученного результата (в том числе выполняет commit)
    """

    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        res_matcher: Callable[[Any], Any],
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.cur_key = cur_key
        self.res_matcher = res_matcher

    def __call__(self, res: Any, context):
        self.render_template_fields(context)

        res = self.res_matcher(res)
        self.template_render(res, context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]
        pg_cur.execute(res)

        return res


def pg_res_execute_and_commit(
    res: Callable[[Any], Any] = lambda res: res,
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Выполнить sql запрос результата функции (в том числе commit)

    sql запрос может содержать jinja шаблоны
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresExecuteAndCommitResultAfterModule(
                builder.context_key,
                builder.template_render,
                res,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper


class PostgresFetchallToXComModuleAfterModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        cur_key: str = pg_cur_key_default,
    ):
        super().__init__(context_key)
        super().set_template_render(template_render)

        self.cur_key = cur_key

    def __call__(self, res, context):
        self.render_template_fields(context)

        pg_cur: psycopg2.extensions.cursor = context[self.context_key][self.cur_key]

        return pg_cur.fetchall()


def pg_res_fetchall_to_xcom(
    cur_key: str = pg_cur_key_default,
    pipe_stage: Optional[PipeStage] = None,
):
    """
    Модуль позволяет сохранить сохранить полученный результат в xco
    """

    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresFetchallToXComModuleAfterModule(
                builder.context_key,
                builder.template_render,
                cur_key=cur_key,
            ),
            pipe_stage,
        )
        return builder

    return wrapper
