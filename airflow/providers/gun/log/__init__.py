import logging
import datetime
import pendulum
import psycopg2.extras

from typing import (
    Callable,
    Optional,
    Union,
    Any,
    Dict,
)
from contextlib import closing, ExitStack

import psycopg2
import psycopg2.extras
import psycopg2.sql

from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import Connection
from airflow.models.taskinstance import TaskInstance


class PostgresLoggerHandler(logging.Handler):
    def __init__(
        self,
        conn_id: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        conn_dsn: Optional[str] = None,
        extra_register: Optional[Callable[[psycopg2.extensions.cursor], None]] = None,
    ):
        self.conn_id = conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.conn_dsn = conn_dsn
        self.extra_register = extra_register

        super().__init__(level=0)

    def get_conn_and_cur(self, stack):
        conn = stack.enter_context(closing(self.get_conn()))
        cur = stack.enter_context(closing(self.get_cur(conn)))

        return conn, cur

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

        conn_args = conn_airflow

        if self.conn_dsn:
            conn_args |= psycopg2.extensions.parse_dsn(self.conn_dsn)

        conn = psycopg2.connect(**conn_args)

        return conn

    def get_cur(self, conn: psycopg2.extensions.connection):
        return conn.cursor()

    def emit(self, record: logging.LogRecord):
        schema_name, table_name = self._get_schema_and_table(record)

        with ExitStack() as stack:
            pg_conn, pg_cur = self.get_conn_and_cur(stack)

            psycopg2.extras.register_uuid(conn_or_curs=pg_cur)
            psycopg2.extras.register_json(conn_or_curs=pg_cur)
            psycopg2.extras.register_default_json(conn_or_curs=pg_cur)
            psycopg2.extensions.register_adapter(
                dict, psycopg2.extras.Json
            )  # необходимо для обработки dict

            if self.extra_register:
                self.extra_register(pg_cur)

            names_stmp, values_stmp, params = self._sql_parts(record)
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
                schema_name=psycopg2.sql.Identifier(schema_name),
                table_name=psycopg2.sql.Identifier(table_name),
            )
            stmp = stmp.as_string(pg_cur)

            try:
                pg_cur.execute(stmp, params)
                pg_conn.commit()
            except Exception as e:
                print(f"error: {e}")
                print(f"msg: {record.msg}")
                raise

    def _get_schema_and_table(self, record):
        schema_name = None

        if hasattr(record, "schema_name"):
            schema_name = record.schema_name
        elif self.schema_name:
            schema_name = self.schema_name
        else:
            raise ValueError(
                f'you forgot to pass the "scheme_name" for recording the log in the object: {type(self)}'
            )

        table_name = None

        if hasattr(record, "table_name"):
            table_name = record.table_name
        elif self.table_name:
            table_name = self.table_name
        else:
            raise ValueError(
                f'you forgot to pass the "table_name" for recording the log in the object: {type(self)}'
            )

        return (schema_name, table_name)

    def _sql_parts(self, record):
        values = map(lambda x: "%s", record.msg.keys())
        values = ", ".join(values)

        names = map(lambda x: psycopg2.sql.Identifier(x), record.msg.keys())
        names = psycopg2.sql.SQL(", ").join(names)

        params = map(lambda x: x, record.msg.values())
        params = tuple(params)

        return (names, values, params)


def get_pg_logger(
    conn_id: str,
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    conn_dsn: Optional[str] = None,
    extra_register: Union[Callable[[psycopg2.extensions.cursor], None], None] = None,
    logger_name: Optional[str] = __name__,
):
    """
    Возвращает логгер который пишет dict в таблицу postgres
    умеет писать Json поля в виде Json

    пример использования:
    logger = get_pg_logger('mylogger', 'airflow_connection_id', 'my_postgres_schema', 'my_postgres_table')
    logger.info({
        "log_id": uuid.uuid4(),
        "integration_id": 0,
        "process_name": "dag_name",
        "task_name": "my task name",
        "status": 1,
        "payload": {
            "target_row": pg_cur.rownumber,
            "source_row": 0,
            "error_row": 0,
        },
    })

    - logger_name - имя логера
    - conn_id - Airflow Connectio Id в котором содержится учётная запись для подключения к postgres
    - schema_name - схема по умолчанию, в которую буде писать logger
    - table_name - таблица по умолчанию, в которую буде писать logger
    - conn_dsn - дополнительные dsn параметры для postgres, например можно передать ApplicationName: "application_name=my_application_name"
    - extra_register - обработчик позволяющий сконфигурировать pg cursor перед выполнением insert. Может потребоваться, если объект логирования (Dict) содержит сложные типы данных, например hstore
    """
    pg_logger = logging.getLogger(logger_name)
    pg_logger.addHandler(
        PostgresLoggerHandler(
            conn_id, schema_name, table_name, conn_dsn, extra_register
        )
    )

    return pg_logger


def pg_log_callback(
    conn_id: str,
):
    logger = get_pg_logger(conn_id=conn_id)

    def wrapper(
        payload: Dict[str, Any],
        schema_name: str = "ctl",
        table_name: str = "t_integration",
    ):
        def wrapper(context):
            task: TaskInstance = context["task"]
            jinja_env = task.get_template_env()
            jinja_env.globals.update(
                {
                    "get_task_state": get_task_state,
                    "get_dag_run_state": get_dag_run_state,
                    "get_task_additional_data": get_task_additional_data,
                    "get_dag_run_additional_data": get_dag_run_additional_data,
                }
            )

            res = task.render_template(
                payload,
                context,
                jinja_env,
                set(),
            )

            logger.info(
                res,
                extra={
                    "schema_name": schema_name,
                    "table_name": table_name,
                },
            )

        return wrapper

    return wrapper


def pg_task_log_callback(
    conn_id: str,
):
    logger = get_pg_logger(conn_id=conn_id)

    def wrapper(
        payload: Dict[str, Any],
        schema_name: str = "ctl",
        table_name: str = "t_integration",
    ):
        def wrapper(context):
            base = {
                "process_id": "{{ run_id }}",
                "process_name": "{{ ti.dag_id }}",
                "process_system": "airflow [{{ ti.hostname }}]",
                "task_id": "{{ ti.task_id }}",
                "task_system": "airflow [{{ ti.hostname }}]",
                "status": "{{ get_task_state(task_instance) }}",
                "start_time": "{{ ti.start_date if ti.start_date is defined else None }}",
                "end_time": "{{ ti.end_date if ti.end_date is defined else None }}",
                "error_text": "{{ str(exception) if exception is defined else None }}",
                "additional_data": "{{ get_task_additional_data(task_instance) }}",
            }

            res = base | payload

            task: TaskInstance = context["task"]
            jinja_env = task.get_template_env()
            jinja_env.globals.update(
                {
                    "get_task_state": get_task_state,
                    "get_dag_run_state": get_dag_run_state,
                    "get_task_additional_data": get_task_additional_data,
                    "get_dag_run_additional_data": get_dag_run_additional_data,
                }
            )

            try:
                res = task.render_template(
                    res,
                    context,
                    jinja_env,
                    set(),
                )
            except Exception as e:
                print(f"{e}")
                print(f"res: {res}")
                raise

            logger.info(
                res,
                extra={
                    "schema_name": schema_name,
                    "table_name": table_name,
                },
            )

        return wrapper

    return wrapper


def get_task_state(ti: TaskInstance):
    match ti.state:
        case "success":
            return 1
        case "failed" | "upstream_failed" | "shutdown" | "restarting":
            return -1
        case "running" | "queued" | "scheduled":
            return 0
        case "up_for_retry" | "up_for_reschedule":
            return 0
        case None:
            return 0
        case _:
            return 0


def get_dag_run_state(ti: TaskInstance):
    match ti.dag_run.state:
        case "success":
            return 1
        case "failed":
            return -1
        case "running" | "queued":
            return get_task_state(ti)
        case _:
            return 0


def get_task_additional_data(ti):
    match ti.execution_date:
        case d if isinstance(d, datetime.datetime):
            execution_date = d.isoformat()
        case d if isinstance(d, pendulum.DateTime):
            execution_date = d.to_iso8601_string()
        case d:
            execution_date = str(d)

    return {
        "duration": ti.duration if ti.duration else None,
        "state": str(ti.state) if ti.state else None,
        "hostname": ti.hostname,
        "pool": ti.pool,
        "queue": ti.queue,
        "pid": ti.pid,
        "job_id": ti.job_id if ti.job_id else None,
        "execution_date": execution_date,
        "run_as_user": ti.run_as_user if hasattr(ti, "run_as_user") else None,
        "unixname": ti.unixname,
        "task_id": ti.task_id,
        "try_number": ti.try_number,
    }


def get_dag_run_additional_data(ti: TaskInstance):
    match ti.dag_run.execution_date:
        case d if isinstance(d, datetime.datetime):
            execution_date = d.isoformat()
        case d if isinstance(d, pendulum.DateTime):
            execution_date = d.to_iso8601_string()
        case d:
            execution_date = str(d)

    return {
        "state": str(ti.dag_run.state),
        "id": ti.dag_run.id,
        "execution_date": execution_date,
        "run_type": str(ti.dag_run.run_type),
    }


def pg_task_log_dp_callback(conn_id: str):
    base_log_callback = pg_task_log_callback(conn_id)

    return base_log_callback(
        {
            "log_uuid": "{{ log_uuid }}",
            "integration_id": "{{ s__integration_id }}",
            "source_type": "{{ squilified_1 if squilified_1 is defined else None }}",
            "dest_type": "{{ dest_type if dest_type is defined else None }}",
            "squilified_1": "{{ squilified_1 if squilified_1 is defined else None }}",
            "squilified_2": "{{ squilified_2 if squilified_2 is defined else None }}",
            "squilified_3": "{{ squilified_3 if squilified_3 is defined else None }}",
            "squilified_4": "{{ squilified_4 if squilified_4 is defined else None }}",
            "squilified_5": "{{ squilified_5 if squilified_5 is defined else None }}",
            "tquilified_1": "{{ tquilified_1 if tquilified_1 is defined else None }}",
            "tquilified_2": "{{ tquilified_2 if tquilified_2 is defined else None }}",
            "tquilified_3": "{{ tquilified_3 if tquilified_3 is defined else None }}",
            "tquilified_4": "{{ tquilified_4 if tquilified_4 is defined else None }}",
            "tquilified_5": "{{ tquilified_5 if tquilified_5 is defined else None }}",
        }
    )
