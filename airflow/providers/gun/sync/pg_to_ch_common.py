from typing import (
    Any,
    Optional,
    Dict,
    Sequence,
)
from datetime import datetime

import psycopg2.extras
import psycopg2.sql

from airflow.providers.gun.sync.pg_to_pg_common import (
    pg_convert_null_attr_to_type,
    pg_type_stmp_text,
)

from airflow.providers.gun.ch.hook import ClickhouseCursor

ch_precision_max = 76
ch_precision_scale_max = 8


def ch_convert_null_attr_to_type(type: str, attr: str):
    if attr == "YES" or attr == "1":
        return f"Nullable({type})"
    elif attr == "NO" or attr == "0":
        return type
    else:
        raise RuntimeError(
            f"""the is_nullable attribute is invalid: {attr}
is_nullable attribute can take one of the values: YES, NO, 1, 0"""
        )


def ch_type_stmp_text(attrs: Dict):
    # проверяем типы данных и их специфику
    match attrs:
        case {
            "data_type": ch_type,
        }:
            return ch_type
        case _:
            raise RuntimeError(
                f"At the moment I don't know how to form a data type: {attrs}"
            )


def check_pg_to_ch_nullable(src_attr, tgt_attr):
    column_name = "column_name"
    res = True, "ok"

    match src_attr, tgt_attr:
        case {"is_nullable": "YES"}, {"is_nullable": "1"}:
            res = True, "ok"

        case {"is_nullable": "NO"}, {"is_nullable": "0"}:
            res = True, "ok"

        case {"is_nullable": "YES"}, {"is_nullable": "0"}:
            res = (
                False,
                f"src {src_attr[column_name]} is nullable, but tgt {tgt_attr[column_name]} not null",
            )

        case {"is_nullable": "NO"}, {"is_nullable": "1"}:
            res = (
                True,
                f"warning: src {src_attr[column_name]} not null, but tgt {tgt_attr[column_name]} is nullable",
            )
        case _:
            res = False, ""

    return res


def pg_type_to_ch_type(attrs: Dict):
    """Формирует Tuple[Clickhouse Type, Clickhouse type] из переданных атрибутов attrs

    Clickhouse Type могут быть использованы для создания таблицы
    """

    res = "", ""

    to_pg_type_text = pg_type_stmp_text(attrs)

    # проверяем типы данных и их специфику
    match attrs:
        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "uuid":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("UUID", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "datetime_precision": precision,
        } if pg_type == "timestamp without time zone":
            if precision is not None:
                to_ch_type_text = f"DateTime64({precision})"
            else:
                to_ch_type_text = f"DateTime64"
            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f"{ch_convert_null_attr_to_type(to_ch_type_text, is_null)}",
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "datetime_precision": precision,
        } if pg_type == "timestamp with time zone":
            if precision is not None:
                to_ch_type_text = f"DateTime64({precision})"
            else:
                to_ch_type_text = f"DateTime64"

            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f"{ch_convert_null_attr_to_type(to_ch_type_text, is_null)}",
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "real":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Float32", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "double precision":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Float64", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "serial":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("UInt32", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "bigserial":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("UInt64", is_null)}',
            )

        case {
            "table_schema": table_schema,
            "table_name": table_name,
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "numeric_precision": pg_precision,
            "numeric_scale": pg_scale,
        } if pg_type == "numeric":
            if pg_precision is None and pg_scale is None:
                pg_precision = ch_precision_max
                pg_scale = ch_precision_scale_max
            elif pg_precision is not None and pg_scale is None:
                pg_scale = 0
            elif pg_precision is None and pg_scale is not None:
                raise RuntimeError(
                    f"""The received data about pg column {attrs} cannot be convert to column
Most likely this is an error, since scale without precision is not possible"""
                )

            pg_precision = int(pg_precision)
            pg_scale = int(pg_scale)

            if pg_precision >= 0 and pg_precision <= 9:
                to_ch_type_text = f"Decimal32({pg_scale})"
            elif pg_precision >= 10 and pg_precision <= 18:
                to_ch_type_text = f"Decimal64({pg_scale})"
            elif pg_precision >= 19 and pg_precision <= 38:
                to_ch_type_text = f"Decimal128({pg_scale})"
            elif pg_precision >= 39 and pg_precision <= 76:
                to_ch_type_text = f"Decimal256({pg_scale})"
            else:
                raise RuntimeError(
                    f"""in postgres table {table_schema}.{table_name} column {column_name} has unsupported data type {pg_type}
Presidion: {pg_precision} and scale: {pg_scale} cannot be automatically converted to the same type in clickhouse
Please create the table in clickhouse manually"""
                )

            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f"{ch_convert_null_attr_to_type(to_ch_type_text, is_null)}",
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "smallint":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Int16", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "integer":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Int32", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "bigint":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Int64", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "date":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Date", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "text":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("String", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "character":
            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f'{ch_convert_null_attr_to_type("String", is_null)}',
            )

        case {
            "table_schema": table_schema,
            "table_name": table_name,
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "ARRAY":
            raise RuntimeError(
                f"""In postgres table {table_schema}.{table_name} in column {column_name} the data type {pg_type} is used
This data type cannot be automatically converted to the clickhouse data type
Please create the table in clickhouse manually and try loading with the ch_schema_strategy parameter:
- error_if_table_not_exist
- do_nothing"""
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "real":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Float32", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "boolean":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("Bool", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "character varying":
            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f'{ch_convert_null_attr_to_type("String", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "bit varying" or pg_type == "bit":
            res = (
                f"{pg_convert_null_attr_to_type(to_pg_type_text, is_null)}",
                f'{ch_convert_null_attr_to_type("String", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "bytea":
            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f'{ch_convert_null_attr_to_type("String", is_null)}',
            )

        case {
            "column_name": column_name,
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "json" or pg_type == "jsonb":
            # колонка JSON в clickhouse не может быть null, соответственно для аналогичного типа данных может подойти только Nullable(String)
            if is_null == "1" or is_null == "YES":
                ch_type = "String"
            else:
                ch_type = "JSON"

            res = (
                f"{pg_convert_null_attr_to_type(pg_type, is_null)}",
                f"{ch_convert_null_attr_to_type(ch_type, is_null)}",
            )

        case _:
            raise RuntimeError(
                f"At the moment I don't know how to form a data type: {attrs}"
            )

    return res


def check_pg_to_ch_type_equal(src_attr: Dict, tgt_attr: Dict):
    res = True, "ok"

    typical_reason = (
        lambda src_type, tgt_type: f"tgt type: '{tgt_type}' does not match src type: '{src_type}'"
    )

    # проверяем типы данных и их специфику
    src_type_text = pg_type_stmp_text(src_attr)
    tgt_type_text = ch_type_stmp_text(tgt_attr)

    match src_attr, tgt_attr:
        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "uuid":
            if ch_type == "UUID" or ch_type == "Nullable(UUID)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {
            "data_type": ch_type
        } if pg_type == "timestamp without time zone" or pg_type == "timestamp with time zone":
            if "DateTime64" in ch_type or "DateTime" in ch_type:
                pg_len = src_attr["datetime_precision"]
                ch_len = tgt_attr["datetime_precision"]

                if pg_len is None and ch_len is None:
                    res = (
                        True,
                        f"ok",
                    )
                elif pg_len is None and ch_len is not None:
                    res = (
                        True,
                        f"ok",
                    )
                elif pg_len < ch_len:
                    res = (
                        True,
                        f"""src {src_type_text} < tgt {tgt_type_text}""",
                    )
                elif pg_len > ch_len:
                    res = (
                        False,
                        f"src {src_type_text} > tgt {tgt_type_text}",
                    )
                else:
                    res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "real":
            if ch_type == "Float32" or ch_type == "Nullable(Float32)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {
            "data_type": ch_type
        } if pg_type == "double precision":
            if ch_type == "Float64" or ch_type == "Nullable(Float64)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {
            "data_type": pg_type,
            "numeric_precision": pg_precision,
            "numeric_scale": pg_scale,
        }, {
            "data_type": ch_type,
            "numeric_precision": ch_precision,
            "numeric_scale": ch_scale,
        } if pg_type == "numeric":
            if "Decimal" in ch_type:
                check_decimal = True
                if pg_precision is None and pg_scale is None:
                    if (
                        ch_precision == ch_precision_max
                        and ch_scale == ch_precision_scale_max
                    ):
                        res = (True, "ok")
                        check_decimal = False
                    else:
                        res = (
                            False,
                            f"""
src {src_type_text} > tgt {tgt_type_text}
truncating value
""",
                        )
                        check_decimal = False
                elif pg_precision is None and pg_scale is not None:
                    raise RuntimeError(
                        f"""The received data about src column {src_attr} cannot be compared correctly with tgt column {tgt_attr}
Since src precision is None but scale is specified {pg_scale}
Most likely this is an error, since scale without precision is not possible"""
                    )
                elif pg_precision is not None and pg_scale is None:
                    pg_scale = 0

                if check_decimal:
                    pg_precision = int(pg_precision)
                    pg_scale = int(pg_scale)

                    if pg_precision > ch_precision:
                        res = (
                            False,
                            f"""
    src {src_type_text} > tgt {tgt_type_text}
    truncating precision, try expanding type in tgt to value numeric({pg_precision}, {pg_scale})
    """,
                        )
                    elif pg_scale > ch_scale:
                        res = (
                            False,
                            f"""
    src {src_type_text} > tgt {tgt_type_text}
    truncating scale, try expanding type in tgt to value numeric({pg_precision}, {pg_scale})
    """,
                        )
                    elif pg_precision < ch_precision or pg_scale < ch_scale:
                        res = (
                            True,
                            f"""warning: tgt {tgt_type_text} > src {src_type_text}
    It may be necessary to reduce the tgt type to numeric({pg_precision}, {pg_scale})
    """,
                        )
                    else:
                        res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "smallint":
            if ch_type == "Int16" or ch_type == "Nullable(Int16)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "integer":
            if (
                ch_type == "Int32"
                or ch_type == "UInt32"
                or ch_type == "Nullable(Int32)"
                or ch_type == "Nullable(UInt32)"
            ):
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "bigint":
            if (
                ch_type == "Int64"
                or ch_type == "UInt64"
                or ch_type == "Nullable(Int64)"
                or ch_type == "Nullable(UInt64)"
            ):
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "date":
            if ch_type == "Date" or ch_type == "Nullable(Date)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "text":
            if ch_type == "String" or ch_type == "Nullable(String)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "character":
            if ch_type == "String" or ch_type == "Nullable(String)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "ARRAY":
            if "Array" in ch_type:
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "real":
            if ch_type == "Float32" or ch_type == "Nullable(Float32)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "boolean":
            if ch_type == "Bool" or ch_type == "Nullable(Bool)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {
            "data_type": ch_type
        } if pg_type == "character varying":
            if ch_type == "String" or ch_type == "Nullable(String)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {
            "data_type": ch_type
        } if pg_type == "bit varying" or pg_type == "bit":
            if ch_type == "String" or ch_type == "Nullable(String)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type}, {"data_type": ch_type} if pg_type == "bytea":
            if ch_type == "String" or ch_type == "Nullable(String)":
                res = True, "ok"
            else:
                res = False, typical_reason(pg_type, ch_type)

        case {"data_type": pg_type, "is_nullable": pg_is_nullable}, {
            "data_type": ch_type,
            "is_nullable": ch_is_nullable,
        } if pg_type == "json" or pg_type == "jsonb":
            # обрати внимание, что json и jsonb из postgres может быть сконвертирован как JSON (если в postgres колонка отмечена как not null) либо в String если колонка отмечена как is null
            # поэтому проверка типа немного сложная и возможны три варианта:
            if (
                ch_type == "String"
                and (ch_is_nullable == "NO" or ch_is_nullable == "0")
                and (pg_is_nullable == "NO" or pg_is_nullable == "0")
            ):
                res = True, "ok"
            elif (
                ch_type == "Nullable(String)"
                and (ch_is_nullable == "YES" or ch_is_nullable == "1")
                and (pg_is_nullable == "YES" or pg_is_nullable == "1")
            ):
                res = True, "ok"
            elif (
                ch_type == "Object('json')"
                and (ch_is_nullable == "NO" or ch_is_nullable == "0")
                and (pg_is_nullable == "NO" or pg_is_nullable == "0")
            ):
                res = True, "ok"
            elif (
                ch_type == "Object('json')"
                and (ch_is_nullable == "NO" or ch_is_nullable == "0")
                and (pg_is_nullable == "YES" or pg_is_nullable == "1")
            ):
                res = (
                    False,
                    f"""clickhouse type JSON Cannot contain null values
However postgres column is marked as is_null

In this situation you can use the Nullable(String) type. Please convert the column manually.
Or run with drop_and_create_table_if_schema_changed strategy""",
                )
            else:
                res = False, typical_reason(pg_type, ch_type)

    # после проверки типов, выполняю проверку на null
    # ошибка только если в clickhouse is not null, а в postgres is null
    match res:
        case True, _:
            res = check_pg_to_ch_nullable(src_attr, tgt_attr)
        case _:
            pass

    match res:
        case True, _:
            pass
        case False, x:
            pass

    return res


class ClickhouseManipulator:
    def __init__(
        self,
        logger,
    ):
        self.log = logger

    def _database_path_exp(self, database: str):
        return "`{}`".format(database)

    def _table_path_exp(self, database: str, table: str):
        return "`{}`.`{}`".format(database, table)

    def _on_cluster_exp(self, cluster: Optional[str]):
        if cluster is not None and isinstance(cluster, str):
            return "on cluster {}".format(cluster)

        return ""

    def _order_by_exp(self, order_by: Optional[str]):
        if order_by is not None and isinstance(order_by, str):
            return "order by ({})".format(order_by)

        return ""

    def _sync_exp(self, sync: Optional[bool]):
        if sync is not None and isinstance(sync, bool) and sync == True:
            return "sync"

        return ""

    def _temporary_exp(self, temporary: Optional[bool]):
        if temporary is not None and isinstance(temporary, bool) and temporary == True:
            return "temporary"

        return ""

    def _engine_exp(self, engine_name: Optional[str]):
        if engine_name is not None and isinstance(engine_name, str):
            return "engine = {}".format(engine_name)

        return ""

    def _comment_exp(self, comment: Optional[str]):
        if comment is not None and isinstance(comment, str):
            return "comment '{}'".format(comment)

        return ""

    def _if_exists_exp(self, if_exists: Optional[bool]):
        if if_exists is not None and isinstance(if_exists, bool):
            return "if exists"

        return ""

    def _if_not_exists_exp(self, if_not_exists: Optional[bool]):
        if if_not_exists is not None and isinstance(if_not_exists, bool):
            return "if not exists"

        return ""

    def ch_check_database_exists(
        self,
        cursor: ClickhouseCursor,
        database: str,
    ):
        res = cursor.execute(
            """select 1 from system.databases where name = %(database)s""",
            {
                "database": database,
            },
        )

        res = cursor.fetchone()

        if res:
            res = res[0]
        else:
            res = 0

        if res > 1:
            raise RuntimeError(
                f"""
More than one table {database} found
This situation cannot be handled automatically.
Please check manually
"""
            )

        if res == 1:
            return True

        return False

    def ch_check_table_exist(
        self,
        cursor: ClickhouseCursor,
        schema: str,
        table: str,
    ):
        query = """
    select
        count(1)
    from
    information_schema.tables c
    where 1=1
        and c.table_schema in (%(schema)s)
        and c.table_name in (%(table)s)
    """

        cursor.execute(
            query,
            {
                "schema": schema,
                "table": table,
            },
        )
        res = cursor.fetchone()

        if res:
            res = res[0]
        else:
            res = 0

        if res > 1:
            raise RuntimeError(
                f"""
    More than one table {schema} found.{table}
    This situation cannot be handled automatically.
    Please check manually
    """
            )

        if res == 1:
            return True

        return False

    def ch_get_fields(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
    ):
        query = """
    select
        c.table_schema,
        c.table_name,
        c.column_name,
        c.column_default,
        c.is_nullable,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_precision_radix,
        c.numeric_scale,
        c.datetime_precision,
        c.ordinal_position
    from
        information_schema.columns c
    where 1=1
        and c.table_schema in (%(schema)s)
        and c.table_name in (%(table)s)
    order by
        c.ordinal_position
        """

        cursor.execute(
            query,
            {
                "schema": database,
                "table": table,
            },
        )
        res = cursor.fetchall()
        if res is None:
            raise RuntimeError(
                f"""an error occurred while getting information about clickhouse table columns: {database}.{table}
The clickhouse driver returned: None
Probably an error occurred, manual debugging of the error is required"""
            )

        if cursor.description:
            res_columns = list(map(lambda x: x.name, cursor.description))

            res = [dict(zip(res_columns, x)) for x in res]
            return res
        else:
            raise RuntimeError(
                "column 'description' is None, this situation cannot be handled automatically"
            )

    def ch_create_database(
        self,
        cursor: ClickhouseCursor,
        database: str,
        if_not_exists: Optional[bool],
        cluster: Optional[str],
        engine: Optional[str],
        comment: Optional[str],
    ):
        _if_not_exists = self._if_not_exists_exp(if_not_exists)
        _database = self._database_path_exp(database)
        _cluster = self._on_cluster_exp(cluster)
        _engine = self._engine_exp(engine)
        _comment = self._comment_exp(comment)

        stmp = "create database {if_not_exists} {database} {cluster} {engine} {comment}".format(
            **{
                "if_not_exists": _if_not_exists,
                "database": _database,
                "cluster": _cluster,
                "engine": _engine,
                "comment": _comment,
            }
        )

        self.log.info(
            f"""create a database: "{database}" using the following operation:"""
        )
        self.log.info(f"{stmp}")
        cursor.execute(stmp)
        cursor.fetchall()

    def ch_create_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        order_by: Optional[str],
        cluster: Optional[str],
        columns: Sequence[str],
        create_table_template: str,
    ):
        _table_path = self._table_path_exp(database, table)
        _columns = ",\n".join(columns)
        _cluster = self._on_cluster_exp(cluster)
        _order_by = self._order_by_exp(order_by)

        stmp = create_table_template.format(
            **{
                "ch_table": _table_path,
                "ch_columns": _columns,
                "ch_cluster": _cluster,
                "ch_order_by": _order_by,
            }
        )

        self.log.info(
            f"create a table: {database}.{table} using the following operation:"
        )
        self.log.info(f"{stmp}")
        cursor.execute(stmp)
        cursor.fetchall()

    def ch_drop_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        cluster: Optional[str],
        temporary: Optional[bool],
        sync: Optional[bool],
    ):
        _cluster = self._on_cluster_exp(cluster)
        _sync = self._sync_exp(sync)
        _table_path = self._table_path_exp(database, table)
        _temp = self._temporary_exp(temporary)
        query_template = "drop {temporary} table {table_path} {cluster} {sync}"
        stmp = query_template.format(
            **{
                "temporary": _temp,
                "table_path": _table_path,
                "cluster": _cluster,
                "sync": _sync,
            }
        )

        cursor.execute(stmp)
        cursor.fetchall()

    def ch_rename_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        new_database: str,
        new_table: str,
        cluster: Optional[str],
    ):
        _table_path_old = self._table_path_exp(database, table)
        _table_path_new = self._table_path_exp(new_database, new_table)
        _cluster = self._on_cluster_exp(cluster)
        stmp = "rename table {table_path_old} to {table_path_new} {cluster}".format(
            **{
                "table_path_old": _table_path_old,
                "table_path_new": _table_path_new,
                "cluster": _cluster,
            }
        )

        self.log.info(stmp)
        cursor.execute(stmp)
        cursor.fetchall()

    def ch_backup_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        cluster: Optional[str],
    ):
        now = datetime.now()

        database_back = f"{ database }__back"
        table_back = f"{ table }__{now.strftime('%Y%m%d_%H%M%S')}"
        self.log.info(
            f"table backup in clickhouse: {database}.{table} -> {database_back}.{table_back} ..."
        )

        # создаю backup базу если она отсутствует
        self.log.info(f"""check backup database exists: "{database_back}" ...""")
        if not self.ch_check_database_exists(cursor, database_back):
            self.log.warning(f"backup database is missing: {database_back}")
            self.log.info(f"try to create backup database: {database_back}")
            self.ch_create_database(
                cursor,
                database_back,
                if_not_exists=True,
                cluster=cluster,
                engine=None,
                comment="backup database: {}".format(database),
            )
        else:
            self.log.info(f"backup database exists: {database}")

        try:
            self.ch_rename_table(
                cursor,
                database,
                table,
                database_back,
                table_back,
                cluster=cluster,
            )

            self.log.info(f"backup success")
        except Exception as e:
            raise RuntimeError(
                f"""Error creating backup of table {database}.{table}:
    {e}
"""
            )

    def ch_truncate_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        cluster: Optional[str],
    ):
        _table_path = self._table_path_exp(database, table)
        _cluster = self._on_cluster_exp(cluster)
        stmp = "truncate table {table_path} {cluster}".format(
            **{
                "table_path": _table_path,
                "cluster": _cluster,
            }
        )

        self.log.info(stmp)
        cursor.execute(stmp)
        cursor.fetchall()

    def ch_exchange_table(
        self,
        cursor: ClickhouseCursor,
        database: str,
        table: str,
        new_database: str,
        new_table: str,
        cluster: Optional[str],
    ):
        _table_path_old = self._table_path_exp(database, table)
        _table_path_new = self._table_path_exp(new_database, new_table)
        _cluster = self._on_cluster_exp(cluster)
        stmp = "exchange tables {table_path_old} and {table_path_new} {cluster}".format(
            **{
                "table_path_old": _table_path_old,
                "table_path_new": _table_path_new,
                "cluster": _cluster,
            }
        )

        self.log.info(stmp)
        cursor.execute(stmp)
        cursor.fetchall()

    def ch_insert_select_from_postgres_use_named_collection(
        self,
        cursor: ClickhouseCursor,
        insert_database: str,
        insert_table: str,
        insert_fields: Sequence[str],
        select_database: str,
        select_table: str,
        select_fields: Sequence[str],
        select_params: Optional[Dict[str, Any]],
        ch_named_collection: str,
    ):
        _insert_table = self._table_path_exp(insert_database, insert_table)
        _insert_fields = ",\n".join(insert_fields)
        _select_fields = ",\n".join(select_fields)
        stmp = """insert into {insert_table} (
{insert_fields}
)
select
{select_fields}
from
    postgresql({ch_named_collection}, schema = '{select_database}', table = '{select_table}')""".format(
            **{
                "insert_table": _insert_table,
                "insert_fields": _insert_fields,
                "select_fields": _select_fields,
                "ch_named_collection": ch_named_collection,
                "select_database": select_database,
                "select_table": select_table,
            }
        )

        self.log.info(stmp)
        cursor.execute(stmp, select_params)
        cursor.fetchall()
