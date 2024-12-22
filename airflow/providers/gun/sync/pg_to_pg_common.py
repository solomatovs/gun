import re
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
)
from datetime import datetime

import psycopg2.extras
import psycopg2.sql


def pg_convert_null_attr_to_type(type: str, attr: str):
    if attr == "YES":
        return f"{type} null"
    elif attr == "NO":
        return f"{type} not null"
    else:
        raise RuntimeError(
            f"""the is_nullable attribute is invalid: {attr}
is_nullable attribute can take one of the values: YES, NO"""
        )


def pg_type_stmp_text(attrs: Dict):
    # проверяем типы данных и их специфику
    match attrs:
        case {
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type in [
            "uuid",
            "real",
            "double precision",
            "serial",
            "bigserial",
            "smallint",
            "integer",
            "bigint",
            "date",
            "text",
            "boolean",
            "bytea",
            "json",
            "jsonb",
        ]:
            return f"{pg_convert_null_attr_to_type(pg_type, is_null)}"
        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "datetime_precision": precision,
        } if pg_type == "timestamp without time zone":
            if precision is not None:
                to_pg_type = f"timestamp({precision})"
            else:
                to_pg_type = f"timestamp"

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "datetime_precision": precision,
        } if pg_type == "timestamp with time zone":
            if precision is not None:
                to_pg_type = f"timestamptz({precision})"
            else:
                to_pg_type = f"timestamptz"

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "numeric_precision": pg_precision,
            "numeric_scale": pg_scale,
        } if pg_type == "numeric":
            if pg_precision is not None and pg_scale is not None:
                to_pg_type = f"numeric({pg_precision}, {pg_scale})"
            elif pg_precision is not None and pg_scale is None:
                to_pg_type = f"numeric({pg_precision})"
            else:
                to_pg_type = f"numeric"

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "character":
            if character_maximum_length is not None:
                to_pg_type = f"char({character_maximum_length})"
            else:
                to_pg_type = f"char"

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
        } if pg_type == "ARRAY":
            return f"{pg_convert_null_attr_to_type(pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "character varying":
            if character_maximum_length is not None:
                to_pg_type = f"character varying({character_maximum_length})"
            else:
                to_pg_type = f"character varying"

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
            "character_maximum_length": character_maximum_length,
        } if pg_type == "bit varying" or pg_type == "bit":
            if character_maximum_length is not None:
                to_pg_type = f"{pg_type}({character_maximum_length})"
            else:
                to_pg_type = pg_type

            return f"{pg_convert_null_attr_to_type(to_pg_type, is_null)}"

        case {
            "data_type": pg_type,
            "is_nullable": is_null,
        }:
            return f"{pg_convert_null_attr_to_type(pg_type, is_null)}"

        case _:
            raise RuntimeError(
                f"At the moment I don't know how to form a data type: {attrs}"
            )


def check_pg_to_pg_nullable(src_attr, tgt_attr):
    column_name = "column_name"
    res = True, "ok"

    match src_attr, tgt_attr:
        case {"is_nullable": "YES"}, {"is_nullable": "YES"}:
            res = True, "ok"

        case {"is_nullable": "NO"}, {"is_nullable": "NO"}:
            res = True, "ok"

        case {"is_nullable": "YES"}, {"is_nullable": "NO"}:
            res = (
                False,
                f"src {src_attr[column_name]} is nullable, but tgt {tgt_attr[column_name]} not null",
            )

        case {"is_nullable": "NO"}, {"is_nullable": "YES"}:
            res = (
                True,
                f"warning: src {src_attr[column_name]} not null, but tgt {tgt_attr[column_name]} is nullable",
            )
        case _:
            res = False, ""

    return res


def check_pg_to_pg_type_equal(src_attr: Dict, tgt_attr: Dict):
    res = True, "ok"

    typical_reason = (
        lambda src_type, tgt_type: f"tgt type: '{tgt_type}' does not match src type: '{src_type}'"
    )

    # проверяем типы данных и их специфику
    src_type_text = pg_type_stmp_text(src_attr)
    tgt_type_text = pg_type_stmp_text(tgt_attr)

    match src_attr, tgt_attr:
        case {"data_type": src_type}, {
            "data_type": tgt_type
        } if src_type == "uuid" or src_type == "real" or src_type == "double precision" or src_type == "smallint" or src_type == "integer" or src_type == "bigint" or src_type == "date" or src_type == "text" or src_type == "boolean" or src_type == "bytea" or src_type == "json" or src_type == "jsonb":
            if tgt_type == src_type:
                res = True, "ok"
            else:
                res = False, typical_reason(src_type, tgt_type)

        case {"data_type": src_type}, {
            "data_type": tgt_type
        } if src_type == "timestamp without time zone" or src_type == "timestamp with time zone":
            if src_type != tgt_type:
                res = False, typical_reason(src_type, tgt_type)
            else:
                src_len = src_attr["datetime_precision"]
                tgt_len = tgt_attr["datetime_precision"]

                if src_len < tgt_len:
                    res = (
                        True,
                        f"""src {src_type_text} < tgt {tgt_type_text}""",
                    )
                elif src_len > tgt_len:
                    res = (
                        False,
                        f"src {src_type_text} > tgt {tgt_type_text}",
                    )
                else:
                    res = True, "ok"

        case {
            "data_type": src_type,
            "numeric_precision": src_precision,
            "numeric_scale": src_scale,
        }, {
            "data_type": tgt_type,
            "numeric_precision": tgt_precision,
            "numeric_scale": tgt_scale,
        } if src_type == "numeric" or src_type == "decimal":
            if src_type != tgt_type:
                res = False, typical_reason(src_type, tgt_type)
            else:
                decimal_error_precision = f"""The received data about src column {src_attr} cannot be compared correctly with tgt column {tgt_attr}
Since src precision is None but scale is specified {src_scale}
Most likely this is an error, since scale without precision is not possible"""
                check_decimal = True
                if src_precision is None and src_scale is None:
                    if tgt_precision is None and tgt_scale is None:
                        res = (True, "ok")
                        check_decimal = False
                    else:
                        res = (
                            False,
                            f"""src {src_type_text} > tgt {tgt_type_text}
truncating scale, try expanding type in tgt to value numeric({src_precision}, {src_scale})""",
                        )
                        check_decimal = False
                elif src_precision is not None and src_scale is None:
                    src_scale = 0

                    if tgt_precision is not None and tgt_scale is None:
                        tgt_scale = 0
                    elif tgt_precision is None:
                        res = (
                            True,
                            f"""src {src_type_text} < tgt {tgt_type_text}
truncating scale, try expanding type in tgt to value numeric({src_precision}, {src_scale})""",
                        )
                        check_decimal = False
                elif src_precision is None and src_scale is not None:
                    raise RuntimeError(decimal_error_precision)

                if check_decimal:
                    src_precision = int(src_precision)
                    src_scale = int(src_scale)

                    if src_precision > tgt_precision:
                        res = (
                            False,
                            f"""
    src {src_type_text} > tgt {tgt_type_text}
    truncating precision, try expanding type in tgt to value numeric({src_precision}, {src_scale})
    """,
                        )
                    elif src_scale > tgt_scale:
                        res = (
                            False,
                            f"""
    src {src_type_text} > tgt {tgt_type_text}
    truncating scale, try expanding type in tgt to value numeric({src_precision}, {src_scale})
    """,
                        )
                    elif src_precision < tgt_precision or src_scale < tgt_scale:
                        res = (
                            True,
                            f"""warning: tgt {tgt_type_text} > src {src_type_text}
    It may be necessary to reduce the tgt type to numeric({src_precision}, {src_scale})
    """,
                        )
                    else:
                        res = True, "ok"

        case {
            "data_type": src_type,
            "character_maximum_length": src_character_maximum_length,
        }, {
            "data_type": tgt_type,
            "character_maximum_length": tgt_character_maximum_length,
        } if src_type == "character" or src_type == "character varying" or src_type == "bit" or src_type == "bit varying":
            if src_type != tgt_type:
                res = False, typical_reason(src_type, tgt_type)
            else:
                if (
                    src_character_maximum_length is None
                    and tgt_character_maximum_length is not None
                ):
                    res = (
                        False,
                        f"""
src {src_type_text} > tgt {tgt_type_text}
truncating max length, try expanding type in tgt to value {src_type}
""",
                    )
                elif (
                    src_character_maximum_length is not None
                    and tgt_character_maximum_length is None
                ):
                    res = (
                        True,
                        f"""tgt {tgt_type_text} > src {src_type_text}
It may be necessary to reduce the tgt type to {src_type}({src_character_maximum_length})
""",
                    )
                elif (
                    src_character_maximum_length is None
                    and tgt_character_maximum_length is None
                ):
                    res = True, "ok"
                elif src_character_maximum_length > tgt_character_maximum_length:
                    res = (
                        False,
                        f"""
src {src_type_text} > tgt {tgt_type_text}
truncating max length, try expanding type in tgt to value {src_type}({src_character_maximum_length})
""",
                    )
                elif src_character_maximum_length < tgt_character_maximum_length:
                    res = (
                        True,
                        f"""tgt {tgt_type_text} > src {src_type_text}
It may be necessary to reduce the tgt type to {src_type}({src_character_maximum_length})
""",
                    )
                else:
                    res = True, "ok"

        case {"data_type": src_type}, {"data_type": tgt_type} if src_type == "ARRAY":
            if tgt_type == "Array":
                res = True, "ok"
            else:
                res = False, typical_reason(src_type, tgt_type)

    # после проверки типов, выполняю проверку на null
    # ошибка только если в clickhouse is not null, а в postgres is null
    match res:
        case True, _:
            res = check_pg_to_pg_nullable(src_attr, tgt_attr)
        case _:
            pass

    match res:
        case True, _:
            pass
        case False, x:
            pass

    return res


class PostgresManipulator:
    """Необходим для формирования корректных запросов к postgres
    Класс только формирует запросы, без выполнения
    """

    def __init__(
        self,
        logger,
    ):
        self.log = logger

    def _schema_path_exp(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
    ):
        _schema = (
            psycopg2.sql.SQL("{}")
            .format(psycopg2.sql.Identifier(schema))
            .as_string(cursor)
        )
        return _schema

    def _table_path_exp(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: Optional[str],
        table: str,
    ):
        if schema is None:
            _table_path = (
                psycopg2.sql.SQL("{}")
                .format(psycopg2.sql.Identifier(table))
                .as_string(cursor)
            )
        else:
            _table_path = (
                psycopg2.sql.SQL("{}.{}")
                .format(
                    psycopg2.sql.Identifier(schema),
                    psycopg2.sql.Identifier(table)
                )
                .as_string(cursor)
            )

        return _table_path

    def _temporary_exp(self, temporary: Optional[bool]):
        if temporary is not None and isinstance(temporary, bool) and temporary == True:
            return "temporary"

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

    def pg_check_schema_exists(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
    ):
        res = cursor.execute(
            """select 1 from pg_namespace where nspname = %(schema)s""",
            {
                "schema": schema,
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
More than one table {schema} found
This situation cannot be handled automatically.
Please check manually
"""
            )

        if res == 1:
            return True

        return False

    def pg_check_table_exist(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
    ):
        # выполняю проверку через запрос в information schema
        # можно выполнить запрос вида select 1 from {schema}.{table} where 0=1
        # однако в случае ошибки (например отсутствуют права на траблицу) то транзакция абортится
        # это приводит к тому, что следующие операторы выполнены не будут, без принудительного коммита
        # я не хочу влиять ошибкой на всю транзакцию, поэтому выполняю проверку через select from information_schema
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

    def pg_get_fields(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
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
                "schema": schema,
                "table": table,
            },
        )
        res = cursor.fetchall()
        if cursor.description:
            res_columns = list(map(lambda x: x.name, cursor.description))
            res = [dict(zip(res_columns, x)) for x in res]
            return res
        else:
            raise RuntimeError(
                "column 'description' is None, this situation cannot be handled automatically"
            )

    def pg_create_schema(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
    ):
        stmp = (
            psycopg2.sql.SQL("create schema {}")
            .format(psycopg2.sql.Identifier(schema))
            .as_string(cursor)
        )
        self.log.info(f"""create a schema: "{schema}" using the following operation:""")
        self.log.info(f"{stmp}")
        cursor.execute(stmp)

    def pg_create_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: Optional[str],
        table: str,
        columns: Sequence[str],
        create_table_template: str,
    ):
        _columns = ",\n".join(columns)
        _table_path = self._table_path_exp(cursor, schema, table)

        stmp = create_table_template.format(
            **{
                "pg_table": _table_path,
                "pg_columns": _columns,
            }
        )

        self.log.info(f"{stmp}")
        cursor.execute(stmp)

    def pg_drop_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
        with_cascade: bool,
    ):
        try:
            cascade_part = " cascade" if with_cascade else ""
            stmp = "drop table {schema}.{table}" + cascade_part
            stmp = psycopg2.sql.SQL(stmp).format(
                **{
                    "schema": psycopg2.sql.Identifier(schema),
                    "table": psycopg2.sql.Identifier(table),
                }
            )
            stmp = stmp.as_string(cursor)
            self.log.warning(stmp)
            cursor.execute(stmp)
        except Exception as e:
            raise RuntimeError(
                f"""Error drop of table {schema}.{table}:
    {e}
"""
            )

    def pg_backup_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
        pg_info,
    ):
        now = datetime.now()

        # проверяю, существует ли sequence привязаный к таблице
        # на данный момент сделаю через анализ выражения в колонке tgt_info["default_value"]
        # потом можно будет переписать на адекватный запрос к метаданныv
        column_default = "column_default"
        column_name = "column_name"

        # формирую название backup схемы
        pg_schema_back = f"{ schema }__back"
        if len(pg_schema_back) >= 64:
            self.log.warning(
                f"The backup schema name: {pg_schema_back} is too long, more than 64 characters. I generate a schema name based on the current date"
            )
            pg_schema_back = f"dp__back__{now.strftime('%Y%m')}"

        # создаю backup схему если она отсутствует
        self.log.info(f"""check backup schema exists: "{pg_schema_back}" ...""")
        if not self.pg_check_schema_exists(cursor, pg_schema_back):
            self.log.warning(f"backup schema is missing: {pg_schema_back}")
            self.log.info(f"try to create backup schema: {pg_schema_back}")
            self.pg_create_schema(
                cursor,
                pg_schema_back,
            )
            # после создания схемы нужно выполнить commit, что бы последующие операции не abort'нулись
            cursor.connection.commit()
        else:
            self.log.info(f"backup schema exists: {schema}")

        # формирую alter sequence для тех sequence которые привязаны к таблице
        # это обязательно, так как обычный rename table не переименовывает sequence
        # при повторном создании таблицы можно наткнуться на ошибку создания нового sequence с таким же названием как текущий
        def match_sequence_from_column_meta(x):
            if not x:
                return None

            m = re.match(
                r"nextval\('(?P<sequence_schema>.*)\.(?P<sequence_name>.*)'::regclass\)",
                x,
            )
            try:
                if m:
                    sequence_schema = m.group("sequence_schema")
                    sequence_name = m.group("sequence_name")
                    return (sequence_schema, sequence_name)
                else:
                    return None
            except Exception:
                return None

        sequence_stmp = map(
            lambda x: match_sequence_from_column_meta(x[column_default]), pg_info
        )
        sequence_stmp = [x for x in sequence_stmp if x is not None]

        def alter_sequence_stmp(pg_schema: str, pg_sequense, pg_cursor):
            pg_sequense_back = f"{ pg_sequense }__{now.strftime('%Y%m%d_%H%M%S')}"
            if len(pg_sequense_back) >= 64:
                self.log.warning(
                    f"The backup sequence name: {pg_sequense_back} is too long, more than 64 characters. I generate a table name based on the current date"
                )
                pg_sequense_back = f"s_{now.strftime('%Y%m%d_%H%M%S_%f')}"

            stmp = psycopg2.sql.SQL(
                """alter sequence {pg_schema}.{pg_sequense} rename to {pg_sequense_back};"""
            ).format(
                **{
                    "pg_schema": psycopg2.sql.Identifier(pg_schema),
                    "pg_sequense": psycopg2.sql.Identifier(pg_sequense),
                    "pg_sequense_back": psycopg2.sql.Identifier(pg_sequense_back),
                }
            )
            stmp = stmp.as_string(pg_cursor)
            return stmp

        sequence_stmp = map(
            lambda x: alter_sequence_stmp(x[0], x[1], cursor), sequence_stmp
        )
        sequence_stmp = "\n".join(sequence_stmp)

        # формирую запрос на переименование таблицы
        # сначала выполняется перенос в backup схему через alter table {} set schema {}
        # после чего выполняется переименование таблицы через alter table {} rename to {}
        def alter_table_stmp(
            pg_schema: str,
            pg_schema_back: str,
            table_name: str,
            pg_cursor: psycopg2.extensions.cursor,
        ):
            pg_table_back = f"{ table_name }__{now.strftime('%Y%m%d_%H%M%S')}"
            if len(pg_table_back) >= 64:
                self.log.warning(
                    f"The backup table name: {pg_table_back} is too long, more than 64 characters. I generate a table name based on the current date"
                )
                pg_table_back = f"t_{now.strftime('%Y%m%d_%H%M%S_%f')}"

            stmp = psycopg2.sql.SQL(
                """alter table {pg_schema}.{pg_table} set schema {pg_schema_back};
alter table {pg_schema_back}.{pg_table} rename to {pg_table_back};"""
            ).format(
                **{
                    "pg_schema": psycopg2.sql.Identifier(pg_schema),
                    "pg_table": psycopg2.sql.Identifier(table),
                    "pg_schema_back": psycopg2.sql.Identifier(pg_schema_back),
                    "pg_table_back": psycopg2.sql.Identifier(pg_table_back),
                }
            )
            stmp = stmp.as_string(pg_cursor)
            return stmp

        table_stmp = alter_table_stmp(schema, pg_schema_back, table, cursor)

        try:
            stmp = "{}\n{}".format(sequence_stmp, table_stmp)

            self.log.info(
                f"""backup table: "{schema}"."{table}" using the following operation:"""
            )
            self.log.info(f"{stmp}")
            cursor.execute(stmp)
            cursor.connection.commit()
            self.log.info(f"backup success")
        except Exception as e:
            raise RuntimeError(
                f"""Error creating backup of table {schema}.{table}:
    {e}
"""
            )

    def pg_truncate_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
    ):
        _table_path = self._table_path_exp(cursor, schema, table)

        stmp = "truncate table {table_path}".format(**{
                "table_path": _table_path,
            }
        )

        cursor.execute(stmp)


    def pg_delete_all_from_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
    ):
        tmpl = """
delete from {table_path} as {alias}
"""
        del_alias = "d"

        _table_path = self._table_path_exp(cursor, schema, table)
        _alias = psycopg2.sql.SQL("{}").format(
            psycopg2.sql.Identifier(del_alias),
        ).as_string(cursor)

        stmp = tmpl.format(**{
                "table_path": _table_path,
                "alias": _alias,
            }
        )
        
        cursor.execute(stmp)

    def pg_delete_period_from_table(
        self,
        cursor: psycopg2.extensions.cursor,
        schema: str,
        table: str,
        field: str,
        period_from: datetime,
        period_to: datetime,
    ):
        tmpl = """
delete from {table_path} as {alias}
where 1=1
    and {field} >= %(period_from)s
    and {field} <  %(period_to)s
"""
        del_alias = "d"

        _table_path = self._table_path_exp(cursor, schema, table)
        _field = (
            psycopg2.sql.SQL("{}.{}")
            .format(
                psycopg2.sql.Identifier(del_alias),
                psycopg2.sql.Identifier(field),
            )
            .as_string(cursor)
        )
        _alias = psycopg2.sql.SQL("{}").format(
            psycopg2.sql.Identifier(del_alias),
        ).as_string(cursor)

        stmp = tmpl.format(**{
                "table_path": _table_path,
                "field": _field,
                "alias": _alias,
            }
        )

        stmp = cursor.mogrify(stmp, {
            "period_from": period_from,
            "period_to": period_to,
        }).decode(encoding='utf-8', errors='strict')
        
        cursor.execute(stmp)

    def pg_delete_from_column_another_table_in_one_postgres(
        self,
        cursor,
        delete_schema,
        delete_table,
        delete_alias,
        delete_field,
        select_schema,
        select_table,
        select_alias,
        select_field,
    ):
        tmpl = """
delete from
    {delete_path} as {delete_alias}
using
    {select_path} as {select_alias}
where 1=1
    and {delete_field} = {select_field}
"""
        _delete_path = self._table_path_exp(cursor, delete_schema, delete_table)
        _select_path = self._table_path_exp(cursor, select_schema, select_table)

        _delete_alias = (
             psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(delete_alias),
            )
            .as_string(cursor)
        )
        _select_alias = (
             psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
            )
            .as_string(cursor)
        )

        _delete_field = (
            psycopg2.sql.SQL("{}.{}")
            .format(
                psycopg2.sql.Identifier(delete_alias),
                psycopg2.sql.Identifier(delete_field),
            )
            .as_string(cursor)
        )
        _select_field = (
            psycopg2.sql.SQL("{}.{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
                psycopg2.sql.Identifier(select_field),
            )
            .as_string(cursor)
        )

        stmp = tmpl.format(**{
                "delete_path": _delete_path,
                "select_path": _select_path,
                "delete_alias": _delete_alias,
                "select_alias": _select_alias,
                "delete_field": _delete_field,
                "select_field": _select_field,
            }
        )
        
        cursor.execute(stmp)



    def pg_transfer_one_column_between_two_postgres(
        self,
        select_cursor,
        select_schema,
        select_table,
        select_field,
        insert_cursor,
        insert_schema,
        insert_table,
        insert_field,
    ):
        _insert_table = self._table_path_exp(insert_cursor, insert_schema, insert_table)
        _insert_field = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(insert_field ),
            )
            .as_string(insert_cursor)
        )
        copy_to_stmp = """copy {insert_table} (
{insert_field}
) from stdin""".format(
            **{
                "insert_table": _insert_table,
                "insert_field": _insert_field,
            }
        )
        
        _select_table = self._table_path_exp(select_cursor, select_schema, select_table)
        _select_field = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_field ),
            )
            .as_string(select_cursor)
        )
        copy_from_stmp = """copy (
select
{select_field}
from
    {select_table}
) to stdout""".format(
            **{
                "select_table": _select_table,
                "select_field": _select_field,
            }
        )

        return copy_from_stmp, copy_to_stmp


    def pg_insert_select_in_one_postgres(
        self,
        cursor: psycopg2.extensions.cursor,
        insert_schema: str,
        insert_table: str,
        insert_fields: Sequence[str],
        select_schema: str,
        select_table: str,
        select_fields: Sequence[str],
        select_where: Optional[str],
        select_alias: str,
    ):
        _insert_table = self._table_path_exp(
            cursor,
            insert_schema,
            insert_table,
        )
        _select_table = self._table_path_exp(
            cursor,
            select_schema,
            select_table,
        )
        _insert_fields = ",\n".join(insert_fields)
        _select_fields = ",\n".join(select_fields)
        if select_where is not None:
            _select_where = """where 1=1
{}""".format(select_where)
        else:
            _select_where = ""

        _select_alias = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
            )
            .as_string(cursor)
        )

        stmp = """insert into {insert_table} (
{insert_fields}
)
select
{select_fields}
from
    {select_table} as {select_alias}
{select_where}
""".format(
            **{
                "insert_table": _insert_table,
                "insert_fields": _insert_fields,
                "select_table": _select_table,
                "select_fields": _select_fields,
                "select_where": _select_where,
                "select_alias": _select_alias,
            }
        )

        return stmp


    def pg_insert_select_between_two_postgres(
        self,
        insert_cursor: psycopg2.extensions.cursor,
        insert_schema: str,
        insert_table: str,
        insert_fields: Sequence[str],
        select_cursor: psycopg2.extensions.cursor,
        select_schema: str,
        select_table: str,
        select_fields: Sequence[str],
        select_where: Optional[str],
        select_alias: str,
    ):
        _insert_table = self._table_path_exp(
            insert_cursor,
            insert_schema,
            insert_table,
        )
        _select_table = self._table_path_exp(
            select_cursor,
            select_schema,
            select_table,
        )
        _insert_fields = ",\n".join(insert_fields)
        _select_fields = ",\n".join(select_fields)
        _select_alias = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
            )
            .as_string(select_cursor)
        )

        if select_where is not None:
            _select_where = """where 1=1
{}""".format(select_where)
        else:
            _select_where = ""

        copy_to_stmp = """copy {insert_table} (
{insert_fields}
) from stdin""".format(
            **{
                "insert_table": _insert_table,
                "insert_fields": _insert_fields,
            }
        )

        copy_from_stmp = """copy (
select
{select_fields}
from
    {select_table} as {select_alias}
{select_where}
) to stdout""".format(
            **{
                "select_table": _select_table,
                "select_fields": _select_fields,
                "select_alias": _select_alias,
                "select_where": _select_where,
            }
        )

        return copy_from_stmp, copy_to_stmp


    def pg_insert_select_period_in_one_postgres(
        self,
        cursor: psycopg2.extensions.cursor,
        insert_schema: str,
        insert_table: str,
        insert_fields: Sequence[str],
        select_schema: str,
        select_table: str,
        select_fields: Sequence[str],
        select_alias: str,
        where_field: str,
        where_period_from: datetime,
        where_period_to: datetime,
    ):
        _insert_table = self._table_path_exp(
            cursor,
            insert_schema,
            insert_table,
        )
        _select_table = self._table_path_exp(
            cursor,
            select_schema,
            select_table,
        )
        _insert_fields = ",\n".join(insert_fields)
        _select_fields = ",\n".join(select_fields)

        _select_alias = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
            )
            .as_string(cursor)
        )
        _where_field = (
            psycopg2.sql.SQL("{}.{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
                psycopg2.sql.Identifier(where_field),
            )
            .as_string(cursor)
        )


        stmp = """insert into {insert_table} (
{insert_fields}
)
select
{select_fields}
from
    {select_table} as {select_alias}
where 1=1
    and {where_field} >= %(period_from)s
    and {where_field} <  %(period_to)s
""".format(
            **{
                "insert_table": _insert_table,
                "insert_fields": _insert_fields,
                "select_table": _select_table,
                "select_fields": _select_fields,
                "select_alias": _select_alias,
                "where_field": _where_field,
            }
        )

        stmp = cursor.mogrify(stmp, {
            "period_from": where_period_from,
            "period_to": where_period_to,
        }).decode(encoding='utf-8', errors='strict')

        return stmp


    def pg_insert_select_period_between_two_postgres(
        self,
        insert_cursor: psycopg2.extensions.cursor,
        insert_schema: str,
        insert_table: str,
        insert_fields: Sequence[str],
        select_cursor: psycopg2.extensions.cursor,
        select_schema: str,
        select_table: str,
        select_fields: Sequence[str],
        select_alias: str,
        where_field: str,
        where_period_from: datetime,
        where_period_to: datetime,
    ):
        _insert_table = self._table_path_exp(
            insert_cursor,
            insert_schema,
            insert_table,
        )
        _select_table = self._table_path_exp(
            select_cursor,
            select_schema,
            select_table,
        )
        _insert_fields = ",\n".join(insert_fields)
        _select_fields = ",\n".join(select_fields)
        _select_alias = (
            psycopg2.sql.SQL("{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
            )
            .as_string(select_cursor)
        )
        _where_field = (
            psycopg2.sql.SQL("{}.{}")
            .format(
                psycopg2.sql.Identifier(select_alias),
                psycopg2.sql.Identifier(where_field),
            )
            .as_string(select_cursor)
        )

        copy_to_stmp = """copy {insert_table} (
{insert_fields}
) from stdin""".format(
            **{
                "insert_table": _insert_table,
                "insert_fields": _insert_fields,
            }
        )

        copy_from_stmp = """copy (
select
{select_fields}
from
    {select_table} as {select_alias}
where 1=1
    and {where_field} >= %(period_from)s
    and {where_field} <  %(period_to)s
) to stdout""".format(
            **{
                "select_table": _select_table,
                "select_fields": _select_fields,
                "select_alias": _select_alias,
                "where_field": _where_field,
            }
        )

        copy_from_stmp = select_cursor.mogrify(copy_from_stmp, {
            "period_from": where_period_from,
            "period_to": where_period_to,
        }).decode(encoding='utf-8', errors='strict')

        return copy_from_stmp, copy_to_stmp
