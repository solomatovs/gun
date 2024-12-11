from typing import (
    Any,
    Callable,
    List,
    Optional,
    Dict,
    Sequence,
    Tuple,
    Union,
    TypedDict,
)
from typing import Protocol
import itertools
import logging
from functools import total_ordering
from contextlib import closing, ExitStack
from enum import Enum
from datetime import datetime

import psycopg2.extras
import psycopg2.sql

from airflow.models.baseoperator import BaseOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.gun.pg import PostgresCopyExpertToPostgres
from airflow.providers.gun.pipe import (
    PipeStage,
    PipeTask,
    PipeTaskBuilder,
)
from airflow.providers.gun.sync.pg_to_pg_common import (
    pg_type_stmp_text,
    PostgresManipulator,
)
from airflow.providers.gun.sync.pg_to_pg_schema_sync import (
    PostgresToPostgresSchemaStrategy,
    PostgresToPostgresSchemaSyncModule,
)


class PostgresToPostgresDataReloadOverrideColumn:
    def __init__(
        self,
        name: str,
        rendering_type: str,
        rendering_value: Any = None,
        rename_from: Optional[str] = None,
        exclude: Optional[bool] = None,
    ) -> None:
        self.name = name
        self.rendering_type = rendering_type
        self.rendering_value = rendering_value
        self.rename_from = rename_from
        self.exclude = exclude

    def __repr__(self) -> str:
        over_rename = f"{self.rename_from} -> " if self.rename_from else ""
        over_value = f"{self.rendering_type}: {self.rendering_value}"
        over_exclude = "✖" if self.exclude else "✓"
        return f"{over_exclude} {over_rename}{self.name} -> {over_value}"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, PostgresToPostgresDataReloadOverrideColumn):
            return self.name == other.name

        return NotImplemented


@total_ordering
class PostgresToPostgresDataReloadCompareColumn:
    def __init__(
        self,
        name: str,
        src_schema: str,
        src_table: str,
        tgt_schema: str,
        tgt_table: str,
        ordinal_position: int = 0,
        src: Optional[Dict] = None,
        tgt: Optional[Dict] = None,
        rendering_type: Optional[str] = None,
        rendering_value: Any = None,
        rename_from: Optional[str] = None,
        exclude: Optional[bool] = None,
    ):
        self.name = name
        self.src = src
        self.tgt = tgt
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.rendering_type = rendering_type
        self.rendering_value = rendering_value
        self.rename_from = rename_from
        self.exclude = exclude
        self.ordinal_position = ordinal_position
        self.column_name_key = "column_name"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, PostgresToPostgresDataReloadCompareColumn):
            return self.name == other.name

        return NotImplemented

    def __lt__(self, other):
        return self.ordinal_position < other.ordinal_position

    def __repr__(self) -> str:
        column_name = "column_name"
        over_rename = (
            f" ({self.rename_from} -> {self.name})" if self.rename_from else ""
        )
        over_type = (
            f" ({self.rendering_type}: {self.rendering_value})"
            if self.is_exclude_column() is not None
            else ""
        )
        over_exclude = "✖" if self.is_exclude_column() else "✓"
        src_info = (
            f" ; src: {self.src[column_name]}({pg_type_stmp_text(self.src)})"
            if self.src
            else ""
        )
        tgt_info = (
            f" ; tgt: {self.tgt[column_name]}({pg_type_stmp_text(self.tgt)})"
            if self.tgt
            else ""
        )
        return f"{over_exclude} {self.name}{over_rename}{over_type}{src_info}{tgt_info}"

    def is_exclude_column(self):
        return (
            self.exclude
            or (self.src is None and self.rendering_value is None)
            or self.tgt is None
        )

    def select_field(self, alias: str, pg_cursor):
        if self.is_exclude_column():
            return None
        elif self.rendering_value is not None:
            match self.rendering_type:
                case "column":
                    val = psycopg2.sql.Identifier(alias, self.rendering_value)
                    val = val.as_string(pg_cursor)
                    return val
                case "exp":
                    return self.rendering_value
                case "native":
                    return f"%s"
                case _:
                    raise RuntimeError(
                        f"unsupported rendering type: {self.rendering_type}"
                    )
        elif self.src:
            val = psycopg2.sql.Identifier(self.src[self.column_name_key])
            val = val.as_string(pg_cursor)
            return val
        else:
            return None

    def select_param(self, pg_cursor):
        if self.is_exclude_column():
            return None
        elif self.rendering_value is not None:
            match self.rendering_type:
                case "column":
                    return None
                case "exp":
                    return None
                case "native":
                    return self.rendering_value
                case _:
                    raise RuntimeError(
                        f"unsupported rendering type: {self.rendering_type}"
                    )
        elif self.src:
            return None
        else:
            return None

    def insert_field(self, pg_cursor):
        if self.is_exclude_column():
            return None
        else:
            val = psycopg2.sql.Identifier(self.name)
            val = val.as_string(pg_cursor)
            return val


class PostgresDeleteRunner():
    def execute(self,
        context,
        cursor,
        schema,
        table,
    ):
        pass

    @staticmethod
    def type_dict_validate(typ: Any, instance: Any):
        res = True
        res_text = ""

        for property_name, property_type in typ.__annotations__.items():
            value = instance.get(property_name, None)

            if value is None:
                res_text += f"\nmissing key: {property_name}"
                res = False

            elif property_type not in (int, float, bool, str):
                inner_res, inner_text = PostgresDeleteRunner.type_dict_validate(property_type, value)

                res_text += inner_text
                if inner_res is False:
                    res = False
            
            elif not isinstance(value, property_type):
                res_text += f"\nWrong type: {property_name}. Expected {property_type}, got {type(value)}"
                res = False
        
        return res, res_text

class PostgresDoNothingRunner(PostgresDeleteRunner):
    def execute(self,
        context,
        cursor,
        schema,
        table,
    ):
        log = logging.getLogger(self.__class__.__name__)
        log.info("removal strategy: do nothing")


class PostgresTruncateRunner(PostgresDeleteRunner):
    def execute(self,
        context,
        cursor,
        schema,
        table,
    ):
        log = logging.getLogger(self.__class__.__name__)
        log.info("removal strategy: truncate")

        pg_man = PostgresManipulator(log)
        pg_man.pg_truncate_table(
            cursor,
            schema,
            table,
        )


class PostgresToPostgresDataReload:
    def __init__(
        self,
        logger,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor: psycopg2.extensions.cursor,
        tgt_schema: str,
        tgt_table: str,
        tgt_delete: PostgresDeleteRunner,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_columns: Optional[
            Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
        ] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
    ) -> None:
        self.log = logger
        self.src_cursor = src_cursor
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_cursor = tgt_cursor
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.tgt_delete = tgt_delete
        self.rename_columns = rename_columns
        self.override_columns = override_columns
        self.exclude_columns = exclude_columns
        self.pg_man = PostgresManipulator(logger)

    def execute(self, context):
        src_cursor = self.src_cursor
        tgt_cursor = self.tgt_cursor

        (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            tgt_delete,
            rule_columns,
        ) = self.clean_validate_and_flatten_params(
            self.src_schema,
            self.src_table,
            self.tgt_schema,
            self.tgt_table,
            self.tgt_delete,
            self.rename_columns,
            self.override_columns,
            self.exclude_columns,
        )

        self.sync_data(
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            tgt_delete,
            rule_columns,
            context,
        )

    def sync_data(
        self,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor: psycopg2.extensions.cursor,
        tgt_schema: str,
        tgt_table: str,
        tgt_delete: PostgresDeleteRunner,
        rule_columns: List[PostgresToPostgresDataReloadOverrideColumn],
        context,
    ):
        self.log.info(
            f"sync data: {src_schema}.{src_table} -> {tgt_schema}.{tgt_table}"
        )
        self.log.info(f"pg src: {src_cursor.connection.dsn}")
        self.log.info(f"pg tgt: {tgt_cursor.connection.dsn}")

        union_columns = self.make_fields_info(
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            rule_columns,
        )

        self.log.info("matching rules:")
        for rule in union_columns:
            self.log.info(f"{rule}")

        columns = sorted(union_columns)

        select_alias = "s"
        select_fields = map(
            lambda column: column.select_field(select_alias, src_cursor), columns.copy()
        )
        select_fields = [x for x in select_fields if x is not None]

        select_params = map(
            lambda column: column.select_param(src_cursor), columns.copy()
        )
        select_params = [x for x in select_params if x is not None]

        insert_fields = map(
            lambda column: column.insert_field(tgt_cursor), columns.copy()
        )
        insert_fields = [x for x in insert_fields if x is not None]

        target_row = 0

        tgt_delete.execute(
            context,
            tgt_cursor,
            tgt_schema,
            tgt_table,
        )
        self.log.info(f"tgt status: {tgt_cursor.statusmessage}")
        if tgt_cursor.rowcount != -1:
            target_row += tgt_cursor.rowcount
        
        # здесь я пытаюсь обойти ограничение которое было заложено в предыдущем релизе
        # весь класс был спроектирован на то, что будет работать с одним postgres курсором (одно postgres подключение)
        # все запросы должны были выполняться в одном подключении, соответственно для
        # перезагрузки данных из одной таблицы в другую нужно было осуществить простой insert .. select .. запрос
        # но так как сейчас возникла необходимость реализовать переливку данных между двумя postgres используя разные подключения
        # здесь я сравниваю два курсора на условие "это один и тот же объект?"
        # если да, то выполняю перезаливку данных как раньше, в один запрос
        # если нет, то выполняю перезаливку данных через запуск команды copy на двух разных курсорах
        if src_cursor is tgt_cursor:
            # обрати внимание, что используется только select_cursor
            # так как insert_cursor это тот же самый объект, что и select_cursor
            query_stmp = self.pg_man.pg_insert_select_in_one_postgres(
                src_cursor,
                tgt_schema,
                tgt_table,
                insert_fields,
                src_schema,
                src_table,
                select_fields,
                select_alias,
            )
            
            src_cursor.execute(query_stmp, select_params)
            self.log.info(f"rows: {src_cursor.rowcount}")
            if src_cursor.rowcount != -1:
                target_row += src_cursor.rowcount
            
            context["target_row"] = target_row
            src_cursor.connection.commit()
        else:
            # обрати внимание, что используется оба курсора select_cursor и insert_cursor
            # потому что были переданы разные объекты в эти переменные
            copy_from_stmp, copy_to_stmp = self.pg_man.pg_insert_select_between_two_postgres(
                tgt_cursor,
                tgt_schema,
                tgt_table,
                insert_fields,
                src_cursor,
                src_schema,
                src_table,
                select_fields,
                select_alias,
            )

            PostgresCopyExpertToPostgres.execute(
                src_cursor=src_cursor,
                src_query=copy_from_stmp,
                src_params=select_params,
                tgt_cursor=tgt_cursor,
                tgt_query=copy_to_stmp,
                tgt_params=None,
            )

            self.log.info(f"select rows: {src_cursor.rowcount}")
            self.log.info(f"insert rows: {tgt_cursor.rowcount}")

            if tgt_cursor.rowcount != -1:
                target_row += tgt_cursor.rowcount

            context["target_row"] = target_row
            tgt_cursor.connection.commit()

    def union_src_tgt_and_rules(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_info: List[Dict],
        tgt_info: List[Dict],
        rule_columns: List[PostgresToPostgresDataReloadOverrideColumn],
    ):
        column_name = "column_name"
        ordinal_position = "ordinal_position"

        # для начала составлю полный список филдов
        # выбиру из src имена и переопределю их через override_columns
        src_fields = list(
            map(
                lambda x: PostgresToPostgresDataReloadCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    rendering_type="column",
                    rendering_value=x[column_name],
                    src=x,
                    ordinal_position=x[ordinal_position],
                ),
                src_info,
            )
        )
        tgt_fields = list(
            map(
                lambda x: PostgresToPostgresDataReloadCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    # намеренно не проставляю rendering_value, так как далее происходит проверка на None
                    # rendering_value это значение которое идёт из исходной таблицы, а не из таблицы назначения
                    # оно либо придёт с src, либо через rule_columns
                    # если значение не пришло, значит колонка исключается из insert select запроса
                    rendering_type=None,
                    rendering_value=None,
                    tgt=x,
                    ordinal_position=x[ordinal_position],
                ),
                tgt_info,
            )
        )

        # нужно пройтись по rule_columns найти соответствия правилам и прописать их в PostgresToPostgresFullReloadCompareColumn
        # это необходимо что бы корректно выполнить матчинг между src, tgt и rules
        for rule in rule_columns:
            # выполняю ренейм колонок из src, что бы они корректно матчились с tgt
            for i in range(len(src_fields)):
                val = src_fields[i]
                if val.name == rule.rename_from:
                    val.name = rule.name
                    val.rename_from = rule.rename_from

            # выполняю простановку правил rendering_type, rendering_value и исключение колонок exclude
            for i in range(len(tgt_fields)):
                val = tgt_fields[i]
                if val.name == rule.name:
                    if rule.rendering_type is not None:
                        val.rendering_type = rule.rendering_type
                    if rule.rendering_value is not None:
                        val.rendering_value = rule.rendering_value
                    if rule.exclude is not None:
                        val.exclude = rule.exclude

        # объединяю информацию в один список PostgresToPostgresFullReloadCompareColumn, где будут находится все атрибуты сравнения
        # src_info, tgt_info, override_columns
        columns: List[PostgresToPostgresDataReloadCompareColumn] = []
        max_ordinal = 0
        for src, tgt, rule in itertools.zip_longest(
            src_fields, tgt_fields, rule_columns, fillvalue=None
        ):
            if src:
                max_ordinal = (
                    src.ordinal_position
                    if src.ordinal_position > max_ordinal
                    else max_ordinal
                )

                if src.name in columns:
                    val = columns[columns.index(src.name)]
                    val.src = src.src

                    # под вопросом, надо ли
                    if src.rename_from is not None:
                        val.rename_from = src.rename_from

                    if src.ordinal_position is not None:
                        val.ordinal_position = src.ordinal_position
                else:
                    columns.append(src)

            if tgt:
                max_ordinal = (
                    tgt.ordinal_position
                    if tgt.ordinal_position > max_ordinal
                    else max_ordinal
                )

                if tgt.name in columns:
                    val = columns[columns.index(tgt.name)]
                    val.tgt = tgt.tgt

                    # if tgt.rendering_type:
                    #     val.rendering_type = tgt.rendering_type

                    # if tgt.rendering_value:
                    #     val.rendering_value = tgt.rendering_value

                    if tgt.exclude is not None:
                        val.exclude = tgt.exclude

                    if tgt.ordinal_position is not None:
                        val.ordinal_position = tgt.ordinal_position
                else:
                    columns.append(tgt)

            if rule:
                if rule.name in columns:
                    val = columns[columns.index(rule.name)]

                    if rule.rename_from is not None:
                        val.rename_from = rule.rename_from
                        val.rendering_value = rule.rename_from

                    if rule.rendering_type is not None:
                        val.rendering_type = rule.rendering_type

                    if rule.rendering_value is not None:
                        val.rendering_value = rule.rendering_value

                    if rule.exclude is not None:
                        val.exclude = rule.exclude
                else:
                    max_ordinal += 1
                    columns.append(
                        PostgresToPostgresDataReloadCompareColumn(
                            name=rule.name,
                            src_schema=src_schema,
                            src_table=src_table,
                            tgt_schema=tgt_schema,
                            tgt_table=tgt_table,
                            rendering_type=rule.rendering_type,
                            rendering_value=rule.rendering_value,
                            src=None,
                            tgt=None,
                            ordinal_position=max_ordinal,
                            rename_from=rule.rename_from,
                            exclude=rule.exclude,
                        )
                    )

        return columns

    def make_fields_info(
        self,
        src_cursor,
        src_schema,
        src_table,
        tgt_cursor,
        tgt_schema,
        tgt_table,
        rule_columns,
    ):
        src_info = self.pg_man.pg_get_fields(src_cursor, src_schema, src_table)
        tgt_info = self.pg_man.pg_get_fields(tgt_cursor, tgt_schema, tgt_table)

        union_columns = self.union_src_tgt_and_rules(
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            src_info,
            tgt_info,
            rule_columns,
        )

        return union_columns

    def clean_validate_and_flatten_params(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        tgt_delete,
        rename_columns,
        override_columns,
        exclude_columns,
    ):
        rules: List[PostgresToPostgresDataReloadOverrideColumn] = []

        if not rename_columns:
            rename_columns = {}

        type_error = """
Error validating parameter "rename_columns"

rename_columns has an unsupported type.
rename_columns represents a dict of {
    "column_name_to_1": "column_name_from_2",
    "column_name_to_2": "column_name_from_2",
    ...
}
"""
        if not isinstance(rename_columns, Dict):
            raise RuntimeError(type_error)

        for name, rename_from in rename_columns.items():
            if isinstance(name, str) and isinstance(rename_from, str):
                index = None
                for i in range(len(rules)):
                    val = rules[i]
                    if val.name == rename_from:
                        index = i
                        break

                if index is not None:
                    val = rules[index]
                    val.name = name
                    val.rename_from = rename_from
                else:
                    rules.append(
                        PostgresToPostgresDataReloadOverrideColumn(
                            name=name,
                            rendering_type="column",
                            rename_from=rename_from,
                            rendering_value=rename_from,
                        )
                    )
            else:
                raise RuntimeError(type_error)

        # проверяю существует ли переопределение колонок, если нет то выставляю пустой словарь, для удобства работы в дальнейшем
        if not override_columns:
            override_columns = {}

        type_error = """
Error validating parameter "override_columns"

override_columns has an unsupported type.: {}
override_columns represents a dict of {
    "column_name": "column_value",
    "column_name_2": ("column_value", "rendering_type"),
} (dictionary)
where
column_value is any variable that can be rendered in a sql query
rendering_type is the type of variable rendering, which can take any of the following values:
- native - the variable is rendered by the psycopg2 driver, so column_value must be
- column - the column name was passed to column_value
- exp - a sql expression was passed to column_value, which must be rendered as is
"""
        # проверяю, является ли override_columns словарём и если это не так, то выдаём ошибку
        if not isinstance(override_columns, Dict):
            raise RuntimeError(type_error.format(type(override_columns)))

        # здесь я делаю нормализацию override_columns и привожу к классу PostgresToPostgresFullReloadOverrideColumn
        for name, override_value in override_columns.items():
            if isinstance(name, str) and isinstance(override_value, Tuple):
                if len(override_value) == 1:
                    rendering_type = "native"
                    rendering_value = override_value[0]

                elif len(override_value) == 2:
                    if override_value[1] not in ["native", "column", "exp"]:
                        raise RuntimeError(
                            f"""'override_columns' validation error
The value {override_value} is invalid
'override_type' can take only ​​{["native", "column", "exp"]}"""
                        )

                    if override_value[1] in ["column", "exp"] and not isinstance(
                        override_value[1], str
                    ):
                        raise RuntimeError(
                            f"""'override_columns' validation error
The value {override_value} is invalid
if 'override_type' = {override_value[1]} then 'override_value' can only be string"""
                        )

                    rendering_type = override_value[1]
                    rendering_value = override_value[0]
                else:
                    raise RuntimeError(type_error.format(type(override_columns)))

                if name in rules:
                    val = rules[rules.index(name)]
                    val.rendering_type = rendering_type
                    val.rendering_value = rendering_value
                else:
                    rules.append(
                        PostgresToPostgresDataReloadOverrideColumn(
                            name=name,
                            rendering_type=rendering_type,
                            rendering_value=rendering_value,
                        )
                    )

            elif isinstance(name, str):
                if name in rules:
                    val = rules[rules.index(name)]
                    val.rendering_type = "native"
                    val.rendering_value = override_value
                else:
                    rules.append(
                        PostgresToPostgresDataReloadOverrideColumn(
                            name=name,
                            rendering_type="native",
                            rendering_value=override_value,
                        )
                    )
            else:
                raise RuntimeError(type_error.format(type(override_value)))

        # выполняю валидацию exclude_columns
        if not exclude_columns:
            exclude_columns = []

        type_error = """
Error validating parameter "exclude_columns"

exclude_columns has an unsupported type.
exclude_columns represents a list of ["column_name_1", "column_name_2", ...]
"""
        if not isinstance(exclude_columns, List):
            raise RuntimeError(type_error)

        for name in exclude_columns:
            if isinstance(name, str):
                if name in rules:
                    val = rules[rules.index(name)]
                    val.exclude = True
                else:
                    rules.append(
                        PostgresToPostgresDataReloadOverrideColumn(
                            name=name,
                            rendering_type="column",
                            exclude=True,
                            rendering_value=name,
                        )
                    )
            else:
                raise RuntimeError(type_error)

        # выполняю валидацию tgt_delete
        if tgt_delete is None:
            # по умолчанию выбираю отсутствие удаления, если ничего небыло передано
            tgt_delete = PostgresDoNothingRunner()

        if not isinstance(tgt_delete, PostgresDeleteRunner):
            raise RuntimeError(f"""tgt_delete parameter unsupported type: {type(tgt_delete)}
parameter tgt_delete must be of type PostgresDeleteRunner and support variants:
- PostgresDoNothingRunner
- PostgresTruncateRunner
- PostgresDeleteRangeRunner
    """)

        return (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            tgt_delete,
            rules,
        )


class PostgresToPostgresFullReload(PostgresToPostgresDataReload):
    def __init__(
        self,
        logger,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor: psycopg2.extensions.cursor,
        tgt_schema: str,
        tgt_table: str,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_columns: Optional[Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
    ) -> None:
        super().__init__(
            logger,
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            tgt_delete=PostgresTruncateRunner(),
            rename_columns=rename_columns,
            override_columns=override_columns,
            exclude_columns=exclude_columns,
        )

class PostgresToPostgresFullReloadOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "src_conn_id",
        "src_schema",
        "src_table",
        "tgt_conn_id",
        "tgt_schema",
        "tgt_table",
        "rename_columns",
        "override_columns",
        "exclude_columns",
    )

    def __init__(
        self,
        src_conn_id: str,
        src_schema: str,
        src_table: str,
        tgt_conn_id: str,
        tgt_schema: str,
        tgt_table: str,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_columns: Optional[
            Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
        ] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.src_conn_id = src_conn_id
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_conn_id = tgt_conn_id
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.rename_columns = rename_columns
        self.override_columns = override_columns
        self.exclude_columns = exclude_columns
        self.tgt_delete = PostgresTruncateRunner()
        self.stack = ExitStack()

    def execute(self, context):
        src_hook = PostgresHook(postgres_conn_id=self.src_conn_id)
        src_cursor = self.stack.enter_context(closing(src_hook.get_cursor()))

        tgt_hook = PostgresHook(postgres_conn_id=self.tgt_conn_id)
        tgt_cursor = self.stack.enter_context(closing(tgt_hook.get_cursor()))

        base_module = PostgresToPostgresDataReload(
            self.log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            tgt_delete=self.tgt_delete,
            rename_columns=self.rename_columns,
            override_columns=self.override_columns,
            exclude_columns=self.exclude_columns,
        )

        base_module.execute(context)


class PostgresToPostgresDataReloadModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        src_cur_key: Optional[str],
        src_schema: str,
        src_table: str,
        tgt_cur_key: Optional[str],
        tgt_schema: str,
        tgt_table: str,
        tgt_delete: PostgresDeleteRunner,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_columns: Optional[
            Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
        ] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(context_key)
        super().set_template_fields(
            (
                "src_cur_key",
                "src_schema",
                "src_table",
                "tgt_cur_key",
                "tgt_schema",
                "tgt_table",
                "rename_columns",
                "override_columns",
                "exclude_columns",
            )
        )
        super().set_template_render(template_render)

        if src_cur_key:
            self.src_cur_key = src_cur_key
        else:
            self.src_cur_key = "pg_cur"

        if tgt_cur_key:
            self.tgt_cur_key = tgt_cur_key
        else:
            self.tgt_cur_key = "pg_cur"

        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.tgt_delete = tgt_delete
        self.rename_columns = rename_columns
        self.override_columns = override_columns
        self.exclude_columns = exclude_columns

    def __call__(self, context):
        self.render_template_fields(context)

        match context[self.context_key].get(self.src_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find src postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case src_cursor:
                src_cursor: psycopg2.extensions.cursor = src_cursor
        
        match context[self.context_key].get(self.tgt_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find tgt postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case tgt_cursor:
                tgt_cursor: psycopg2.extensions.cursor = tgt_cursor

        log = logging.getLogger(self.__class__.__name__)

        base_module = PostgresToPostgresDataReload(
            log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            self.tgt_delete,
            rename_columns=self.rename_columns,
            override_columns=self.override_columns,
            exclude_columns=self.exclude_columns,
        )

        base_module.execute(context)


def pg_full_reload(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    src_table_check: bool = True,
    schema_strategy: Union[
        PostgresToPostgresSchemaStrategy, str
    ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
    override_columns: Optional[
        Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
    ] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    create_table_template: str = "create table {pg_table} ({pg_columns})",
    pg_cur_key: Optional[str] = None,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresToPostgresSchemaSyncModule(
                builder.context_key,
                builder.template_render,
                pg_cur_key,
                src_schema,
                src_table,
                pg_cur_key,
                tgt_schema,
                tgt_table,
                src_table_check,
                schema_strategy,
                rename_columns,
                override_schema,
                exclude_columns,
                create_table_template,
            ),
            pipe_stage,
        )

        builder.add_module(
            PostgresToPostgresDataReloadModule(
                builder.context_key,
                builder.template_render,
                pg_cur_key,
                src_schema,
                src_table,
                pg_cur_key,
                tgt_schema,
                tgt_table,
                tgt_delete=PostgresTruncateRunner(),
                rename_columns=rename_columns,
                override_columns=override_columns,
                exclude_columns=exclude_columns,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


def pg_to_pg_full_reload(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    src_table_check: bool = True,
    schema_strategy: Union[
        PostgresToPostgresSchemaStrategy, str
    ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
    override_columns: Optional[
        Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
    ] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    create_table_template: str = "create table {pg_table} ({pg_columns})",
    src_cur_key: Optional[str] = None,
    tgt_cur_key: Optional[str] = None,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresToPostgresSchemaSyncModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                src_table_check,
                schema_strategy,
                rename_columns,
                override_schema,
                exclude_columns,
                create_table_template,
            ),
            pipe_stage,
        )

        builder.add_module(
            PostgresToPostgresDataReloadModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                tgt_delete=PostgresTruncateRunner(),
                rename_columns=rename_columns,
                override_columns=override_columns,
                exclude_columns=exclude_columns,
            ),
            pipe_stage,
        )

        return builder

    return wrapper


class PostgresDeletePeriodModel(TypedDict):
    field: str
    period_from: datetime
    period_to: datetime


def validate_delete_period_model(self: PostgresDeletePeriodModel):
    res = True
    res_text = ""

    field_name = "field"
    match self.get(field_name):
        case None:
            res = False
            res_text += f"\nMissing '{field_name}'"
        case x if isinstance(x, str):
            pass
        case x:
            res = False
            res_text += f"\nWrong type: '{field_name}'. Expected type str, got {type(x)}"

    for field_name in ["period_from", "period_to"]:
        match self.get(field_name):
            case None:
                res = False
                res_text += f"\nMissing '{field_name}'"
            case x if isinstance(x, datetime):
                pass
            case x:
                res = False
                res_text += f"\nWrong type: '{field_name}'. Expected type str, got {type(x)}"

    return res, res_text

class PostgresDeletePeriodRunner(PostgresDeleteRunner):
    """
    delete from {pg_table}
    where 1=1
        and {field} >= {period_start}
        and {field} <  {period_end}
    """

    def __init__(
        self,
        inner: PostgresDeletePeriodModel | str,
    ):
        if inner is None:
            raise RuntimeError("PostgresDeletePeriodModel is None. Make sure it passed to the function correctly")
        
        # model может быть только str (так как jinja template), либо Dict
        if not isinstance(inner, str) and inner is not Dict:
            raise RuntimeError(f"""Invalid PostgresDeletePeriodModel value
Invalid type was passed: {type(inner)}
{inner}

Possible values:
- Jinja template -> Dict
- Dict""")

        self.inner = inner

    @staticmethod
    def render_template(val, context):
            task = context["task"]
            jinja_env = task.get_template_env()
            val = task.render_template(
                val,
                context,
                jinja_env,
                set(),
            )
            return val
    
    def execute(self,
        context,
        cursor,
        schema,
        table,
    ):
        log = logging.getLogger(self.__class__.__name__)
        log.info("removal strategy: delete period")

        # нужно отрендерить модель через Jinja
        inner = PostgresDeletePeriodRunner.render_template(self.inner, context)
        if inner is None:
            raise RuntimeError("PostgresDeletePeriodModel is None. Make sure it passed to the function correctly")
        
        # model может быть только Dict
        if not isinstance(inner, Dict):
            raise RuntimeError(f"""Invalid PostgresDeletePeriodModel value
Invalid type was passed: {type(inner)}
{inner}

Possible values:
- Jinja template -> Dict
- Dict""")
        
        inner: PostgresDeletePeriodModel = PostgresDeletePeriodModel(**inner)

        # эх, в python 3.11 появится нормальная валидация на основе TypedDict, а пока делаю самостоятельно
        res = validate_delete_period_model(inner)
        if res[0] is False:
            raise RuntimeError("Error validation PostgresDeletePeriodModel:{}\npassed value: {}".format(res[1], inner))

        pg_man = PostgresManipulator(log)
        pg_man.pg_delete_range_from_table(
            cursor,
            schema,
            table,
            inner['field'],
            inner['period_from'],
            inner['period_to'],
        )

def pg_to_pg_period_reload(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    tgt_delete: PostgresDeletePeriodModel | str,
    src_table_check: bool = True,
    schema_strategy: Union[
        PostgresToPostgresSchemaStrategy, str
    ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
    override_columns: Optional[
        Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
    ] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    create_table_template: str = "create table {pg_table} ({pg_columns})",
    src_cur_key: Optional[str] = None,
    tgt_cur_key: Optional[str] = None,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresToPostgresSchemaSyncModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                src_table_check,
                schema_strategy,
                rename_columns,
                override_schema,
                exclude_columns,
                create_table_template,
            ),
            pipe_stage,
        )

        builder.add_module(
            PostgresToPostgresDataReloadModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                PostgresDeletePeriodRunner(tgt_delete),
                rename_columns,
                override_columns,
                exclude_columns,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
