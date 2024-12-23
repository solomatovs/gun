from typing import (
    Callable,
    List,
    Optional,
    Dict,
    Sequence,
    Union,
)
import itertools
import logging
from enum import Enum
from functools import total_ordering

from contextlib import closing, ExitStack

import psycopg2.extras
import psycopg2.sql

from airflow.models.baseoperator import BaseOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.gun.pipe import (
    PipeStage,
    PipeTask,
    PipeTaskBuilder,
)
from airflow.providers.gun.sync.pg_to_pg_common import (
    pg_type_stmp_text,
    check_pg_to_pg_type_equal,
    PostgresManipulator,
)


class PostgresToPostgresSchemaStrategy(Enum):
    create_table_if_not_exists = "create_table_if_not_exists"
    backup_and_create_table_if_schema_changed = (
        "backup_and_create_table_if_schema_changed"
    )
    backup_and_create_table_if_exist = "backup_and_create_table_if_exist"
    drop_and_create_table_if_schema_changed = "drop_and_create_table_if_schema_changed"
    drop_and_create_table_if_exist = "drop_and_create_table_if_exist"
    error_if_table_not_exist = "error_if_table_not_exist"
    error_if_table_schema_changed = "error_if_table_schema_changed"
    do_nothing = "do_nothing"


class PostgresToPostgresSchemaSyncOverrideColumn:
    def __init__(
        self,
        name: str,
        override_type: Optional[str] = None,
        rename_from: Optional[str] = None,
        exclude: Optional[bool] = None,
    ) -> None:
        self.name = name
        self.override_type = override_type
        self.rename_from = rename_from
        self.exclude = exclude

    def __repr__(self) -> str:
        over_rename = f"{self.rename_from} -> " if self.rename_from else ""
        over_type = f"{self.override_type}" if self.override_type else ""
        over_exclude = "✖" if self.exclude else "✓"
        return f"{over_exclude} {over_rename}{self.name} {over_type}"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, PostgresToPostgresSchemaSyncOverrideColumn):
            return self.name == other.name

        return NotImplemented


@total_ordering
class PostgresToPostgresSchemaSyncCompareColumn:
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
        override_type: Optional[str] = None,
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
        self.override_type = override_type
        self.rename_from = rename_from
        self.exclude = exclude
        self.ordinal_position = ordinal_position

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif isinstance(other, PostgresToPostgresSchemaSyncCompareColumn):
            return self.name == other.name

        return NotImplemented

    def __lt__(self, other):
        return self.ordinal_position < other.ordinal_position

    def create_tgt_fields_stmp(self, pg_cursor):
        if self.exclude:
            # если колонку исключили, значит не выполняю рендеринг
            return None
        elif not self.override_type and not self.src:
            # если колонка отсутствует и в src и в мануальном определении, то не выполняю рендеринг
            return None
        else:
            if self.override_type:
                ty = self.override_type
            else:
                if self.src:
                    ty = pg_type_stmp_text(self.src)
                elif self.tgt:
                    ty = pg_type_stmp_text(self.tgt)
                else:
                    # нет определения типа колонки, значит не рендерим её
                    # это может возникнуть если к примеру было добавлено правило в rename одако ни в src ни в tgt такой колонки не существует
                    return None

            res = psycopg2.sql.Identifier(self.name)
            res = psycopg2.sql.SQL("{}").format(res)
            res = res.as_string(pg_cursor)
            res = "{} {}".format(res, ty)
            return res

    def __repr__(self) -> str:
        column_name = "column_name"
        over_rename = (
            f" ({self.rename_from} -> {self.name})" if self.rename_from else ""
        )
        over_type = (
            f" (type override: {self.override_type})" if self.override_type else ""
        )
        over_exclude = (
            "✖"
            if self.exclude
            or (not self.tgt and not self.src and not self.override_type)
            else "✓"
        )
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

    def compare_column(self):
        column_name = "column_name"

        if not self.src and not self.tgt:
            # колонки нет ни в src ни в tgt
            if self.exclude:
                message = f"""column "{self.name}" {self.override_type} has been excluded and will not be added to table {self.tgt_schema}.{self.tgt_table}
missing in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
define in rule: {self}
"""
                return True, message

            if self.override_type:
                message = f"""column "{self.name}" is missing in tgt, but is defined manually:
missing in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
define in rule: {self}"""
                return False, message

            if self.rename_from:
                message = f"""column "{self.name}" is missing in src and tgt but is rename rule manually:
missing in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
define in rule: {self}"""
                return True, message

            raise RuntimeError(
                f"""incorrect rule comparable, this is undefined behavior
Please look at the code {__file__}"""
            )

        elif not self.src and self.tgt:
            # колонки нет в src но есть в tgt
            if self.exclude:
                message = f"""column "{self.tgt[column_name]}" excluded, bug exists in tgt
missing in src: {self.src_schema}.{self.src_table}
exists in tgt: {self.tgt_schema}.{self.tgt_table}
excluding in rule: {self}"""
                return False, message

            if not self.override_type:
                message = f"""column "{self.name}" found only in tgt but missing in src and rule
missing in src: {self.src_schema}.{self.src_table}
exists tgt: {self.tgt_schema}.{self.tgt_table}
missing in rule: {self}"""
                return False, message

            return True, "ok"

        elif self.src and not self.tgt:
            # колонка присутствует в src и отсутствует в tgt

            if self.exclude:
                message = f"""column "{self.name}" excluded from tgt {self.tgt_schema}.{self.tgt_table}
exists in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
excluded in rule: {self}"""
                return True, message

            if self.override_type:
                message = f"""column "{self.name}" is missing in tgt {self.tgt_schema}.{self.tgt_table}, but is defined manually:
exists in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
define in rule: {self}"""
                return False, message

            message = f"""column "{self.name}" exists in src but missing tgt:
exists in src: {self.src_schema}.{self.src_table}
missing in tgt: {self.tgt_schema}.{self.tgt_table}
rule: {self}"""
            return False, message
        elif self.src and self.tgt:
            # колонка есть и в src и tgt
            if self.exclude:
                message = f"""column "{self.name}" must be removed from tgt {self.tgt_schema}.{self.tgt_table}
exists in src: {self.src_schema}.{self.src_table}
exists in tgt: {self.tgt_schema}.{self.tgt_table}
excluded in rule: {self}"""
                return False, message

            if self.override_type:
                r, m = check_pg_to_pg_type_equal(self.src, self.tgt)
                if not r:
                    message = f"""src column "{self.src[column_name]}" not equal tgt column "{self.tgt[column_name]}"
{m}
exists in src: {self.src_schema}.{self.src_table}
exists in tgt: {self.tgt_schema}.{self.tgt_table}
define in rule: {self}
However, due to type overriding, there is no way to decide whether this is a valid situation or not
{self}"""
                    return True, message
                else:
                    return True, "ok"
            else:
                # запускаю проверку типов
                r, m = check_pg_to_pg_type_equal(self.src, self.tgt)
                if not r:
                    message = f"""src column "{self.src[column_name]}" not equal tgt column "{self.tgt[column_name]}"
reason: {m}
exists in src: {self.src_schema}.{self.src_table}
exists in tgt: {self.tgt_schema}.{self.tgt_table}
rule: {self}
"""
                    return False, message
                else:
                    return r, m

        raise RuntimeError(
            f"""Unhandled behavior, please see code {__file__}
src: {self.src_schema}.{self.src_table}
tgt: {self.tgt_schema}.{self.tgt_table}

src: {self.src}
tgt: {self.tgt}
rule: {self}"""
        )


class PostgresToPostgresSchemaCheck:
    def __init__(
        self,
        logger,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: Optional[str],
        src_table: Optional[str],
        tgt_cursor: psycopg2.extensions.cursor,
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_schema: Optional[Union[str, Dict[str, str]]] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
    ) -> None:
        self.log = logger
        self.src_cursor = src_cursor
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_cursor = tgt_cursor
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.src_table_check = src_table_check
        self.rename_columns = rename_columns
        self.override_schema = override_schema
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
            src_table_check,
            rule_columns,
        ) = self.clean_validate_and_flatten_params(
            src_schema=self.src_schema,
            src_table=self.src_table,
            tgt_schema=self.tgt_schema,
            tgt_table=self.tgt_table,
            src_table_check=self.src_table_check,
            rename_columns=self.rename_columns,
            override_schema=self.override_schema,
            exclude_columns=self.exclude_columns,
        )

        self.check_schema(
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            src_table_check,
            rule_columns,
            context,
        )

    def clean_validate_and_flatten_params(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_table_check,
        rename_columns,
        override_schema,
        exclude_columns,
    ):
        if src_table_check is None or not isinstance(src_table_check, bool):
            raise RuntimeError(
                f"""'src_table_check' parameter must be bool
{type(src_table_check)}: {src_table_check}"""
            )

        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn] = []

        if not rename_columns:
            rename_columns = {}

        if isinstance(rename_columns, Dict):
            for name, rename_from in rename_columns.items():
                if isinstance(name, str) and isinstance(rename_from, str):
                    index = None
                    for i in range(len(rule_columns)):
                        val = rule_columns[i]
                        if val.name == rename_from:
                            index = i
                            break

                    if index is not None:
                        val = rule_columns[index]
                        val.name = name
                        val.rename_from = rename_from
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                rename_from=rename_from,
                            )
                        )
                else:
                    raise RuntimeError(
                        """
Error validating parameter "rename_columns[{}]: unsupported type

rename_columns has an unsupported type.
rename_columns represents a set of {
    "column_name_in_tgt": "column_name_in_src",
    "column_name_in_tgt_1": "column_name_in_src_2",
} (dictionary)


The parameter passed was of type: {}
Its value: {}""".format(
                            name, type(rename_from), rename_from
                        )
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "rename_columns"

rename_columns has an unsupported type.
rename_columns represents a set of {
    "column_name_in_tgt": "column_name_in_src",
    "column_name_in_tgt_1": "column_name_in_src_2",
} (dictionary)


The parameter passed was of type: {}
Its value: {}""".format(
                    type(rename_columns), rename_columns
                )
            )

        # проверяю существует ли переопределение колонок, если нет то выставляю пустой словарь, для удобства работы в дальнейшем
        if not override_schema:
            override_schema = {}

        if isinstance(override_schema, Dict):
            # здесь я делаю нормализацию override_schema и привожу к классу PostgresSchemaSyncOverrideColumn
            for name, override_type in override_schema.items():
                if isinstance(name, str) and isinstance(override_type, str):
                    if name in rule_columns:
                        val = rule_columns[rule_columns.index(name)]
                        val.override_type = override_type
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                override_type=override_type,
                            )
                        )
                else:
                    raise RuntimeError(
                        f"""
Error validating parameter "override_schema[{name}]: unsupported type
the value type must be str
and must be the type of the column, for example:
- varchar(200) not null
- bigserial
- integer not null default 0

type: {type(override_type)}
value: {override_type}"""
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "override_schema"

override_schema has an unsupported type.
override_schema represents a set of {
    "column_name": "data type",
    "column_name_2": "varchar(200) not null default now()",
} (dictionary)
with which to override the schema of individual columns

The parameter passed was of type: {}
Its value: {}""".format(
                    type(override_schema), override_schema
                )
            )

        if not exclude_columns:
            exclude_columns = []

        if isinstance(exclude_columns, List):
            for name in exclude_columns:
                if isinstance(name, str):
                    if name in rule_columns:
                        val = rule_columns[rule_columns.index(name)]
                        val.exclude = True
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                exclude=True,
                            )
                        )
                else:
                    raise RuntimeError(
                        """
Error validating parameter "exclude_columns"

exclude_columns has an unsupported type.
exclude_columns represents a List[str] of ["column_name_in_src", "column_name_in_src", ..]

The parameter passed was of type: {}
Its value: {}""".format(
                            type(name), name
                        )
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "exclude_columns"

exclude_columns has an unsupported type.
exclude_columns represents a List[str] of ["column_name_in_src", "column_name_in_src", ..]

The parameter passed was of type: {}
Its value: {}""".format(
                    type(exclude_columns), exclude_columns
                )
            )

        return (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            src_table_check,
            rule_columns,
        )

    def check_equal_structure(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        union_columns: List[PostgresToPostgresSchemaSyncCompareColumn],
    ):
        self.log.info("matching rules:")
        for rule in union_columns:
            self.log.info(f"{rule}")

        diff = map(lambda column: column.compare_column(), union_columns)
        diff = filter(lambda x: not x[0], diff)
        diff = list(diff)

        if len(diff) > 0:
            error = f"""Error validation columns

src: {src_schema}.{src_table}
tgt: {tgt_schema}.{tgt_table}
        """
            for k, v in diff:
                error += f"""
reason: {v}
        """
            error += f"""

Please make the columns src and tgt equal or disable validation
        """
            return False, error
        else:
            return True, "ok"

    def union_src_tgt_and_rules(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_info: List[Dict],
        tgt_info: List[Dict],
        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn],
    ):
        column_name = "column_name"
        ordinal_position = "ordinal_position"

        # для начала составлю полный список филдов
        # выбиру из src имена и переопределю их через override_schema
        src_fields = list(
            map(
                lambda x: PostgresToPostgresSchemaSyncCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    src=x,
                    ordinal_position=x[ordinal_position],
                ),
                src_info,
            )
        )
        tgt_fields = list(
            map(
                lambda x: PostgresToPostgresSchemaSyncCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    tgt=x,
                    ordinal_position=x[ordinal_position],
                ),
                tgt_info,
            )
        )

        # нужно пройтись по rule_columns найти соответствия правилам и прописать их в PostgresCompareColumn
        # это необходимо что бы корректно выполнить матчинг между src, tgt и rules
        for rule in rule_columns:
            # выполняю ренейм колонок из src, что бы они корректно матчились с tgt
            for i in range(len(src_fields)):
                val = src_fields[i]
                if val.name == rule.rename_from:
                    val.name = rule.name
                    val.rename_from = rule.rename_from

            # выполняю простановку правил override type и исключение колонок exclude
            for i in range(len(tgt_fields)):
                val = tgt_fields[i]
                if val.name == rule.name:
                    if rule.override_type:
                        val.override_type = rule.override_type
                    if rule.exclude:
                        val.exclude = rule.exclude

        # объединяю информацию в один список PostgresCompareColumn, где будут находится все атрибуты сравнения
        # src_info, tgt_info, override_schema
        columns: List[PostgresToPostgresSchemaSyncCompareColumn] = []
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

                    if src.rename_from:
                        val.rename_from = src.rename_from

                    if src.ordinal_position:
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

                    if tgt.override_type:
                        val.override_type = tgt.override_type

                    if tgt.exclude:
                        val.exclude = tgt.exclude

                    if tgt.ordinal_position:
                        val.ordinal_position = tgt.ordinal_position
                else:
                    columns.append(tgt)

            if rule:
                if rule.name in columns:
                    val = columns[columns.index(rule.name)]

                    if rule.rename_from:
                        val.rename_from = rule.rename_from

                    if rule.override_type:
                        val.override_type = rule.override_type

                    if rule.exclude:
                        val.exclude = rule.exclude
                else:
                    max_ordinal += 1
                    columns.append(
                        PostgresToPostgresSchemaSyncCompareColumn(
                            name=rule.name,
                            src_schema=src_schema,
                            src_table=src_table,
                            tgt_schema=tgt_schema,
                            tgt_table=tgt_table,
                            src=None,
                            tgt=None,
                            ordinal_position=max_ordinal,
                            rename_from=rule.rename_from,
                            override_type=rule.override_type,
                            exclude=rule.exclude,
                        )
                    )

        if len(columns) == 0:
            raise RuntimeError(
                f"""It was not possible to generate a list of rules for synchronizing schemas.
Pay attention to the src and tgt data, maybe the problem lies in them

{src_schema}.{src_table} -> {tgt_schema}.{tgt_table}

src_columns: {src_info}
tgt_columns {tgt_info}
manual_columns: {rule_columns}
"""
            )

        return columns

    def make_fields_info(
        self,
        src_table_check,
        src_cursor,
        src_schema,
        src_table,
        tgt_cursor,
        tgt_schema,
        tgt_table,
        rule_columns,
    ):
        if src_table_check:
            if src_schema is None or src_table is None:
                raise RuntimeError(
                    f"""src_schema or src_table is None
src_schema = {src_schema}
src_table = {src_table}

Please pass "src_schema" and "src_table", or disable the "src_table_check" check
"""
                )
            self.log.info(f"Checking src table existence: {src_schema}.{src_table} ...")
            if not self.pg_man.pg_check_table_exist(src_cursor, src_schema, src_table):
                raise RuntimeError(
                    f"""src_table_check = {src_table_check}
The table: {src_schema}.{src_table} not found in {src_cursor.connection.dsn}

Please make sure of this manually, or disable the "src_table_check" parameter"""
                )
        else:
            self.log.info(f"src_table_check = {src_table_check}")

        if src_schema is not None and src_table is not None:
            src_info = self.pg_man.pg_get_fields(src_cursor, src_schema, src_table)
        else:
            src_info = []

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

    def check_schema(
        self,
        src_cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor,
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn],
        context,
    ):
        self.log.info(f"")
        self.log.info(
            f"schema check: {src_schema}.{src_table} -> {tgt_schema}.{tgt_table}"
        )
        self.log.info(f"pg src: {src_cursor.connection.dsn}")
        self.log.info(f"pg tgt: {tgt_cursor.connection.dsn}")

        union_columns = self.make_fields_info(
            src_table_check=src_table_check,
            src_cursor=src_cursor,
            src_schema=src_schema,
            src_table=src_table,
            tgt_cursor=tgt_cursor,
            tgt_schema=tgt_schema,
            tgt_table=tgt_table,
            rule_columns=rule_columns,
        )

        self.log.info(
            f"""Checking equal structure between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}"""
        )
        equal_schema, error = self.check_equal_structure(
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            union_columns,
        )
        
        

class PostgresToPostgresSchemaSync:
    def __init__(
        self,
        logger,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: Optional[str],
        src_table: Optional[str],
        tgt_cursor: psycopg2.extensions.cursor,
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        schema_strategy: Union[
            PostgresToPostgresSchemaStrategy, str
        ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_schema: Optional[Union[str, Dict[str, str]]] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
        create_table_template: Optional[str] = None,
    ) -> None:
        self.log = logger
        self.src_cursor = src_cursor
        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_cursor = tgt_cursor
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.src_table_check = src_table_check
        self.schema_strategy = schema_strategy
        self.create_table_template = create_table_template
        self.rename_columns = rename_columns
        self.override_schema = override_schema
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
            src_table_check,
            schema_strategy,
            create_table_template,
            rule_columns,
        ) = self.clean_validate_and_flatten_params(
            self.src_schema,
            self.src_table,
            self.tgt_schema,
            self.tgt_table,
            self.src_table_check,
            self.schema_strategy,
            self.rename_columns,
            self.override_schema,
            self.exclude_columns,
            self.create_table_template,
        )

        self.sync_schema(
            schema_strategy,
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            src_table_check,
            rule_columns,
            create_table_template,
            context,
        )

    def clean_validate_and_flatten_params(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_table_check,
        schema_strategy,
        rename_columns,
        override_schema,
        exclude_columns,
        create_table_template,
    ):
        if isinstance(schema_strategy, str):
            schema_strategy = PostgresToPostgresSchemaStrategy(schema_strategy)
        elif isinstance(schema_strategy, PostgresToPostgresSchemaStrategy):
            schema_strategy = schema_strategy
        else:
            raise TypeError(
                f"schema_strategy type not valid: {type(schema_strategy)}: {schema_strategy}. Please use class PostgresSchemaStrategy"
            )

        if src_table_check is None or not isinstance(src_table_check, bool):
            raise RuntimeError(
                f"""'src_table_check' parameter must be bool
{type(src_table_check)}: {src_table_check}"""
            )

        if create_table_template is None:
            create_table_template = "create table {pg_table} ({pg_columns})"

        if not isinstance(create_table_template, str):
            raise RuntimeError(
                f""" "create_table_template" parameter must be str
{type(create_table_template)}: {create_table_template}"""
            )

        error_render_template = (
            """'create_table_template' parameter is incorrect:
"""
            + create_table_template
            + """

create_table_template must contain all parameters:
{pg_table}     - the full path to the table will be substituted here, for example: {pg_table} => "public"."t1"
{pg_columns}   - columns that will be generated during synchronization, for example: {pg_columns} => col_1 integer, col_2 timestamptz, "COL_3" varchar(4000) not null"

For example, the default template:

create table {pg_table} ({pg_columns})
"""
        )
        try:
            # тестирую возможность отрендерить шаблон
            # если тест неудачный, значит есть переменные в шаблоне, которые невозможно отрендерить
            create_table_template.format(
                **{
                    "pg_table": "ch_table",
                    "pg_columns": "ch_columns",
                }
            )
        except Exception as e:
            raise RuntimeError(error_render_template)

        if (
            create_table_template.find("{pg_table}") == -1
            or create_table_template.find("{pg_columns}") == -1
        ):
            raise RuntimeError(error_render_template)

        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn] = []

        if not rename_columns:
            rename_columns = {}

        if isinstance(rename_columns, Dict):
            for name, rename_from in rename_columns.items():
                if isinstance(name, str) and isinstance(rename_from, str):
                    index = None
                    for i in range(len(rule_columns)):
                        val = rule_columns[i]
                        if val.name == rename_from:
                            index = i
                            break

                    if index is not None:
                        val = rule_columns[index]
                        val.name = name
                        val.rename_from = rename_from
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                rename_from=rename_from,
                            )
                        )
                else:
                    raise RuntimeError(
                        """
Error validating parameter "rename_columns[{}]: unsupported type

rename_columns has an unsupported type.
rename_columns represents a set of {
    "column_name_in_tgt": "column_name_in_src",
    "column_name_in_tgt_1": "column_name_in_src_2",
} (dictionary)


The parameter passed was of type: {}
Its value: {}""".format(
                            name, type(rename_from), rename_from
                        )
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "rename_columns"

rename_columns has an unsupported type.
rename_columns represents a set of {
    "column_name_in_tgt": "column_name_in_src",
    "column_name_in_tgt_1": "column_name_in_src_2",
} (dictionary)


The parameter passed was of type: {}
Its value: {}""".format(
                    type(rename_columns), rename_columns
                )
            )

        # проверяю существует ли переопределение колонок, если нет то выставляю пустой словарь, для удобства работы в дальнейшем
        if not override_schema:
            override_schema = {}

        if isinstance(override_schema, Dict):
            # здесь я делаю нормализацию override_schema и привожу к классу PostgresSchemaSyncOverrideColumn
            for name, override_type in override_schema.items():
                if isinstance(name, str) and isinstance(override_type, str):
                    if name in rule_columns:
                        val = rule_columns[rule_columns.index(name)]
                        val.override_type = override_type
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                override_type=override_type,
                            )
                        )
                else:
                    raise RuntimeError(
                        f"""
Error validating parameter "override_schema[{name}]: unsupported type
the value type must be str
and must be the type of the column, for example:
- varchar(200) not null
- bigserial
- integer not null default 0

type: {type(override_type)}
value: {override_type}"""
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "override_schema"

override_schema has an unsupported type.
override_schema represents a set of {
    "column_name": "data type",
    "column_name_2": "varchar(200) not null default now()",
} (dictionary)
with which to override the schema of individual columns

The parameter passed was of type: {}
Its value: {}""".format(
                    type(override_schema), override_schema
                )
            )

        if not exclude_columns:
            exclude_columns = []

        if isinstance(exclude_columns, List):
            for name in exclude_columns:
                if isinstance(name, str):
                    if name in rule_columns:
                        val = rule_columns[rule_columns.index(name)]
                        val.exclude = True
                    else:
                        rule_columns.append(
                            PostgresToPostgresSchemaSyncOverrideColumn(
                                name=name,
                                exclude=True,
                            )
                        )
                else:
                    raise RuntimeError(
                        """
Error validating parameter "exclude_columns"

exclude_columns has an unsupported type.
exclude_columns represents a List[str] of ["column_name_in_src", "column_name_in_src", ..]

The parameter passed was of type: {}
Its value: {}""".format(
                            type(name), name
                        )
                    )
        else:
            raise RuntimeError(
                """
Error validating parameter "exclude_columns"

exclude_columns has an unsupported type.
exclude_columns represents a List[str] of ["column_name_in_src", "column_name_in_src", ..]

The parameter passed was of type: {}
Its value: {}""".format(
                    type(exclude_columns), exclude_columns
                )
            )

        return (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            src_table_check,
            schema_strategy,
            create_table_template,
            rule_columns,
        )

    def make_fields_info(
        self,
        src_table_check,
        src_cursor,
        src_schema,
        src_table,
        tgt_cursor,
        tgt_schema,
        tgt_table,
        rule_columns,
    ):
        if src_table_check:
            if src_schema is None or src_table is None:
                raise RuntimeError(
                    f"""src_schema or src_table is None
src_schema = {src_schema}
src_table = {src_table}

Please pass "src_schema" and "src_table", or disable the "src_table_check" check
"""
                )
            self.log.info(f"Checking src table existence: {src_schema}.{src_table} ...")
            if not self.pg_man.pg_check_table_exist(src_cursor, src_schema, src_table):
                raise RuntimeError(
                    f"""src_table_check = {src_table_check}
The table: {src_schema}.{src_table} not found in {src_cursor.connection.dsn}

Please make sure of this manually, or disable the "src_table_check" parameter"""
                )
        else:
            self.log.info(f"src_table_check = {src_table_check}")

        if src_schema is not None and src_table is not None:
            src_info = self.pg_man.pg_get_fields(src_cursor, src_schema, src_table)
        else:
            src_info = []

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

    def sync_schema(
        self,
        schema_strategy,
        src_cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor,
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn],
        create_table_template: str,
        context,
    ):
        self.log.info(f"")
        self.log.info(
            f"schema sync: {src_schema}.{src_table} -> {tgt_schema}.{tgt_table}"
        )
        self.log.info(f"pg src: {src_cursor.connection.dsn}")
        self.log.info(f"pg tgt: {tgt_cursor.connection.dsn}")

        error_table_missing_ods = f"""Table missing in src: {tgt_schema}.{tgt_table}
To automatically create a schema, use the strategy:
- create_table_if_not_exists
- backup_and_create_table_if_schema_changed
- backup_and_create_table_if_exist
- drop_and_create_table_if_schema_changed
- drop_and_create_table_if_exist
"""

        union_columns = self.make_fields_info(
            src_table_check,
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            rule_columns,
        )

        match schema_strategy:
            case PostgresToPostgresSchemaStrategy.create_table_if_not_exists:
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                exists = self.pg_man.pg_check_table_exist(
                    tgt_cursor, tgt_schema, tgt_table
                )
                if not exists:
                    self.log.warning(
                        f"""Table is missing {tgt_schema}.{tgt_table}
I'm trying to create a table"""
                    )

                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )

                    # после создания таблицы нужно обновить информацию о колонках
                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

            case PostgresToPostgresSchemaStrategy.error_if_table_schema_changed:
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                exists = self.pg_man.pg_check_table_exist(
                    tgt_cursor, tgt_schema, tgt_table
                )
                if not exists:
                    raise RuntimeError(error_table_missing_ods)

                self.log.info(
                    f"""Checking equal structure between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}"""
                )
                equal_schema, error = self.check_equal_structure(
                    src_schema,
                    src_table,
                    tgt_schema,
                    tgt_table,
                    union_columns,
                )
                if not equal_schema:
                    raise RuntimeError(error)

            case PostgresToPostgresSchemaStrategy.error_if_table_not_exist:
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                exists = self.pg_man.pg_check_table_exist(
                    tgt_cursor, tgt_schema, tgt_table
                )
                if not exists:
                    raise RuntimeError(error_table_missing_ods)

            case (
                PostgresToPostgresSchemaStrategy.backup_and_create_table_if_schema_changed
            ):
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                exists = self.pg_man.pg_check_table_exist(
                    tgt_cursor, tgt_schema, tgt_table
                )
                if not exists:
                    self.log.warning(
                        f"""Table is missing {tgt_schema}.{tgt_table}
I'm trying to create a table"""
                    )

                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )
                    self.log.info(
                        f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                    )

                    self.log.info(
                        f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                    )
                    exists = self.pg_man.pg_check_table_exist(
                        tgt_cursor, tgt_schema, tgt_table
                    )
                    if not exists:
                        raise RuntimeError(
                            f"""Failed to automatically create tgt {tgt_schema}.{tgt_table}
Probably an error occurred that could not be processed.
Please create table manually"""
                        )

                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

                self.log.info(
                    f"""Checking equal structure between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}"""
                )
                equal_schema, error = self.check_equal_structure(
                    src_schema,
                    src_table,
                    tgt_schema,
                    tgt_table,
                    union_columns,
                )
                if not equal_schema:
                    self.log.warning(error)
                    self.log.info(
                        f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                    )
                    exists = self.pg_man.pg_check_table_exist(
                        tgt_cursor, tgt_schema, tgt_table
                    )
                    if exists:
                        self.log.warning(
                            f"""Table already exists {tgt_schema}.{tgt_table}
Trying to backup the table"""
                        )

                        self.backup_tgt_table(
                            tgt_cursor, tgt_schema, tgt_table, union_columns
                        )

                        self.log.info(
                            f"""Table backup completed successfully
creating a new table {tgt_schema}.{tgt_table}"""
                        )
                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )
                    self.log.info(
                        f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                    )

                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

            case PostgresToPostgresSchemaStrategy.backup_and_create_table_if_exist:
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                if self.pg_man.pg_check_table_exist(tgt_cursor, tgt_schema, tgt_table):
                    self.log.warning(
                        f"""Table already exists {tgt_schema}.{tgt_table}
Trying to backup the table"""
                    )

                    self.backup_tgt_table(
                        tgt_cursor, tgt_schema, tgt_table, union_columns
                    )

                    self.log.warning(
                        f"""Table backup completed successfully
creating a new table {tgt_schema}.{tgt_table}"""
                    )
                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )
                    self.log.info(
                        f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                    )

                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

            case (
                PostgresToPostgresSchemaStrategy.drop_and_create_table_if_schema_changed
            ):
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                exists = self.pg_man.pg_check_table_exist(
                    tgt_cursor, tgt_schema, tgt_table
                )
                if not exists:
                    self.log.warning(
                        f"""Table is missing {tgt_schema}.{tgt_table}
I'm trying to create a table"""
                    )

                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )
                    self.log.info(
                        f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                    )

                    self.log.info(
                        f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                    )
                    exists = self.pg_man.pg_check_table_exist(
                        tgt_cursor, tgt_schema, tgt_table
                    )
                    if not exists:
                        raise RuntimeError(
                            f"""Failed to automatically create tgt {tgt_schema}.{tgt_table}
Probably an error occurred that could not be processed.
Please create table manually"""
                        )

                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

                self.log.info(
                    f"""Checking equal schema between {src_schema}.{src_table} and {tgt_schema}.{tgt_table}"""
                )
                equal_schema, error = self.check_equal_structure(
                    src_schema,
                    src_table,
                    tgt_schema,
                    tgt_table,
                    union_columns,
                )
                if not equal_schema:
                    self.log.warning(error)
                    self.log.info(
                        f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                    )
                    exists = self.pg_man.pg_check_table_exist(
                        tgt_cursor, tgt_schema, tgt_table
                    )
                    if exists:
                        self.log.warning(
                            f"""The table has been dropped: {tgt_schema}.{tgt_table}
Trying to drop the table"""
                        )

                        self.drop_tgt_table(
                            tgt_cursor, tgt_schema, tgt_table, union_columns
                        )

                        self.log.info(
                            f"""Table droping success: {tgt_schema}.{tgt_table}
I'm trying to create a table"""
                        )
                    self.create_tgt_table(
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        union_columns,
                        create_table_template,
                    )

                    self.log.info(
                        f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                    )

                    union_columns = self.make_fields_info(
                        src_table_check,
                        src_cursor,
                        src_schema,
                        src_table,
                        tgt_cursor,
                        tgt_schema,
                        tgt_table,
                        rule_columns,
                    )

            case PostgresToPostgresSchemaStrategy.drop_and_create_table_if_exist:
                self.log.info(
                    f"Checking tgt table existence: {tgt_schema}.{tgt_table} ..."
                )
                if self.pg_man.pg_check_table_exist(tgt_cursor, tgt_schema, tgt_table):
                    self.log.warning(
                        f"""Table already exists {tgt_schema}.{tgt_table}
Trying to drop the table"""
                    )

                    self.drop_tgt_table(
                        tgt_cursor, tgt_schema, tgt_table, union_columns
                    )

                    self.log.info(
                        f"""Table droping success: {tgt_schema}.{tgt_table}
trying to create it"""
                    )
                else:
                    self.log.info(
                        f"""The table is missing: {tgt_schema}.{tgt_table}
trying to create it"""
                    )

                self.log.info(f"""trying to create table: {tgt_schema}.{tgt_table}""")

                self.create_tgt_table(
                    tgt_cursor,
                    tgt_schema,
                    tgt_table,
                    union_columns,
                    create_table_template,
                )

                self.log.info(
                    f"""Table created successfully: {tgt_schema}.{tgt_table}"""
                )

                union_columns = self.make_fields_info(
                    src_table_check,
                    src_cursor,
                    src_schema,
                    src_table,
                    tgt_cursor,
                    tgt_schema,
                    tgt_table,
                    rule_columns,
                )

            case PostgresToPostgresSchemaStrategy.do_nothing:
                self.log.info(
                    f"""Strategy do_nothing selected
I do not perform any operations on the table schema {tgt_schema}.{tgt_table}"""
                )

            case x:
                raise RuntimeError(
                    f"""Strategy {x} does not have a handler
Please check the correctness of the parameter 'schema_strategy'"""
                )

    def create_tgt_table(
        self,
        cursor,
        schema,
        table,
        union_columns: List[PostgresToPostgresSchemaSyncCompareColumn],
        create_table_template,
    ):
        self.log.info(f"Creating a tgt table in postgres {schema}.{table} ...")

        # проверяем наличие схемы и создаём её если отсутствует
        self.log.info(f"""Checking for schema existence in postgres: "{schema}" ...""")
        if not self.pg_man.pg_check_schema_exists(cursor, schema):
            self.log.warning(f"Schema is missing: {schema}")
            self.pg_man.pg_create_schema(
                cursor,
                schema,
            )
            # в большенстве postgres операция create schema нетранзакционная
            # поэтому я вынужден сделать commit так как следующая операция может быть abort'нута
            cursor.connection.commit()
        else:
            self.log.info(f"The schema exists: {schema}")

        # формируем список столбцов для выражения create table
        # нужно отсортировать колонки строго так, как они сделаны в postgres (что бы красиво было ^:)
        # для этого я использую ordinal_position
        sorted_fields = sorted(union_columns)

        self.log.info("rules for creating table columns:")
        for x in sorted_fields.copy():
            self.log.info(f"{x}")

        # делаю список столбцов, который будет частью выражения create table (create_fields)
        create_fields = map(
            lambda x: x.create_tgt_fields_stmp(cursor), sorted_fields.copy()
        )
        create_fields = [x for x in create_fields if x is not None]

        # отправляем запрос на создание таблицы
        self.pg_man.pg_create_table(
            cursor,
            schema,
            table,
            create_fields,
            create_table_template=create_table_template,
        )
        cursor.connection.commit()

        self.log.info(f"Table create success")

    def drop_tgt_table(
        self,
        cursor,
        schema,
        table,
        union_columns: List[PostgresToPostgresSchemaSyncCompareColumn],
    ):
        self.log.info(f"drop table {schema}.{table} ...")
        self.pg_man.pg_drop_table(
            cursor,
            schema,
            table,
            with_cascade=True,
        )
        cursor.connection.commit()
        self.log.info(f"drop success")

    def backup_tgt_table(
        self,
        cursor,
        schema,
        table,
        union_columns: List[PostgresToPostgresSchemaSyncCompareColumn],
    ):
        # через union_columns получаю колонки tgt
        pg_info = map(lambda x: x.tgt, union_columns)
        pg_info = [x for x in pg_info if x is not None]
        
        self.log.info(f'backup table: "{schema}"."{table}"')
        self.pg_man.pg_backup_table(
            cursor,
            schema,
            table,
            pg_info,
        )

    def check_equal_structure(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        union_columns: List[PostgresToPostgresSchemaSyncCompareColumn],
    ):
        self.log.info("matching rules:")
        for rule in union_columns:
            self.log.info(f"{rule}")

        diff = map(lambda column: column.compare_column(), union_columns)
        diff = filter(lambda x: not x[0], diff)
        diff = list(diff)

        if len(diff) > 0:
            error = f"""Error validation columns

src: {src_schema}.{src_table}
tgt: {tgt_schema}.{tgt_table}
        """
            for k, v in diff:
                error += f"""
reason: {v}
        """
            error += f"""

Please make the columns src and tgt equal or disable validation
        """
            return False, error
        else:
            return True, "ok"

    def union_src_tgt_and_rules(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_info: List[Dict],
        tgt_info: List[Dict],
        rule_columns: List[PostgresToPostgresSchemaSyncOverrideColumn],
    ):
        column_name = "column_name"
        ordinal_position = "ordinal_position"

        # для начала составлю полный список филдов
        # выбиру из src имена и переопределю их через override_schema
        src_fields = list(
            map(
                lambda x: PostgresToPostgresSchemaSyncCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    src=x,
                    ordinal_position=x[ordinal_position],
                ),
                src_info,
            )
        )
        tgt_fields = list(
            map(
                lambda x: PostgresToPostgresSchemaSyncCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    tgt=x,
                    ordinal_position=x[ordinal_position],
                ),
                tgt_info,
            )
        )

        # нужно пройтись по rule_columns найти соответствия правилам и прописать их в PostgresCompareColumn
        # это необходимо что бы корректно выполнить матчинг между src, tgt и rules
        for rule in rule_columns:
            # выполняю ренейм колонок из src, что бы они корректно матчились с tgt
            for i in range(len(src_fields)):
                val = src_fields[i]
                if val.name == rule.rename_from:
                    val.name = rule.name
                    val.rename_from = rule.rename_from

            # выполняю простановку правил override type и исключение колонок exclude
            for i in range(len(tgt_fields)):
                val = tgt_fields[i]
                if val.name == rule.name:
                    if rule.override_type:
                        val.override_type = rule.override_type
                    if rule.exclude:
                        val.exclude = rule.exclude

        # объединяю информацию в один список PostgresCompareColumn, где будут находится все атрибуты сравнения
        # src_info, tgt_info, override_schema
        columns: List[PostgresToPostgresSchemaSyncCompareColumn] = []
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

                    if src.rename_from:
                        val.rename_from = src.rename_from

                    if src.ordinal_position:
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

                    if tgt.override_type:
                        val.override_type = tgt.override_type

                    if tgt.exclude:
                        val.exclude = tgt.exclude

                    if tgt.ordinal_position:
                        val.ordinal_position = tgt.ordinal_position
                else:
                    columns.append(tgt)

            if rule:
                if rule.name in columns:
                    val = columns[columns.index(rule.name)]

                    if rule.rename_from:
                        val.rename_from = rule.rename_from

                    if rule.override_type:
                        val.override_type = rule.override_type

                    if rule.exclude:
                        val.exclude = rule.exclude
                else:
                    max_ordinal += 1
                    columns.append(
                        PostgresToPostgresSchemaSyncCompareColumn(
                            name=rule.name,
                            src_schema=src_schema,
                            src_table=src_table,
                            tgt_schema=tgt_schema,
                            tgt_table=tgt_table,
                            src=None,
                            tgt=None,
                            ordinal_position=max_ordinal,
                            rename_from=rule.rename_from,
                            override_type=rule.override_type,
                            exclude=rule.exclude,
                        )
                    )

        if len(columns) == 0:
            raise RuntimeError(
                f"""It was not possible to generate a list of rules for synchronizing schemas.
Pay attention to the src and tgt data, maybe the problem lies in them

{src_schema}.{src_table} -> {tgt_schema}.{tgt_table}

src_columns: {src_info}
tgt_columns {tgt_info}
manual_columns: {rule_columns}
"""
            )

        return columns


class PostgresToPostgresSchemaSyncOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "src_conn_id",
        "src_schema",
        "src_table",
        "tgt_conn_id",
        "tgt_schema",
        "tgt_table",
        "src_table_check",
        "schema_strategy",
        "rename_columns",
        "override_schema",
        "exclude_columns",
        "create_table_template",
    )

    def __init__(
        self,
        src_conn_id: str,
        src_schema: Optional[str],
        src_table: Optional[str],
        tgt_conn_id: str,
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool = True,
        schema_strategy: Union[
            PostgresToPostgresSchemaStrategy, str
        ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_schema: Optional[Union[str, Dict[str, str]]] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
        create_table_template: Optional[str] = None,
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
        self.src_table_check = src_table_check
        self.schema_strategy = schema_strategy
        self.rename_columns = rename_columns
        self.override_schema = override_schema
        self.exclude_columns = exclude_columns
        self.create_table_template = create_table_template
        self.stack = ExitStack()

    def execute(self, context):
        src_hook = PostgresHook(postgres_conn_id=self.src_conn_id)
        src_cursor = self.stack.enter_context(closing(src_hook.get_cursor()))

        tgt_hook = PostgresHook(postgres_conn_id=self.tgt_conn_id)
        tgt_cursor = self.stack.enter_context(closing(tgt_hook.get_cursor()))

        base_module = PostgresToPostgresSchemaSync(
            self.log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            self.src_table_check,
            self.schema_strategy,
            self.rename_columns,
            self.override_schema,
            self.exclude_columns,
            self.create_table_template,
        )

        base_module.execute(context)


class PostgresToPostgresSchemaSyncModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        src_cur_key: Optional[str],
        src_schema: Optional[str],
        src_table: Optional[str],
        tgt_cur_key: Optional[str],
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        schema_strategy: Union[
            PostgresToPostgresSchemaStrategy, str
        ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_schema: Optional[Union[str, Dict[str, str]]] = None,
        exclude_columns: Optional[Union[str, List[str]]] = None,
        create_table_template: Optional[str] = None,
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
                "src_table_check",
                "schema_strategy",
                "rename_columns",
                "override_schema",
                "exclude_columns",
                "create_table_template",
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
        self.src_table_check = src_table_check
        self.schema_strategy = schema_strategy
        self.rename_columns = rename_columns
        self.override_schema = override_schema
        self.exclude_columns = exclude_columns
        self.create_table_template = create_table_template

    def __call__(self, context):
        self.render_template_fields(context)

        match context[self.context_key].get(self.src_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case pg_cur:
                src_cursor: psycopg2.extensions.cursor = pg_cur

        match context[self.context_key].get(self.tgt_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case pg_cur:
                tgt_cursor: psycopg2.extensions.cursor = pg_cur

        log = logging.getLogger(self.__class__.__name__)

        base_module = PostgresToPostgresSchemaSync(
            log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            self.src_table_check,
            self.schema_strategy,
            self.rename_columns,
            self.override_schema,
            self.exclude_columns,
            self.create_table_template,
        )

        base_module.execute(context)


def pg_to_pg_schema_sync(
    src_schema: Optional[str],
    src_table: Optional[str],
    tgt_schema: str,
    tgt_table: str,
    src_table_check: bool = True,
    schema_strategy: Union[
        PostgresToPostgresSchemaStrategy, str
    ] = PostgresToPostgresSchemaStrategy("create_table_if_not_exists"),
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
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

        return builder

    return wrapper


class PostgresToPostgresSchemaCheckModule(PipeTask):
    def __init__(
        self,
        context_key: str,
        template_render: Callable,
        src_cur_key: Optional[str],
        src_schema: Optional[str],
        src_table: Optional[str],
        tgt_cur_key: Optional[str],
        tgt_schema: str,
        tgt_table: str,
        src_table_check: bool,
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_schema: Optional[Union[str, Dict[str, str]]] = None,
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
                "src_table_check",
                "rename_columns",
                "override_schema",
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
        self.src_table_check = src_table_check
        self.rename_columns = rename_columns
        self.override_schema = override_schema
        self.exclude_columns = exclude_columns

    def __call__(self, context):
        self.render_template_fields(context)

        match context[self.context_key].get(self.src_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case pg_cur:
                src_cursor: psycopg2.extensions.cursor = pg_cur

        match context[self.context_key].get(self.tgt_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case pg_cur:
                tgt_cursor: psycopg2.extensions.cursor = pg_cur

        log = logging.getLogger(self.__class__.__name__)

        base_module = PostgresToPostgresSchemaCheck(
            logger=log,
            src_cursor=src_cursor,
            src_schema=self.src_schema,
            src_table=self.src_table,
            tgt_cursor=tgt_cursor,
            tgt_schema=self.tgt_schema,
            tgt_table=self.tgt_table,
            src_table_check=self.src_table_check,
            rename_columns=self.rename_columns,
            override_schema=self.override_schema,
            exclude_columns=self.exclude_columns,
        )

        base_module.execute(context)


def pg_to_pg_schema_check(
    src_schema: Optional[str],
    src_table: Optional[str],
    tgt_schema: str,
    tgt_table: str,
    src_table_check: bool = True,
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    src_cur_key: Optional[str] = None,
    tgt_cur_key: Optional[str] = None,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresToPostgresSchemaCheckModule(
                context_key=builder.context_key,
                template_render=builder.template_render,
                src_cur_key=src_cur_key,
                src_schema=src_schema,
                src_table=src_table,
                tgt_cur_key=tgt_cur_key,
                tgt_schema=tgt_schema,
                tgt_table=tgt_table,
                src_table_check=src_table_check,
                rename_columns=rename_columns,
                override_schema=override_schema,
                exclude_columns=exclude_columns,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
