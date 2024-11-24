from typing import (
    Any,
    Callable,
    List,
    Optional,
    Dict,
    Sequence,
    Tuple,
    Union,
)
import itertools
import logging
from functools import total_ordering
from contextlib import closing, ExitStack

import psycopg2.extras
import psycopg2.sql

from airflow.models.baseoperator import BaseOperator
from airflow.providers.gun.ch import ClickhouseCursor
from airflow.providers.gun.ch.hook import ClickhouseNativeHook

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.gun.pipe import (
    PipeStage,
    PipeTask,
    PipeTaskBuilder,
)
from airflow.providers.gun.sync.pg_to_pg_common import (
    pg_type_stmp_text,
    PostgresManipulator,
)
from airflow.providers.gun.sync.pg_to_ch_common import (
    ch_type_stmp_text,
    pg_type_to_ch_type,
    ClickhouseManipulator,
)
from airflow.providers.gun.sync.pg_to_ch_schema_sync import (
    PostgresToClickhouseSchemaStrategy,
    PostgresToClickhouseSchemaSyncModule,
)


class PostgresToClickhouseFullReloadOverrideColumn:
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
        elif isinstance(other, PostgresToClickhouseFullReloadOverrideColumn):
            return self.name == other.name

        return NotImplemented


@total_ordering
class PostgresToClickhouseFullReloadCompareColumn:
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
        elif isinstance(other, PostgresToClickhouseFullReloadCompareColumn):
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
            f" ; tgt: {self.tgt[column_name]}({ch_type_stmp_text(self.tgt)})"
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

    def select_field_column(self):
        if self.src and self.tgt:
            return self.src[self.column_name_key]
        elif self.src:
            return self.src[self.column_name_key]
        else:
            return self.rendering_value

    def select_field(self, alias: str, ch_cursor):
        if self.is_exclude_column():
            return None
        elif self.rendering_value is not None:
            match self.rendering_type:
                case "column":
                    val = "`{}`".format(self.select_field_column())
                    return val
                case "exp":
                    return self.rendering_value
                case "native":
                    return f"%({self.name})s"
                case _:
                    raise RuntimeError(
                        f"unsupported rendering type: {self.rendering_type}"
                    )
        elif self.src:
            val = "`{}`".format(self.src[self.column_name_key])
            return val
        else:
            return None

    def select_param(self, ch_cursor):
        if self.is_exclude_column():
            return None
        elif self.rendering_value is not None:
            match self.rendering_type:
                case "column":
                    return None
                case "exp":
                    return None
                case "native":
                    return (self.name, self.rendering_value)
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
            val = "`{}`".format(self.name)
            return val


class PostgresToClickhouseFullReload:
    def __init__(
        self,
        logger,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor: ClickhouseCursor,
        tgt_schema: str,
        tgt_table: str,
        ch_named_collection: str,
        ch_cluster: Optional[str] = "gpn",
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_select: Optional[
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
        self.ch_named_collection = ch_named_collection
        self.ch_cluster = ch_cluster
        self.rename_columns = rename_columns
        self.override_select = override_select
        self.exclude_columns = exclude_columns
        self.pg_man = PostgresManipulator(logger)
        self.ch_man = ClickhouseManipulator(logger)

    def execute(self, context):
        src_cursor = self.src_cursor
        tgt_cursor = self.tgt_cursor

        (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            tgt_schema_ex,
            tgt_table_ex,
            ch_named_collection,
            ch_cluster,
            rule_columns,
        ) = self.clean_validate_and_flatten_params(
            self.src_schema,
            self.src_table,
            self.tgt_schema,
            self.tgt_table,
            self.ch_named_collection,
            self.ch_cluster,
            self.rename_columns,
            self.override_select,
            self.exclude_columns,
        )

        self.sync_exchange_table(
            tgt_cursor,
            tgt_schema,
            tgt_table,
            tgt_schema_ex,
            tgt_table_ex,
            ch_cluster,
        )

        self.sync_data(
            src_cursor,
            src_schema,
            src_table,
            tgt_cursor,
            tgt_schema,
            tgt_table,
            tgt_schema_ex,
            tgt_table_ex,
            ch_named_collection,
            ch_cluster,
            rule_columns,
            context,
        )

    def sync_data(
        self,
        src_cursor: psycopg2.extensions.cursor,
        src_schema: str,
        src_table: str,
        tgt_cursor: ClickhouseCursor,
        tgt_schema: str,
        tgt_table: str,
        tgt_schema_ex: str,
        tgt_table_ex: str,
        ch_named_collection: str,
        ch_cluster: str,
        rule_columns: List[PostgresToClickhouseFullReloadOverrideColumn],
        context,
    ):
        self.log.info(
            f"Full reload {src_schema}.{src_table} -> {tgt_schema}.{tgt_table}"
        )
        self.log.info(f"pg: {src_cursor.connection.dsn}")
        self.log.info(f"ch: {tgt_cursor._client.connection}")

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

        alias = "s"
        select_fields = map(
            lambda column: column.select_field(alias, tgt_cursor), columns.copy()
        )
        select_fields = [x for x in select_fields if x is not None]

        select_params = map(
            lambda column: column.select_param(tgt_cursor), columns.copy()
        )

        select_params = [x for x in select_params if x is not None]
        select_params = dict(select_params)

        insert_fields = map(
            lambda column: column.insert_field(tgt_cursor), columns.copy()
        )
        insert_fields = [x for x in insert_fields if x is not None]

        self.ch_man.ch_truncate_table(
            tgt_cursor,
            tgt_schema_ex,
            tgt_table_ex,
            ch_cluster,
        )

        self.ch_man.ch_insert_select_from_postgres_use_named_collection(
            tgt_cursor,
            tgt_schema_ex,
            tgt_table_ex,
            insert_fields,
            src_schema,
            src_table,
            select_fields,
            select_params,
            ch_named_collection,
        )

        self.ch_man.ch_exchange_table(
            tgt_cursor,
            tgt_schema,
            tgt_table,
            tgt_schema_ex,
            tgt_table_ex,
            ch_cluster,
        )

        self.log.info(
            f"Full reload successfuly: {src_schema}.{src_table} -> {tgt_schema}.{tgt_table}"
        )

    def sync_exchange_table(
        self,
        ch_cursor: ClickhouseCursor,
        ch_schema,
        ch_table,
        ch_schema_ex,
        ch_table_ex,
        ch_cluster,
    ):
        self.log.info(
            f"Schema synchronization {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
        )

        self.log.info(f"Checking if table exists: {ch_schema_ex}.{ch_table_ex}")
        exists = self.ch_man.ch_check_table_exist(ch_cursor, ch_schema_ex, ch_table_ex)
        if exists:
            self.log.info(
                f"The table exists, I am checking the equivalence of the structure between {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
            )
            ex_equal, error = self.check_ch_tables_equal(
                ch_cursor, ch_schema, ch_table, ch_schema_ex, ch_table_ex
            )
            if not ex_equal:
                self.log.warning(
                    f"schemes are not equal, recreate duplicate table: {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
                )
                self.log.warning(f"drop table: {ch_schema_ex}.{ch_table_ex}")
                self.ch_drop_table(ch_cursor, ch_schema_ex, ch_table_ex, ch_cluster)
                self.log.warning(
                    f"try create dublicate table: {ch_schema_ex}.{ch_table_ex}..."
                )
                self.ch_create_exchange_table(
                    ch_cursor,
                    ch_schema,
                    ch_table,
                    ch_schema_ex,
                    ch_table_ex,
                    ch_cluster,
                )
                self.log.info(
                    f"Duplicate table created successfully: {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
                )
            else:
                self.log.info(
                    f"Duplicate table equal: {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
                )
        else:
            self.log.warning(
                f"The duplicate table is missing, I create it: {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
            )
            self.ch_create_exchange_table(
                ch_cursor,
                ch_schema,
                ch_table,
                ch_schema_ex,
                ch_table_ex,
                ch_cluster,
            )
            self.log.info(
                f"Duplicate table created successfully: {ch_schema}.{ch_table} -> {ch_schema_ex}.{ch_table_ex}"
            )

    def ch_drop_table(
        self,
        ch_cursor,
        ch_database,
        ch_table,
        ch_cluster,
    ):
        self.ch_man.ch_drop_table(
            ch_cursor,
            ch_database,
            ch_table,
            ch_cluster,
            temporary=False,
            sync=False,
        )

    def check_ch_tables_equal(
        self, ch_cursor, ch_schema, ch_table, ch_schema_ex, ch_table_ex
    ):
        self.log.info(
            f"compare clickhouse tables: {ch_schema}.{ch_table} === {ch_schema_ex}.{ch_table_ex} ..."
        )

        query = """
select
   name,
   type,
   position,
   default_kind,
   default_expression,
   is_in_partition_key,
   is_in_sorting_key,
   is_in_primary_key,
   is_in_sampling_key
from system.columns
where 1=1
  and database = %(schema)s
  and table = %(table)s
order by
  position
"""

        try:
            ch_cursor.execute(
                query,
                {
                    "schema": ch_schema,
                    "table": ch_table,
                },
            )
            ch_src = ch_cursor.fetchall()
            ch_cursor.execute(
                query,
                {
                    "schema": ch_schema_ex,
                    "table": ch_table_ex,
                },
            )
            ch_tgt = ch_cursor.fetchall()
        except Exception as e:
            raise RuntimeError(
                f"""
Failed to compare clickhouse tables:
{ch_schema}.{ch_table} === {ch_schema_ex}.{ch_table_ex}

Because a query error occurred in clickhouse:
{e}
"""
            )

        ch_src = set(ch_src)
        ch_tgt = set(ch_tgt)
        src_only = ch_src.copy()
        src_only.difference_update(ch_tgt)
        tgt_only = ch_tgt.copy()
        tgt_only.difference_update(ch_src)

        if src_only or tgt_only:
            error = f"""clickhouse tables do not equal: {ch_schema}.{ch_table} <> {ch_schema_ex}.{ch_table_ex}
diff columns:
"""
            for x in src_only:
                error += f"{x}"

            for x in tgt_only:
                error += f"{x}"

            return False, error
        else:
            return True, "ok"

    def ch_create_exchange_table(
        self, ch_cursor, ch_schema, ch_table, ch_schema_ex, ch_table_ex, ch_cluster
    ):
        self.log.info(
            f"create duplicate table: {ch_schema}.{ch_table} => {ch_schema_ex}.{ch_table_ex} ..."
        )

        query_table = """
select
  database,
  name,
  engine,
  partition_key,
  sorting_key,
  primary_key,
  sampling_key
from
  system.tables c
where 1=1
    and c.database in (%(schema)s)
    and c.table in (%(table)s)
"""
        try:
            ch_cursor.execute(
                query_table,
                {
                    "schema": ch_schema,
                    "table": ch_table,
                },
            )
            res = ch_cursor.fetchall()
            res_columns = map(lambda x: x.name, ch_cursor.description)
            res_table = [dict(zip(res_columns, x)) for x in res]
            res_len = len(res)
            if res_len == 0:
                raise RuntimeError(
                    f"""Could not find source table {ch_schema}.{ch_table} to copy structure and create duplicate {ch_schema_ex}.{ch_table_ex}
Please check this manually"""
                )
            elif res_len > 1:
                raise RuntimeError(
                    f"""More than one source table {ch_schema}.{ch_table} was found for
This situation cannot be handled automatically
Please create a duplicate table manually"""
                )

            res_table = res_table[0]
        except Exception as e:
            raise RuntimeError(
                f"""Failed to create duplicate of table {ch_schema}.{ch_table}
- Error getting table metadata: {e}
"""
            )

        query_columns = """
select
   name,
   type,
   position,
   default_kind,
   default_expression,
   is_in_partition_key,
   is_in_sorting_key,
   is_in_primary_key,
   is_in_sampling_key
from system.columns
where 1=1
  and database in (%(schema)s)
  and table in (%(table)s)
order by
  position
"""
        try:
            ch_cursor.execute(
                query_columns,
                {
                    "schema": ch_schema,
                    "table": ch_table,
                },
            )
            res = ch_cursor.fetchall()
            res_columns = map(lambda x: x.name, ch_cursor.description)
            res_columns = list(res_columns)
            res_columns = [dict(zip(res_columns, x)) for x in res]
        except Exception as e:
            raise RuntimeError(
                f"""Failed to create duplicate of table {ch_schema}.{ch_table}
- Error getting columns metadata: {e}
"""
            )

        ch_engine = res_table["engine"]
        ch_order_by = res_table["sorting_key"]
        if not ch_order_by:
            ch_order_by = "order by tuple()"
        else:
            ch_order_by = f"order by ({ch_order_by})"

        ch_partition_key_by = res_table["partition_key"]
        if ch_partition_key_by:
            ch_partition_key_by = f"partition by {ch_partition_key_by}"

        ch_primary_key_by = res_table["primary_key"]
        if ch_primary_key_by:
            ch_primary_key_by = f"primary key ({ch_primary_key_by})"

        ch_sampling_key = res_table["sampling_key"]
        if ch_sampling_key:
            ch_sampling_key = f"sample by {ch_sampling_key}"

        ch_columns = map(lambda x: "`{}` {}".format(x["name"], x["type"]), res_columns)
        ch_columns = ",\n".join(ch_columns)

        if ch_cluster:
            ch_cluster_exp = f"on cluster {ch_cluster}"
        else:
            ch_cluster_exp = ""

        query_create = f"""
create table `{ch_schema_ex}`.`{ch_table_ex}` {ch_cluster_exp}
(
{ch_columns}
)
engine = {ch_engine}
{ch_order_by}
{ch_partition_key_by}
{ch_primary_key_by}
{ch_sampling_key}
"""

        try:
            ch_cursor.execute(query_create)
            ch_cursor.fetchall()
        except Exception as e:
            raise RuntimeError(
                f"""Failed to create duplicate of table {ch_schema}.{ch_table}
- Error executing query: {e}
Please review the query, edit it and create the table manually

{query_create}
"""
            )

    def union_src_tgt_and_rules(
        self,
        src_schema,
        src_table,
        tgt_schema,
        tgt_table,
        src_info: List[Dict],
        tgt_info: List[Dict],
        rule_columns: List[PostgresToClickhouseFullReloadOverrideColumn],
    ):
        column_name = "column_name"
        ordinal_position = "ordinal_position"

        # для начала составлю полный список филдов
        # выбиру из src имена и переопределю их через override_select
        src_fields = list(
            map(
                lambda x: PostgresToClickhouseFullReloadCompareColumn(
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
                lambda x: PostgresToClickhouseFullReloadCompareColumn(
                    name=x[column_name],
                    src_schema=src_schema,
                    src_table=src_table,
                    tgt_schema=tgt_schema,
                    tgt_table=tgt_table,
                    # намеренно не проставляю rendering_value, так как далее происходит проверка на None
                    # rendering_value это значение которое идёт из исходной таблицы, а не из таблицы назначения
                    # одно либо придёт с src, либо через rule_columns
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
        # src_info, tgt_info, override_select
        columns: List[PostgresToClickhouseFullReloadCompareColumn] = []
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
                        # val.rendering_value = rule.rename_from

                    if rule.rendering_type is not None:
                        val.rendering_type = rule.rendering_type

                    if rule.rendering_value is not None:
                        val.rendering_value = rule.rendering_value

                    if rule.exclude is not None:
                        val.exclude = rule.exclude
                else:
                    max_ordinal += 1
                    columns.append(
                        PostgresToClickhouseFullReloadCompareColumn(
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
        tgt_info = self.ch_man.ch_get_fields(tgt_cursor, tgt_schema, tgt_table)

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
        ch_named_collection,
        ch_cluster,
        rename_columns,
        override_select,
        exclude_columns,
    ):

        if ch_cluster is not None and not isinstance(ch_cluster, str):
            raise TypeError(
                f"""'ch_cluster' parameter must be str
{type(ch_cluster): {ch_cluster}}")"""
            )

        if not isinstance(ch_named_collection, str):
            raise RuntimeError(
                f"""An invalid ch_named_collection was passed:
{ch_named_collection}

Description: https://clickhouse.com/docs/ru/operations/named-collections
named_collection is required for end-to-end access from clickhouse to postgres

To see available named_collection, run a query on clickhouse:
select * from system.named_collections"""
            )

        res: List[PostgresToClickhouseFullReloadOverrideColumn] = []

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
                for i in range(len(res)):
                    val = res[i]
                    if val.name == rename_from:
                        index = i
                        break

                if index is not None:
                    val = res[index]
                    val.name = name
                    val.rename_from = rename_from
                else:
                    res.append(
                        PostgresToClickhouseFullReloadOverrideColumn(
                            name=name,
                            rendering_type="column",
                            rename_from=rename_from,
                            rendering_value=rename_from,
                        )
                    )
            else:
                raise RuntimeError(type_error)

        # проверяю существует ли переопределение колонок, если нет то выставляю пустой словарь, для удобства работы в дальнейшем
        if not override_select:
            override_select = {}

        type_error = """
Error validating parameter "override_select"

override_select has an unsupported type.: {}
override_select represents a dict of {
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
        # проверяю, является ли override_select словарём и если это не так, то выдаём ошибку
        if not isinstance(override_select, Dict):
            raise RuntimeError(type_error.format(type(override_select)))

        # здесь я делаю нормализацию override_select и привожу к классу PostgresToPostgresFullReloadOverrideColumn
        for name, override_value in override_select.items():
            if isinstance(name, str) and isinstance(override_value, Tuple):
                if len(override_value) == 1:
                    rendering_type = "native"
                    rendering_value = override_value[0]

                elif len(override_value) == 2:
                    if override_value[1] not in ["native", "column", "exp"]:
                        raise RuntimeError(
                            f"""'override_select' validation error
The value {override_value} is invalid
'override_type' can take only ​​{["native", "column", "exp"]}"""
                        )

                    if override_value[1] in ["column", "exp"] and not isinstance(
                        override_value[1], str
                    ):
                        raise RuntimeError(
                            f"""'override_select' validation error
The value {override_value} is invalid
if 'override_type' = {override_value[1]} then 'override_value' can only be string"""
                        )

                    rendering_type = override_value[1]
                    rendering_value = override_value[0]
                else:
                    raise RuntimeError(type_error.format(type(override_select)))

                if name in res:
                    val = res[res.index(name)]
                    val.rendering_type = rendering_type
                    val.rendering_value = rendering_value
                else:
                    res.append(
                        PostgresToClickhouseFullReloadOverrideColumn(
                            name=name,
                            rendering_type=rendering_type,
                            rendering_value=rendering_value,
                        )
                    )

            elif isinstance(name, str):
                if name in res:
                    val = res[res.index(name)]
                    val.rendering_type = "native"
                    val.rendering_value = override_value
                else:
                    res.append(
                        PostgresToClickhouseFullReloadOverrideColumn(
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
                if name in res:
                    val = res[res.index(name)]
                    val.exclude = True
                else:
                    res.append(
                        PostgresToClickhouseFullReloadOverrideColumn(
                            name=name,
                            rendering_type="column",
                            exclude=True,
                            rendering_value=name,
                        )
                    )
            else:
                raise RuntimeError(type_error)

        return (
            src_schema,
            src_table,
            tgt_schema,
            tgt_table,
            tgt_schema,
            f"{tgt_table}__ex",
            ch_named_collection,
            ch_cluster,
            res,
        )


class PostgresToClickhouseFullReloadOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "src_conn_id",
        "src_schema",
        "src_table",
        "tgt_conn_id",
        "tgt_schema",
        "tgt_table",
        "ch_named_collection",
        "ch_cluster",
        "rename_columns",
        "override_select",
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
        ch_named_collection: str,
        ch_cluster: Optional[str] = "gpn",
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_select: Optional[
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
        self.ch_named_collection = ch_named_collection
        self.ch_cluster = ch_cluster
        self.rename_columns = rename_columns
        self.override_select = override_select
        self.exclude_columns = exclude_columns
        self.stack = ExitStack()

    def execute(self, context):
        src_hook = PostgresHook(postgres_conn_id=self.src_conn_id)
        src_cursor = self.stack.enter_context(closing(src_hook.get_cursor()))

        tgt_hook = ClickhouseNativeHook(clickhouse_conn_id=self.tgt_conn_id)
        tgt_cursor = self.stack.enter_context(closing(tgt_hook.get_cursor()))

        base_module = PostgresToClickhouseFullReload(
            self.log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            self.ch_named_collection,
            self.ch_cluster,
            self.rename_columns,
            self.override_select,
            self.exclude_columns,
        )

        base_module.execute(context)


class PostgresToClickhouseFullReloadModule(PipeTask):
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
        ch_named_collection: str,
        ch_cluster: Optional[str] = "gpn",
        rename_columns: Optional[Union[str, Dict[str, str]]] = None,
        override_select: Optional[
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
                "ch_named_collection",
                "ch_cluster",
                "rename_columns",
                "override_select",
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
            self.tgt_cur_key = "ch_cur"

        self.src_schema = src_schema
        self.src_table = src_table
        self.tgt_schema = tgt_schema
        self.tgt_table = tgt_table
        self.ch_named_collection = ch_named_collection
        self.ch_cluster = ch_cluster
        self.rename_columns = rename_columns
        self.override_select = override_select
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
            case cur:
                src_cursor: psycopg2.extensions.cursor = cur

        match context[self.context_key].get(self.tgt_cur_key):
            case None:
                raise RuntimeError(
                    """Could not find postgres cursor (postgres connection)
Before using module, you need to define postgres connection.
This can be done via 'pg_auth_airflow_conn'"""
                )
            case cur:
                tgt_cursor: ClickhouseCursor = cur

        log = logging.getLogger(self.__class__.__name__)

        base_module = PostgresToClickhouseFullReload(
            log,
            src_cursor,
            self.src_schema,
            self.src_table,
            tgt_cursor,
            self.tgt_schema,
            self.tgt_table,
            self.ch_named_collection,
            self.ch_cluster,
            self.rename_columns,
            self.override_select,
            self.exclude_columns,
        )

        base_module.execute(context)


def pg_to_ch_full_reload(
    src_schema: str,
    src_table: str,
    tgt_schema: str,
    tgt_table: str,
    ch_named_collection: str,
    src_table_check: bool = True,
    ch_cluster: Optional[str] = "gpn",
    ch_order_by: Optional[str] = None,
    schema_strategy: Union[
        PostgresToClickhouseSchemaStrategy, str
    ] = PostgresToClickhouseSchemaStrategy("create_table_if_not_exists"),
    rename_columns: Optional[Union[str, Dict[str, str]]] = None,
    override_schema: Optional[Union[str, Dict[str, str]]] = None,
    override_select: Optional[
        Union[str, Dict[str, Union[Any, Tuple[Any, str]]]]
    ] = None,
    exclude_columns: Optional[Union[str, List[str]]] = None,
    create_table_template: str = """create table {ch_table} {ch_cluster} (
{ch_columns}
)
engine ReplicatedMergeTree
{ch_order_by}""",
    src_cur_key: Optional[str] = None,
    tgt_cur_key: Optional[str] = None,
    pipe_stage: Optional[PipeStage] = None,
):
    def wrapper(builder: PipeTaskBuilder):
        builder.add_module(
            PostgresToClickhouseSchemaSyncModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                src_table_check,
                ch_cluster,
                ch_order_by,
                schema_strategy,
                rename_columns,
                override_schema,
                exclude_columns,
                create_table_template,
            ),
            pipe_stage,
        )

        builder.add_module(
            PostgresToClickhouseFullReloadModule(
                builder.context_key,
                builder.template_render,
                src_cur_key,
                src_schema,
                src_table,
                tgt_cur_key,
                tgt_schema,
                tgt_table,
                ch_named_collection,
                ch_cluster,
                rename_columns,
                override_select,
                exclude_columns,
            ),
            pipe_stage,
        )

        return builder

    return wrapper
