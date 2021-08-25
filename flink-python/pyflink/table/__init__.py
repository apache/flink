################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""
Entry point classes of Flink Table API:

    - :class:`TableEnvironment` and :class:`StreamTableEnvironment`
      Main entry point for Flink Table API & SQL functionality. :class:`TableEnvironment` is used
      in pure Table API & SQL jobs. Meanwhile, :class:`StreamTableEnvironment` needs to be used when
      mixing use of Table API and DataStream API.
    - :class:`Table`
      The core component of the Table API. Use the methods of :class:`Table` to transform data.
    - :class:`StatementSet`
      The core component of the Table API. It's used to create jobs with multiple sinks.
    - :class:`EnvironmentSettings`
      Defines all the parameters used to initialize a :class:`TableEnvironment`.
    - :class:`TableConfig`
      A config to define the runtime behavior of the Table API.
      It is used together with :class:`pyflink.datastream.StreamExecutionEnvironment` to create
      :class:`StreamTableEnvironment`.

Classes to define user-defined functions:

    - :class:`ScalarFunction`
      Base interface for user-defined scalar function.
    - :class:`TableFunction`
      Base interface for user-defined table function.
    - :class:`AggregateFunction`
      Base interface for user-defined aggregate function.
    - :class:`TableAggregateFunction`
      Base interface for user-defined table aggregate function.
    - :class:`FunctionContext`
      Used to obtain global runtime information about the context in which the
      user-defined function is executed, such as the metric group, and global job parameters, etc.

Classes to define window:

    - :class:`window.GroupWindow`
      Group windows group rows based on time or row-count intervals. See :class:`window.Tumble`,
      :class:`window.Session` and :class:`window.Slide` for more details on how to create a tumble
      window, session window, hop window separately.
    - :class:`window.OverWindow`
      Over window aggregates compute an aggregate for each input row over a range
      of its neighboring rows. See :class:`window.Over` for more details on how to create an over
      window.

Classes for catalog:

    - :class:`catalog.Catalog`
      Responsible for reading and writing metadata such as database/table/views/UDFs
      from and to a catalog.
    - :class:`catalog.HiveCatalog`
      Responsible for reading and writing metadata stored in Hive.

Classes to define source & sink:

    It's recommended to use SQL DDL statements together with :func:`TableEnvironment.execute_sql`
    to define source & sink.

Classes for module:

    - :class:`Module`
      Defines a set of metadata, including functions, user defined types, operators, rules,
      etc. Metadata from modules are regarded as built-in or system metadata that users can take
      advantages of.
    - :class:`module.HiveModule`
      Implementation of :class:`Module` to provide Hive built-in metadata.

Other important classes:

    - :class:`DataTypes`
      Defines a list of data types available in Table API.
    - :class:`Expression`
      Represents a logical tree for producing a computation result for a column in a :class:`Table`.
      Might be literal values, function calls, or field references.
    - :class:`TableSchema`
      Represents a table's structure with field names and data types.
    - :class:`SqlDialect`
      Enumeration of valid SQL compatibility modes.
    - :class:`ChangelogMode`
      The set of changes contained in a changelog.
    - :class:`ExplainDetail`
      Defines the types of details for explain result.

"""
from __future__ import absolute_import

from pyflink.table.data_view import DataView, ListView, MapView
from pyflink.table.environment_settings import EnvironmentSettings
from pyflink.table.explain_detail import ExplainDetail
from pyflink.table.expression import Expression
from pyflink.table.module import Module, ModuleEntry
from pyflink.table.result_kind import ResultKind
from pyflink.table.sinks import CsvTableSink, TableSink, WriteMode
from pyflink.table.sources import CsvTableSource, TableSource
from pyflink.table.sql_dialect import SqlDialect
from pyflink.table.statement_set import StatementSet
from pyflink.table.table import GroupWindowedTable, GroupedTable, OverWindowedTable, Table, \
    WindowGroupedTable
from pyflink.table.table_config import TableConfig
from pyflink.table.table_environment import (TableEnvironment, StreamTableEnvironment,
                                             BatchTableEnvironment)
from pyflink.table.table_result import TableResult
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import DataTypes, UserDefinedType, Row, RowKind
from pyflink.table.udf import FunctionContext, ScalarFunction, TableFunction, AggregateFunction, \
    TableAggregateFunction

__all__ = [
    'TableEnvironment',
    'BatchTableEnvironment',
    'StreamTableEnvironment',
    'Table',
    'StatementSet',
    'EnvironmentSettings',
    'TableConfig',
    'GroupedTable',
    'GroupWindowedTable',
    'OverWindowedTable',
    'WindowGroupedTable',
    'ScalarFunction',
    'TableFunction',
    'AggregateFunction',
    'TableAggregateFunction',
    'FunctionContext',
    'DataView',
    'ListView',
    'MapView',
    'Module',
    'ModuleEntry',
    'SqlDialect',
    'DataTypes',
    'UserDefinedType',
    'Expression',
    'TableSchema',
    'TableResult',
    'Row',
    'RowKind',
    'ExplainDetail',
    'TableSource',
    'TableSink',
    'CsvTableSource',
    'CsvTableSink',
    'WriteMode',
    'ResultKind'
]
