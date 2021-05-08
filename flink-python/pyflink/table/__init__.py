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
Important classes of Flink Table API:

    - :class:`pyflink.table.TableEnvironment`
      Main entry point for :class:`Table` and SQL functionality
    - :class:`pyflink.table.Table`
      The core component of the Table API.
      Use the methods of :class:`Table` to transform data.
    - :class:`pyflink.table.TableConfig`
      A config to define the runtime behavior of the Table API.
      It is necessary when creating :class:`TableEnvironment`.
    - :class:`pyflink.table.EnvironmentSettings`
      Defines all parameters that initialize a table environment.
    - :class:`pyflink.table.TableSource`
      Defines an external data source as a table.
    - :class:`pyflink.table.TableSink`
      Specifies how to emit a table to an external system or location.
    - :class:`pyflink.table.DataTypes`
      Defines a list of data types available.
    - :class:`pyflink.table.Row`
      A row in a :class:`Table`.
    - :class:`pyflink.table.Expression`
      A column expression in a :class:`Table`.
    - :class:`pyflink.table.window`
      Helper classes for working with :class:`pyflink.table.window.GroupWindow`
      (:class:`pyflink.table.window.Tumble`, :class:`pyflink.table.window.Session`,
      :class:`pyflink.table.window.Slide`) and :class:`pyflink.table.window.OverWindow` window
      (:class:`pyflink.table.window.Over`).
    - :class:`pyflink.table.descriptors`
      Helper classes that describes DDL information, such as how to connect to another system,
      the format of data, the schema of table, the event time attribute in the schema, etc.
    - :class:`pyflink.table.catalog`
      Responsible for reading and writing metadata such as database/table/views/UDFs
      from a registered :class:`pyflink.table.catalog.Catalog`.
    - :class:`pyflink.table.TableSchema`
      Represents a table's structure with field names and data types.
    - :class:`pyflink.table.FunctionContext`
      Used to obtain global runtime information about the context in which the
      user-defined function is executed, such as the metric group, and global job parameters, etc.
    - :class:`pyflink.table.ScalarFunction`
      Base interface for user-defined scalar function.
    - :class:`pyflink.table.TableFunction`
      Base interface for user-defined table function.
    - :class:`pyflink.table.AggregateFunction`
      Base interface for user-defined aggregate function.
    - :class:`pyflink.table.TableAggregateFunction`
      Base interface for user-defined table aggregate function.
    - :class:`pyflink.table.StatementSet`
      Base interface accepts DML statements or Tables.
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
    'AggregateFunction',
    'BatchTableEnvironment',
    'CsvTableSink',
    'CsvTableSource',
    'DataTypes',
    'DataView',
    'EnvironmentSettings',
    'ExplainDetail',
    'Expression',
    'FunctionContext',
    'GroupWindowedTable',
    'GroupedTable',
    'ListView',
    'MapView',
    'Module',
    'ModuleEntry',
    'OverWindowedTable',
    'ResultKind',
    'Row',
    'RowKind',
    'ScalarFunction',
    'SqlDialect',
    'StatementSet',
    'StreamTableEnvironment',
    'Table',
    'TableConfig',
    'TableEnvironment',
    'TableFunction',
    'TableResult',
    'TableSchema',
    'TableSink',
    'TableSource',
    'TableAggregateFunction',
    'UserDefinedType',
    'WindowGroupedTable',
    'WriteMode'
]
