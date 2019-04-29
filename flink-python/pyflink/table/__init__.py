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
    - :class:`pyflink.table.TableSource`
      Defines an external data source as a table.
    - :class:`pyflink.table.TableSink`
      Specifies how to emit a table to an external system or location.
"""
from pyflink.table.table import Table
from pyflink.table.table_config import TableConfig
from pyflink.table.table_environment import TableEnvironment, StreamTableEnvironment, BatchTableEnvironment
from pyflink.table.table_sink import TableSink, CsvTableSink
from pyflink.table.table_source import TableSource, CsvTableSource
from pyflink.table.types import DataTypes

__all__ = [
    'TableEnvironment',
    'StreamTableEnvironment',
    'BatchTableEnvironment',
    'Table',
    'TableConfig',
    'TableSink',
    'TableSource',
    'CsvTableSink',
    'CsvTableSource',
    'DataTypes'
]
