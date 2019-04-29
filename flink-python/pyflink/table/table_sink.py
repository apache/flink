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

from pyflink.java_gateway import get_gateway

__all__ = ['TableSink', 'CsvTableSink']


class TableSink(object):
    """
    A :class:`TableSink` specifies how to emit a table to an external system or location.
    """

    def __init__(self, j_table_sink):
        self._j_table_sink = j_table_sink


class WriteMode(object):
    NO_OVERWRITE = 0
    OVERWRITE = 1


class CsvTableSink(TableSink):
    """
    A simple :class:`TableSink` to emit data as CSV files.

    :param path: The output path to write the Table to.
    :param field_delimiter: The field delimiter.
    :param num_files: The number of files to write to.
    :param write_mode: The write mode to specify whether existing files are overwritten or not.
    """

    def __init__(self, path, field_delimiter=',', num_files=1, write_mode=None):
        # type: (str, str, int, int) -> None
        gateway = get_gateway()
        if write_mode == WriteMode.NO_OVERWRITE:
            j_write_mode = gateway.jvm.scala.Option.apply(
                gateway.jvm.org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE)
        elif write_mode == WriteMode.OVERWRITE:
            j_write_mode = gateway.jvm.scala.Option.apply(
                gateway.jvm.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
        elif write_mode is None:
            j_write_mode = gateway.jvm.scala.Option.empty()
        else:
            raise Exception('Unsupported write_mode: %s' % write_mode)
        j_some_field_delimiter = gateway.jvm.scala.Option.apply(field_delimiter)
        j_some_num_files = gateway.jvm.scala.Option.apply(num_files)
        j_csv_table_sink = gateway.jvm.CsvTableSink(
            path, j_some_field_delimiter, j_some_num_files, j_write_mode)
        super(CsvTableSink, self).__init__(j_csv_table_sink)
