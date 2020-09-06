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
from pyflink.table.types import _to_java_type, DataType
from pyflink.util import utils

__all__ = ['TableSink', 'CsvTableSink', 'WriteMode']


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

    Example:
    ::

        >>> CsvTableSink(["a", "b"], [DataTypes.INT(), DataTypes.STRING()],
        ...              "/csv/file/path", "|", 1, WriteMode.OVERWRITE)

    :param field_names: The list of field names.
    :param field_types: The list of field data types.
    :param path: The output path to write the Table to.
    :param field_delimiter: The field delimiter.
    :param num_files: The number of files to write to.
    :param write_mode: The write mode to specify whether existing files are overwritten or not,
                       which contains: :data:`WriteMode.NO_OVERWRITE`
                       and :data:`WriteMode.OVERWRITE`.
    """

    def __init__(self, field_names, field_types, path, field_delimiter=',', num_files=-1,
                 write_mode=None):
        # type: (list[str], list[DataType], str, str, int, int) -> None
        gateway = get_gateway()
        if write_mode == WriteMode.NO_OVERWRITE:
            j_write_mode = gateway.jvm.org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE
        elif write_mode == WriteMode.OVERWRITE:
            j_write_mode = gateway.jvm.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
        elif write_mode is None:
            j_write_mode = None
        else:
            raise Exception('Unsupported write_mode: %s' % write_mode)
        j_csv_table_sink = gateway.jvm.CsvTableSink(
            path, field_delimiter, num_files, j_write_mode)
        j_field_names = utils.to_jarray(gateway.jvm.String, field_names)
        j_field_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(field_type) for field_type in field_types])
        j_csv_table_sink = j_csv_table_sink.configure(j_field_names, j_field_types)
        super(CsvTableSink, self).__init__(j_csv_table_sink)
