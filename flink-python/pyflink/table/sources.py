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
from pyflink.table.types import DataType, _to_java_type
from pyflink.util import utils

__all__ = ['TableSource', 'CsvTableSource']


class TableSource(object):
    """
    Defines a table from an external system or location.
    """

    def __init__(self, j_table_source):
        self._j_table_source = j_table_source


class CsvTableSource(TableSource):
    """
    A :class:`TableSource` for simple CSV files with a
    (logically) unlimited number of fields.

    :param source_path: The path to the CSV file.
    :param field_names: The names of the table fields.
    :param field_types: The types of the table fields.
    """

    def __init__(self, source_path, field_names, field_types):
        # type: (str, list[str], list[DataType]) -> None
        gateway = get_gateway()
        j_field_names = utils.to_jarray(gateway.jvm.String, field_names)
        j_field_types = utils.to_jarray(gateway.jvm.TypeInformation,
                                        [_to_java_type(field_type)
                                         for field_type in field_types])
        super(CsvTableSource, self).__init__(
            gateway.jvm.CsvTableSource(source_path, j_field_names, j_field_types))
