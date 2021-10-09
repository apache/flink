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
import sys
from abc import ABCMeta
from collections import OrderedDict

from py4j.java_gateway import get_method
from typing import Dict, Union

from pyflink.java_gateway import get_gateway
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import _to_java_type, DataType

__all__ = [
    'Rowtime',
    'Schema'
]


class Descriptor(object, metaclass=ABCMeta):
    """
    Base class of the descriptors that adds a set of string-based, normalized properties for
    describing DDL information.

    Typical characteristics of a descriptor are:
    - descriptors have a default constructor
    - descriptors themselves contain very little logic
    - corresponding validators validate the correctness (goal: have a single point of validation)

    A descriptor is similar to a builder in a builder pattern, thus, mutable for building
    properties.
    """

    def __init__(self, j_descriptor):
        self._j_descriptor = j_descriptor

    def to_properties(self) -> Dict:
        """
        Converts this descriptor into a dict of properties.

        :return: Dict object contains all of current properties.
        """
        return dict(self._j_descriptor.toProperties())


class Rowtime(Descriptor):
    """
    Rowtime descriptor for describing an event time attribute in the schema.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_rowtime = gateway.jvm.Rowtime()
        super(Rowtime, self).__init__(self._j_rowtime)

    def timestamps_from_field(self, field_name: str):
        """
        Sets a built-in timestamp extractor that converts an existing LONG or TIMESTAMP field into
        the rowtime attribute.

        :param field_name: The field to convert into a rowtime attribute.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.timestampsFromField(field_name)
        return self

    def timestamps_from_source(self) -> 'Rowtime':
        """
        Sets a built-in timestamp extractor that converts the assigned timestamps from a DataStream
        API record into the rowtime attribute and thus preserves the assigned timestamps from the
        source.

        .. note::

            This extractor only works in streaming environments.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.timestampsFromSource()
        return self

    def timestamps_from_extractor(self, extractor: str) -> 'Rowtime':
        """
        Sets a custom timestamp extractor to be used for the rowtime attribute.

        :param extractor: The java fully-qualified class name of the TimestampExtractor to extract
                          the rowtime attribute from the physical type. The TimestampExtractor must
                          have a public no-argument constructor and can be founded by
                          in current Java classloader.
        :return: This rowtime descriptor.
        """
        gateway = get_gateway()
        self._j_rowtime = self._j_rowtime.timestampsFromExtractor(
            gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(extractor)
                   .newInstance())
        return self

    def watermarks_periodic_ascending(self) -> 'Rowtime':
        """
        Sets a built-in watermark strategy for ascending rowtime attributes.

        Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a
        timestamp equal to the max timestamp are not late.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicAscending()
        return self

    def watermarks_periodic_bounded(self, delay: int) -> 'Rowtime':
        """
        Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a
        bounded time interval.

        Emits watermarks which are the maximum observed timestamp minus the specified delay.

        :param delay: Delay in milliseconds.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicBounded(delay)
        return self

    def watermarks_from_source(self) -> 'Rowtime':
        """
        Sets a built-in watermark strategy which indicates the watermarks should be preserved from
        the underlying DataStream API and thus preserves the assigned watermarks from the source.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksFromSource()
        return self

    def watermarks_from_strategy(self, strategy: str) -> 'Rowtime':
        """
        Sets a custom watermark strategy to be used for the rowtime attribute.

        :param strategy: The java fully-qualified class name of the WatermarkStrategy. The
                         WatermarkStrategy must have a public no-argument constructor and can be
                         founded by in current Java classloader.
        :return: This rowtime descriptor.
        """
        gateway = get_gateway()
        self._j_rowtime = self._j_rowtime.watermarksFromStrategy(
            gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(strategy)
                   .newInstance())
        return self


class Schema(Descriptor):
    """
    Describes a schema of a table.

    .. note::

        Field names are matched by the exact name by default (case sensitive).
    """

    def __init__(self, schema=None, fields=None, rowtime=None):
        """
        Constructor of Schema descriptor.

        :param schema: The :class:`TableSchema` object.
        :param fields: Dict of fields with the field name and the data type or type string stored.
        :param rowtime: A :class:`RowTime` that Specifies the previously defined field as an
                        event-time attribute.
        """
        gateway = get_gateway()
        self._j_schema = gateway.jvm.org.apache.flink.table.descriptors.Schema()
        super(Schema, self).__init__(self._j_schema)

        if schema is not None:
            self.schema(schema)

        if fields is not None:
            self.fields(fields)

        if rowtime is not None:
            self.rowtime(rowtime)

    def schema(self, table_schema: 'TableSchema') -> 'Schema':
        """
        Sets the schema with field names and the types. Required.

        This method overwrites existing fields added with
        :func:`~pyflink.table.descriptors.Schema.field`.

        :param table_schema: The :class:`TableSchema` object.
        :return: This schema object.
        """
        self._j_schema = self._j_schema.schema(table_schema._j_table_schema)
        return self

    def field(self, field_name: str, field_type: Union[DataType, str]) -> 'Schema':
        """
        Adds a field with the field name and the data type or type string. Required.
        This method can be called multiple times. The call order of this method defines
        also the order of the fields in a row. Here is a document that introduces the type strings:
        https://nightlies.apache.org/flink/flink-docs-stable/dev/table/connect.html#type-strings

        :param field_name: The field name.
        :param field_type: The data type or type string of the field.
        :return: This schema object.
        """
        if isinstance(field_type, str):
            self._j_schema = self._j_schema.field(field_name, field_type)
        else:
            self._j_schema = self._j_schema.field(field_name, _to_java_type(field_type))
        return self

    def fields(self, fields: Dict[str, Union[DataType, str]]) -> 'Schema':
        """
        Adds a set of fields with the field name and the data type or type string stored in a
        list.

        :param fields: Dict of fields with the field name and the data type or type string
                       stored.
                       E.g, [('int_field', DataTypes.INT()), ('string_field', DataTypes.STRING())].
        :return: This schema object.

        .. versionadded:: 1.11.0
        """
        if sys.version_info[:2] <= (3, 5) and not isinstance(fields, OrderedDict):
            raise TypeError("Must use OrderedDict type in python3.5 or older version to key the "
                            "schema in insert order.")
        elif sys.version_info[:2] > (3, 5) and not isinstance(fields, (OrderedDict, dict)):
            raise TypeError("fields must be stored in a dict or OrderedDict")

        for field_name, field_type in fields.items():
            self.field(field_name=field_name, field_type=field_type)
        return self

    def from_origin_field(self, origin_field_name: str) -> 'Schema':
        """
        Specifies the origin of the previously defined field. The origin field is defined by a
        connector or format.

        E.g. field("myString", Types.STRING).from_origin_field("CSV_MY_STRING")

        .. note::

            Field names are matched by the exact name by default (case sensitive).

        :param origin_field_name: The origin field name.
        :return: This schema object.
        """
        self._j_schema = get_method(self._j_schema, "from")(origin_field_name)
        return self

    def proctime(self) -> 'Schema':
        """
        Specifies the previously defined field as a processing-time attribute.

        E.g. field("proctime", Types.SQL_TIMESTAMP_LTZ).proctime()

        :return: This schema object.
        """
        self._j_schema = self._j_schema.proctime()
        return self

    def rowtime(self, rowtime: Rowtime) -> 'Schema':
        """
        Specifies the previously defined field as an event-time attribute.

        E.g. field("rowtime", Types.SQL_TIMESTAMP).rowtime(...)

        :param rowtime: A :class:`RowTime`.
        :return: This schema object.
        """
        self._j_schema = self._j_schema.rowtime(rowtime._j_rowtime)
        return self
