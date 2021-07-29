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
    'Schema',
    'OldCsv',
    'FileSystem',
    'Csv',
    'Avro',
    'Json',
    'CustomFormatDescriptor'
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
        https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connect.html#type-strings

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


class FormatDescriptor(Descriptor, metaclass=ABCMeta):
    """
    Describes the format of data.
    """

    def __init__(self, j_format_descriptor):
        self._j_format_descriptor = j_format_descriptor
        super(FormatDescriptor, self).__init__(self._j_format_descriptor)


class OldCsv(FormatDescriptor):
    """
    Format descriptor for comma-separated values (CSV).

    .. note::

        This descriptor describes Flink's non-standard CSV table source/sink. In the future, the
        descriptor will be replaced by a proper RFC-compliant version. Use the RFC-compliant `Csv`
        format in the dedicated `flink-formats/flink-csv` module instead when writing to Kafka. Use
        the old one for stream/batch filesystem operations for now.

    .. note::

        Deprecated: use the RFC-compliant `Csv` format instead when writing to Kafka.
    """

    def __init__(self, schema=None, field_delimiter=None, line_delimiter=None, quote_character=None,
                 comment_prefix=None, ignore_parse_errors=False, ignore_first_line=False):
        """
        Constructor of OldCsv format descriptor.

        :param schema: Data type from :class:`DataTypes` that describes the schema.
        :param field_delimiter: The field delimiter character.
        :param line_delimiter: The line delimiter.
        :param quote_character: The quote character.
        :param comment_prefix: The prefix to indicate comments.
        :param ignore_parse_errors: Skip records with parse error instead to fail. Throw an
                                    exception by default.
        :param ignore_first_line: Ignore the first line. Not skip the first line by default.
        """
        gateway = get_gateway()
        self._j_csv = gateway.jvm.OldCsv()
        super(OldCsv, self).__init__(self._j_csv)

        if schema is not None:
            self.schema(schema)

        if field_delimiter is not None:
            self.field_delimiter(field_delimiter)

        if line_delimiter is not None:
            self.line_delimiter(line_delimiter)

        if quote_character is not None:
            self.quote_character(quote_character)

        if comment_prefix is not None:
            self.comment_prefix(comment_prefix)

        if ignore_parse_errors:
            self.ignore_parse_errors()

        if ignore_first_line:
            self.ignore_first_line()

    def field_delimiter(self, delimiter: str) -> 'OldCsv':
        """
        Sets the field delimiter, "," by default.

        :param delimiter: The field delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter: str) -> 'OldCsv':
        r"""
        Sets the line delimiter, "\\n" by default.

        :param delimiter: The line delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def schema(self, table_schema: 'TableSchema') -> 'OldCsv':
        """
        Sets the schema with field names and the types. Required.

        This method overwrites existing fields added with
        :func:`~pyflink.table.descriptors.OldCsv.field`.

        :param table_schema: The :class:`TableSchema` object.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.schema(table_schema._j_table_schema)
        return self

    def field(self, field_name: str, field_type: Union[DataType, str]) -> 'OldCsv':
        """
        Adds a format field with the field name and the data type or type string. Required.
        This method can be called multiple times. The call order of this method defines
        also the order of the fields in the format.

        :param field_name: The field name.
        :param field_type: The data type or type string of the field.
        :return: This :class:`OldCsv` object.
        """
        if isinstance(field_type, str):
            self._j_csv = self._j_csv.field(field_name, field_type)
        else:
            self._j_csv = self._j_csv.field(field_name, _to_java_type(field_type))
        return self

    def quote_character(self, quote_character: str) -> 'OldCsv':
        """
        Sets a quote character for String values, null by default.

        :param quote_character: The quote character.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def comment_prefix(self, prefix: str) -> 'OldCsv':
        """
        Sets a prefix to indicate comments, null by default.

        :param prefix: The prefix to indicate comments.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.commentPrefix(prefix)
        return self

    def ignore_parse_errors(self) -> 'OldCsv':
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def ignore_first_line(self) -> 'OldCsv':
        """
        Ignore the first line. Not skip the first line by default.

        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.ignoreFirstLine()
        return self


class Csv(FormatDescriptor):
    """
    Format descriptor for comma-separated values (CSV).

    This descriptor aims to comply with RFC-4180 ("Common Format and MIME Type for
    Comma-Separated Values (CSV) Files") proposed by the Internet Engineering Task Force (IETF).

    .. note::

        This descriptor does not describe Flink's old non-standard CSV table
        source/sink. Currently, this descriptor can be used when writing to Kafka. The old one is
        still available as :class:`OldCsv` for stream/batch filesystem operations.
    """

    def __init__(self, schema=None, field_delimiter=None, line_delimiter=None, quote_character=None,
                 allow_comments=False, ignore_parse_errors=False, array_element_delimiter=None,
                 escape_character=None, null_literal=None):
        """
        Constructor of Csv format descriptor.

        :param schema: Data type from :class:`DataTypes` that describes the schema.
        :param field_delimiter: The field delimiter character.
        :param line_delimiter: The line delimiter.
        :param quote_character: The quote character.
        :param allow_comments: Whether Ignores comment lines that start with '#' (disabled by
                               default).
        :param ignore_parse_errors: Whether Skip records with parse error instead to fail. Throw an
                                    exception by default.
        :param array_element_delimiter: The array element delimiter.
        :param escape_character: Escaping character (e.g. backslash).
        :param null_literal: The null literal string.
        """
        gateway = get_gateway()
        self._j_csv = gateway.jvm.Csv()
        super(Csv, self).__init__(self._j_csv)

        if schema is not None:
            self.schema(schema)

        if field_delimiter is not None:
            self.field_delimiter(field_delimiter)

        if line_delimiter is not None:
            self.line_delimiter(line_delimiter)

        if quote_character is not None:
            self.quote_character(quote_character)

        if allow_comments:
            self.allow_comments()

        if ignore_parse_errors:
            self.ignore_parse_errors()

        if array_element_delimiter is not None:
            self.array_element_delimiter(array_element_delimiter)

        if escape_character is not None:
            self.escape_character(escape_character)

        if null_literal is not None:
            self.null_literal(null_literal)

    def field_delimiter(self, delimiter: str) -> 'Csv':
        """
        Sets the field delimiter character (',' by default).

        :param delimiter: The field delimiter character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter: str) -> 'Csv':
        r"""
        Sets the line delimiter ("\\n" by default; otherwise "\\r" or "\\r\\n" are allowed).

        :param delimiter: The line delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def quote_character(self, quote_character: str) -> 'Csv':
        """
        Sets the field delimiter character (',' by default).

        :param quote_character: The quote character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(quote_character, str) or len(quote_character) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def allow_comments(self) -> 'Csv':
        """
        Ignores comment lines that start with '#' (disabled by default). If enabled, make sure to
        also ignore parse errors to allow empty rows.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.allowComments()
        return self

    def ignore_parse_errors(self) -> 'Csv':
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def array_element_delimiter(self, delimiter: str) -> 'Csv':
        """
        Sets the array element delimiter string for separating array or row element
        values (";" by default).

        :param delimiter: The array element delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.arrayElementDelimiter(delimiter)
        return self

    def escape_character(self, escape_character: str) -> 'Csv':
        """
        Sets the escape character for escaping values (disabled by default).

        :param escape_character: Escaping character (e.g. backslash).
        :return: This :class:`Csv` object.
        """
        if not isinstance(escape_character, str) or len(escape_character) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.escapeCharacter(escape_character)
        return self

    def null_literal(self, null_literal: str) -> 'Csv':
        """
        Sets the null literal string that is interpreted as a null value (disabled by default).

        :param null_literal: The null literal string.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.nullLiteral(null_literal)
        return self

    def schema(self, schema_data_type: DataType) -> 'Csv':
        """
        Sets the format schema with field names and the types. Required if schema is not derived.

        :param schema_data_type: Data type from :class:`DataTypes` that describes the schema.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self) -> 'Csv':
        """
        Derives the format schema from the table's schema. Required if no format schema is defined.

        This allows for defining schema information only once.

        The names, types, and fields' order of the format are determined by the table's
        schema. Time attributes are ignored if their origin is not a field. A "from" definition
        is interpreted as a field renaming in the format.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.deriveSchema()
        return self


class Avro(FormatDescriptor):
    """
    Format descriptor for Apache Avro records.
    """

    def __init__(self, record_class=None, avro_schema=None):
        """
        Constructor of Avro format descriptor.

        :param record_class: The java fully-qualified class name of the Avro record.
        :param avro_schema: Avro schema string.
        """
        gateway = get_gateway()
        self._j_avro = gateway.jvm.Avro()
        super(Avro, self).__init__(self._j_avro)

        if record_class is not None:
            self.record_class(record_class)

        if avro_schema is not None:
            self.avro_schema(avro_schema)

    def record_class(self, record_class: str) -> 'Avro':
        """
        Sets the class of the Avro specific record.

        :param record_class: The java fully-qualified class name of the Avro record.
        :return: This object.
        """
        gateway = get_gateway()
        clz = gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(record_class)
        self._j_avro = self._j_avro.recordClass(clz)
        return self

    def avro_schema(self, avro_schema: str) -> 'Avro':
        """
        Sets the Avro schema for specific or generic Avro records.

        :param avro_schema: Avro schema string.
        :return: This object.
        """
        self._j_avro = self._j_avro.avroSchema(avro_schema)
        return self


class Json(FormatDescriptor):
    """
    Format descriptor for JSON.
    """

    def __init__(self, json_schema=None, schema=None, derive_schema=False):
        """
        Constructor of Json format descriptor.

        :param json_schema: The JSON schema string.
        :param schema: Sets the schema using :class:`DataTypes` that describes the schema.
        :param derive_schema: Derives the format schema from the table's schema described.
        """
        gateway = get_gateway()
        self._j_json = gateway.jvm.Json()
        super(Json, self).__init__(self._j_json)

        if json_schema is not None:
            self.json_schema(json_schema)

        if schema is not None:
            self.schema(schema)

        if derive_schema:
            self.derive_schema()

    def fail_on_missing_field(self, fail_on_missing_field: bool) -> 'Json':
        """
        Sets flag whether to fail if a field is missing or not.

        :param fail_on_missing_field: If set to ``True``, the operation fails if there is a missing
                                      field.
                                      If set to ``False``, a missing field is set to null.
        :return: This object.
        """
        if not isinstance(fail_on_missing_field, bool):
            raise TypeError("Only bool value is supported!")
        self._j_json = self._j_json.failOnMissingField(fail_on_missing_field)
        return self

    def ignore_parse_errors(self, ignore_parse_errors: bool) -> 'Json':
        """
        Sets flag whether to fail when parsing json fails.

        :param ignore_parse_errors: If set to true, the operation will ignore parse errors.
                                    If set to false, the operation fails when parsing json fails.
        :return: This object.
        """
        if not isinstance(ignore_parse_errors, bool):
            raise TypeError("Only bool value is supported!")
        self._j_json = self._j_json.ignoreParseErrors(ignore_parse_errors)
        return self

    def json_schema(self, json_schema: str) -> 'Json':
        """
        Sets the JSON schema string with field names and the types according to the JSON schema
        specification: http://json-schema.org/specification.html

        The schema might be nested.

        :param json_schema: The JSON schema string.
        :return: This object.
        """
        self._j_json = self._j_json.jsonSchema(json_schema)
        return self

    def schema(self, schema_data_type: DataType) -> 'Json':
        """
        Sets the schema using :class:`DataTypes`.

        JSON objects are represented as ROW types.

        The schema might be nested.

        :param schema_data_type: Data type that describes the schema.
        :return: This object.
        """
        self._j_json = self._j_json.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self) -> 'Json':
        """
        Derives the format schema from the table's schema described.

        This allows for defining schema information only once.

        The names, types, and fields' order of the format are determined by the table's
        schema. Time attributes are ignored if their origin is not a field. A "from" definition
        is interpreted as a field renaming in the format.

        :return: This object.
        """
        self._j_json = self._j_json.deriveSchema()
        return self


class CustomFormatDescriptor(FormatDescriptor):
    """
    Describes the custom format of data.
    """

    def __init__(self, type, version):
        """
        Constructs a :class:`CustomFormatDescriptor`.

        :param type: String that identifies this format.
        :param version: Property version for backwards compatibility.
        """

        if not isinstance(type, str):
            raise TypeError("type must be of type str.")
        if not isinstance(version, int):
            raise TypeError("version must be of type int.")
        gateway = get_gateway()
        super(CustomFormatDescriptor, self).__init__(
            gateway.jvm.CustomFormatDescriptor(type, version))

    def property(self, key: str, value: str) -> 'CustomFormatDescriptor':
        """
        Adds a configuration property for the format.

        :param key: The property key to be set.
        :param value: The property value to be set.
        :return: This object.
        """

        if not isinstance(key, str):
            raise TypeError("key must be of type str.")
        if not isinstance(value, str):
            raise TypeError("value must be of type str.")
        self._j_format_descriptor = self._j_format_descriptor.property(key, value)
        return self

    def properties(self, property_dict: Dict[str, str]) -> 'CustomFormatDescriptor':
        """
        Adds a set of properties for the format.

        :param property_dict: The dict object contains configuration properties for the format.
                              Both the keys and values should be strings.
        :return: This object.
        """

        if not isinstance(property_dict, dict):
            raise TypeError("property_dict must be of type dict.")
        self._j_format_descriptor = self._j_format_descriptor.properties(property_dict)
        return self


class ConnectorDescriptor(Descriptor, metaclass=ABCMeta):
    """
    Describes a connector to an other system.
    """

    def __init__(self, j_connector_descriptor):
        self._j_connector_descriptor = j_connector_descriptor
        super(ConnectorDescriptor, self).__init__(self._j_connector_descriptor)


class FileSystem(ConnectorDescriptor):
    """
    Connector descriptor for a file system.
    """

    def __init__(self, path=None):
        """
        Constructor of FileSystem descriptor.

        :param path: The path of a file or directory.
        """
        gateway = get_gateway()
        self._j_file_system = gateway.jvm.FileSystem()
        super(FileSystem, self).__init__(self._j_file_system)

        if path is not None:
            self.path(path)

    def path(self, path_str: str) -> 'FileSystem':
        """
        Sets the path to a file or directory in a file system.

        :param path_str: The path of a file or directory.
        :return: This :class:`FileSystem` object.
        """
        self._j_file_system = self._j_file_system.path(path_str)
        return self
