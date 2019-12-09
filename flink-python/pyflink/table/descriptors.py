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
import warnings
from abc import ABCMeta

from py4j.java_gateway import get_method
from pyflink.table.types import _to_java_type

from pyflink.java_gateway import get_gateway

__all__ = [
    'Rowtime',
    'Schema',
    'OldCsv',
    'FileSystem',
    'Kafka',
    'Elasticsearch',
    'Csv',
    'Avro',
    'Json',
    'FileSystem',
    'ConnectTableDescriptor',
    'StreamTableDescriptor',
    'BatchTableDescriptor',
    'CustomConnectorDescriptor',
    'CustomFormatDescriptor'
]


class Descriptor(object):
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

    __metaclass__ = ABCMeta

    def __init__(self, j_descriptor):
        self._j_descriptor = j_descriptor

    def to_properties(self):
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

    def timestamps_from_field(self, field_name):
        """
        Sets a built-in timestamp extractor that converts an existing LONG or TIMESTAMP field into
        the rowtime attribute.

        :param field_name: The field to convert into a rowtime attribute.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.timestampsFromField(field_name)
        return self

    def timestamps_from_source(self):
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

    def timestamps_from_extractor(self, extractor):
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

    def watermarks_periodic_ascending(self):
        """
        Sets a built-in watermark strategy for ascending rowtime attributes.

        Emits a watermark of the maximum observed timestamp so far minus 1. Rows that have a
        timestamp equal to the max timestamp are not late.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicAscending()
        return self

    def watermarks_periodic_bounded(self, delay):
        """
        Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a
        bounded time interval.

        Emits watermarks which are the maximum observed timestamp minus the specified delay.

        :param delay: Delay in milliseconds.
        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksPeriodicBounded(delay)
        return self

    def watermarks_from_source(self):
        """
        Sets a built-in watermark strategy which indicates the watermarks should be preserved from
        the underlying DataStream API and thus preserves the assigned watermarks from the source.

        :return: This rowtime descriptor.
        """
        self._j_rowtime = self._j_rowtime.watermarksFromSource()
        return self

    def watermarks_from_strategy(self, strategy):
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

    def __init__(self):
        gateway = get_gateway()
        self._j_schema = gateway.jvm.Schema()
        super(Schema, self).__init__(self._j_schema)

    def schema(self, table_schema):
        """
        Sets the schema with field names and the types. Required.

        This method overwrites existing fields added with
        :func:`~pyflink.table.descriptors.Schema.field`.

        :param table_schema: The :class:`TableSchema` object.
        :return: This schema object.
        """
        self._j_schema = self._j_schema.schema(table_schema._j_table_schema)
        return self

    def field(self, field_name, field_type):
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

    def from_origin_field(self, origin_field_name):
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

    def proctime(self):
        """
        Specifies the previously defined field as a processing-time attribute.

        E.g. field("proctime", Types.SQL_TIMESTAMP).proctime()

        :return: This schema object.
        """
        self._j_schema = self._j_schema.proctime()
        return self

    def rowtime(self, rowtime):
        """
        Specifies the previously defined field as an event-time attribute.

        E.g. field("rowtime", Types.SQL_TIMESTAMP).rowtime(...)

        :param rowtime: A :class:`RowTime`.
        :return: This schema object.
        """
        self._j_schema = self._j_schema.rowtime(rowtime._j_rowtime)
        return self


class FormatDescriptor(Descriptor):
    """
    Describes the format of data.
    """

    __metaclass__ = ABCMeta

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

    def __init__(self):
        gateway = get_gateway()
        self._j_csv = gateway.jvm.OldCsv()
        super(OldCsv, self).__init__(self._j_csv)

    def field_delimiter(self, delimiter):
        """
        Sets the field delimiter, "," by default.

        :param delimiter: The field delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter):
        r"""
        Sets the line delimiter, "\\n" by default.

        :param delimiter: The line delimiter.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def schema(self, table_schema):
        """
        Sets the schema with field names and the types. Required.

        This method overwrites existing fields added with
        :func:`~pyflink.table.descriptors.OldCsv.field`.

        :param table_schema: The :class:`TableSchema` object.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.schema(table_schema._j_table_schema)
        return self

    def field(self, field_name, field_type):
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

    def quote_character(self, quote_character):
        """
        Sets a quote character for String values, null by default.

        :param quote_character: The quote character.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def comment_prefix(self, prefix):
        """
        Sets a prefix to indicate comments, null by default.

        :param prefix: The prefix to indicate comments.
        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.commentPrefix(prefix)
        return self

    def ignore_parse_errors(self):
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`OldCsv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def ignore_first_line(self):
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

    def __init__(self):
        gateway = get_gateway()
        self._j_csv = gateway.jvm.Csv()
        super(Csv, self).__init__(self._j_csv)

    def field_delimiter(self, delimiter):
        """
        Sets the field delimiter character (',' by default).

        :param delimiter: The field delimiter character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.fieldDelimiter(delimiter)
        return self

    def line_delimiter(self, delimiter):
        r"""
        Sets the line delimiter ("\\n" by default; otherwise "\\r" or "\\r\\n" are allowed).

        :param delimiter: The line delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.lineDelimiter(delimiter)
        return self

    def quote_character(self, quote_character):
        """
        Sets the field delimiter character (',' by default).

        :param quote_character: The quote character.
        :return: This :class:`Csv` object.
        """
        if not isinstance(quote_character, str) or len(quote_character) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.quoteCharacter(quote_character)
        return self

    def allow_comments(self):
        """
        Ignores comment lines that start with '#' (disabled by default). If enabled, make sure to
        also ignore parse errors to allow empty rows.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.allowComments()
        return self

    def ignore_parse_errors(self):
        """
        Skip records with parse error instead to fail. Throw an exception by default.

        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.ignoreParseErrors()
        return self

    def array_element_delimiter(self, delimiter):
        """
        Sets the array element delimiter string for separating array or row element
        values (";" by default).

        :param delimiter: The array element delimiter.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.arrayElementDelimiter(delimiter)
        return self

    def escape_character(self, escape_character):
        """
        Sets the escape character for escaping values (disabled by default).

        :param escape_character: Escaping character (e.g. backslash).
        :return: This :class:`Csv` object.
        """
        if not isinstance(escape_character, str) or len(escape_character) != 1:
            raise TypeError("Only one-character string is supported!")
        self._j_csv = self._j_csv.escapeCharacter(escape_character)
        return self

    def null_literal(self, null_literal):
        """
        Sets the null literal string that is interpreted as a null value (disabled by default).

        :param null_literal:
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.nullLiteral(null_literal)
        return self

    def schema(self, schema_data_type):
        """
        Sets the format schema with field names and the types. Required if schema is not derived.

        :param schema_data_type: Data type from :class:`DataTypes` that describes the schema.
        :return: This :class:`Csv` object.
        """
        self._j_csv = self._j_csv.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self):
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

    def __init__(self):
        gateway = get_gateway()
        self._j_avro = gateway.jvm.Avro()
        super(Avro, self).__init__(self._j_avro)

    def record_class(self, record_class):
        """
        Sets the class of the Avro specific record.

        :param record_class: The java fully-qualified class name of the Avro record.
        :return: This object.
        """
        gateway = get_gateway()
        clz = gateway.jvm.Thread.currentThread().getContextClassLoader().loadClass(record_class)
        self._j_avro = self._j_avro.recordClass(clz)
        return self

    def avro_schema(self, avro_schema):
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

    def __init__(self):
        gateway = get_gateway()
        self._j_json = gateway.jvm.Json()
        super(Json, self).__init__(self._j_json)

    def fail_on_missing_field(self, fail_on_missing_field):
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

    def json_schema(self, json_schema):
        """
        Sets the JSON schema string with field names and the types according to the JSON schema
        specification: http://json-schema.org/specification.html

        The schema might be nested.

        :param json_schema: The JSON schema string.
        :return: This object.
        """
        self._j_json = self._j_json.jsonSchema(json_schema)
        return self

    def schema(self, schema_data_type):
        """
        Sets the schema using :class:`DataTypes`.

        JSON objects are represented as ROW types.

        The schema might be nested.

        :param schema_data_type: Data type that describes the schema.
        :return: This object.
        """
        self._j_json = self._j_json.schema(_to_java_type(schema_data_type))
        return self

    def derive_schema(self):
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

    def property(self, key, value):
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

    def properties(self, property_dict):
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


class ConnectorDescriptor(Descriptor):
    """
    Describes a connector to an other system.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_connector_descriptor):
        self._j_connector_descriptor = j_connector_descriptor
        super(ConnectorDescriptor, self).__init__(self._j_connector_descriptor)


class FileSystem(ConnectorDescriptor):
    """
    Connector descriptor for a file system.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_file_system = gateway.jvm.FileSystem()
        super(FileSystem, self).__init__(self._j_file_system)

    def path(self, path_str):
        """
        Sets the path to a file or directory in a file system.

        :param path_str: The path of a file or directory.
        :return: This :class:`FileSystem` object.
        """
        self._j_file_system = self._j_file_system.path(path_str)
        return self


class Kafka(ConnectorDescriptor):
    """
    Connector descriptor for the Apache Kafka message queue.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_kafka = gateway.jvm.Kafka()
        super(Kafka, self).__init__(self._j_kafka)

    def version(self, version):
        """
        Sets the Kafka version to be used.

        :param version: Kafka version. E.g., "0.8", "0.11", etc.
        :return: This object.
        """
        if not isinstance(version, str):
            version = str(version)
        self._j_kafka = self._j_kafka.version(version)
        return self

    def topic(self, topic):
        """
        Sets the topic from which the table is read.

        :param topic: The topic from which the table is read.
        :return: This object.
        """
        self._j_kafka = self._j_kafka.topic(topic)
        return self

    def properties(self, property_dict):
        """
        Sets the configuration properties for the Kafka consumer. Resets previously set properties.

        :param property_dict: The dict object contains configuration properties for the Kafka
                              consumer. Both the keys and values should be strings.
        :return: This object.
        """
        gateway = get_gateway()
        properties = gateway.jvm.java.util.Properties()
        for key in property_dict:
            properties.setProperty(key, property_dict[key])
        self._j_kafka = self._j_kafka.properties(properties)
        return self

    def property(self, key, value):
        """
        Adds a configuration properties for the Kafka consumer.

        :param key: Property key string for the Kafka consumer.
        :param value: Property value string for the Kafka consumer.
        :return: This object.
        """
        self._j_kafka = self._j_kafka.property(key, value)
        return self

    def start_from_earliest(self):
        """
        Specifies the consumer to start reading from the earliest offset for all partitions.
        This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.

        :return: This object.
        """
        self._j_kafka = self._j_kafka.startFromEarliest()
        return self

    def start_from_latest(self):
        """
        Specifies the consumer to start reading from the latest offset for all partitions.
        This lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.

        :return: This object.
        """
        self._j_kafka = self._j_kafka.startFromLatest()
        return self

    def start_from_group_offsets(self):
        """
        Specifies the consumer to start reading from any committed group offsets found
        in Zookeeper / Kafka brokers. The "group.id" property must be set in the configuration
        properties. If no offset can be found for a partition, the behaviour in "auto.offset.reset"
        set in the configuration properties will be used for the partition.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.

        :return: This object.
        """
        self._j_kafka = self._j_kafka.startFromGroupOffsets()
        return self

    def start_from_specific_offsets(self, specific_offsets_dict):
        """
        Specifies the consumer to start reading partitions from specific offsets, set independently
        for each partition. The specified offset should be the offset of the next record that will
        be read from partitions. This lets the consumer ignore any committed group offsets in
        Zookeeper / Kafka brokers.

        If the provided map of offsets contains entries whose partition is not subscribed by the
        consumer, the entry will be ignored. If the consumer subscribes to a partition that does
        not exist in the provided map of offsets, the consumer will fallback to the default group
        offset behaviour(see :func:`pyflink.table.descriptors.Kafka.start_from_group_offsets`)
        for that particular partition.

        If the specified offset for a partition is invalid, or the behaviour for that partition is
        defaulted to group offsets but still no group offset could be found for it, then the
        "auto.offset.reset" behaviour set in the configuration properties will be used for the
        partition.

        This method does not affect where partitions are read from when the consumer is restored
        from a checkpoint or savepoint. When the consumer is restored from a checkpoint or
        savepoint, only the offsets in the restored state will be used.

        :param specific_offsets_dict: Dict of specific_offsets that the key is int-type partition
                                      id and value is int-type offset value.
        :return: This object.
        """
        for key in specific_offsets_dict:
            self.start_from_specific_offset(key, specific_offsets_dict[key])
        return self

    def start_from_specific_offset(self, partition, specific_offset):
        """
        Configures to start reading partitions from specific offsets and specifies the given offset
        for the given partition.

        see :func:`pyflink.table.descriptors.Kafka.start_from_specific_offsets`

        :param partition: Partition id.
        :param specific_offset: Specified offset in given partition.
        :return: This object.
        """
        self._j_kafka = self._j_kafka.startFromSpecificOffset(int(partition), int(specific_offset))
        return self

    def sink_partitioner_fixed(self):
        """
        Configures how to partition records from Flink's partitions into Kafka's partitions.

        This strategy ensures that each Flink partition ends up in one Kafka partition.

        .. note::

            One Kafka partition can contain multiple Flink partitions. Examples:

            More Flink partitions than Kafka partitions. Some (or all) Kafka partitions contain
            the output of more than one flink partition:

            |    Flink Sinks --------- Kafka Partitions
            |        1    ---------------->    1
            |        2    --------------/
            |        3    -------------/
            |        4    ------------/

            Fewer Flink partitions than Kafka partitions:

            |    Flink Sinks --------- Kafka Partitions
            |        1    ---------------->    1
            |        2    ---------------->    2
            |             ................     3
            |             ................     4
            |             ................     5

        :return: This object.
        """
        self._j_kafka = self._j_kafka.sinkPartitionerFixed()
        return self

    def sink_partitioner_round_robin(self):
        """
        Configures how to partition records from Flink's partitions into Kafka's partitions.

        This strategy ensures that records will be distributed to Kafka partitions in a
        round-robin fashion.

        .. note::

            This strategy is useful to avoid an unbalanced partitioning. However, it will cause a
            lot of network connections between all the Flink instances and all the Kafka brokers.

        :return: This object.
        """
        self._j_kafka = self._j_kafka.sinkPartitionerRoundRobin()
        return self

    def sink_partitioner_custom(self, partitioner_class_name):
        """
        Configures how to partition records from Flink's partitions into Kafka's partitions.

        This strategy allows for a custom partitioner by providing an implementation
        of ``FlinkKafkaPartitioner``.

        :param partitioner_class_name: The java canonical class name of the FlinkKafkaPartitioner.
                                       The FlinkKafkaPartitioner must have a public no-argument
                                       constructor and can be founded by in current Java
                                       classloader.
        :return: This object.
        """
        gateway = get_gateway()
        self._j_kafka = self._j_kafka.sinkPartitionerCustom(
            gateway.jvm.Thread.currentThread().getContextClassLoader()
                   .loadClass(partitioner_class_name))
        return self


class Elasticsearch(ConnectorDescriptor):
    """
    Connector descriptor for the Elasticsearch search engine.
    """

    def __init__(self):
        gateway = get_gateway()
        self._j_elasticsearch = gateway.jvm.Elasticsearch()
        super(Elasticsearch, self).__init__(self._j_elasticsearch)

    def version(self, version):
        """
        Sets the Elasticsearch version to be used. Required.

        :param version: Elasticsearch version. E.g., "6".
        :return: This object.
        """
        if not isinstance(version, str):
            version = str(version)
        self._j_elasticsearch = self._j_elasticsearch.version(version)
        return self

    def host(self, hostname, port, protocol):
        """
        Adds an Elasticsearch host to connect to. Required.

        Multiple hosts can be declared by calling this method multiple times.

        :param hostname: Connection hostname.
        :param port: Connection port.
        :param protocol: Connection protocol; e.g. "http".
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.host(hostname, int(port), protocol)
        return self

    def index(self, index):
        """
        Declares the Elasticsearch index for every record. Required.

        :param index: Elasticsearch index.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.index(index)
        return self

    def document_type(self, document_type):
        """
        Declares the Elasticsearch document type for every record. Required.

        :param document_type: Elasticsearch document type.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.documentType(document_type)
        return self

    def key_delimiter(self, key_delimiter):
        """
        Sets a custom key delimiter in case the Elasticsearch ID needs to be constructed from
        multiple fields. Optional.

        :param key_delimiter: Key delimiter; e.g., "$" would result in IDs "KEY1$KEY2$KEY3".
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.keyDelimiter(key_delimiter)
        return self

    def key_null_literal(self, key_null_literal):
        """
        Sets a custom representation for null fields in keys. Optional.

        :param key_null_literal: key null literal string; e.g. "N/A" would result in IDs
                                 "KEY1_N/A_KEY3".
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.keyNullLiteral(key_null_literal)
        return self

    def failure_handler_fail(self):
        """
        Configures a failure handling strategy in case a request to Elasticsearch fails.

        This strategy throws an exception if a request fails and thus causes a job failure.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.failureHandlerFail()
        return self

    def failure_handler_ignore(self):
        """
        Configures a failure handling strategy in case a request to Elasticsearch fails.

        This strategy ignores failures and drops the request.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.failureHandlerIgnore()
        return self

    def failure_handler_retry_rejected(self):
        """
        Configures a failure handling strategy in case a request to Elasticsearch fails.

        This strategy re-adds requests that have failed due to queue capacity saturation.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.failureHandlerRetryRejected()
        return self

    def failure_handler_custom(self, failure_handler_class_name):
        """
        Configures a failure handling strategy in case a request to Elasticsearch fails.

        This strategy allows for custom failure handling using a ``ActionRequestFailureHandler``.

        :param failure_handler_class_name:
        :return: This object.
        """
        gateway = get_gateway()
        self._j_elasticsearch = self._j_elasticsearch.failureHandlerCustom(
            gateway.jvm.Thread.currentThread().getContextClassLoader()
                   .loadClass(failure_handler_class_name))
        return self

    def disable_flush_on_checkpoint(self):
        """
        Disables flushing on checkpoint. When disabled, a sink will not wait for all pending action
        requests to be acknowledged by Elasticsearch on checkpoints.

        .. note::

            If flushing on checkpoint is disabled, a Elasticsearch sink does NOT
            provide any strong guarantees for at-least-once delivery of action requests.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.disableFlushOnCheckpoint()
        return self

    def bulk_flush_max_actions(self, max_actions_num):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets the maximum number of actions to buffer for each bulk request.

        :param max_actions_num: the maximum number of actions to buffer per bulk request.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushMaxActions(int(max_actions_num))
        return self

    def bulk_flush_max_size(self, max_size):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets the maximum size of buffered actions per bulk request (using the syntax of
        MemorySize).

        :param max_size: The maximum size. E.g. "42 mb". only MB granularity is supported.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushMaxSize(max_size)
        return self

    def bulk_flush_interval(self, interval):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets the bulk flush interval (in milliseconds).

        :param interval: Bulk flush interval (in milliseconds).
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushInterval(int(interval))
        return self

    def bulk_flush_backoff_constant(self):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets a constant backoff type to use when flushing bulk requests.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushBackoffConstant()
        return self

    def bulk_flush_backoff_exponential(self):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets an exponential backoff type to use when flushing bulk requests.

        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushBackoffExponential()
        return self

    def bulk_flush_backoff_max_retries(self, max_retries):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets the maximum number of retries for a backoff attempt when flushing bulk requests.

        Make sure to enable backoff by selecting a strategy (
        :func:`pyflink.table.descriptors.Elasticsearch.bulk_flush_backoff_constant` or
        :func:`pyflink.table.descriptors.Elasticsearch.bulk_flush_backoff_exponential`).

        :param max_retries: The maximum number of retries.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushBackoffMaxRetries(int(max_retries))
        return self

    def bulk_flush_backoff_delay(self, delay):
        """
        Configures how to buffer elements before sending them in bulk to the cluster for
        efficiency.

        Sets the amount of delay between each backoff attempt when flushing bulk requests
        (in milliseconds).

        Make sure to enable backoff by selecting a strategy (
        :func:`pyflink.table.descriptors.Elasticsearch.bulk_flush_backoff_constant` or
        :func:`pyflink.table.descriptors.Elasticsearch.bulk_flush_backoff_exponential`).

        :param delay: Delay between each backoff attempt (in milliseconds).
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.bulkFlushBackoffDelay(int(delay))
        return self

    def connection_max_retry_timeout(self, max_retry_timeout):
        """
        Sets connection properties to be used during REST communication to Elasticsearch.

        Sets the maximum timeout (in milliseconds) in case of multiple retries of the same request.

        :param max_retry_timeout: Maximum timeout (in milliseconds).
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.connectionMaxRetryTimeout(
            int(max_retry_timeout))
        return self

    def connection_path_prefix(self, path_prefix):
        """
        Sets connection properties to be used during REST communication to Elasticsearch.

        Adds a path prefix to every REST communication.

        :param path_prefix: Prefix string to be added to every REST communication.
        :return: This object.
        """
        self._j_elasticsearch = self._j_elasticsearch.connectionPathPrefix(path_prefix)
        return self


class CustomConnectorDescriptor(ConnectorDescriptor):
    """
    Describes a custom connector to an other system.
    """

    def __init__(self, type, version, format_needed):
        """
        Constructs a :class:`CustomConnectorDescriptor`.

        :param type: String that identifies this connector.
        :param version: Property version for backwards compatibility.
        :param format_needed: Flag for basic validation of a needed format descriptor.
        """

        if not isinstance(type, str):
            raise TypeError("type must be of type str.")
        if not isinstance(version, int):
            raise TypeError("version must be of type int.")
        if not isinstance(format_needed, bool):
            raise TypeError("format_needed must be of type bool.")
        gateway = get_gateway()
        super(CustomConnectorDescriptor, self).__init__(
            gateway.jvm.CustomConnectorDescriptor(type, version, format_needed))

    def property(self, key, value):
        """
        Adds a configuration property for the connector.

        :param key: The property key to be set.
        :param value: The property value to be set.
        :return: This object.
        """

        if not isinstance(key, str):
            raise TypeError("key must be of type str.")
        if not isinstance(value, str):
            raise TypeError("value must be of type str.")
        self._j_connector_descriptor = self._j_connector_descriptor.property(key, value)
        return self

    def properties(self, property_dict):
        """
        Adds a set of properties for the connector.

        :param property_dict: The dict object contains configuration properties for the connector.
                              Both the keys and values should be strings.
        :return: This object.
        """

        if not isinstance(property_dict, dict):
            raise TypeError("property_dict must be of type dict.")
        self._j_connector_descriptor = self._j_connector_descriptor.properties(property_dict)
        return self


class ConnectTableDescriptor(Descriptor):
    """
    Common class for table's created with :class:`pyflink.table.TableEnvironment.connect`.
    """

    __metaclass__ = ABCMeta

    def __init__(self, j_connect_table_descriptor):
        self._j_connect_table_descriptor = j_connect_table_descriptor
        super(ConnectTableDescriptor, self).__init__(self._j_connect_table_descriptor)

    def with_format(self, format_descriptor):
        """
        Specifies the format that defines how to read data from a connector.

        :type format_descriptor: The :class:`FormatDescriptor` for the resulting table,
                                 e.g. :class:`OldCsv`.
        :return: This object.
        """
        self._j_connect_table_descriptor = \
            self._j_connect_table_descriptor.withFormat(format_descriptor._j_format_descriptor)
        return self

    def with_schema(self, schema):
        """
        Specifies the resulting table schema.

        :type schema: The :class:`Schema` object for the resulting table.
        :return: This object.
        """
        self._j_connect_table_descriptor = \
            self._j_connect_table_descriptor.withSchema(schema._j_schema)
        return self

    def register_table_sink(self, name):
        """
        Searches for the specified table sink, configures it accordingly, and registers it as
        a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.

        .. note:: Deprecated in 1.10. Use :func:`create_temporary_table` instead.
        """
        warnings.warn("Deprecated in 1.10. Use create_temporary_table instead.", DeprecationWarning)
        self._j_connect_table_descriptor = self._j_connect_table_descriptor.registerTableSink(name)
        return self

    def register_table_source(self, name):
        """
        Searches for the specified table source, configures it accordingly, and registers it as
        a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.

        .. note:: Deprecated in 1.10. Use :func:`create_temporary_table` instead.
        """
        warnings.warn("Deprecated in 1.10. Use create_temporary_table instead.", DeprecationWarning)
        self._j_connect_table_descriptor = \
            self._j_connect_table_descriptor.registerTableSource(name)
        return self

    def register_table_source_and_sink(self, name):
        """
        Searches for the specified table source and sink, configures them accordingly, and
        registers them as a table under the given name.

        :param name: Table name to be registered in the table environment.
        :return: This object.

        .. note:: Deprecated in 1.10. Use :func:`create_temporary_table` instead.
        """
        warnings.warn("Deprecated in 1.10. Use create_temporary_table instead.", DeprecationWarning)
        self._j_connect_table_descriptor = \
            self._j_connect_table_descriptor.registerTableSourceAndSink(name)
        return self

    def create_temporary_table(self, path):
        """
        Registers the table described by underlying properties in a given path.

        There is no distinction between source and sink at the descriptor level anymore as this
        method does not perform actual class lookup. It only stores the underlying properties. The
        actual source/sink lookup is performed when the table is used.

        Temporary objects can shadow permanent ones. If a permanent object in a given path exists,
        it will be inaccessible in the current session. To make the permanent object available
        again you can drop the corresponding temporary object.

        .. note:: The schema must be explicitly defined.

        :param path: path where to register the temporary table
        """
        self._j_connect_table_descriptor.createTemporaryTable(path)
        return self


class StreamTableDescriptor(ConnectTableDescriptor):
    """
    Descriptor for specifying a table source and/or sink in a streaming environment.

    .. seealso:: parent class: :class:`ConnectTableDescriptor`
    """

    def __init__(self, j_stream_table_descriptor):
        self._j_stream_table_descriptor = j_stream_table_descriptor
        super(StreamTableDescriptor, self).__init__(self._j_stream_table_descriptor)

    def in_append_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In append mode, a dynamic table and an external connector only exchange INSERT messages.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inAppendMode()
        return self

    def in_retract_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In retract mode, a dynamic table and an external connector exchange ADD and RETRACT
        messages.

        An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an
        UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for
        the updating (new) row.

        In this mode, a key must not be defined as opposed to upsert mode. However, every update
        consists of two messages which is less efficient.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inRetractMode()
        return self

    def in_upsert_mode(self):
        """
        Declares how to perform the conversion between a dynamic table and an external connector.

        In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE
        messages.

        This mode requires a (possibly composite) unique key by which updates can be propagated. The
        external connector needs to be aware of the unique key attribute in order to apply messages
        correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as
        DELETE messages.

        The main difference to a retract stream is that UPDATE changes are encoded with a single
        message and are therefore more efficient.

        :return: This object.
        """
        self._j_stream_table_descriptor = self._j_stream_table_descriptor.inUpsertMode()
        return self


class BatchTableDescriptor(ConnectTableDescriptor):
    """
    Descriptor for specifying a table source and/or sink in a batch environment.

    .. seealso:: parent class: :class:`ConnectTableDescriptor`
    """

    def __init__(self, j_batch_table_descriptor):
        self._j_batch_table_descriptor = j_batch_table_descriptor
        super(BatchTableDescriptor, self).__init__(self._j_batch_table_descriptor)
