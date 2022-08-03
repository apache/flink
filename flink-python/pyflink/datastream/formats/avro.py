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
from py4j.java_gateway import get_java_class, JavaObject
from pyflink.common.typeinfo import TypeInformation

from pyflink.datastream.connectors.file_system import InputFormat, BulkWriterFactory
from pyflink.datastream.utils import ResultTypeQueryable
from pyflink.java_gateway import get_gateway
from pyflink.util.java_utils import get_field_value


class AvroSchema(object):
    """
    Avro Schema class contains Java org.apache.avro.Schema.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_schema):
        self._j_schema = j_schema
        self._schema_string = None

    def __str__(self):
        if self._schema_string is None:
            self._schema_string = get_field_value(self._j_schema, 'schema').toString()
        return self._schema_string

    @staticmethod
    def parse_string(json_schema: str) -> 'AvroSchema':
        """
        Parse JSON string as Avro Schema.

        :param json_schema: JSON represented schema string.
        :return: the Avro Schema.
        """
        JSchema = get_gateway().jvm.org.apache.flink.avro.shaded.org.apache.avro.Schema
        return AvroSchema(JSchema.Parser().parse(json_schema))

    @staticmethod
    def parse_file(file_path: str) -> 'AvroSchema':
        """
        Parse a schema definition file as Avro Schema.

        :param file_path: path to schema definition file.
        :return: the Avro Schema.
        """
        jvm = get_gateway().jvm
        j_file = jvm.java.io.File(file_path)
        JSchema = jvm.org.apache.flink.avro.shaded.org.apache.avro.Schema
        return AvroSchema(JSchema.Parser().parse(j_file))


class GenericRecordAvroTypeInfo(TypeInformation):
    """
    A :class:`TypeInformation` of Avro's GenericRecord, including the schema. This is a wrapper of
    Java org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo.

    Note that this type cannot be used as the type_info of data in
    :meth:`StreamExecutionEnvironment.from_collection`.

    .. versionadded::1.16.0
    """

    def __init__(self, schema: 'AvroSchema'):
        super().__init__()
        self._schema = schema
        self._j_typeinfo = get_gateway().jvm.org.apache.flink.formats.avro.typeutils \
            .GenericRecordAvroTypeInfo(schema._j_schema)

    def get_java_type_info(self) -> JavaObject:
        return self._j_typeinfo


class AvroInputFormat(InputFormat, ResultTypeQueryable):
    """
    Provides a FileInputFormat for Avro records.

    Example:
    ::

        >>> env = StreamExecutionEnvironment.get_execution_environment()
        >>> schema = AvroSchema.parse_string(JSON_SCHEMA)
        >>> ds = env.create_input(AvroInputFormat(FILE_PATH, schema))

    .. versionadded:: 1.16.0
    """

    def __init__(self, path: str, schema: 'AvroSchema'):
        """
        :param path: The path to Avro data file.
        :param schema: The :class:`Schema` of generic record.
        """
        jvm = get_gateway().jvm
        j_avro_input_format = jvm.org.apache.flink.formats.avro.AvroInputFormat(
            jvm.org.apache.flink.core.fs.Path(path),
            get_java_class(jvm.org.apache.flink.avro.shaded.org.apache.avro.generic.GenericRecord)
        )
        super().__init__(j_avro_input_format)
        self._schema = schema
        self._type_info = GenericRecordAvroTypeInfo(schema)

    def get_produced_type(self) -> GenericRecordAvroTypeInfo:
        return self._type_info


class AvroWriters(object):
    """
    Convenience builder to create AvroWriterFactory instances.

    .. versionadded:: 1.16.0
    """

    @staticmethod
    def for_generic_record(schema: 'AvroSchema') -> 'BulkWriterFactory':
        """
        Creates an AvroWriterFactory that accepts and writes Avro generic types. The Avro writers
        will use the given schema to build and write the records.

        Note that to make this works in PyFlink, you need to declare the output type of the
        predecessor before FileSink to be :class:`GenericRecordAvroTypeInfo`, and the predecessor
        cannot be :meth:`StreamExecutionEnvironment.from_collection`, you can add a pass-through map
        function before the sink, as the example shown below.

        The Python data records should match the Avro schema, and have the same behavior with
        vanilla Python data structure, e.g. an object for Avro array should behave like Python list,
        an object for Avro map should behave like Python dict.

        Example:
        ::

            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> schema = AvroSchema(JSON_SCHEMA)
            >>> avro_type_info = GenericRecordAvroTypeInfo(schema)
            >>> ds = env.from_collection([{'array': [1, 2]}], type_info=Types.PICKLED_BYTE_ARRAY())
            >>> sink = FileSink.for_bulk_format(
            ...     OUTPUT_DIR, AvroWriters.for_generic_record(schema)).build()
            >>> # A map to indicate its Avro type info is necessary for serialization
            >>> ds.map(lambda e: e, output_type=GenericRecordAvroTypeInfo(schema)) \\
            ...     .sink_to(sink)

        :param schema: The avro schema.
        :return: The BulkWriterFactory to write generic records into avro files.
        """
        jvm = get_gateway().jvm
        j_bulk_writer_factory = jvm.org.apache.flink.formats.avro.AvroWriters.forGenericRecord(
            schema._j_schema
        )
        return BulkWriterFactory(j_bulk_writer_factory)
