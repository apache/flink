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

from pyflink.datastream.formats.base import InputFormat
from pyflink.datastream.utils import ResultTypeQueryable
from pyflink.java_gateway import get_gateway


class AvroSchema(object):
    """
    Avro Schema class contains Java org.apache.avro.Schema.

    .. versionadded:: 1.16.0
    """

    def __init__(self, j_schema):
        self._j_schema = j_schema

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
