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
from py4j.java_gateway import java_import, JavaObject
from pyflink.common import typeinfo
from pyflink.common.typeinfo import TypeInformation

from pyflink.util.utils import load_java_class

from pyflink.java_gateway import get_gateway
from typing import Union


class SerializationSchema(object):
    """
    Base class for SerializationSchema. The serialization schema describes how to turn a data object
    into a different serialized representation. Most data sinks (for example Apache Kafka) require
    the data to be handed to them in a specific format (for example as byte strings).
    """
    def __init__(self, j_serialization_schema=None):
        self._j_serialization_schema = j_serialization_schema


class DeserializationSchema(object):
    """
    Base class for DeserializationSchema. The deserialization schema describes how to turn the byte
    messages delivered by certain data sources (for example Apache Kafka) into data types (Java/
    Scala objects) that are processed by Flink.

    In addition, the DeserializationSchema describes the produced type which lets Flink create
    internal serializers and structures to handle the type.
    """
    def __init__(self, j_deserialization_schema=None):
        self._j_deserialization_schema = j_deserialization_schema


class SimpleStringSchema(SerializationSchema, DeserializationSchema):
    """
    Very simple serialization/deserialization schema for strings. By default, the serializer uses
    'UTF-8' for string/byte conversion.
    """

    def __init__(self, charset: str = 'UTF-8'):
        gate_way = get_gateway()
        j_char_set = gate_way.jvm.java.nio.charset.Charset.forName(charset)
        j_simple_string_serialization_schema = gate_way \
            .jvm.org.apache.flink.api.common.serialization.SimpleStringSchema(j_char_set)
        SerializationSchema.__init__(self,
                                     j_serialization_schema=j_simple_string_serialization_schema)
        DeserializationSchema.__init__(
            self, j_deserialization_schema=j_simple_string_serialization_schema)


class JsonRowDeserializationSchema(DeserializationSchema):
    """
    Deserialization schema from JSON to Flink types.

    Deserializes a byte[] message as a JSON object and reads the specified fields.

    Failures during deserialization are forwarded as wrapped IOExceptions.
    """
    def __init__(self, j_deserialization_schema):
        super(JsonRowDeserializationSchema, self).__init__(j_deserialization_schema)

    @staticmethod
    def builder():
        """
        A static method to get a Builder for JsonRowDeserializationSchema.
        """
        return JsonRowDeserializationSchema.Builder()

    class Builder(object):
        """
        Builder for JsonRowDeserializationSchema.
        """

        def __init__(self):
            self._type_info = None
            self._fail_on_missing_field = False
            self._ignore_parse_errors = False

        def type_info(self, type_info: TypeInformation):
            """
            Creates a JSON deserialization schema for the given type information.

            :param type_info: Type information describing the result type. The field names of Row
                              are used to parse the JSON properties.
            """
            self._type_info = type_info
            return self

        def json_schema(self, json_schema: str):
            """
            Creates a JSON deserialization schema for the given JSON schema.

            :param json_schema: JSON schema describing the result type.
            """
            if json_schema is None:
                raise TypeError("The json_schema must not be None.")
            j_type_info = get_gateway().jvm \
                .org.apache.flink.formats.json.JsonRowSchemaConverter.convert(json_schema)
            self._type_info = typeinfo._from_java_type(j_type_info)
            return self

        def fail_on_missing_field(self):
            """
            Configures schema to fail if a JSON field is missing. A missing field is ignored and the
            field is set to null by default.
            """
            self._fail_on_missing_field = True
            return self

        def ignore_parse_errors(self):
            """
            Configures schema to fail when parsing json failed. An exception will be thrown when
            parsing json fails.
            """
            self._ignore_parse_errors = True
            return self

        def build(self):
            JBuilder = get_gateway().jvm.org.apache.flink.formats.json.JsonRowDeserializationSchema\
                .Builder
            j_builder = JBuilder(self._type_info.get_java_type_info())

            if self._fail_on_missing_field:
                j_builder = j_builder.fialOnMissingField()

            if self._ignore_parse_errors:
                j_builder = j_builder.ignoreParseErrors()

            j_deserialization_schema = j_builder.build()
            return JsonRowDeserializationSchema(j_deserialization_schema=j_deserialization_schema)


class JsonRowSerializationSchema(SerializationSchema):
    """
    Serialization schema that serializes an object of Flink types into a JSON bytes. Serializes the
    input Flink object into a JSON string and converts it into byte[].

    Result byte[] message can be deserialized using JsonRowDeserializationSchema.
    """

    def __init__(self, j_serialization_schema):
        super(JsonRowSerializationSchema, self).__init__(j_serialization_schema)

    @staticmethod
    def builder():
        return JsonRowSerializationSchema.Builder()

    class Builder(object):
        """
        Builder for JsonRowSerializationSchema.
        """
        def __init__(self):
            self._type_info = None

        def with_type_info(self, type_info: TypeInformation):
            """
            Creates a JSON serialization schema for the given type information.

            :param type_info: Type information describing the result type. The field names of Row
                              are used to parse the JSON properties.
            """
            self._type_info = type_info
            return self

        def build(self):
            if self._type_info is None:
                raise TypeError("Typeinfo should be set.")

            j_builder = get_gateway().jvm \
                .org.apache.flink.formats.json.JsonRowSerializationSchema.builder()

            j_schema = j_builder.withTypeInfo(self._type_info.get_java_type_info()).build()
            return JsonRowSerializationSchema(j_serialization_schema=j_schema)


class CsvRowDeserializationSchema(DeserializationSchema):
    """
    Deserialization schema from CSV to Flink types. Deserializes a byte[] message as a JsonNode and
    converts it to Row.

    Failure during deserialization are forwarded as wrapped IOException.
    """

    def __init__(self, j_deserialization_schema):
        super(CsvRowDeserializationSchema, self).__init__(
            j_deserialization_schema=j_deserialization_schema)

    class Builder(object):
        """
        A builder for creating a CsvRowDeserializationSchema.
        """
        def __init__(self, type_info: TypeInformation):
            if type_info is None:
                raise TypeError("Type information must not be None")
            self._j_builder = get_gateway().jvm\
                .org.apache.flink.formats.csv.CsvRowDeserializationSchema.Builder(
                type_info.get_java_type_info())

        def set_field_delimiter(self, delimiter: str):
            self._j_builder = self._j_builder.setFieldDelimiter(delimiter)
            return self

        def set_allow_comments(self, allow_comments: bool):
            self._j_builder = self._j_builder.setAllowComments(allow_comments)
            return self

        def set_array_element_delimiter(self, delimiter: str):
            self._j_builder = self._j_builder.setArrayElementDelimiter(delimiter)
            return self

        def set_quote_character(self, c: str):
            self._j_builder = self._j_builder.setQuoteCharacter(c)
            return self

        def set_escape_character(self, c: str):
            self._j_builder = self._j_builder.setEscapeCharacter(c)
            return self

        def set_null_literal(self, null_literal: str):
            self._j_builder = self._j_builder.setNullLiteral(null_literal)
            return self

        def set_ignore_parse_errors(self, ignore_parse_errors: bool):
            self._j_builder = self._j_builder.setIgnoreParseErrors(ignore_parse_errors)
            return self

        def build(self):
            j_csv_row_deserialization_schema = self._j_builder.build()
            return CsvRowDeserializationSchema(
                j_deserialization_schema=j_csv_row_deserialization_schema)


class CsvRowSerializationSchema(SerializationSchema):
    """
    Serialization schema that serializes an object of Flink types into a CSV bytes. Serializes the
    input row into an ObjectNode and converts it into byte[].

    Result byte[] messages can be deserialized using CsvRowDeserializationSchema.
    """
    def __init__(self, j_csv_row_serialization_schema):
        super(CsvRowSerializationSchema, self).__init__(j_csv_row_serialization_schema)

    class Builder(object):
        """
        A builder for creating a CsvRowSerializationSchema.
        """
        def __init__(self, type_info: TypeInformation):
            if type_info is None:
                raise TypeError("Type information must not be None")
            self._j_builder = get_gateway().jvm\
                .org.apache.flink.formats.csv.CsvRowSerializationSchema.Builder(
                type_info.get_java_type_info())

        def set_field_delimiter(self, c: str):
            self._j_builder = self._j_builder.setFieldDelimiter(c)
            return self

        def set_line_delimiter(self, delimiter: str):
            self._j_builder = self._j_builder.setLineDelimiter(delimiter)
            return self

        def set_array_element_delimiter(self, delimiter: str):
            self._j_builder = self._j_builder.setArrayElementDelimiter(delimiter)
            return self

        def disable_quote_character(self):
            self._j_builder = self._j_builder.disableQuoteCharacter()
            return self

        def set_quote_character(self, c: str):
            self._j_builder = self._j_builder.setQuoteCharacter(c)
            return self

        def set_escape_character(self, c: str):
            self._j_builder = self._j_builder.setEscapeCharacter(c)
            return self

        def set_null_literal(self, s: str):
            self._j_builder = self._j_builder.setNullLiteral(s)
            return self

        def build(self):
            j_serialization_schema = self._j_builder.build()
            return CsvRowSerializationSchema(j_csv_row_serialization_schema=j_serialization_schema)


class AvroRowDeserializationSchema(DeserializationSchema):
    """
    Deserialization schema from Avro bytes to Row. Deserializes the byte[] messages into (nested)
    Flink rows. It converts Avro types into types that are compatible with Flink's Table & SQL API.

    Projects with Avro records containing logical date/time types need to add a JodaTime dependency.
    """
    def __init__(self, record_class: str = None, avro_schema_string: str = None):
        """
        Creates an Avro deserialization schema for the given specific record class or Avro schema
        string. Having the concrete Avro record class might improve performance.

        :param record_class: Avro record class used to deserialize Avro's record to Flink's row.
        :param avro_schema_string: Avro schema string to deserialize Avro's record to Flink's row.
        """

        if avro_schema_string is None and record_class is None:
            raise TypeError("record_class or avro_schema_string should be specified.")
        j_deserialization_schema = None
        if record_class is not None:
            gateway = get_gateway()
            java_import(gateway.jvm, record_class)
            j_record_class = load_java_class(record_class)
            JAvroRowDeserializationSchema = get_gateway().jvm \
                .org.apache.flink.formats.avro.AvroRowDeserializationSchema
            j_deserialization_schema = JAvroRowDeserializationSchema(j_record_class)

        elif avro_schema_string is not None:
            JAvroRowDeserializationSchema = get_gateway().jvm \
                .org.apache.flink.formats.avro.AvroRowDeserializationSchema
            j_deserialization_schema = JAvroRowDeserializationSchema(avro_schema_string)

        super(AvroRowDeserializationSchema, self).__init__(j_deserialization_schema)


class AvroRowSerializationSchema(SerializationSchema):
    """
    Serialization schema that serializes to Avro binary format.
    """

    def __init__(self, record_class: str = None, avro_schema_string: str = None):
        """
        Creates AvroSerializationSchema that serializes SpecificRecord using provided schema or
        record class.

        :param record_class: Avro record class used to serialize  Flink's row to Avro's record.
        :param avro_schema_string: Avro schema string to serialize Flink's row to Avro's record.
        """
        if avro_schema_string is None and record_class is None:
            raise TypeError("record_class or avro_schema_string should be specified.")

        j_serialization_schema = None
        if record_class is not None:
            gateway = get_gateway()
            java_import(gateway.jvm, record_class)
            j_record_class = load_java_class(record_class)
            JAvroRowSerializationSchema = get_gateway().jvm \
                .org.apache.flink.formats.avro.AvroRowSerializationSchema
            j_serialization_schema = JAvroRowSerializationSchema(j_record_class)

        elif avro_schema_string is not None:
            JAvroRowSerializationSchema = get_gateway().jvm \
                .org.apache.flink.formats.avro.AvroRowSerializationSchema
            j_serialization_schema = JAvroRowSerializationSchema(avro_schema_string)

        super(AvroRowSerializationSchema, self).__init__(j_serialization_schema)


class Encoder(object):
    """
    A `Encoder` is used by the streaming file sink to perform the actual writing
    of the incoming elements to the files in a bucket.
    """

    def __init__(self, j_encoder: Union[str, JavaObject]):
        if isinstance(j_encoder, str):
            j_encoder_class = get_gateway().jvm.__getattr__(j_encoder)
            j_encoder = j_encoder_class()
        self.j_encoder = j_encoder

    def get_java_encoder(self):
        return self.j_encoder


class SimpleStringEncoder(Encoder):
    """
    A simple `Encoder` that uses `toString()` on the input elements and
    writes them to the output bucket file separated by newline.
    """
    def __init__(self, charset_name: str = "UTF-8"):
        j_encoder = get_gateway().\
            jvm.org.apache.flink.api.common.serialization.SimpleStringEncoder(charset_name)
        super(SimpleStringEncoder, self).__init__(j_encoder)
