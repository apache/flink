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
from pyflink.common import SerializationSchema, TypeInformation, typeinfo, DeserializationSchema
from pyflink.java_gateway import get_gateway


__all__ = [
    'JsonRowDeserializationSchema',
    'JsonRowSerializationSchema'
]


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
