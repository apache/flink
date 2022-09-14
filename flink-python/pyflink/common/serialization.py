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

from pyflink.common.utils import JavaObjectWrapper
from pyflink.java_gateway import get_gateway

__all__ = [
    'SerializationSchema',
    'DeserializationSchema',
    'SimpleStringSchema',
    'Encoder',
    'BulkWriterFactory'
]


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


class Encoder(object):
    """
    Encoder is used by the file sink to perform the actual writing of the
    incoming elements to the files in a bucket.
    """

    def __init__(self, j_encoder):
        self._j_encoder = j_encoder

    @staticmethod
    def simple_string_encoder(charset_name: str = "UTF-8") -> 'Encoder':
        """
        A simple Encoder that uses toString() on the input elements and writes them to
        the output bucket file separated by newline.
        """
        j_encoder = get_gateway().jvm.org.apache.flink.api.common.serialization.\
            SimpleStringEncoder(charset_name)
        return Encoder(j_encoder)


class BulkWriterFactory(JavaObjectWrapper):
    """
    The Python wrapper of Java BulkWriter.Factory interface, which is the base interface for data
    sinks that write records into files in a bulk manner.
    """

    def __init__(self, j_bulk_writer_factory):
        super().__init__(j_bulk_writer_factory)


class RowDataBulkWriterFactory(BulkWriterFactory):
    """
    A :class:`～BulkWriterFactory` that receives records with RowData type. This is for indicating
    that Row record from Python must be first converted to RowData.
    """

    def __init__(self, j_bulk_writer_factory, row_type):
        super().__init__(j_bulk_writer_factory)
        self._row_type = row_type

    def get_row_type(self):
        return self._row_type
