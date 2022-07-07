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
from pyflink.datastream.connectors.file_system import StreamFormat
from pyflink.datastream.formats.avro import AvroSchema
from pyflink.java_gateway import get_gateway


class AvroParquetReaders(object):
    """
    A convenience builder to create AvroParquetRecordFormat instances for the different kinds of
    Avro record types. Only GenericRecord is supported in PyFlink.

    .. versionadded:: 1.16.0
    """

    @staticmethod
    def for_generic_record(schema: 'AvroSchema') -> 'StreamFormat':
        """
        Creates a new AvroParquetRecordFormat that reads the parquet file into Avro GenericRecords.

        To read into GenericRecords, this method needs an Avro Schema. That is because Flink needs
        to be able to serialize the results in its data flow, which is very inefficient without the
        schema. And while the Schema is stored in the Avro file header, Flink needs this schema
        during 'pre-flight' time when the data flow is set up and wired, which is before there is
        access to the files.

        Example:
        ::

            >>> env = StreamExecutionEnvironment.get_execution_environment()
            >>> schema = AvroSchema.parse_string(JSON_SCHEMA)
            >>> source = FileSource.for_record_stream_format(
            ...     AvroParquetReaders.for_generic_record(schema),
            ...     FILE_PATH
            ... ).build()
            >>> ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "parquet-source")

        :param schema: the Avro Schema.
        :return: StreamFormat for reading Avro GenericRecords.
        """
        jvm = get_gateway().jvm
        JAvroParquetReaders = jvm.org.apache.flink.formats.parquet.avro.AvroParquetReaders
        return StreamFormat(JAvroParquetReaders.forGenericRecord(schema._j_schema))
