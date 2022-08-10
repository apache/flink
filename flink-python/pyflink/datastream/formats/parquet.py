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
from typing import Optional, TYPE_CHECKING

from pyflink.common import Configuration
from pyflink.common.serialization import BulkWriterFactory, RowDataBulkWriterFactory
from pyflink.datastream.connectors.file_system import StreamFormat, BulkFormat
from pyflink.datastream.formats.avro import AvroSchema
from pyflink.datastream.utils import create_hadoop_configuration
from pyflink.java_gateway import get_gateway

if TYPE_CHECKING:
    from pyflink.table.types import RowType

__all__ = [
    'AvroParquetReaders',
    'AvroParquetWriters',
    'ParquetColumnarRowInputFormat',
    'ParquetBulkWriters'
]


class AvroParquetReaders(object):
    """
    A convenience builder to create reader format that reads individual Avro records from a
    Parquet stream. Only GenericRecord is supported in PyFlink.

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
            ...     PARQUET_FILE_PATH
            ... ).build()
            >>> ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "parquet-source")

        :param schema: the Avro Schema.
        :return: StreamFormat for reading Avro GenericRecords.
        """
        jvm = get_gateway().jvm
        JAvroParquetReaders = jvm.org.apache.flink.formats.parquet.avro.AvroParquetReaders
        return StreamFormat(JAvroParquetReaders.forGenericRecord(schema._j_schema))


class AvroParquetWriters(object):
    """
    Convenient builder to create Parquet BulkWriterFactory instances for Avro types.
    Only GenericRecord is supported at present.

    .. versionadded:: 1.16.0
    """

    @staticmethod
    def for_generic_record(schema: 'AvroSchema') -> 'BulkWriterFactory':
        """
        Creates a ParquetWriterFactory that accepts and writes Avro generic types. The Parquet
        writers will use the given schema to build and write the columnar data.

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
            ...     OUTPUT_DIR, AvroParquetWriters.for_generic_record(schema)).build()
            >>> # A map to indicate its Avro type info is necessary for serialization
            >>> ds.map(lambda e: e, output_type=GenericRecordAvroTypeInfo(schema)) \\
            ...     .sink_to(sink)

        :param schema: The avro schema.
        :return: The BulkWriterFactory to write generic records into parquet files.
        """
        jvm = get_gateway().jvm
        JAvroParquetWriters = jvm.org.apache.flink.formats.parquet.avro.AvroParquetWriters
        return BulkWriterFactory(JAvroParquetWriters.forGenericRecord(schema._j_schema))


class ParquetColumnarRowInputFormat(BulkFormat):
    """
    A ParquetVectorizedInputFormat to provide RowData iterator. Using ColumnarRowData to
    provide a row view of column batch. Only **primitive** types are supported for a column,
    composite types such as array, map are not supported.

    Example:
    ::

        >>> row_type = DataTypes.ROW([
        ...     DataTypes.FIELD('a', DataTypes.INT()),
        ...     DataTypes.FIELD('b', DataTypes.STRING()),
        ... ])
        >>> source = FileSource.for_bulk_file_format(ParquetColumnarRowInputFormat(
        ...     row_type=row_type,
        ...     hadoop_config=Configuration(),
        ...     batch_size=2048,
        ...     is_utc_timestamp=False,
        ...     is_case_sensitive=True,
        ... ), PARQUET_FILE_PATH).build()
        >>> ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "parquet-source")

    .. versionadded:: 1.16.0
    """

    def __init__(self,
                 row_type: 'RowType',
                 hadoop_config: Optional[Configuration] = None,
                 batch_size: int = 2048,
                 is_utc_timestamp: bool = False,
                 is_case_sensitive: bool = True):
        if not hadoop_config:
            hadoop_config = Configuration()

        from pyflink.table.types import _to_java_data_type
        jvm = get_gateway().jvm
        j_row_type = _to_java_data_type(row_type).getLogicalType()
        produced_type_info = jvm.org.apache.flink.table.runtime.typeutils. \
            InternalTypeInfo.of(j_row_type)
        j_parquet_columnar_format = jvm.org.apache.flink.formats.parquet. \
            ParquetColumnarRowInputFormat(create_hadoop_configuration(hadoop_config),
                                          j_row_type, produced_type_info, batch_size,
                                          is_utc_timestamp, is_case_sensitive)
        super().__init__(j_parquet_columnar_format)


class ParquetBulkWriters(object):
    """
    Convenient builder to create a :class:`~pyflink.common.serialization.BulkWriterFactory` that
    writes records with a predefined schema into Parquet files in a batch fashion.

    Example:
    ::

        >>> row_type = DataTypes.ROW([
        ...     DataTypes.FIELD('string', DataTypes.STRING()),
        ...     DataTypes.FIELD('int_array', DataTypes.ARRAY(DataTypes.INT()))
        ... ])
        >>> sink = FileSink.for_bulk_format(
        ...     OUTPUT_DIR, ParquetBulkWriters.for_row_type(
        ...         row_type,
        ...         hadoop_config=Configuration(),
        ...         utc_timestamp=True,
        ...     )
        ... ).build()
        >>> ds.sink_to(sink)

    .. versionadded:: 1.16.0
    """

    @staticmethod
    def for_row_type(row_type: 'RowType',
                     hadoop_config: Optional[Configuration] = None,
                     utc_timestamp: bool = False) -> 'BulkWriterFactory':
        """
        Create a :class:`~pyflink.common.serialization.BulkWriterFactory` that writes records
        with a predefined schema into Parquet files in a batch fashion.

        :param row_type: The RowType of records, it should match the RowTypeInfo of Row records.
        :param hadoop_config: Hadoop configuration.
        :param utc_timestamp: Use UTC timezone or local timezone to the conversion between epoch
            time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC
            timezone.
        """
        if not hadoop_config:
            hadoop_config = Configuration()

        from pyflink.table.types import _to_java_data_type
        jvm = get_gateway().jvm
        JParquetRowDataBuilder = jvm.org.apache.flink.formats.parquet.row.ParquetRowDataBuilder
        return RowDataBulkWriterFactory(JParquetRowDataBuilder.createWriterFactory(
            _to_java_data_type(row_type).getLogicalType(),
            create_hadoop_configuration(hadoop_config),
            utc_timestamp
        ), row_type)
