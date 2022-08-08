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
import glob
import os
import tempfile
import unittest
from typing import List

from pyflink.common import Configuration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import MapFunction
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.tests.test_avro import \
    _create_basic_avro_schema_and_py_objects, _check_basic_avro_schema_results, \
    _create_enum_avro_schema_and_py_objects, _check_enum_avro_schema_results, \
    _create_union_avro_schema_and_py_objects, _check_union_avro_schema_results, \
    _create_array_avro_schema_and_py_objects, _check_array_avro_schema_results, \
    _create_map_avro_schema_and_py_objects, _check_map_avro_schema_results, \
    _create_map_avro_schema_and_records, _create_array_avro_schema_and_records, \
    _create_union_avro_schema_and_records, _create_enum_avro_schema_and_records, \
    _create_basic_avro_schema_and_records, _import_avro_classes
from pyflink.datastream.formats.avro import GenericRecordAvroTypeInfo, AvroSchema
from pyflink.datastream.formats.parquet import AvroParquetReaders, ParquetColumnarRowInputFormat, \
    AvroParquetWriters
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.table.types import RowType, DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSourceAvroParquetReadersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        _import_avro_classes()

    def test_parquet_avro_basic(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_basic_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_basic")
        results = self.test_sink.get_results(True, False)
        _check_basic_avro_schema_results(self, results)

    def test_parquet_avro_enum(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_enum_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_enum")
        results = self.test_sink.get_results(True, False)
        _check_enum_avro_schema_results(self, results)

    def test_parquet_avro_union(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_union_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_union")
        results = self.test_sink.get_results(True, False)
        _check_union_avro_schema_results(self, results)

    def test_parquet_avro_array(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_array_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_array")
        results = self.test_sink.get_results(True, False)
        _check_array_avro_schema_results(self, results)

    def test_parquet_avro_map(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_map_avro_schema_and_records()
        self._create_parquet_avro_file(parquet_file_name, schema, records)
        self._build_parquet_avro_job(schema, parquet_file_name)
        self.env.execute("test_parquet_avro_map")
        results = self.test_sink.get_results(True, False)
        _check_map_avro_schema_results(self, results)

    def _build_parquet_avro_job(self, record_schema, *parquet_file_name):
        ds = self.env.from_source(
            FileSource.for_record_stream_format(
                AvroParquetReaders.for_generic_record(record_schema),
                *parquet_file_name
            ).build(),
            WatermarkStrategy.for_monotonous_timestamps(),
            "parquet-source"
        )
        ds.map(PassThroughMapFunction()).add_sink(self.test_sink)

    @staticmethod
    def _create_parquet_avro_file(file_path: str, schema: AvroSchema, records: list):
        jvm = get_gateway().jvm
        j_path = jvm.org.apache.flink.core.fs.Path(file_path)
        writer = jvm.org.apache.flink.formats.parquet.avro.AvroParquetWriters \
            .forGenericRecord(schema._j_schema) \
            .create(j_path.getFileSystem().create(
                j_path,
                jvm.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE
            ))
        for record in records:
            writer.addElement(record)
        writer.flush()
        writer.finish()


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSinkAvroParquetWritersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        # NOTE: parallelism == 1 is required to keep the order of results
        self.env.set_parallelism(1)
        self.parquet_dir_name = tempfile.mkdtemp(dir=self.tempdir)
        self.test_sink = DataStreamTestSinkFunction()

    def test_parquet_avro_basic_write(self):
        schema, objects = _create_basic_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_basic_write')
        results = self._read_parquet_avro_file(schema)
        _check_basic_avro_schema_results(self, results)

    def test_parquet_avro_enum_write(self):
        schema, objects = _create_enum_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_enum_write')
        results = self._read_parquet_avro_file(schema)
        _check_enum_avro_schema_results(self, results)

    def test_parquet_avro_union_write(self):
        schema, objects = _create_union_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_union_write')
        results = self._read_parquet_avro_file(schema)
        _check_union_avro_schema_results(self, results)

    def test_parquet_avro_array_write(self):
        schema, objects = _create_array_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_array_write')
        results = self._read_parquet_avro_file(schema)
        _check_array_avro_schema_results(self, results)

    def test_parquet_avro_map_write(self):
        schema, objects = _create_map_avro_schema_and_py_objects()
        self._build_avro_parquet_job(schema, objects)
        self.env.execute('test_parquet_avro_map_write')
        results = self._read_parquet_avro_file(schema)
        _check_map_avro_schema_results(self, results)

    def _build_avro_parquet_job(self, schema, objects):
        ds = self.env.from_collection(objects)
        avro_type_info = GenericRecordAvroTypeInfo(schema)
        sink = FileSink.for_bulk_format(
            self.parquet_dir_name, AvroParquetWriters.for_generic_record(schema)
        ).build()
        ds.map(lambda e: e, output_type=avro_type_info).sink_to(sink)

    def _read_parquet_avro_file(self, schema) -> List[dict]:
        parquet_files = [f for f in glob.glob(self.parquet_dir_name, recursive=True)]
        FileSourceAvroParquetReadersTests._build_parquet_avro_job(self, schema, *parquet_files)
        self.env.execute()
        return self.test_sink.get_results(True, False)


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet Columnar format tests')
class FileSourceParquetColumnarRowInputFormatTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        _import_avro_classes()

    def test_parquet_columnar_basic(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        schema, records = _create_basic_avro_schema_and_records()
        FileSourceAvroParquetReadersTests._create_parquet_avro_file(
            parquet_file_name, schema, records)
        row_type = DataTypes.ROW([
            DataTypes.FIELD('null', DataTypes.STRING()),  # DataTypes.NULL cannot be serialized
            DataTypes.FIELD('boolean', DataTypes.BOOLEAN()),
            DataTypes.FIELD('int', DataTypes.INT()),
            DataTypes.FIELD('long', DataTypes.BIGINT()),
            DataTypes.FIELD('float', DataTypes.FLOAT()),
            DataTypes.FIELD('double', DataTypes.DOUBLE()),
            DataTypes.FIELD('string', DataTypes.STRING()),
            DataTypes.FIELD('unknown', DataTypes.STRING())
        ])
        self._build_parquet_columnar_job(row_type, parquet_file_name)
        self.env.execute('test_parquet_columnar_basic')
        results = self.test_sink.get_results(True, False)
        _check_basic_avro_schema_results(self, results)
        self.assertIsNone(results[0]['unknown'])
        self.assertIsNone(results[1]['unknown'])

    def _build_parquet_columnar_job(self, row_type: RowType, parquet_file_name: str):
        source = FileSource.for_bulk_file_format(
            ParquetColumnarRowInputFormat(Configuration(), row_type, 10, True, True),
            parquet_file_name
        ).build()
        ds = self.env.from_source(source, WatermarkStrategy.no_watermarks(), 'parquet-source')
        ds.map(PassThroughMapFunction()).add_sink(self.test_sink)


class PassThroughMapFunction(MapFunction):

    def map(self, value):
        return value
