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
from typing import Tuple, List

from avro.datafile import DataFileReader
from avro.io import DatumReader
from py4j.java_gateway import java_import, JavaObject

from pyflink.common import Types, Configuration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.formats.csv import CsvSchema, CsvReaderFormat, CsvBulkWriter
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.avro import (
    AvroInputFormat,
    AvroSchema,
    AvroWriters,
    GenericRecordAvroTypeInfo,
)
from pyflink.datastream.formats.parquet import AvroParquetReaders, ParquetColumnarRowInputFormat, \
    AvroParquetWriters
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.table.types import RowType, DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


class FileSourceCsvReaderFormatTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        self.csv_file_name = tempfile.mktemp(suffix='.csv', dir=self.tempdir)

    def test_csv_primitive_column(self):
        schema, lines = _create_csv_primitive_column_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_primitive_column')
        _check_csv_primitive_column_results(self, self.test_sink.get_results(True, False))

    def test_csv_array_column(self):
        schema, lines = _create_csv_array_column_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_array_column')
        _check_csv_array_column_results(self, self.test_sink.get_results(True, False))

    def test_csv_allow_comments(self):
        schema, lines = _create_csv_allow_comments_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_allow_comments')
        _check_csv_allow_comments_results(self, self.test_sink.get_results(True, False))

    def test_csv_use_header(self):
        schema, lines = _create_csv_use_header_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_use_header')
        _check_csv_use_header_results(self, self.test_sink.get_results(True, False))

    def test_csv_strict_headers(self):
        schema, lines = _create_csv_strict_headers_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_strict_headers')
        _check_csv_strict_headers_results(self, self.test_sink.get_results(True, False))

    def test_csv_default_quote_char(self):
        schema, lines = _create_csv_default_quote_char_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_default_quote_char')
        _check_csv_default_quote_char_results(self, self.test_sink.get_results(True, False))

    def test_csv_customize_quote_char(self):
        schema, lines = _create_csv_customize_quote_char_schema_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_customize_quote_char')
        _check_csv_customize_quote_char_results(self, self.test_sink.get_results(True, False))

    def test_csv_use_escape_char(self):
        schema, lines = _create_csv_set_escape_char_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_use_escape_char')
        _check_csv_set_escape_char_results(self, self.test_sink.get_results(True, False))

    def _build_csv_job(self, schema, lines):
        with open(self.csv_file_name, 'w') as f:
            for line in lines:
                f.write(line)
        source = FileSource.for_record_stream_format(
            CsvReaderFormat.for_schema(schema), self.csv_file_name).build()
        ds = self.env.from_source(source, WatermarkStrategy.no_watermarks(), 'csv-source')
        ds.map(PassThroughMapFunction(), output_type=Types.PICKLED_BYTE_ARRAY()) \
            .add_sink(self.test_sink)


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


class FileSourceAvroInputFormatTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        self.avro_file_name = tempfile.mktemp(suffix='.avro', dir=self.tempdir)
        _import_avro_classes()

    def test_avro_basic_read(self):
        schema, records = _create_basic_avro_schema_and_records()
        self._create_avro_file(schema, records)
        self._build_avro_job(schema)
        self.env.execute('test_avro_basic_read')
        results = self.test_sink.get_results(True, False)
        _check_basic_avro_schema_results(self, results)

    def test_avro_enum_read(self):
        schema, records = _create_enum_avro_schema_and_records()
        self._create_avro_file(schema, records)
        self._build_avro_job(schema)
        self.env.execute('test_avro_enum_read')
        results = self.test_sink.get_results(True, False)
        _check_enum_avro_schema_results(self, results)

    def test_avro_union_read(self):
        schema, records = _create_union_avro_schema_and_records()
        self._create_avro_file(schema, records)
        self._build_avro_job(schema)
        self.env.execute('test_avro_union_read')
        results = self.test_sink.get_results(True, False)
        _check_union_avro_schema_results(self, results)

    def test_avro_array_read(self):
        schema, records = _create_array_avro_schema_and_records()
        self._create_avro_file(schema, records)
        self._build_avro_job(schema)
        self.env.execute('test_avro_array_read')
        results = self.test_sink.get_results(True, False)
        _check_array_avro_schema_results(self, results)

    def test_avro_map_read(self):
        schema, records = _create_map_avro_schema_and_records()
        self._create_avro_file(schema, records)
        self._build_avro_job(schema)
        self.env.execute('test_avro_map_read')
        results = self.test_sink.get_results(True, False)
        _check_map_avro_schema_results(self, results)

    def _build_avro_job(self, record_schema):
        ds = self.env.create_input(AvroInputFormat(self.avro_file_name, record_schema))
        ds.map(PassThroughMapFunction()).add_sink(self.test_sink)

    def _create_avro_file(self, schema: AvroSchema, records: list):
        jvm = get_gateway().jvm
        j_file = jvm.java.io.File(self.avro_file_name)
        j_datum_writer = jvm.org.apache.flink.avro.shaded.org.apache.avro.generic \
            .GenericDatumWriter()
        j_file_writer = jvm.org.apache.flink.avro.shaded.org.apache.avro.file \
            .DataFileWriter(j_datum_writer)
        j_file_writer.create(schema._j_schema, j_file)
        for r in records:
            j_file_writer.append(r)
        j_file_writer.close()


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSinkAvroParquetWritersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        # NOTE: parallelism == 1 is required to keep the order of results
        self.env.set_parallelism(1)
        self.parquet_dir_name = tempfile.mktemp(dir=self.tempdir)
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


class FileSinkAvroWritersTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        # NOTE: parallelism == 1 is required to keep the order of results
        self.env.set_parallelism(1)
        self.avro_dir_name = tempfile.mkdtemp(dir=self.tempdir)

    def test_avro_basic_write(self):
        schema, objects = _create_basic_avro_schema_and_py_objects()
        self._build_avro_job(schema, objects)
        self.env.execute('test_avro_basic_write')
        results = self._read_avro_file()
        _check_basic_avro_schema_results(self, results)

    def test_avro_enum_write(self):
        schema, objects = _create_enum_avro_schema_and_py_objects()
        self._build_avro_job(schema, objects)
        self.env.execute('test_avro_enum_write')
        results = self._read_avro_file()
        _check_enum_avro_schema_results(self, results)

    def test_avro_union_write(self):
        schema, objects = _create_union_avro_schema_and_py_objects()
        self._build_avro_job(schema, objects)
        self.env.execute('test_avro_union_write')
        results = self._read_avro_file()
        _check_union_avro_schema_results(self, results)

    def test_avro_array_write(self):
        schema, objects = _create_array_avro_schema_and_py_objects()
        self._build_avro_job(schema, objects)
        self.env.execute('test_avro_array_write')
        results = self._read_avro_file()
        _check_array_avro_schema_results(self, results)

    def test_avro_map_write(self):
        schema, objects = _create_map_avro_schema_and_py_objects()
        self._build_avro_job(schema, objects)
        self.env.execute('test_avro_map_write')
        results = self._read_avro_file()
        _check_map_avro_schema_results(self, results)

    def _build_avro_job(self, schema, objects):
        ds = self.env.from_collection(objects)
        sink = FileSink.for_bulk_format(
            self.avro_dir_name, AvroWriters.for_generic_record(schema)
        ).build()
        ds.map(lambda e: e, output_type=GenericRecordAvroTypeInfo(schema)).sink_to(sink)

    def _read_avro_file(self) -> List[dict]:
        records = []
        for file in glob.glob(os.path.join(os.path.join(self.avro_dir_name, '**/*'))):
            for record in DataFileReader(open(file, 'rb'), DatumReader()):
                records.append(record)
        return records


class FileSinkCsvBulkWriterTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.env.set_parallelism(1)
        self.csv_file_name = tempfile.mktemp(dir=self.tempdir)
        self.csv_dir_name = tempfile.mkdtemp(dir=self.tempdir)

    def test_csv_primitive_column_write(self):
        schema, lines = _create_csv_primitive_column_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_primitive_column_write')
        results = self._read_csv_file()
        self.assertTrue(len(results) == 1)
        self.assertEqual(
            results[0],
            '127,-32767,2147483647,-9223372036854775808,3.0E38,2.0E-308,2,true,string\n'
        )

    def test_csv_array_column_write(self):
        schema, lines = _create_csv_array_column_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_array_column_write')
        results = self._read_csv_file()
        self.assertTrue(len(results) == 1)
        self.assertListEqual(results, lines)

    def test_csv_default_quote_char_write(self):
        schema, lines = _create_csv_default_quote_char_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_default_quote_char_write')
        results = self._read_csv_file()
        self.assertTrue(len(results) == 1)
        self.assertListEqual(results, lines)

    def test_csv_customize_quote_char_write(self):
        schema, lines = _create_csv_customize_quote_char_schema_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_customize_quote_char_write')
        results = self._read_csv_file()
        self.assertTrue(len(results) == 1)
        self.assertListEqual(results, lines)

    def test_csv_use_escape_char_write(self):
        schema, lines = _create_csv_set_escape_char_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_use_escape_char_write')
        results = self._read_csv_file()
        self.assertTrue(len(results) == 1)
        self.assertListEqual(results, ['"string,","""string2"""\n'])

    def _build_csv_job(self, schema: CsvSchema, lines):
        with open(self.csv_file_name, 'w') as f:
            for line in lines:
                f.write(line)
        source = FileSource.for_record_stream_format(
            CsvReaderFormat.for_schema(schema), self.csv_file_name
        ).build()
        ds = self.env.from_source(source, WatermarkStrategy.no_watermarks(), 'csv-source')
        sink = FileSink.for_bulk_format(
            self.csv_dir_name, CsvBulkWriter.for_schema(schema)
        ).build()
        ds.map(lambda e: e, output_type=schema.get_type_info()).sink_to(sink)

    def _read_csv_file(self) -> List[str]:
        lines = []
        for file in glob.glob(os.path.join(self.csv_dir_name, '**/*')):
            with open(file, 'r') as f:
                lines.extend(f.readlines())
        return lines


class PassThroughMapFunction(MapFunction):

    def map(self, value):
        return value


def _import_avro_classes():
    jvm = get_gateway().jvm
    classes = ['org.apache.avro.generic.GenericData']
    prefix = 'org.apache.flink.avro.shaded.'
    for cls in classes:
        java_import(jvm, prefix + cls)


def _create_csv_primitive_column_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_number_column('tinyint', DataTypes.TINYINT()) \
        .add_number_column('smallint', DataTypes.SMALLINT()) \
        .add_number_column('int', DataTypes.INT()) \
        .add_number_column('bigint', DataTypes.BIGINT()) \
        .add_number_column('float', DataTypes.FLOAT()) \
        .add_number_column('double', DataTypes.DOUBLE()) \
        .add_number_column('decimal', DataTypes.DECIMAL(2, 0)) \
        .add_boolean_column('boolean') \
        .add_string_column('string') \
        .build()
    lines = [
        '127,'
        '-32767,'
        '2147483647,'
        '-9223372036854775808,'
        '3e38,'
        '2e-308,'
        '1.5,'
        'true,'
        'string\n',
    ]
    return schema, lines


def _check_csv_primitive_column_results(test, results):
    row = results[0]
    test.assertEqual(row['tinyint'], 127)
    test.assertEqual(row['smallint'], -32767)
    test.assertEqual(row['int'], 2147483647)
    test.assertEqual(row['bigint'], -9223372036854775808)
    test.assertAlmostEqual(row['float'], 3e38, delta=1e31)
    test.assertAlmostEqual(row['double'], 2e-308, delta=2e-301)
    test.assertAlmostEqual(row['decimal'], 2)
    test.assertEqual(row['boolean'], True)
    test.assertEqual(row['string'], 'string')


def _create_csv_array_column_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_array_column('number_array', separator=';', element_type=DataTypes.INT()) \
        .add_array_column('boolean_array', separator=':', element_type=DataTypes.BOOLEAN()) \
        .add_array_column('string_array', separator=',', element_type=DataTypes.STRING()) \
        .set_column_separator('|') \
        .disable_quote_char() \
        .build()
    lines = [
        '1;2;3|'
        'true:false|'
        'a,b,c\n',
    ]
    return schema, lines


def _check_csv_array_column_results(test, results):
    row = results[0]
    test.assertListEqual(row['number_array'], [1, 2, 3])
    test.assertListEqual(row['boolean_array'], [True, False])
    test.assertListEqual(row['string_array'], ['a', 'b', 'c'])


def _create_csv_allow_comments_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .set_allow_comments() \
        .build()
    lines = [
        'a\n',
        '# this is comment\n',
        'b\n',
    ]
    return schema, lines


def _check_csv_allow_comments_results(test, results):
    test.assertEqual(results[0]['string'], 'a')
    test.assertEqual(results[1]['string'], 'b')


def _create_csv_use_header_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .add_number_column('number') \
        .set_use_header() \
        .build()
    lines = [
        'h1,h2\n',
        'string,123\n',
    ]
    return schema, lines


def _check_csv_use_header_results(test, results):
    row = results[0]
    test.assertEqual(row['string'], 'string')
    test.assertEqual(row['number'], 123)


def _create_csv_strict_headers_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .add_number_column('number') \
        .set_use_header() \
        .set_strict_headers() \
        .build()
    lines = [
        'string,number\n',
        'string,123\n',
    ]
    return schema, lines


def _check_csv_strict_headers_results(test, results):
    row = results[0]
    test.assertEqual(row['string'], 'string')
    test.assertEqual(row['number'], 123)


def _create_csv_default_quote_char_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .add_string_column('string2') \
        .set_column_separator('|') \
        .build()
    lines = [
        '"string"|"string2"\n',
    ]
    return schema, lines


def _check_csv_default_quote_char_results(test, results):
    row = results[0]
    test.assertEqual(row['string'], 'string')


def _create_csv_customize_quote_char_schema_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .add_string_column('string2') \
        .set_column_separator('|') \
        .set_quote_char('`') \
        .build()
    lines = [
        '`string`|`string2`\n',
    ]
    return schema, lines


def _check_csv_customize_quote_char_results(test, results):
    row = results[0]
    test.assertEqual(row['string'], 'string')


def _create_csv_set_escape_char_schema_and_lines() -> Tuple[CsvSchema, List[str]]:
    schema = CsvSchema.builder() \
        .add_string_column('string') \
        .add_string_column('string2') \
        .set_column_separator(',') \
        .set_escape_char('\\') \
        .build()
    lines = [
        'string\\,,\\"string2\\"\n',
    ]
    return schema, lines


def _check_csv_set_escape_char_results(test, results):
    row = results[0]
    test.assertEqual(row['string'], 'string,')
    test.assertEqual(row['string2'], '"string2"')


BASIC_SCHEMA = """
{
    "type": "record",
    "name": "test",
    "fields": [
        { "name": "null", "type": "null" },
        { "name": "boolean", "type": "boolean" },
        { "name": "int", "type": "int" },
        { "name": "long", "type": "long" },
        { "name": "float", "type": "float" },
        { "name": "double", "type": "double" },
        { "name": "string", "type": "string" }
    ]
}
"""


def _create_basic_avro_schema_and_records() -> Tuple[AvroSchema, List[JavaObject]]:
    schema = AvroSchema.parse_string(BASIC_SCHEMA)
    records = [_create_basic_avro_record(schema, True, 0, 1, 2, 3, 's1'),
               _create_basic_avro_record(schema, False, 4, 5, 6, 7, 's2')]
    return schema, records


def _create_basic_avro_schema_and_py_objects() -> Tuple[AvroSchema, List[dict]]:
    schema = AvroSchema.parse_string(BASIC_SCHEMA)
    objects = [
        {'null': None, 'boolean': True, 'int': 0, 'long': 1,
         'float': 2., 'double': 3., 'string': 's1'},
        {'null': None, 'boolean': False, 'int': 4, 'long': 5,
         'float': 6., 'double': 7., 'string': 's2'},
    ]
    return schema, objects


def _check_basic_avro_schema_results(test, results):
    result1 = results[0]
    result2 = results[1]
    test.assertEqual(result1['null'], None)
    test.assertEqual(result1['boolean'], True)
    test.assertEqual(result1['int'], 0)
    test.assertEqual(result1['long'], 1)
    test.assertAlmostEqual(result1['float'], 2, delta=1e-3)
    test.assertAlmostEqual(result1['double'], 3, delta=1e-3)
    test.assertEqual(result1['string'], 's1')
    test.assertEqual(result2['null'], None)
    test.assertEqual(result2['boolean'], False)
    test.assertEqual(result2['int'], 4)
    test.assertEqual(result2['long'], 5)
    test.assertAlmostEqual(result2['float'], 6, delta=1e-3)
    test.assertAlmostEqual(result2['double'], 7, delta=1e-3)
    test.assertEqual(result2['string'], 's2')


ENUM_SCHEMA = """
{
    "type": "record",
    "name": "test",
    "fields": [
        {
            "name": "suit",
            "type": {
                "type": "enum",
                "name": "Suit",
                "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
            }
        }
    ]
}
"""


def _create_enum_avro_schema_and_records() -> Tuple[AvroSchema, List[JavaObject]]:
    schema = AvroSchema.parse_string(ENUM_SCHEMA)
    records = [_create_enum_avro_record(schema, 'SPADES'),
               _create_enum_avro_record(schema, 'DIAMONDS')]
    return schema, records


def _create_enum_avro_schema_and_py_objects() -> Tuple[AvroSchema, List[dict]]:
    schema = AvroSchema.parse_string(ENUM_SCHEMA)
    records = [
        {'suit': 'SPADES'},
        {'suit': 'DIAMONDS'},
    ]
    return schema, records


def _check_enum_avro_schema_results(test, results):
    test.assertEqual(results[0]['suit'], 'SPADES')
    test.assertEqual(results[1]['suit'], 'DIAMONDS')


UNION_SCHEMA = """
{
    "type": "record",
    "name": "test",
    "fields": [
        {
            "name": "union",
            "type": [ "int", "double", "null" ]
        }
    ]
}
"""


def _create_union_avro_schema_and_records() -> Tuple[AvroSchema, List[JavaObject]]:
    schema = AvroSchema.parse_string(UNION_SCHEMA)
    records = [_create_union_avro_record(schema, 1),
               _create_union_avro_record(schema, 2.),
               _create_union_avro_record(schema, None)]
    return schema, records


def _create_union_avro_schema_and_py_objects() -> Tuple[AvroSchema, List[dict]]:
    schema = AvroSchema.parse_string(UNION_SCHEMA)
    records = [
        {'union': 1},
        {'union': 2.},
        {'union': None},
    ]
    return schema, records


def _check_union_avro_schema_results(test, results):
    test.assertEqual(results[0]['union'], 1)
    test.assertAlmostEqual(results[1]['union'], 2.0, delta=1e-3)
    test.assertEqual(results[2]['union'], None)


# It seems there's bug when array item record contains only one field, which throws
# java.lang.ClassCastException: required ... is not a group when reading
ARRAY_SCHEMA = """
{
    "type": "record",
    "name": "test",
    "fields": [
        {
            "name": "array",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "item",
                    "fields": [
                        { "name": "int", "type": "int" },
                        { "name": "double", "type": "double" }
                    ]
                }
            }
        }
    ]
}
"""


def _create_array_avro_schema_and_records() -> Tuple[AvroSchema, List[JavaObject]]:
    schema = AvroSchema.parse_string(ARRAY_SCHEMA)
    records = [_create_array_avro_record(schema, [(1, 2.), (3, 4.)]),
               _create_array_avro_record(schema, [(5, 6.), (7, 8.)])]
    return schema, records


def _create_array_avro_schema_and_py_objects() -> Tuple[AvroSchema, List[dict]]:
    schema = AvroSchema.parse_string(ARRAY_SCHEMA)
    records = [
        {'array': [{'int': 1, 'double': 2.}, {'int': 3, 'double': 4.}]},
        {'array': [{'int': 5, 'double': 6.}, {'int': 7, 'double': 8.}]},
    ]
    return schema, records


def _check_array_avro_schema_results(test, results):
    result1 = results[0]
    result2 = results[1]
    test.assertEqual(result1['array'][0]['int'], 1)
    test.assertAlmostEqual(result1['array'][0]['double'], 2., delta=1e-3)
    test.assertEqual(result1['array'][1]['int'], 3)
    test.assertAlmostEqual(result1['array'][1]['double'], 4., delta=1e-3)
    test.assertEqual(result2['array'][0]['int'], 5)
    test.assertAlmostEqual(result2['array'][0]['double'], 6., delta=1e-3)
    test.assertEqual(result2['array'][1]['int'], 7)
    test.assertAlmostEqual(result2['array'][1]['double'], 8., delta=1e-3)


MAP_SCHEMA = """
{
    "type": "record",
    "name": "test",
    "fields": [
        {
            "name": "map",
            "type": {
                "type": "map",
                "values": "long"
            }
        }
    ]
}
"""


def _create_map_avro_schema_and_records() -> Tuple[AvroSchema, List[JavaObject]]:
    schema = AvroSchema.parse_string(MAP_SCHEMA)
    records = [_create_map_avro_record(schema, {'a': 1, 'b': 2}),
               _create_map_avro_record(schema, {'c': 3, 'd': 4})]
    return schema, records


def _create_map_avro_schema_and_py_objects() -> Tuple[AvroSchema, List[dict]]:
    schema = AvroSchema.parse_string(MAP_SCHEMA)
    records = [
        {'map': {'a': 1, 'b': 2}},
        {'map': {'c': 3, 'd': 4}},
    ]
    return schema, records


def _check_map_avro_schema_results(test, results):
    result1 = results[0]
    result2 = results[1]
    test.assertEqual(result1['map']['a'], 1)
    test.assertEqual(result1['map']['b'], 2)
    test.assertEqual(result2['map']['c'], 3)
    test.assertEqual(result2['map']['d'], 4)


def _create_basic_avro_record(schema: AvroSchema, boolean_value, int_value, long_value,
                              float_value, double_value, string_value):
    jvm = get_gateway().jvm
    j_record = jvm.GenericData.Record(schema._j_schema)
    j_record.put('boolean', boolean_value)
    j_record.put('int', int_value)
    j_record.put('long', long_value)
    j_record.put('float', float_value)
    j_record.put('double', double_value)
    j_record.put('string', string_value)
    return j_record


def _create_enum_avro_record(schema: AvroSchema, enum_value):
    jvm = get_gateway().jvm
    j_record = jvm.GenericData.Record(schema._j_schema)
    j_enum = jvm.GenericData.EnumSymbol(schema._j_schema.getField('suit').schema(), enum_value)
    j_record.put('suit', j_enum)
    return j_record


def _create_union_avro_record(schema, union_value):
    jvm = get_gateway().jvm
    j_record = jvm.GenericData.Record(schema._j_schema)
    j_record.put('union', union_value)
    return j_record


def _create_array_avro_record(schema, item_values: list):
    jvm = get_gateway().jvm
    j_record = jvm.GenericData.Record(schema._j_schema)
    item_schema = AvroSchema(schema._j_schema.getField('array').schema().getElementType())
    j_array = jvm.java.util.ArrayList()
    for idx, item_value in enumerate(item_values):
        j_item = jvm.GenericData.Record(item_schema._j_schema)
        j_item.put('int', item_value[0])
        j_item.put('double', item_value[1])
        j_array.add(j_item)
    j_record.put('array', j_array)
    return j_record


def _create_map_avro_record(schema, map: dict):
    jvm = get_gateway().jvm
    j_record = jvm.GenericData.Record(schema._j_schema)
    j_map = jvm.java.util.HashMap()
    for k, v in map.items():
        j_map.put(k, v)
    j_record.put('map', j_map)
    return j_record
