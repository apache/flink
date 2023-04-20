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
from typing import Tuple, List

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import MapFunction
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.csv import CsvSchema, CsvReaderFormat, CsvBulkWriters, \
    CsvRowSerializationSchema, CsvRowDeserializationSchema
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase, PyFlinkTestCase
from pyflink.util.java_utils import get_j_env_configuration


class FileSourceCsvReaderFormatTests(object):

    def setUp(self):
        super(FileSourceCsvReaderFormatTests, self).setUp()
        self.test_sink = DataStreamTestSinkFunction()
        self.csv_file_name = tempfile.mktemp(suffix='.csv', dir=self.tempdir)

    def test_csv_primitive_column(self):
        schema, lines = _create_csv_primitive_column_schema_and_lines()
        self._build_csv_job(schema, lines)
        self.env.execute('test_csv_primitive_column')
        _check_csv_primitive_column_results(self, self.test_sink.get_results(True, False))

    def test_csv_add_columns_from(self):
        original_schema, lines = _create_csv_primitive_column_schema_and_lines()
        schema = CsvSchema.builder().add_columns_from(original_schema).build()
        self._build_csv_job(schema, lines)

        self.env.execute('test_csv_schema_copy')
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


class ProcessFileSourceCsvReaderFormatTests(FileSourceCsvReaderFormatTests,
                                            PyFlinkStreamingTestCase):
    pass


class EmbeddedFileSourceCsvReaderFormatTests(FileSourceCsvReaderFormatTests,
                                             PyFlinkStreamingTestCase):
    def setUp(self):
        super(EmbeddedFileSourceCsvReaderFormatTests, self).setUp()
        config = get_j_env_configuration(self.env._j_stream_execution_environment)
        config.setString("python.execution-mode", "thread")


class FileSinkCsvBulkWriterTests(PyFlinkStreamingTestCase):

    def setUp(self):
        super().setUp()
        self.env.set_parallelism(1)
        self.csv_file_name = tempfile.mktemp(suffix='.csv', dir=self.tempdir)
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
            self.csv_dir_name, CsvBulkWriters.for_schema(schema)
        ).build()
        ds.sink_to(sink)

    def _read_csv_file(self) -> List[str]:
        lines = []
        for file in glob.glob(os.path.join(self.csv_dir_name, '**/*')):
            with open(file, 'r') as f:
                lines.extend(f.readlines())
        return lines


class JsonSerializationSchemaTests(PyFlinkTestCase):

    def test_csv_row_serialization_schema(self):
        jvm = get_gateway().jvm
        JRow = jvm.org.apache.flink.types.Row

        j_row = JRow(3)
        j_row.setField(0, "BEGIN")
        j_row.setField(2, "END")

        def field_assertion(field_info, csv_value, value, field_delimiter):
            row_info = Types.ROW([Types.STRING(), field_info, Types.STRING()])
            expected_csv = "BEGIN" + field_delimiter + csv_value + field_delimiter + "END\n"
            j_row.setField(1, value)

            csv_row_serialization_schema = CsvRowSerializationSchema.Builder(row_info)\
                .set_escape_character('*').set_quote_character('\'')\
                .set_array_element_delimiter(':').set_field_delimiter(';').build()
            csv_row_deserialization_schema = CsvRowDeserializationSchema.Builder(row_info)\
                .set_escape_character('*').set_quote_character('\'')\
                .set_array_element_delimiter(':').set_field_delimiter(';').build()
            csv_row_serialization_schema._j_serialization_schema.open(
                jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext())
            csv_row_deserialization_schema._j_deserialization_schema.open(
                jvm.org.apache.flink.connector.testutils.formats.DummyInitializationContext())

            serialized_bytes = csv_row_serialization_schema._j_serialization_schema.serialize(j_row)
            self.assertEqual(expected_csv, str(serialized_bytes, encoding='utf-8'))

            j_deserialized_row = csv_row_deserialization_schema._j_deserialization_schema\
                .deserialize(expected_csv.encode("utf-8"))
            self.assertTrue(j_row.equals(j_deserialized_row))

        field_assertion(Types.STRING(), "'123''4**'", "123'4*", ";")
        field_assertion(Types.STRING(), "'a;b''c'", "a;b'c", ";")
        field_assertion(Types.INT(), "12", 12, ";")

        test_j_row = JRow(2)
        test_j_row.setField(0, "1")
        test_j_row.setField(1, "hello")

        field_assertion(Types.ROW([Types.STRING(), Types.STRING()]), "'1:hello'", test_j_row, ";")
        test_j_row.setField(1, "hello world")
        field_assertion(Types.ROW([Types.STRING(), Types.STRING()]), "'1:hello world'", test_j_row,
                        ";")
        field_assertion(Types.STRING(), "null", "null", ";")


class PassThroughMapFunction(MapFunction):

    def map(self, value):
        return value


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
    test.assertListEqual(list(row['number_array']), [1, 2, 3])
    test.assertListEqual(list(row['boolean_array']), [True, False])
    test.assertListEqual(list(row['string_array']), ['a', 'b', 'c'])


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
