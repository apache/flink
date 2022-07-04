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
import os
import tempfile
import unittest

from py4j.java_gateway import java_import

from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.file_system import FileSource
from pyflink.datastream.formats.avro import Schema as AvroSchema
from pyflink.datastream.formats.parquet import AvroParquetReaders
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


@unittest.skipIf(os.environ.get('HADOOP_CLASSPATH') is None,
                 'Some Hadoop lib is needed for Parquet-Avro format tests')
class FileSourceParquetAvroFormatTests(PyFlinkStreamingTestCase):

    def setUp(self):
        assert os.environ.get('HADOOP_CLASSPATH') is not None, 'Hadoop is needed for this test'
        super().setUp()
        self.test_sink = DataStreamTestSinkFunction()
        self._import_avro_classes()

    def test_avro_parquet_basic(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        record_schema = """
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
        schema = AvroSchema.parse_string(record_schema)
        records = [self._create_basic_avro_record(schema, True, 0, 1, 2, 3, 's1'),
                   self._create_basic_avro_record(schema, False, 4, 5, 6, 7, 's2')]
        self._create_avro_parquet_file(parquet_file_name, schema, records)

        self._build_avro_parquet_job(schema, parquet_file_name)
        self.env.execute("test_avro_parquet_basic")
        results = self.test_sink.get_results(True, False)
        result1 = results[0]
        result2 = results[1]
        self.assertEqual(result1['null'], None)
        self.assertEqual(result1['boolean'], True)
        self.assertEqual(result1['int'], 0)
        self.assertEqual(result1['long'], 1)
        self.assertAlmostEqual(result1['float'], 2, delta=1e-3)
        self.assertAlmostEqual(result1['double'], 3, delta=1e-3)
        self.assertEqual(result1['string'], 's1')
        self.assertEqual(result2['null'], None)
        self.assertEqual(result2['boolean'], False)
        self.assertEqual(result2['int'], 4)
        self.assertEqual(result2['long'], 5)
        self.assertAlmostEqual(result2['float'], 6, delta=1e-3)
        self.assertAlmostEqual(result2['double'], 7, delta=1e-3)
        self.assertEqual(result2['string'], 's2')

    def test_avro_parquet_enum(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        record_schema = """
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
        schema = AvroSchema.parse_string(record_schema)
        records = [self._create_enum_avro_record(schema, 'SPADES'),
                   self._create_enum_avro_record(schema, 'DIAMONDS')]
        self._create_avro_parquet_file(parquet_file_name, schema, records)

        self._build_avro_parquet_job(schema, parquet_file_name)
        self.env.execute("test_avro_record_enum")
        results = self.test_sink.get_results(True, False)
        self.assertEqual(results[0]['suit'], 'SPADES')
        self.assertEqual(results[1]['suit'], 'DIAMONDS')

    def test_avro_parquet_union(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        record_schema = """
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
        schema = AvroSchema.parse_string(record_schema)
        records = [self._create_union_avro_record(schema, 1),
                   self._create_union_avro_record(schema, 2.),
                   self._create_union_avro_record(schema, None)]
        self._create_avro_parquet_file(parquet_file_name, schema, records)

        self._build_avro_parquet_job(schema, parquet_file_name)
        self.env.execute("test_avro_record_union")
        results = self.test_sink.get_results(True, False)
        self.assertEqual(results[0]['union'], 1)
        self.assertAlmostEqual(results[1]['union'], 2.0, delta=1e-3)
        self.assertEqual(results[2]['union'], None)

    def test_avro_parquet_array(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        # It seems there's bug when array item record contains only one field, which throws
        # java.lang.ClassCastException: required ... is not a group when reading
        record_schema = """
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
        schema = AvroSchema.parse_string(record_schema)
        records = [self._create_array_avro_record(schema, [(1, 2.), (3, 4.)]),
                   self._create_array_avro_record(schema, [(5, 6.), (7, 8.)])]
        self._create_avro_parquet_file(parquet_file_name, schema, records)

        self._build_avro_parquet_job(schema, parquet_file_name)
        self.env.execute("test_avro_record_array")
        results = self.test_sink.get_results(True, False)
        result1 = results[0]
        result2 = results[1]
        self.assertEqual(result1['array'][0]['int'], 1)
        self.assertAlmostEqual(result1['array'][0]['double'], 2., delta=1e-3)
        self.assertEqual(result1['array'][1]['int'], 3)
        self.assertAlmostEqual(result1['array'][1]['double'], 4., delta=1e-3)
        self.assertEqual(result2['array'][0]['int'], 5)
        self.assertAlmostEqual(result2['array'][0]['double'], 6., delta=1e-3)
        self.assertEqual(result2['array'][1]['int'], 7)
        self.assertAlmostEqual(result2['array'][1]['double'], 8., delta=1e-3)

    def test_avro_parquet_map(self):
        parquet_file_name = tempfile.mktemp(suffix='.parquet', dir=self.tempdir)
        record_schema = """
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
        schema = AvroSchema.parse_string(record_schema)
        records = [self._create_map_avro_record(schema, {'a': 1, 'b': 2}),
                   self._create_map_avro_record(schema, {'c': 3, 'd': 4})]
        self._create_avro_parquet_file(parquet_file_name, schema, records)

        self._build_avro_parquet_job(schema, parquet_file_name)
        self.env.execute("test_avro_record_map")
        results = self.test_sink.get_results(True, False)
        result1 = results[0]
        result2 = results[1]
        self.assertEqual(result1['map']['a'], 1)
        self.assertEqual(result1['map']['b'], 2)
        self.assertEqual(result2['map']['c'], 3)
        self.assertEqual(result2['map']['d'], 4)

    def _build_avro_parquet_job(self, record_schema, parquet_file_name):
        ds = self.env.from_source(
            FileSource.for_record_stream_format(
                AvroParquetReaders.for_generic_record(record_schema),
                parquet_file_name
            ).build(),
            WatermarkStrategy.for_monotonous_timestamps(),
            "parquet-source"
        )
        ds.map(self.PassThroughMapFunction()).add_sink(self.test_sink)

    class PassThroughMapFunction(MapFunction):

        def map(self, value):
            return value

    @staticmethod
    def _import_avro_classes():
        jvm = get_gateway().jvm
        classes = ['org.apache.avro.generic.GenericData']
        prefix = 'org.apache.flink.avro.shaded.'
        for cls in classes:
            java_import(jvm, prefix + cls)

    @staticmethod
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

    @staticmethod
    def _create_enum_avro_record(schema: AvroSchema, enum_value):
        jvm = get_gateway().jvm
        j_record = jvm.GenericData.Record(schema._j_schema)
        j_record.put('suit', enum_value)
        return j_record

    @staticmethod
    def _create_union_avro_record(schema, union_value):
        jvm = get_gateway().jvm
        j_record = jvm.GenericData.Record(schema._j_schema)
        j_record.put('union', union_value)
        return j_record

    @staticmethod
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

    @staticmethod
    def _create_map_avro_record(schema, map: dict):
        jvm = get_gateway().jvm
        j_record = jvm.GenericData.Record(schema._j_schema)
        j_map = jvm.java.util.HashMap()
        for k, v in map.items():
            j_map.put(k, v)
        j_record.put('map', j_map)
        return j_record

    @staticmethod
    def _create_avro_parquet_file(file_path: str, schema: AvroSchema, records: list):
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
