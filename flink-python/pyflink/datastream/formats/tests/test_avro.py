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

from avro.datafile import DataFileReader
from avro.io import DatumReader
from py4j.java_gateway import JavaObject, java_import

from pyflink.datastream import MapFunction
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.formats.avro import AvroSchema, GenericRecordAvroTypeInfo, \
    AvroBulkWriters, AvroInputFormat
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkStreamingTestCase


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
            self.avro_dir_name, AvroBulkWriters.for_generic_record(schema)
        ).build()
        ds.map(lambda e: e, output_type=GenericRecordAvroTypeInfo(schema)).sink_to(sink)

    def _read_avro_file(self) -> List[dict]:
        records = []
        for file in glob.glob(os.path.join(os.path.join(self.avro_dir_name, '**/*'))):
            for record in DataFileReader(open(file, 'rb'), DatumReader()):
                records.append(record)
        return records


class PassThroughMapFunction(MapFunction):

    def map(self, value):
        return value


def _import_avro_classes():
    jvm = get_gateway().jvm
    classes = ['org.apache.avro.generic.GenericData']
    prefix = 'org.apache.flink.avro.shaded.'
    for cls in classes:
        java_import(jvm, prefix + cls)


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
