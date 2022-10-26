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
import collections
import sys

from pyflink.table.descriptors import (Rowtime, Schema)
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import DataTypes
from pyflink.testing.test_case_utils import PyFlinkTestCase


class RowTimeDescriptorTests(PyFlinkTestCase):

    def test_timestamps_from_field(self):
        rowtime = Rowtime().timestamps_from_field("rtime")

        properties = rowtime.to_properties()
        expected = {'rowtime.timestamps.type': 'from-field', 'rowtime.timestamps.from': 'rtime'}
        self.assertEqual(expected, properties)

    def test_timestamps_from_source(self):
        rowtime = Rowtime().timestamps_from_source()

        properties = rowtime.to_properties()
        expected = {'rowtime.timestamps.type': 'from-source'}
        self.assertEqual(expected, properties)

    def test_timestamps_from_extractor(self):
        rowtime = Rowtime().timestamps_from_extractor(
            "org.apache.flink.table.utils.TestingDescriptors$CustomExtractor")

        properties = rowtime.to_properties()
        expected = {
            'rowtime.timestamps.type': 'custom',
            'rowtime.timestamps.class':
                'org.apache.flink.table.utils.TestingDescriptors$CustomExtractor',
            'rowtime.timestamps.serialized':
                'rO0ABXNyAD9vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnV0aWxzLlRlc3RpbmdEZXNjcmlwdG9ycyRDdXN0b2'
                '1FeHRyYWN0b3K-MntVKO8Z7QIAAUwABWZpZWxkdAASTGphdmEvbGFuZy9TdHJpbmc7eHIAPm9yZy5hcGFj'
                'aGUuZmxpbmsudGFibGUuc291cmNlcy50c2V4dHJhY3RvcnMuVGltZXN0YW1wRXh0cmFjdG9yX9WOqYhTbB'
                'gCAAB4cHQAAnRz'}
        self.assertEqual(expected, properties)

    def test_watermarks_periodic_ascending(self):
        rowtime = Rowtime().watermarks_periodic_ascending()

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'periodic-ascending'}
        self.assertEqual(expected, properties)

    def test_watermarks_periodic_bounded(self):
        rowtime = Rowtime().watermarks_periodic_bounded(1000)

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'periodic-bounded',
                    'rowtime.watermarks.delay': '1000'}
        self.assertEqual(expected, properties)

    def test_watermarks_from_source(self):
        rowtime = Rowtime().watermarks_from_source()

        properties = rowtime.to_properties()
        expected = {'rowtime.watermarks.type': 'from-source'}
        self.assertEqual(expected, properties)

    def test_watermarks_from_strategy(self):
        rowtime = Rowtime().watermarks_from_strategy(
            "org.apache.flink.table.utils.TestingDescriptors$CustomAssigner")

        properties = rowtime.to_properties()
        expected = {
            'rowtime.watermarks.type': 'custom',
            'rowtime.watermarks.class':
                'org.apache.flink.table.utils.TestingDescriptors$CustomAssigner',
            'rowtime.watermarks.serialized':
                'rO0ABXNyAD5vcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnV0aWxzLlRlc3RpbmdEZXNjcmlwdG9ycyRDdXN0b2'
                '1Bc3NpZ25lcsY_Xt96bBjDAgAAeHIAR29yZy5hcGFjaGUuZmxpbmsudGFibGUuc291cmNlcy53bXN0cmF0'
                'ZWdpZXMuUHVuY3R1YXRlZFdhdGVybWFya0Fzc2lnbmVygVHOe6GlrvQCAAB4cgA9b3JnLmFwYWNoZS5mbG'
                'luay50YWJsZS5zb3VyY2VzLndtc3RyYXRlZ2llcy5XYXRlcm1hcmtTdHJhdGVned57foNjlmk-AgAAeHA'}
        self.assertEqual(expected, properties)


class SchemaDescriptorTests(PyFlinkTestCase):

    def test_field(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("string_field", DataTypes.STRING())\
            .field("timestamp_field", DataTypes.TIMESTAMP(3))\
            .field("time_field", DataTypes.TIME())\
            .field("date_field", DataTypes.DATE())\
            .field("double_field", DataTypes.DOUBLE())\
            .field("float_field", DataTypes.FLOAT())\
            .field("byte_field", DataTypes.TINYINT())\
            .field("short_field", DataTypes.SMALLINT())\
            .field("boolean_field", DataTypes.BOOLEAN())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

    def test_fields(self):
        fields = collections.OrderedDict([
            ("int_field", DataTypes.INT()),
            ("long_field", DataTypes.BIGINT()),
            ("string_field", DataTypes.STRING()),
            ("timestamp_field", DataTypes.TIMESTAMP(3)),
            ("time_field", DataTypes.TIME()),
            ("date_field", DataTypes.DATE()),
            ("double_field", DataTypes.DOUBLE()),
            ("float_field", DataTypes.FLOAT()),
            ("byte_field", DataTypes.TINYINT()),
            ("short_field", DataTypes.SMALLINT()),
            ("boolean_field", DataTypes.BOOLEAN())
        ])

        schema = Schema().fields(fields)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

        if sys.version_info[:2] <= (3, 5):
            fields = {
                "int_field": DataTypes.INT(),
                "long_field": DataTypes.BIGINT(),
                "string_field": DataTypes.STRING(),
                "timestamp_field": DataTypes.TIMESTAMP(3),
                "time_field": DataTypes.TIME(),
                "date_field": DataTypes.DATE(),
                "double_field": DataTypes.DOUBLE(),
                "float_field": DataTypes.FLOAT(),
                "byte_field": DataTypes.TINYINT(),
                "short_field": DataTypes.SMALLINT(),
                "boolean_field": DataTypes.BOOLEAN()
            }
            self.assertRaises(TypeError, Schema().fields, fields)

    def test_field_in_string(self):
        schema = Schema()\
            .field("int_field", 'INT')\
            .field("long_field", 'BIGINT')\
            .field("string_field", 'VARCHAR')\
            .field("timestamp_field", 'SQL_TIMESTAMP')\
            .field("time_field", 'SQL_TIME')\
            .field("date_field", 'SQL_DATE')\
            .field("double_field", 'DOUBLE')\
            .field("float_field", 'FLOAT')\
            .field("byte_field", 'TINYINT')\
            .field("short_field", 'SMALLINT')\
            .field("boolean_field", 'BOOLEAN')

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR',
                    'schema.3.name': 'timestamp_field',
                    'schema.3.data-type': 'TIMESTAMP(3)',
                    'schema.4.name': 'time_field',
                    'schema.4.data-type': 'TIME(0)',
                    'schema.5.name': 'date_field',
                    'schema.5.data-type': 'DATE',
                    'schema.6.name': 'double_field',
                    'schema.6.data-type': 'DOUBLE',
                    'schema.7.name': 'float_field',
                    'schema.7.data-type': 'FLOAT',
                    'schema.8.name': 'byte_field',
                    'schema.8.data-type': 'TINYINT',
                    'schema.9.name': 'short_field',
                    'schema.9.data-type': 'SMALLINT',
                    'schema.10.name': 'boolean_field',
                    'schema.10.data-type': 'BOOLEAN'}
        self.assertEqual(expected, properties)

    def test_from_origin_field(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT()).from_origin_field("origin_field_a")\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.1.from': 'origin_field_a',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_proctime(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("ptime", DataTypes.BIGINT()).proctime()\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'ptime',
                    'schema.1.data-type': 'BIGINT',
                    'schema.1.proctime': 'true',
                    'schema.2.name': 'string_field',
                    'schema.2.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_rowtime(self):
        schema = Schema()\
            .field("int_field", DataTypes.INT())\
            .field("long_field", DataTypes.BIGINT())\
            .field("rtime", DataTypes.BIGINT())\
            .rowtime(
                Rowtime().timestamps_from_field("long_field").watermarks_periodic_bounded(5000))\
            .field("string_field", DataTypes.STRING())

        properties = schema.to_properties()
        print(properties)
        expected = {'schema.0.name': 'int_field',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'long_field',
                    'schema.1.data-type': 'BIGINT',
                    'schema.2.name': 'rtime',
                    'schema.2.data-type': 'BIGINT',
                    'schema.2.rowtime.timestamps.type': 'from-field',
                    'schema.2.rowtime.timestamps.from': 'long_field',
                    'schema.2.rowtime.watermarks.type': 'periodic-bounded',
                    'schema.2.rowtime.watermarks.delay': '5000',
                    'schema.3.name': 'string_field',
                    'schema.3.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)

    def test_schema(self):
        table_schema = TableSchema(["a", "b"], [DataTypes.INT(), DataTypes.STRING()])

        schema = Schema().schema(table_schema)

        properties = schema.to_properties()
        expected = {'schema.0.name': 'a',
                    'schema.0.data-type': 'INT',
                    'schema.1.name': 'b',
                    'schema.1.data-type': 'VARCHAR(2147483647)'}
        self.assertEqual(expected, properties)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
