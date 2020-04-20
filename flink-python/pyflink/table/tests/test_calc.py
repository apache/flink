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

import array
import datetime
from decimal import Decimal

from pyflink.table import DataTypes, Row, BatchTableEnvironment, EnvironmentSettings
from pyflink.table.tests.test_types import ExamplePoint, PythonOnlyPoint, ExamplePointUDT, \
    PythonOnlyUDT
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableCalcTests(PyFlinkStreamTableTestCase):

    def test_select(self):
        t = self.t_env.from_elements([(1, 'hi', 'hello')], ['a', 'b', 'c'])
        result = t.select("a + 1, b, c")
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual('[plus(a, 1), b, c]',
                         query_operation.getProjectList().toString())

    def test_alias(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.alias("d, e, f").select("d, e, f")
        table_schema = result._j_table.getQueryOperation().getTableSchema()
        self.assertEqual(['d', 'e', 'f'], list(table_schema.getFieldNames()))

    def test_where(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.where("a > 1 && b = 'Hello'")
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual("and("
                         "greaterThan(a, 1), "
                         "equals(b, 'Hello'))",
                         query_operation.getCondition().toString())

    def test_filter(self):
        t = self.t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c'])
        result = t.filter("a > 1 && b = 'Hello'")
        query_operation = result._j_table.getQueryOperation()
        self.assertEqual("and("
                         "greaterThan(a, 1), "
                         "equals(b, 'Hello'))",
                         query_operation.getCondition().toString())

    def test_from_element(self):
        t_env = self.t_env
        field_names = ["a", "b", "c", "d", "e", "f", "g", "h",
                       "i", "j", "k", "l", "m", "n", "o", "p", "q", "r"]
        field_types = [DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.STRING(),
                       DataTypes.STRING(), DataTypes.DATE(),
                       DataTypes.TIME(),
                       DataTypes.TIMESTAMP(3),
                       DataTypes.INTERVAL(DataTypes.SECOND(3)),
                       DataTypes.ARRAY(DataTypes.DOUBLE()),
                       DataTypes.ARRAY(DataTypes.DOUBLE(False)),
                       DataTypes.ARRAY(DataTypes.STRING()),
                       DataTypes.ARRAY(DataTypes.DATE()),
                       DataTypes.DECIMAL(38, 18),
                       DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.DOUBLE())]),
                       DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()),
                       DataTypes.BYTES(), ExamplePointUDT(),
                       PythonOnlyUDT()]
        schema = DataTypes.ROW(
            list(map(lambda field_name, field_type: DataTypes.FIELD(field_name, field_type),
                 field_names,
                 field_types)))
        table_sink = source_sink_utils.TestAppendSink(field_names, field_types)
        t_env.register_table_sink("Results", table_sink)
        t = t_env.from_elements(
            [(1, 1.0, "hi", "hello", datetime.date(1970, 1, 2), datetime.time(1, 0, 0),
              datetime.datetime(1970, 1, 2, 0, 0),
              datetime.timedelta(days=1, microseconds=10),
              [1.0, None], array.array("d", [1.0, 2.0]),
              ["abc"], [datetime.date(1970, 1, 2)], Decimal(1), Row("a", "b")(1, 2.0),
              {"key": 1.0}, bytearray(b'ABCD'), ExamplePoint(1.0, 2.0),
              PythonOnlyPoint(3.0, 4.0))],
            schema)
        t.insert_into("Results")
        t_env.execute("test")
        actual = source_sink_utils.results()

        expected = ['1,1.0,hi,hello,1970-01-02,01:00:00,1970-01-02 00:00:00.0,'
                    '86400000,[1.0, null],[1.0, 2.0],[abc],[1970-01-02],'
                    '1,1,2.0,{key=1.0},[65, 66, 67, 68],[1.0, 2.0],[3.0, 4.0]']
        self.assert_equals(actual, expected)

    def test_blink_from_element(self):
        t_env = BatchTableEnvironment.create(environment_settings=EnvironmentSettings
                                             .new_instance().use_blink_planner()
                                             .in_batch_mode().build())
        field_names = ["a", "b", "c", "d", "e", "f", "g", "h",
                       "i", "j", "k", "l", "m", "n", "o", "p", "q"]
        field_types = [DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.STRING(),
                       DataTypes.STRING(), DataTypes.DATE(),
                       DataTypes.TIME(),
                       DataTypes.TIMESTAMP(3),
                       DataTypes.INTERVAL(DataTypes.SECOND(3)),
                       DataTypes.ARRAY(DataTypes.DOUBLE()),
                       DataTypes.ARRAY(DataTypes.DOUBLE(False)),
                       DataTypes.ARRAY(DataTypes.STRING()),
                       DataTypes.ARRAY(DataTypes.DATE()),
                       DataTypes.DECIMAL(38, 18),
                       DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.DOUBLE())]),
                       DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()),
                       DataTypes.BYTES(),
                       PythonOnlyUDT()]
        schema = DataTypes.ROW(
            list(map(lambda field_name, field_type: DataTypes.FIELD(field_name, field_type),
                 field_names,
                 field_types)))
        table_sink = source_sink_utils.TestAppendSink(field_names, field_types)
        t_env.register_table_sink("Results", table_sink)
        t = t_env.from_elements(
            [(1, 1.0, "hi", "hello", datetime.date(1970, 1, 2), datetime.time(1, 0, 0),
              datetime.datetime(1970, 1, 2, 0, 0),
              datetime.timedelta(days=1, microseconds=10),
              [1.0, None], array.array("d", [1.0, 2.0]),
              ["abc"], [datetime.date(1970, 1, 2)], Decimal(1), Row("a", "b")(1, 2.0),
              {"key": 1.0}, bytearray(b'ABCD'),
              PythonOnlyPoint(3.0, 4.0))],
            schema)
        t.insert_into("Results")
        t_env.execute("test")
        actual = source_sink_utils.results()

        expected = ['1,1.0,hi,hello,1970-01-02,01:00:00,1970-01-02 00:00:00.0,'
                    '86400000,[1.0, null],[1.0, 2.0],[abc],[1970-01-02],'
                    '1.000000000000000000,1,2.0,{key=1.0},[65, 66, 67, 68],[3.0, 4.0]']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
