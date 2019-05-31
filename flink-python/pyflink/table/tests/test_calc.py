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

from pyflink.table import DataTypes, Row
from pyflink.table.tests.test_types import ExamplePoint, PythonOnlyPoint, ExamplePointUDT, \
    PythonOnlyUDT
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableCalcTests(PyFlinkStreamTableTestCase):

    def test_select(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        t.select("a + 1, b, c") \
            .insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,hi,hello', '3,hi,hello']
        self.assert_equals(actual, expected)

    def test_alias(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.alias("d, e, f").select("d, e, f")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,Hi,Hello', '2,Hi,Hello']
        self.assert_equals(actual, expected)

    def test_where(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.where("a > 1 && b = 'Hello'")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_filter(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.filter("a > 1 && b = 'Hello'")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_from_element(self):
        t_env = self.t_env
        a = array.array('b')
        a.fromstring('ABCD')
        t = t_env.from_elements(
            [(1, 1.0, "hi", "hello", datetime.date(1970, 1, 2), datetime.time(1, 0, 0),
             datetime.datetime(1970, 1, 2, 0, 0), array.array("d", [1]), ["abc"],
             [datetime.date(1970, 1, 2)], Decimal(1), Row("a", "b")(1, 2.0),
             {"key": 1.0}, a, ExamplePoint(1.0, 2.0),
             PythonOnlyPoint(3.0, 4.0))])
        field_names = ["a", "b", "c", "d", "e", "f", "g", "h",
                       "i", "j", "k", "l", "m", "n", "o", "p"]
        field_types = [DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.STRING(),
                       DataTypes.STRING(), DataTypes.DATE(),
                       DataTypes.TIME(),
                       DataTypes.TIMESTAMP(),
                       DataTypes.ARRAY(DataTypes.DOUBLE()),
                       DataTypes.ARRAY(DataTypes.STRING()),
                       DataTypes.ARRAY(DataTypes.DATE()),
                       DataTypes.DECIMAL(),
                       DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT()),
                                      DataTypes.FIELD("b", DataTypes.DOUBLE())]),
                       DataTypes.MAP(DataTypes.VARCHAR(), DataTypes.DOUBLE()),
                       DataTypes.VARBINARY(), ExamplePointUDT(),
                       PythonOnlyUDT()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        t.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,1.0,hi,hello,1970-01-02,01:00:00,1970-01-02 00:00:00.0,[1.0],[abc],'
                    '[1970-01-02],1,1,2.0,{key=1.0},[65, 66, 67, 68],[1.0, 2.0],[3.0, 4.0]']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
