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

from pyflink.table.types import DataTypes
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableJoinTests(PyFlinkStreamTableTestCase):

    def test_join_without_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (3, "Flink")], ['d', 'e'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.join(t2, "a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_join_with_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (3, "Flink")], ['d', 'e'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.join(t2).where("a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_left_outer_join_without_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (3, "Flink")], ['d', 'e'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.left_outer_join(t2, "a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,null', '2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_left_outer_join_with_where(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (3, "Flink")], ['d', 'e'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.left_outer_join(t2).where("a = d").select("a, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '3,HelloFlink']
        self.assert_equals(actual, expected)

    def test_right_outer_join(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (4, "Flink")], ['d', 'e'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.right_outer_join(t2, "a = d").select("d, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['2,HiFlink', '3,HelloPython', '4,null']
        self.assert_equals(actual, expected)

    def test_full_outer_join(self):
        t_env = self.t_env
        t1 = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hi", "Hello"), (3, "Hello", "Hello")],
                                 ['a', 'b', 'c'])
        t2 = t_env.from_elements([(2, "Flink"), (3, "Python"), (4, "Flink")], ['d', 'e'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t1.full_outer_join(t2, "a = d").select("a, d, b + e")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,null,null', '2,2,HiFlink', '3,3,HelloPython', 'null,4,null']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
