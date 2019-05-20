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


class StreamTableColumnsOperationTests(PyFlinkStreamTableTestCase):

    def test_add_columns(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.select("a").add_columns("a + 1 as b, a + 2 as c")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,2,3', '2,3,4']
        self.assert_equals(actual, expected)

    def test_add_or_replace_columns(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["b", "a"]
        field_types = [DataTypes.BIGINT(), DataTypes.BIGINT()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.select("a").add_or_replace_columns("a + 1 as b, a + 2 as a")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['3,2', '4,3']
        self.assert_equals(actual, expected)

    def test_rename_columns(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["d", "e", "f"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.select("a, b, c").rename_columns("a as d, c as f, b as e").select("d, e, f")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['1,Hi,Hello', '2,Hello,Hello']
        self.assert_equals(actual, expected)

    def test_drop_columns(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello')], ['a', 'b', 'c'])
        field_names = ["b"]
        field_types = [DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestAppendSink())

        result = t.select("a, b, c").drop_columns("a, c").select("b")
        result.insert_into("Results")
        t_env.execute()
        actual = source_sink_utils.results()

        expected = ['Hi', 'Hello']
        self.assert_equals(actual, expected)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
