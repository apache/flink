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
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import DataTypes
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class StreamTableSchemaTests(PyFlinkStreamTableTestCase):

    def test_print_schema(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello'), (2, 'Hello', 'Hello')],
                                ['a', 'b', 'c'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t.group_by("c").select("a.sum, c as b")
        result.print_schema()

    def test_get_schema(self):
        t_env = self.t_env
        t = t_env.from_elements([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hello'), (2, 'Hello', 'Hello')],
                                ['a', 'b', 'c'])
        field_names = ["a", "b"]
        field_types = [DataTypes.BIGINT(), DataTypes.STRING()]
        t_env.register_table_sink(
            "Results",
            field_names, field_types, source_sink_utils.TestRetractSink())

        result = t.group_by("c").select("a.sum as a, c as b")
        schema = result.get_schema()

        assert schema == TableSchema(["a", "b"], [DataTypes.BIGINT(), DataTypes.STRING()])


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
