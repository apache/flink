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

import datetime

from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, PartitionedTable, TableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ProcessTableFunctionAPITests(PyFlinkTestCase):

    @staticmethod
    def _create_table_environment():
        table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        function_prefix = (
            "org.apache.flink.table.runtime.operators.python.process."
            "TestJavaProcessTableFunctions$")
        table_env.create_java_temporary_system_function(
            "java_row_ptf", function_prefix + "RowSemanticFunction")
        table_env.create_java_temporary_system_function(
            "java_multi_ptf", function_prefix + "MultiInputFunction")
        return table_env

    @staticmethod
    def _create_input(table_env):
        return table_env.from_elements(
            [("Alice", 1, datetime.datetime(2026, 1, 1, 0, 0, 0))],
            DataTypes.ROW([
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.INT()),
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
            ]))

    def test_table_process_calls_registered_java_function(self):
        table_env = self._create_table_environment()
        orders = self._create_input(table_env)

        result = orders.process("java_row_ptf", lit(42).as_argument("increment"))

        self.assertEqual(["out"], result.get_schema().get_field_names())
        self.assertIn("ProcessTableFunction", result.explain())
        with result.execute().collect() as rows:
            self.assertEqual([Row("Alice:42")], list(rows))

    def test_from_call_accepts_multiple_partitioned_table_arguments(self):
        table_env = self._create_table_environment()
        orders = self._create_input(table_env)
        profiles = self._create_input(table_env)

        partitioned_orders = orders.partition_by(col("name"))
        self.assertIsInstance(partitioned_orders, PartitionedTable)
        ordered_orders = partitioned_orders.order_by(col("ts"))
        self.assertIsInstance(ordered_orders, PartitionedTable)
        result = table_env.from_call(
            "java_multi_ptf",
            partitioned_orders.as_argument("in1"),
            profiles.partition_by(col("name")).as_argument("in2"),
        )

        self.assertEqual(["name", "name0", "out"], result.get_schema().get_field_names())
        self.assertIn("ProcessTableFunction", result.explain())

    def test_process_requires_registered_function_name(self):
        table_env = self._create_table_environment()
        orders = self._create_input(table_env)

        with self.assertRaisesRegex(TypeError, "registered name"):
            orders.process(object())
        with self.assertRaisesRegex(TypeError, "registered name"):
            orders.partition_by(col("name")).process(object())
        with self.assertRaisesRegex(TypeError, "registered name"):
            table_env.from_call(object(), orders.as_argument("in1"))


if __name__ == '__main__':
    import unittest
    unittest.main()
