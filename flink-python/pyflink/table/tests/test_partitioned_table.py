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

from pyflink.table.expressions import col
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class PartitionedTableTests(PyFlinkStreamTableTestCase):
    """
    Tests built-in process table function calls through the Python Table API.
    """

    def test_partition_by_to_changelog(self):
        table = self.t_env.from_elements([(1, "a"), (2, "b")], ["id", "name"])

        result = table.partition_by(col("id")).to_changelog()

        rows = [tuple(row) for row in result.execute().collect()]
        self.assert_equals(rows, [(1, "INSERT", "a"), (2, "INSERT", "b")])

    def test_partition_by_from_changelog(self):
        table = self.t_env.from_elements(
            [(1, "INSERT", "a"), (2, "INSERT", "b"), (1, "DELETE", "a")], ["id", "op", "name"])

        result = table.partition_by(col("name")).from_changelog()

        rows = [(row.get_row_kind().name,) + tuple(row) for row in result.execute().collect()]
        self.assert_equals(
            rows, [("INSERT", "a", 1), ("INSERT", "b", 2), ("DELETE", "a", 1)])

    def test_order_by_from_changelog_schema(self):
        self.t_env.execute_sql(
            "CREATE TEMPORARY TABLE cdc_stream ("
            " id INT, op STRING, event_time TIMESTAMP(3),"
            " WATERMARK FOR event_time AS event_time"
            ") WITH ('connector' = 'datagen')")

        result = self.t_env.from_path("cdc_stream") \
            .partition_by(col("id")) \
            .order_by(col("event_time").asc) \
            .from_changelog()

        self.assertEqual(
            list(result.get_resolved_schema().get_column_names()), ["id", "event_time"])


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
