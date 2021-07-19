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
from pyflink.table import DataTypes
from pyflink.table.expressions import call_sql
from pyflink.table.schema import Schema
from pyflink.testing.test_case_utils import PyFlinkTestCase


class SchemaTest(PyFlinkTestCase):
    def test_schema_basic(self):
        old_schema = Schema.new_builder() \
            .from_row_data_type(DataTypes.ROW(
                [DataTypes.FIELD("a", DataTypes.TINYINT()),
                 DataTypes.FIELD("b", DataTypes.SMALLINT()),
                 DataTypes.FIELD("c", DataTypes.INT())])) \
            .from_fields(["d", "e"], [DataTypes.STRING(), DataTypes.BOOLEAN()]) \
            .build()
        self.schema = Schema.new_builder() \
            .from_schema(old_schema) \
            .primary_key_named("primary_constraint", "id") \
            .column("id", DataTypes.INT().not_null()) \
            .column("counter", DataTypes.INT().not_null()) \
            .column("payload", "ROW<name STRING, age INT, flag BOOLEAN>") \
            .column_by_metadata("topic", DataTypes.STRING(), None, True) \
            .column_by_expression("ts", call_sql("orig_ts - INTERVAL '60' MINUTE")) \
            .column_by_metadata("orig_ts", DataTypes.TIMESTAMP(3), "timestamp") \
            .watermark("ts", "ts - INTERVAL '5' SECOND") \
            .column_by_expression("proctime", "PROCTIME()") \
            .build()
        self.assertEqual("""(
  `a` TINYINT,
  `b` SMALLINT,
  `c` INT,
  `d` STRING,
  `e` BOOLEAN,
  `id` INT NOT NULL,
  `counter` INT NOT NULL,
  `payload` [ROW<name STRING, age INT, flag BOOLEAN>],
  `topic` METADATA VIRTUAL,
  `ts` AS [orig_ts - INTERVAL '60' MINUTE],
  `orig_ts` METADATA FROM 'timestamp',
  `proctime` AS [PROCTIME()],
  WATERMARK FOR `ts` AS [ts - INTERVAL '5' SECOND],
  CONSTRAINT `primary_constraint` PRIMARY KEY (`id`) NOT ENFORCED
)""", str(self.schema))


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
