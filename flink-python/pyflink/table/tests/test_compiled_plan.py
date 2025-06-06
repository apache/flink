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
import os.path
import re
from pathlib import Path

from pyflink.table import Schema, DataTypes, TableDescriptor, PlanReference
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkTestCase

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_PLAN_DIR = os.path.join(THIS_DIR, "jsonplan")


def _replace_flink_version(plan: str) -> str:
    """
    Ignore the value for the Flink Version in the compiled plan.
    """
    return re.sub(r'"flinkVersion"\s*:\s*"[\w.-]*"', r'"flinkVersion" : ""', plan)


def _replace_exec_node_id(plan: str) -> str:
    """
    Ignore id/source/target node ids, as these increment between test runs.
    """
    id = re.sub(r'"id"\s*:\s*\d+', r'"id" : 0', plan)
    source = re.sub(r'"source"\s*:\s*\d+', r'"source" : 0', id)
    target = re.sub(r'"target"\s*:\s*\d+', r'"target" : 0', source)
    return target


class CompiledPlanTest(PyFlinkStreamTableTestCase, PyFlinkTestCase):

    def test_compile_plan_sql(self):
        src = """
        CREATE TABLE MyTable (a BIGINT, b INT, c VARCHAR)
        WITH ('connector' = 'datagen', 'number-of-rows' = '5')
        """
        self.t_env.execute_sql(src)

        sink = """
        CREATE TABLE MySink (a BIGINT, b INT, c VARCHAR)
        WITH ('connector' = 'blackhole')
        """
        self.t_env.execute_sql(sink)

        compiled_plan = self.t_env.compile_plan_sql("INSERT INTO MySink SELECT * FROM MyTable")
        with open(os.path.join(JSON_PLAN_DIR, "testGetJsonPlan.out")) as file:
            expected = file.read()

        self.maxDiff = None
        self.assertEqual(
            _replace_exec_node_id(_replace_flink_version(compiled_plan.as_json_string())), expected
        )

    def test_write_load_compiled_plan(self):
        schema = Schema.new_builder().column("f0", DataTypes.STRING()).build()
        table = self.t_env.from_descriptor(
            TableDescriptor.for_connector("datagen")
            .option("number-of-rows", "10")
            .schema(schema)
            .build()
        )
        self.t_env.create_temporary_table(
            "RegisteredSink",
            TableDescriptor.for_connector("blackhole").schema(schema).build(),
        )
        table_pipeline = table.insert_into("RegisteredSink")
        compiled_plan = table_pipeline.compile_plan()

        plan_path = Path(self.tempdir + "/plan.out")
        compiled_plan.write_to_file(plan_path)

        plan_reference_from_file = PlanReference.from_file(plan_path)
        compiled_plan_from_file = self.t_env.load_plan(plan_reference_from_file)
        self.assertEqual(compiled_plan.as_json_string(), compiled_plan_from_file.as_json_string())

        with open(plan_path) as file:
            content = file.read()
            plan_reference_from_string = PlanReference.from_json_string(content)
            compiled_plan_from_string = self.t_env.load_plan(plan_reference_from_string)
            self.assertEqual(
                compiled_plan.as_json_string(), compiled_plan_from_string.as_json_string()
            )


if __name__ == "__main__":
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports")
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
