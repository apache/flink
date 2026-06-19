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
from pyflink.table import DataTypes, ResultKind
from pyflink.table.schema import Schema
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase


class TablePipelineTest(PyFlinkStreamTableTestCase):

    def test_table_pipeline_with_anonymous_sink(self):
        schema = Schema.new_builder().column("f0", DataTypes.STRING()).build()
        table = self.t_env.from_descriptor(
            TableDescriptor.for_connector("datagen")
            .option("number-of-rows", "10")
            .schema(schema)
            .build()
        )
        table_pipeline = table.insert_into(
            TableDescriptor.for_connector("blackhole").schema(schema).build()
        )
        self.assertEqual(table_pipeline.get_sink_identifier(), None)

    def test_table_pipeline_with_registered_sink(self):
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
        object_identifier = table_pipeline.get_sink_identifier()

        self.assertEqual(object_identifier.get_catalog_name(), "default_catalog")
        self.assertEqual(object_identifier.get_database_name(), "default_database")
        self.assertEqual(object_identifier.get_object_name(), "RegisteredSink")
        self.assertEqual(
            object_identifier.as_summary_string(),
            "default_catalog.default_database.RegisteredSink",
        )

    def test_table_pipeline_execute(self):
        schema = Schema.new_builder().column("f0", DataTypes.STRING()).build()
        table = self.t_env.from_descriptor(
            TableDescriptor.for_connector("datagen")
            .option("number-of-rows", "10")
            .schema(schema)
            .build()
        )
        self.t_env.create_temporary_table(
            "RegisteredSinkForExecute",
            TableDescriptor.for_connector("blackhole").schema(schema).build(),
        )
        table_pipeline = table.insert_into("RegisteredSinkForExecute")

        table_result = table_pipeline.execute()
        self.assertEqual(
            table_result.get_result_kind(), ResultKind.SUCCESS_WITH_CONTENT
        )
        self.assertEqual(
            table_result.get_table_schema().get_field_names()[0],
            "default_catalog.default_database.RegisteredSinkForExecute",
        )

    def test_table_pipeline_compile(self):
        schema = Schema.new_builder().column("f0", DataTypes.STRING()).build()
        table = self.t_env.from_descriptor(
            TableDescriptor.for_connector("datagen")
            .option("number-of-rows", "10")
            .schema(schema)
            .build()
        )
        self.t_env.create_temporary_table(
            "RegisteredSinkForPlanCompile",
            TableDescriptor.for_connector("blackhole").schema(schema).build(),
        )
        table_pipeline = table.insert_into("RegisteredSinkForPlanCompile")
        compiled_plan = table_pipeline.compile_plan()
        self.assertIsNotNone(compiled_plan.as_json_string())


if __name__ == "__main__":
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports")
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
