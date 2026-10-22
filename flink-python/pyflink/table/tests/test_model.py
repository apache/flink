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
#  limitations under the License.
################################################################################
from pyflink.common import Row
from pyflink.table import ModelDescriptor, Schema, DataTypes
from pyflink.table.catalog import ResolvedSchema
from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkUTTestCase


class AnonymousModelTests(PyFlinkUTTestCase):

    def test_anonymous_model_descriptor(self):
        descriptor = ModelDescriptor.for_provider("python-values") \
            .input_schema(Schema.new_builder()
                          .column("id", DataTypes.BIGINT())
                          .build()) \
            .output_schema(Schema.new_builder()
                           .column("x", DataTypes.STRING())
                           .column("y", DataTypes.INT())
                           .column("z", DataTypes.STRING())
                           .build()) \
            .build()
        registered_models = self.t_env.list_models()

        model = self.t_env.from_model(descriptor)

        self.assertEqual(list(model.get_resolved_input_schema().get_column_names()), ["id"])
        self.assertEqual(model.get_resolved_input_schema().get_column_data_types(),
                         [DataTypes.BIGINT()])
        self.assertEqual(list(model.get_resolved_output_schema().get_column_names()),
                         ["x", "y", "z"])
        self.assertEqual(model.get_resolved_output_schema().get_column_data_types(),
                         [DataTypes.STRING(), DataTypes.INT(), DataTypes.STRING()])

        self.assertEqual(self.t_env.list_models(), registered_models)


class ModelTest(PyFlinkStreamTableTestCase):

    def test_model_predict(self):
        # Model input: (id BIGINT), output: (x STRING, y INT, z STRING)
        # Factory generates output ("x" + id, id, "z" + id) based on input id
        model_descriptor = ModelDescriptor.for_provider("python-values") \
            .input_schema(Schema.new_builder()
                          .column("id", DataTypes.BIGINT())
                          .build()) \
            .output_schema(Schema.new_builder()
                           .column("x", DataTypes.STRING())
                           .column("y", DataTypes.INT())
                           .column("z", DataTypes.STRING())
                           .build()) \
            .build()

        self.t_env.create_temporary_model("my_model", model_descriptor, True)

        model = self.t_env.from_model("my_model")
        self.assertIsNotNone(model)

        data = [
            (1, 12, "Julian"),
            (2, 15, "Hello"),
            (3, 15, "Fabian"),
            (8, 11, "Hello world"),
            (9, 12, "Hello world!")
        ]

        schema = DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("len", DataTypes.INT()),
            DataTypes.FIELD("content", DataTypes.STRING())
        ])

        input_table = self.t_env.from_elements(data, schema)
        prediction_table = model.predict(input_table, ["id"])
        self.assertIsNotNone(prediction_table)
        result = [i for i in prediction_table.execute().collect()]
        expected = [Row(1, 12, 'Julian', 'x1', 1, 'z1'),
                    Row(2, 15, 'Hello', 'x2', 2, 'z2'),
                    Row(3, 15, 'Fabian', 'x3', 3, 'z3'),
                    Row(8, 11, 'Hello world', 'x8', 8, 'z8'),
                    Row(9, 12, 'Hello world!', 'x9', 9, 'z9')]
        self.assertEqual(expected, result)

    def test_model_predict_async(self):
        model_descriptor = ModelDescriptor.for_provider("python-values") \
            .input_schema(Schema.new_builder()
                          .column("id", DataTypes.BIGINT())
                          .build()) \
            .output_schema(Schema.new_builder()
                           .column("x", DataTypes.STRING())
                           .column("y", DataTypes.INT())
                           .column("z", DataTypes.STRING())
                           .build()) \
            .option("async", "true") \
            .build()

        self.t_env.create_temporary_model("my_async_model", model_descriptor, True)

        model = self.t_env.from_model("my_async_model")
        self.assertIsNotNone(model)

        data = [
            (1, 12, "Julian"),
            (2, 15, "Hello"),
            (3, 15, "Fabian"),
            (8, 11, "Hello world"),
            (9, 12, "Hello world!")
        ]

        schema = DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("len", DataTypes.INT()),
            DataTypes.FIELD("content", DataTypes.STRING())
        ])

        input_table = self.t_env.from_elements(data, schema)
        prediction_table = model.predict(input_table, ["id"])
        self.assertIsNotNone(prediction_table)
        result = [i for i in prediction_table.execute().collect()]
        expected = [Row(1, 12, 'Julian', 'x1', 1, 'z1'),
                    Row(2, 15, 'Hello', 'x2', 2, 'z2'),
                    Row(3, 15, 'Fabian', 'x3', 3, 'z3'),
                    Row(8, 11, 'Hello world', 'x8', 8, 'z8'),
                    Row(9, 12, 'Hello world!', 'x9', 9, 'z9')]
        # Async may return results in different order
        self.assertEqual(sorted(expected, key=lambda r: r[0]),
                         sorted(result, key=lambda r: r[0]))

    def test_model_predict_with_options(self):
        model_descriptor = ModelDescriptor.for_provider("python-values") \
            .input_schema(Schema.new_builder()
                          .column("id", DataTypes.BIGINT())
                          .build()) \
            .output_schema(Schema.new_builder()
                           .column("x", DataTypes.STRING())
                           .column("y", DataTypes.INT())
                           .column("z", DataTypes.STRING())
                           .build()) \
            .build()

        self.t_env.create_temporary_model("opt_model", model_descriptor, True)
        model = self.t_env.from_model(model_path="opt_model")

        data = [
            (1, 12, "Julian"),
            (2, 15, "Hello"),
            (3, 15, "Fabian"),
        ]

        schema = DataTypes.ROW([
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("len", DataTypes.INT()),
            DataTypes.FIELD("content", DataTypes.STRING())
        ])

        input_table = self.t_env.from_elements(data, schema)
        # Pass options (async=false is a no-op for sync model, just testing the API)
        prediction_table = model.predict(input_table, ["id"], {"async": "false"})
        self.assertIsNotNone(prediction_table)
        result = [i for i in prediction_table.execute().collect()]
        expected = [Row(1, 12, 'Julian', 'x1', 1, 'z1'),
                    Row(2, 15, 'Hello', 'x2', 2, 'z2'),
                    Row(3, 15, 'Fabian', 'x3', 3, 'z3')]
        self.assertEqual(expected, result)

    def test_model_schema(self):
        model_descriptor = ModelDescriptor.for_provider("python-values") \
            .input_schema(Schema.new_builder()
                          .column("id", DataTypes.BIGINT())
                          .build()) \
            .output_schema(Schema.new_builder()
                           .column("x", DataTypes.STRING())
                           .column("y", DataTypes.INT())
                           .column("z", DataTypes.STRING())
                           .build()) \
            .build()

        self.t_env.create_temporary_model("schema_model", model_descriptor, True)
        model = self.t_env.from_model("schema_model")

        # Test input schema
        input_schema = model.get_resolved_input_schema()
        self.assertIsNotNone(input_schema)
        self.assertIsInstance(input_schema, ResolvedSchema)

        # Test output schema
        output_schema = model.get_resolved_output_schema()
        self.assertIsNotNone(output_schema)
        self.assertIsInstance(output_schema, ResolvedSchema)


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
