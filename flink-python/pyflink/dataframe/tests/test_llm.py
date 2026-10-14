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

import json
import unittest
from types import MappingProxyType
from unittest.mock import Mock, call, patch, sentinel

import pyflink.dataframe as pf
from pyflink.dataframe import llm
from pyflink.table import DataTypes
from pyflink.testing.test_case_utils import PyFlinkDataFrameUTTestCase


class ProviderOverrideTests(unittest.TestCase):
    def test_builtin_overrides_merge_without_mutating_provider(self):
        provider = pf.OpenAIProvider(
            "https://example.test/chat/completions", "key", temperature=0.7,
            options={"top-p": "0.9", "seed": "42", "future-option": "old"},
        )
        configured = provider.to_options()
        overrides = {
            "temperature": 0.2,
            "options": MappingProxyType({"top-p": "0.3", "seed": None}),
        }

        self.assertEqual(llm._provider_options(provider, overrides), {
            "endpoint": "https://example.test/chat/completions", "api-key": "key",
            "temperature": "0.2", "top-p": "0.3", "future-option": "old",
        })
        self.assertEqual(provider.to_options(), configured)
        self.assertEqual(overrides["options"], {"top-p": "0.3", "seed": None})
        self.assertNotIn("top-p", llm._provider_options(provider, {"top_p": None}))

    def test_none_removes_inherited_parameters_or_raw_options(self):
        provider = pf.TritonProvider(
            "http://localhost:8000", timeout="10 s", flatten_batch_dim=False,
            options={"priority": "2", "future-option": "value"},
        )
        self.assertEqual(llm._provider_options(provider, {"timeout": None}), {
            "endpoint": "http://localhost:8000", "flatten-batch-dim": "false",
            "priority": "2", "future-option": "value",
        })
        self.assertEqual(llm._provider_options(provider, {"options": None}), {
            "endpoint": "http://localhost:8000", "timeout": "10 s",
            "flatten-batch-dim": "false",
        })
        self.assertEqual(llm._provider_options(provider, {"options": {}}), provider.to_options())

    def test_builtin_overrides_validate_typed_values_and_raw_conflicts(self):
        provider = pf.OpenAIProvider("https://example.test/chat/completions", "key")
        for overrides, error in [
            ({"unknown": "value"}, TypeError),
            ({"temperature": True}, TypeError),
            ({"top_p": 1.1}, ValueError),
            ({"endpoint": None}, TypeError),
            ({"api_key": None}, TypeError),
            ({"options": []}, TypeError),
            ({"options": {1: "value"}}, TypeError),
            ({"options": {"seed": 42}}, TypeError),
            ({"options": {"api-key": None}}, TypeError),
            ({"options": {"api-key": " "}}, ValueError),
            ({"options": {"endpoint": None}}, TypeError),
            ({"temperature": 0.2, "options": {"temperature": "0.3"}}, ValueError),
        ]:
            with self.subTest(overrides=overrides):
                with self.assertRaises(error):
                    llm._provider_options(provider, overrides)
        self.assertEqual(llm._provider_options(provider, {
            "temperature": 0.2, "options": {"temperature": "0.2"},
        })["temperature"], "0.2")

    def test_triton_overrides_preserve_typed_parameters_and_copy_headers(self):
        headers = {"X-Request": "original"}
        provider = pf.TritonProvider(
            "http://localhost:8000", custom_headers=headers, flatten_batch_dim=False,
            max_retries=2, compression="gzip")
        headers["X-Request"] = "changed"

        options = llm._provider_options(provider, {"max_retries": 0})

        self.assertEqual(options["max-retries"], "0")
        self.assertEqual(options["flatten-batch-dim"], "false")
        self.assertEqual(json.loads(options["custom-headers"]), {"X-Request": "original"})
        self.assertEqual(provider.to_options()["max-retries"], "2")
        overridden = llm._provider_options(provider, {"custom_headers": {"X-Request": "new"}})
        self.assertEqual(json.loads(overridden["custom-headers"]), {"X-Request": "new"})
        self.assertNotIn("custom-headers", llm._provider_options(
            provider, {"custom_headers": None}))
        for value in (None, " "):
            with self.subTest(endpoint=value):
                with self.assertRaises((TypeError, ValueError)):
                    llm._provider_options(provider, {"options": {"endpoint": value}})

    def test_custom_provider_keeps_no_argument_serialization_contract(self):
        class CustomProvider(pf.ModelProvider):
            def provider_identifier(self):
                return "custom"

            def to_options(self):
                return {"endpoint": "original", "seed": "42"}

        for provider in (CustomProvider(), pf.GenericProvider(
                "custom", endpoint="original", seed="42")):
            with self.subTest(provider=type(provider).__name__):
                self.assertEqual(llm._provider_options(provider, {
                    "endpoint": "override", "seed": None, "new-option": "value",
                }), {"endpoint": "override", "new-option": "value"})
                self.assertEqual(provider.to_options(), {"endpoint": "original", "seed": "42"})
                with self.assertRaises(TypeError):
                    llm._provider_options(provider, {"seed": 1})


class DataFramePredictTests(unittest.TestCase):
    def setUp(self):
        previous = dict(llm._provider_registry)
        self.addCleanup(llm._provider_registry.update, previous)
        self.addCleanup(llm._provider_registry.clear)
        self.addCleanup(setattr, llm, "_default_provider", llm._default_provider)
        llm._provider_registry.clear()
        llm._default_provider = None
        self.provider = pf.OpenAIProvider(
            "https://example.test/chat/completions", "key", model="configured",
            response_format="text",
        )
        pf.set_model_provider("chat", self.provider)
        self.table = Mock()
        resolved = self.table.get_resolved_schema.return_value
        resolved.get_column_names.return_value = ["id", "prompt"]
        resolved.get_column_data_types.return_value = [DataTypes.BIGINT(), DataTypes.STRING()]
        j_column_types = resolved._j_resolved_schema.getColumnDataTypes.return_value
        j_column_types.get.side_effect = resolved.get_column_data_types.return_value.__getitem__
        conversion_patch = patch.object(
            llm, "_from_java_data_type", side_effect=lambda data_type: data_type)
        self.addCleanup(conversion_patch.stop)
        conversion_patch.start()
        self.dataframe = pf.DataFrame(self.table)
        self.model = self.table._t_env.from_model.return_value
        self.model.predict.return_value = sentinel.prediction
        self.builders = []

        def schema_builder():
            builder = Mock()
            self.builders.append(builder)
            return builder

        builder_patch = patch.object(llm.Schema, "new_builder", side_effect=schema_builder)
        self.addCleanup(builder_patch.stop)
        builder_patch.start()
        self.descriptor = Mock()
        self.descriptor.input_schema.return_value = self.descriptor
        self.descriptor.output_schema.return_value = self.descriptor
        descriptor_patch = patch.object(
            llm.ModelDescriptor, "for_provider", return_value=self.descriptor)
        self.addCleanup(descriptor_patch.stop)
        self.for_provider = descriptor_patch.start()

    def test_predict_forwards_input_order_runtime_config_and_model_precedence(self):
        self.assertIsInstance(self.dataframe.llm, llm.LLMAccessor)
        resolved = self.table.get_resolved_schema.return_value
        resolved.get_column_names.return_value += [f"metadata_{i}" for i in range(1000)]
        resolved.get_column_data_types.return_value += [DataTypes.STRING()] * 1000
        provider = pf.TritonProvider(
            "http://localhost:8000", model_name="configured", options={"provider": "other"})
        pf.set_model_provider("triton", provider)
        pf.set_default_model_provider("chat")
        config = MappingProxyType({"timeout": "30 s", "custom-runtime-option": "value"})

        result = self.dataframe.llm.predict(
            "prompt", "id", provider="triton", model="selected", config=config,
            model_name="overridden", timeout="10 s",
        )

        self.assertIs(result._table, sentinel.prediction)
        self.for_provider.assert_called_once_with("triton")
        self.assertEqual(self.builders[0].column.call_args_list, [
            call("prompt", DataTypes.STRING()), call("id", DataTypes.BIGINT()),
        ])
        self.assertEqual(self.builders[1].column.call_args_list,
                         [call("output", DataTypes.STRING())])
        self.table._t_env.from_model.assert_called_once_with(self.descriptor.build.return_value)
        self.model.predict.assert_called_once_with(self.table, ["prompt", "id"], dict(config))
        self.assertEqual(dict(args.args for args in self.descriptor.option.call_args_list), {
            "endpoint": "http://localhost:8000", "model-name": "selected", "timeout": "10 s",
        })
        self.assertEqual(provider.to_options()["model-name"], "configured")
        self.assertIs(llm._resolve_provider(), self.provider)

    def test_predict_rejects_invalid_inputs_models_and_config_before_planning(self):
        for columns, kwargs, error in [
            ((), {}, ValueError),
            ((1,), {}, TypeError),
            ((" ",), {}, ValueError),
            (("prompt", "prompt"), {}, ValueError),
            (("missing",), {}, ValueError),
            (("prompt",), {"model": ""}, ValueError),
            (("prompt",), {"provider": "missing"}, ValueError),
            (("prompt",), {"config": []}, TypeError),
            (("prompt",), {"config": {"async": True}}, TypeError),
            (("prompt",), {"config": {1: "value"}}, TypeError),
            (("prompt",), {"output_type": DataTypes.STRING()}, TypeError),
        ]:
            with self.subTest(columns=columns, kwargs=kwargs):
                with self.assertRaises(error):
                    self.dataframe.llm.predict(*columns, **kwargs)
        self.assertEqual(self.builders, [])
        self.table._t_env.from_model.assert_not_called()

    def test_predict_rejects_empty_duplicate_and_colliding_output_fields(self):
        for fields in ({}, [("x", pf.DataType.string()), ("x", pf.DataType.int32())],
                       {" ": pf.DataType.string()}, {"prompt": pf.DataType.string()}):
            with self.subTest(fields=fields):
                with self.assertRaises(ValueError):
                    self.dataframe.llm.predict("prompt", output_type=pf.DataType.struct(fields))
        self.table.get_resolved_schema.return_value.get_column_names.return_value = [
            "prompt", "output",
        ]
        with self.assertRaisesRegex(ValueError, "conflict with existing columns"):
            self.dataframe.llm.predict("prompt")
        self.assertEqual(self.builders, [])

    def test_default_output_is_string_for_all_providers(self):
        pf.set_model_provider("other-task", pf.OpenAIProvider(
            "https://example.test/embeddings", "key"))
        pf.set_model_provider("triton", pf.TritonProvider("http://localhost:8000"))
        pf.set_model_provider("generic", pf.GenericProvider("custom"))
        for provider in ("chat", "other-task", "triton", "generic"):
            with self.subTest(provider=provider):
                self.dataframe.llm.predict("prompt", provider=provider)
                self.assertEqual(self.builders[-1].column.call_args_list,
                                 [call("output", DataTypes.STRING())])

    def test_prediction_declares_native_output_types_and_preserves_nested_fields(self):
        pf.set_model_provider("custom", pf.GenericProvider("custom"))
        pf.set_model_provider("embeddings", pf.OpenAIProvider(
            "https://example.test/embeddings", "key", model="embedding"))
        nested = pf.DataType.struct({"label": pf.DataType.string()})
        for provider, output_type, fields in [
            ("custom", None, [("output", DataTypes.STRING())]),
            ("custom", pf.DataType.int32(), [("output", DataTypes.INT())]),
            ("custom", pf.DataType(DataTypes.VARCHAR(5)), [("output", DataTypes.VARCHAR(5))]),
            ("custom", pf.DataType.string().not_null(),
             [("output", DataTypes.STRING().not_null())]),
            ("custom", pf.DataType.struct({"answer": nested, "score": pf.DataType.int32()}),
             [("answer", nested._to_table_data_type()), ("score", DataTypes.INT())]),
            ("embeddings", pf.DataType.list(pf.DataType.float32()),
             [("output", DataTypes.ARRAY(DataTypes.FLOAT()))]),
        ]:
            with self.subTest(output_type=output_type):
                self.descriptor.reset_mock()
                self.dataframe.llm.predict("prompt", provider=provider, output_type=output_type)
                self.assertEqual(self.builders[-1].column.call_args_list,
                                 [call(name, dtype) for name, dtype in fields])
                options = dict(args.args for args in self.descriptor.option.call_args_list)
                self.assertNotIn("response-format", options)

    def test_prediction_preserves_inherited_and_explicit_response_formats(self):
        for overrides, response_format in [
            ({}, "text"), ({"response_format": "json_object"}, "json_object"),
        ]:
            with self.subTest(overrides=overrides):
                self.descriptor.reset_mock()
                self.dataframe.llm.predict(
                    "prompt", output_type=pf.DataType.int32(), **overrides)
                options = dict(args.args for args in self.descriptor.option.call_args_list)
                self.assertEqual(options["response-format"], response_format)
        self.assertEqual(self.provider.to_options()["response-format"], "text")


class DataFramePredictSchemaTests(PyFlinkDataFrameUTTestCase):
    def setUp(self):
        super().setUp()
        previous = dict(llm._provider_registry)
        self.addCleanup(llm._provider_registry.update, previous)
        self.addCleanup(llm._provider_registry.clear)
        self.addCleanup(setattr, llm, "_default_provider", llm._default_provider)
        llm._provider_registry.clear()
        llm._default_provider = None
        pf.set_model_provider(pf.GenericProvider("python-values"))
        self.dataframe = pf.from_records([(1, "question")], schema=["id", "prompt"])

    def test_prediction_preserves_input_columns_and_is_not_registered(self):
        registered_models = self.t_env.list_models()
        prediction = self.dataframe.llm.predict("id", "prompt")

        self.assert_dataframe_schema(prediction, ["id", "prompt", "output"], [
            DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING(),
        ])
        self.assertEqual(self.t_env.list_models(), registered_models)

    def test_sql_row_is_one_column_and_struct_expands_only_top_level(self):
        nested = DataTypes.ROW([DataTypes.FIELD("label", DataTypes.STRING())])
        for output_type, names, types in [
            ("ROW<label STRING>", ["output"], [nested]),
            (pf.DataType.struct({"details": pf.DataType(nested), "score": pf.DataType.int32()}),
             ["details", "score"], [nested, DataTypes.INT()]),
        ]:
            with self.subTest(output_type=output_type):
                prediction = self.dataframe.llm.predict("id", output_type=output_type)
                self.assert_dataframe_schema(prediction, ["id", "prompt"] + names, [
                    DataTypes.BIGINT(), DataTypes.STRING(),
                ] + types)


if __name__ == '__main__':
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
