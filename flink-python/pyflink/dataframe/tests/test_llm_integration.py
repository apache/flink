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

import unittest

import pyflink.dataframe as pf
from pyflink.common import Row
from pyflink.dataframe import llm
from pyflink.testing.test_case_utils import PyFlinkStreamDataFrameTestCase


class DataFrameLlmITCase(PyFlinkStreamDataFrameTestCase):

    def setUp(self):
        previous = dict(llm._provider_registry)
        self.addCleanup(llm._provider_registry.update, previous)
        self.addCleanup(llm._provider_registry.clear)
        self.addCleanup(setattr, llm, "_default_provider", llm._default_provider)
        llm._provider_registry.clear()
        llm._default_provider = None
        self.provider = pf.GenericProvider("python-values", {"async": "false", "latency": "0"})
        pf.set_model_provider("values", self.provider)
        self.output_type = pf.DataType.struct({
            "x": pf.DataType.string(),
            "y": pf.DataType.int32(),
            "z": pf.DataType.string(),
        })

    def test_sync_prediction_appends_typed_columns(self):
        registered_models = self.t_env.list_models()
        dataframe = pf.from_records([(1, "first"), (3, "third")], schema=["id", "content"])

        prediction = dataframe.llm.predict(
            "id", output_type=self.output_type, config={"async": "false"},
        )

        self.assertEqual(prediction.columns, ["id", "content", "x", "y", "z"])
        self.assertCountEqual(prediction.collect(), [
            Row(1, "first", "x1", 1, "z1"),
            Row(3, "third", "x3", 3, "z3"),
        ])
        self.assertEqual(self.t_env.list_models(), registered_models)

    def test_async_prediction_with_provider_override_and_runtime_config(self):
        dataframe = pf.from_records([(2, "second"), (4, "fourth")], schema=["id", "content"])

        prediction = dataframe.llm.predict(
            "id", provider="values", output_type=self.output_type,
            config={
                "async": "true",
                "output-mode": "allow_unordered",
                "max-concurrent-operations": "2",
                "timeout": "30 s",
            },
            **{"async": "true"},
        )
        selected = prediction.select("content", "x", (pf.col("y") + 1).alias("next_id"), "z")

        self.assertCountEqual(selected.collect(), [
            Row("second", "x2", 3, "z2"),
            Row("fourth", "x4", 5, "z4"),
        ])
        self.assertEqual(self.provider.to_options(), {"async": "false", "latency": "0"})


if __name__ == '__main__':
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
