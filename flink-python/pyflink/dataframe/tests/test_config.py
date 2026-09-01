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

import unittest
from typing import get_type_hints, Optional

import pyflink.dataframe as pf
from pyflink.testing.test_case_utils import PyFlinkUTTestCase


class DataFrameConfigValidationTests(unittest.TestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.addCleanup(pf.config._buffered.clear)
        pf.set_table_environment(None)
        pf.config._buffered.clear()

    def test_config_is_a_dataframe_config_singleton(self):
        self.assertIsInstance(pf.config, pf.DataFrameConfig)

    def test_public_type_hints_are_resolvable(self):
        self.assertEqual(
            get_type_hints(pf.DataFrameConfig.set),
            {
                "key": str,
                "value": str,
                "return": pf.DataFrameConfig,
            },
        )
        self.assertEqual(
            get_type_hints(pf.DataFrameConfig.get),
            {
                "key": str,
                "default": Optional[str],
                "return": Optional[str],
            },
        )

    def test_set_rejects_non_string_key_without_buffering(self):
        with self.assertRaisesRegex(TypeError, "key must be a string"):
            pf.config.set(1, "value")

        self.assertEqual(pf.config._buffered, {})

    def test_set_rejects_non_string_value_without_buffering(self):
        with self.assertRaisesRegex(TypeError, "value must be a string"):
            pf.config.set("pipeline.name", 1)

        self.assertEqual(pf.config._buffered, {})

    def test_get_rejects_non_string_key(self):
        with self.assertRaisesRegex(TypeError, "key must be a string"):
            pf.config.get(1)

    def test_get_rejects_non_string_default(self):
        with self.assertRaisesRegex(TypeError, "default must be a string or None"):
            pf.config.get("pipeline.name", 1)

    def test_set_returns_the_config_for_chaining(self):
        result = pf.config.set("pipeline.name", "a").set("parallelism.default", "4")

        self.assertIs(result, pf.config)

    def test_buffered_value_is_returned_before_an_environment_exists(self):
        pf.config.set("pipeline.name", "buffered")

        self.assertEqual(pf.config.get("pipeline.name"), "buffered")

    def test_default_is_returned_when_not_buffered(self):
        self.assertIsNone(pf.config.get("pipeline.name"))
        self.assertEqual(pf.config.get("pipeline.name", "fallback"), "fallback")


class DataFrameConfigTests(PyFlinkUTTestCase):
    def setUp(self):
        super().setUp()
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.addCleanup(pf.config._buffered.clear)
        pf.set_table_environment(None)
        pf.config._buffered.clear()

    def test_buffered_values_are_applied_when_an_environment_is_injected(self):
        pf.config.set("pipeline.name", "buffered-name")

        pf.set_table_environment(self.t_env)

        self.assertEqual(
            self.t_env.get_config().get("pipeline.name", None), "buffered-name"
        )

    def test_buffered_values_are_applied_to_the_lazily_created_environment(self):
        pf.config.set("pipeline.name", "lazy-name")

        created_environment = pf.get_or_create_table_environment()

        self.assertEqual(
            created_environment.get_config().get("pipeline.name", None), "lazy-name"
        )

    def test_set_writes_through_to_the_active_environment(self):
        pf.set_table_environment(self.t_env)

        pf.config.set("pipeline.name", "write-through")

        self.assertEqual(
            self.t_env.get_config().get("pipeline.name", None), "write-through"
        )

    def test_get_reads_from_the_active_environment(self):
        pf.set_table_environment(self.t_env)
        self.t_env.get_config().set("pipeline.name", "from-environment")

        self.assertEqual(pf.config.get("pipeline.name"), "from-environment")

    def test_get_returns_default_when_missing_from_the_active_environment(self):
        pf.set_table_environment(self.t_env)

        self.assertEqual(pf.config.get("pipeline.name", "fallback"), "fallback")


if __name__ == "__main__":
    unittest.main()
