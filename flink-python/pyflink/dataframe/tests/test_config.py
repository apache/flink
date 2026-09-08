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
from pyflink.dataframe.dataframe_config import _DataFrameConfig
from pyflink.testing.test_case_utils import PyFlinkUTTestCase


class DataFrameConfigValidationTests(unittest.TestCase):
    def setUp(self):
        previous_environment = pf.get_table_environment()
        self.addCleanup(pf.set_table_environment, previous_environment)
        self.addCleanup(pf.config._buffered.clear)
        pf.set_table_environment(None)
        pf.config._buffered.clear()

    def test_config_is_a_dataframe_config_singleton(self):
        self.assertIsInstance(pf.config, _DataFrameConfig)
        self.assertNotIn("DataFrameConfig", pf.__all__)
        self.assertNotIn("_DataFrameConfig", pf.__all__)

    def test_public_type_hints_are_resolvable(self):
        self.assertEqual(
            get_type_hints(_DataFrameConfig.set),
            {
                "key": str,
                "value": str,
                "return": _DataFrameConfig,
            },
        )
        self.assertEqual(
            get_type_hints(_DataFrameConfig.get),
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

    def test_buffered_values_are_applied_to_the_lazily_created_environment(self):
        pf.config.set("pipeline.name", "lazy-name")

        created_environment = pf.get_or_create_table_environment()

        self.assertEqual(
            created_environment.get_config().get("pipeline.name", None), "lazy-name"
        )

    def test_table_creation_time_option_takes_effect(self):
        # The built-in catalog is chosen when the TableEnvironment is instantiated, so the
        # buffered value has to reach EnvironmentSettings rather than TableConfig afterwards.
        pf.config.set("table.builtin-catalog-name", "my_catalog")

        created_environment = pf.get_or_create_table_environment()

        self.assertEqual(created_environment.get_current_catalog(), "my_catalog")

    def test_set_is_rejected_while_an_environment_is_active(self):
        pf.set_table_environment(self.t_env)

        with self.assertRaisesRegex(RuntimeError, "before the table environment exists"):
            pf.config.set("pipeline.name", "too-late")

        self.assertEqual(pf.config._buffered, {})
        self.assertIsNone(self.t_env.get_config().get("pipeline.name", None))

    def test_set_is_allowed_again_after_the_environment_is_cleared(self):
        pf.set_table_environment(self.t_env)
        pf.set_table_environment(None)

        pf.config.set("pipeline.name", "after-clear")

        self.assertEqual(pf.config.get("pipeline.name"), "after-clear")

    def test_injecting_an_environment_is_rejected_when_values_are_buffered(self):
        pf.config.set("pipeline.name", "buffered-name")

        with self.assertRaisesRegex(RuntimeError, "buffered values"):
            pf.set_table_environment(self.t_env)

        self.assertIsNone(pf.get_table_environment())
        self.assertIsNone(self.t_env.get_config().get("pipeline.name", None))

    def test_injected_environment_is_not_modified(self):
        self.t_env.get_config().set("pipeline.name", "explicit")

        pf.set_table_environment(self.t_env)

        self.assertIs(pf.get_table_environment(), self.t_env)
        self.assertEqual(self.t_env.get_config().get("pipeline.name", None), "explicit")

    def test_clearing_the_environment_keeps_buffered_values(self):
        pf.config.set("pipeline.name", "kept")
        pf.get_or_create_table_environment()

        pf.set_table_environment(None)

        self.assertEqual(pf.config.get("pipeline.name"), "kept")
        created_environment = pf.get_or_create_table_environment()
        self.assertEqual(created_environment.get_config().get("pipeline.name", None), "kept")

    def test_get_reads_from_the_active_environment(self):
        pf.set_table_environment(self.t_env)
        self.t_env.get_config().set("pipeline.name", "from-environment")

        self.assertEqual(pf.config.get("pipeline.name"), "from-environment")

    def test_get_returns_default_when_missing_from_the_active_environment(self):
        pf.set_table_environment(self.t_env)

        self.assertEqual(pf.config.get("pipeline.name", "fallback"), "fallback")

    def test_runtime_mode_buffered_before_creation_takes_effect(self):
        # The planner is chosen when the environment is instantiated, so a buffered
        # runtime mode must be visible at creation time rather than applied afterwards.
        pf.config.set("execution.runtime-mode", "batch")

        created_environment = pf.get_or_create_table_environment()
        table = created_environment.from_elements([(1, "a"), (2, "b")], ["id", "name"])
        with table.execute().collect() as rows:
            self.assertEqual(len(list(rows)), 2)


if __name__ == "__main__":
    unittest.main()
