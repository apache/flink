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
from pyflink.java_gateway import get_gateway

from pyflink.table import EnvironmentSettings
from pyflink.testing.test_case_utils import PyFlinkTestCase, get_private_field


class EnvironmentSettingsTests(PyFlinkTestCase):

    def test_planner_selection(self):

        gateway = get_gateway()

        CLASS_NAME = gateway.jvm.EnvironmentSettings.CLASS_NAME

        builder = EnvironmentSettings.new_instance()

        OLD_PLANNER_FACTORY = get_private_field(builder._j_builder, "OLD_PLANNER_FACTORY")
        OLD_EXECUTOR_FACTORY = get_private_field(builder._j_builder, "OLD_EXECUTOR_FACTORY")
        BLINK_PLANNER_FACTORY = get_private_field(builder._j_builder, "BLINK_PLANNER_FACTORY")
        BLINK_EXECUTOR_FACTORY = get_private_field(builder._j_builder, "BLINK_EXECUTOR_FACTORY")

        # test the default behaviour to make sure it is consistent with the python doc
        envrionment_settings = builder.build()

        self.assertEqual(
            envrionment_settings._j_environment_settings.toPlannerProperties()[CLASS_NAME],
            BLINK_PLANNER_FACTORY)

        self.assertEqual(
            envrionment_settings._j_environment_settings.toExecutorProperties()[CLASS_NAME],
            BLINK_EXECUTOR_FACTORY)

        # test use_old_planner
        envrionment_settings = builder.use_old_planner().build()

        self.assertEqual(
            envrionment_settings._j_environment_settings.toPlannerProperties()[CLASS_NAME],
            OLD_PLANNER_FACTORY)

        self.assertEqual(
            envrionment_settings._j_environment_settings.toExecutorProperties()[CLASS_NAME],
            OLD_EXECUTOR_FACTORY)

        # test use_blink_planner
        envrionment_settings = builder.use_blink_planner().build()

        self.assertEqual(
            envrionment_settings._j_environment_settings.toPlannerProperties()[CLASS_NAME],
            BLINK_PLANNER_FACTORY)

        self.assertEqual(
            envrionment_settings._j_environment_settings.toExecutorProperties()[CLASS_NAME],
            BLINK_EXECUTOR_FACTORY)

        # test use_any_planner
        envrionment_settings = builder.use_any_planner().build()

        self.assertTrue(
            CLASS_NAME not in envrionment_settings._j_environment_settings.toPlannerProperties())

        self.assertTrue(
            CLASS_NAME not in envrionment_settings._j_environment_settings.toExecutorProperties())

    def test_mode_selection(self):

        builder = EnvironmentSettings.new_instance()

        # test the default behaviour to make sure it is consistent with the python doc
        envrionment_settings = builder.build()

        self.assertTrue(envrionment_settings.is_streaming_mode())

        # test in_streaming_mode
        envrionment_settings = builder.in_streaming_mode().build()

        self.assertTrue(envrionment_settings.is_streaming_mode())

        # test in_batch_mode
        envrionment_settings = builder.in_batch_mode().build()

        self.assertFalse(envrionment_settings.is_streaming_mode())

    def test_with_built_in_catalog_name(self):

        gateway = get_gateway()

        DEFAULT_BUILTIN_CATALOG = gateway.jvm.EnvironmentSettings.DEFAULT_BUILTIN_CATALOG

        builder = EnvironmentSettings.new_instance()

        # test the default behaviour to make sure it is consistent with the python doc
        envrionment_settings = builder.build()

        self.assertEqual(envrionment_settings.get_built_in_catalog_name(), DEFAULT_BUILTIN_CATALOG)

        envrionment_settings = builder.with_built_in_catalog_name("my_catalog").build()

        self.assertEqual(envrionment_settings.get_built_in_catalog_name(), "my_catalog")

    def test_with_built_in_database_name(self):

        gateway = get_gateway()

        DEFAULT_BUILTIN_DATABASE = gateway.jvm.EnvironmentSettings.DEFAULT_BUILTIN_DATABASE

        builder = EnvironmentSettings.new_instance()

        # test the default behaviour to make sure it is consistent with the python doc
        envrionment_settings = builder.build()

        self.assertEqual(envrionment_settings.get_built_in_database_name(),
                         DEFAULT_BUILTIN_DATABASE)

        envrionment_settings = builder.with_built_in_database_name("my_database").build()

        self.assertEqual(envrionment_settings.get_built_in_database_name(), "my_database")
