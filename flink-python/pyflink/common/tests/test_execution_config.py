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
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import (ExecutionConfig, Configuration)
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.util.java_utils import get_j_env_configuration


class ExecutionConfigTests(PyFlinkTestCase):

    def setUp(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.execution_config = self.env.get_config()

    def test_constant(self):
        gateway = get_gateway()
        JExecutionConfig = gateway.jvm.org.apache.flink.api.common.ExecutionConfig

        self.assertEqual(ExecutionConfig.PARALLELISM_DEFAULT, JExecutionConfig.PARALLELISM_DEFAULT)
        self.assertEqual(ExecutionConfig.PARALLELISM_UNKNOWN, JExecutionConfig.PARALLELISM_UNKNOWN)

    def test_get_set_closure_cleaner(self):

        self.assertTrue(self.execution_config.is_closure_cleaner_enabled())

        self.execution_config.disable_closure_cleaner()

        self.assertFalse(self.execution_config.is_closure_cleaner_enabled())

        self.execution_config.enable_closure_cleaner()

        self.assertTrue(self.execution_config.is_closure_cleaner_enabled())

    def test_get_set_auto_watermark_interval(self):

        self.assertEqual(self.execution_config.get_auto_watermark_interval(), 200)

        self.execution_config.set_auto_watermark_interval(1000)

        self.assertEqual(self.execution_config.get_auto_watermark_interval(), 1000)

    def test_get_set_parallelism(self):

        self.execution_config.set_parallelism(8)

        self.assertEqual(self.execution_config.get_parallelism(), 8)

        self.execution_config.set_parallelism(4)

        self.assertEqual(self.execution_config.get_parallelism(), 4)

    def test_get_set_max_parallelism(self):

        self.execution_config.set_max_parallelism(12)

        self.assertEqual(self.execution_config.get_max_parallelism(), 12)

        self.execution_config.set_max_parallelism(16)

        self.assertEqual(self.execution_config.get_max_parallelism(), 16)

    def test_get_set_task_cancellation_interval(self):

        self.assertEqual(self.execution_config.get_task_cancellation_interval(), 30000)

        self.execution_config.set_task_cancellation_interval(1000)

        self.assertEqual(self.execution_config.get_task_cancellation_interval(), 1000)

    def test_get_set_task_cancellation_timeout(self):

        self.assertEqual(self.execution_config.get_task_cancellation_timeout(), 180000)

        self.execution_config.set_task_cancellation_timeout(3000)

        self.assertEqual(self.execution_config.get_task_cancellation_timeout(), 3000)

    def test_disable_enable_auto_generated_uids(self):

        self.execution_config.disable_auto_generated_uids()

        self.assertFalse(self.execution_config.has_auto_generated_uids_enabled())

        self.execution_config.enable_auto_generated_uids()

        self.assertTrue(self.execution_config.has_auto_generated_uids_enabled())

    def test_disable_enable_object_reuse(self):

        self.execution_config.disable_object_reuse()

        self.assertFalse(self.execution_config.is_object_reuse_enabled())

        self.execution_config.enable_object_reuse()

        self.assertTrue(self.execution_config.is_object_reuse_enabled())

    def test_get_set_global_job_parameters(self):

        self.execution_config.set_global_job_parameters({"hello": "world"})

        self.assertEqual(self.execution_config.get_global_job_parameters(), {"hello": "world"})

    def test_get_set_use_snapshot_compression(self):

        self.execution_config.set_use_snapshot_compression(False)

        self.assertFalse(self.execution_config.is_use_snapshot_compression())

        self.execution_config.set_use_snapshot_compression(True)

        self.assertTrue(self.execution_config.is_use_snapshot_compression())

    def test_equals_and_hash(self):

        config1 = StreamExecutionEnvironment.get_execution_environment().get_config()

        config2 = StreamExecutionEnvironment.get_execution_environment().get_config()

        self.assertEqual(config1, config2)

        self.assertEqual(hash(config1), hash(config2))

        config1.set_parallelism(12)
        config2.set_parallelism(11)

        self.assertNotEqual(config1, config2)

        # it is allowed for hashes to be equal even if objects are not

        config2.set_parallelism(12)

        self.assertEqual(config1, config2)

        self.assertEqual(hash(config1), hash(config2))

    def test_get_execution_environment_with_config(self):
        configuration = Configuration()
        configuration.set_integer('parallelism.default', 12)
        configuration.set_string('pipeline.name', 'haha')
        env = StreamExecutionEnvironment.get_execution_environment(configuration)
        execution_config = env.get_config()

        self.assertEqual(execution_config.get_parallelism(), 12)
        config = Configuration(
            j_configuration=get_j_env_configuration(env._j_stream_execution_environment))
        self.assertEqual(config.get_string('pipeline.name', ''), 'haha')
