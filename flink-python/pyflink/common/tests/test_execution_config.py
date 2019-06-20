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
from pyflink.dataset import ExecutionEnvironment
from pyflink.common import (ExecutionConfig, RestartStrategies, ExecutionMode,
                            InputDependencyConstraint)
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ExecutionConfigTests(PyFlinkTestCase):

    def setUp(self):
        self.env = ExecutionEnvironment.get_execution_environment()
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

        self.assertEqual(self.execution_config.get_auto_watermark_interval(), 0)

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

        self.assertEqual(self.execution_config.get_task_cancellation_interval(), -1)

        self.execution_config.set_task_cancellation_interval(1000)

        self.assertEqual(self.execution_config.get_task_cancellation_interval(), 1000)

    def test_get_set_task_cancellation_timeout(self):

        self.assertEqual(self.execution_config.get_task_cancellation_timeout(), -1)

        self.execution_config.set_task_cancellation_timeout(3000)

        self.assertEqual(self.execution_config.get_task_cancellation_timeout(), 3000)

    def test_get_set_restart_strategy(self):

        self.execution_config.set_restart_strategy(RestartStrategies.no_restart())

        self.assertEqual(self.execution_config.get_restart_strategy(),
                         RestartStrategies.no_restart())

        self.execution_config.set_restart_strategy(
            RestartStrategies.failure_rate_restart(5, 10000, 5000))

        self.assertIsInstance(self.execution_config.get_restart_strategy(),
                              RestartStrategies.FailureRateRestartStrategyConfiguration)

        self.execution_config.set_restart_strategy(RestartStrategies.fixed_delay_restart(4, 10000))

        self.assertIsInstance(self.execution_config.get_restart_strategy(),
                              RestartStrategies.FixedDelayRestartStrategyConfiguration)

        self.execution_config.set_restart_strategy(RestartStrategies.fall_back_restart())

        self.assertEqual(self.execution_config.get_restart_strategy(),
                         RestartStrategies.fall_back_restart())

    def test_get_set_execution_mode(self):

        self.execution_config.set_execution_mode(ExecutionMode.BATCH)

        self.assertEqual(self.execution_config.get_execution_mode(), ExecutionMode.BATCH)

        self.execution_config.set_execution_mode(ExecutionMode.PIPELINED)

        self.assertEqual(self.execution_config.get_execution_mode(), ExecutionMode.PIPELINED)

        self.execution_config.set_execution_mode(ExecutionMode.BATCH_FORCED)

        self.assertEqual(self.execution_config.get_execution_mode(), ExecutionMode.BATCH_FORCED)

        self.execution_config.set_execution_mode(ExecutionMode.PIPELINED_FORCED)

        self.assertEqual(self.execution_config.get_execution_mode(), ExecutionMode.PIPELINED_FORCED)

    def test_get_set_default_input_dependency_constraint(self):

        self.execution_config.set_default_input_dependency_constraint(
            InputDependencyConstraint.ALL)

        self.assertEqual(self.execution_config.get_default_input_dependency_constraint(),
                         InputDependencyConstraint.ALL)

        self.execution_config.set_default_input_dependency_constraint(
            InputDependencyConstraint.ANY)

        self.assertEqual(self.execution_config.get_default_input_dependency_constraint(),
                         InputDependencyConstraint.ANY)

    def test_disable_enable_force_kryo(self):

        self.execution_config.disable_force_kryo()

        self.assertFalse(self.execution_config.is_force_kryo_enabled())

        self.execution_config.enable_force_kryo()

        self.assertTrue(self.execution_config.is_force_kryo_enabled())

    def test_disable_enable_generic_types(self):

        self.execution_config.disable_generic_types()

        self.assertTrue(self.execution_config.has_generic_types_disabled())

        self.execution_config.enable_generic_types()

        self.assertFalse(self.execution_config.has_generic_types_disabled())

    def test_disable_enable_auto_generated_uids(self):

        self.execution_config.disable_auto_generated_uids()

        self.assertFalse(self.execution_config.has_auto_generated_uids_enabled())

        self.execution_config.enable_auto_generated_uids()

        self.assertTrue(self.execution_config.has_auto_generated_uids_enabled())

    def test_disable_enable_force_avro(self):

        self.execution_config.disable_force_avro()

        self.assertFalse(self.execution_config.is_force_avro_enabled())

        self.execution_config.enable_force_avro()

        self.assertTrue(self.execution_config.is_force_avro_enabled())

    def test_disable_enable_object_reuse(self):

        self.execution_config.disable_object_reuse()

        self.assertFalse(self.execution_config.is_object_reuse_enabled())

        self.execution_config.enable_object_reuse()

        self.assertTrue(self.execution_config.is_object_reuse_enabled())

    def test_disable_enable_sysout_logging(self):

        self.execution_config.disable_sysout_logging()

        self.assertFalse(self.execution_config.is_sysout_logging_enabled())

        self.execution_config.enable_sysout_logging()

        self.assertTrue(self.execution_config.is_sysout_logging_enabled())

    def test_get_set_global_job_parameters(self):

        self.execution_config.set_global_job_parameters({"hello": "world"})

        self.assertEqual(self.execution_config.get_global_job_parameters(), {"hello": "world"})

    def test_add_default_kryo_serializer(self):

        self.execution_config.add_default_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.execution_config.get_default_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_type_with_kryo_serializer(self):

        self.execution_config.register_type_with_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.execution_config.get_registered_types_with_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_pojo_type(self):

        self.execution_config.register_pojo_type(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

        type_list = self.execution_config.get_registered_pojo_types()

        self.assertEqual(type_list,
                         ["org.apache.flink.runtime.state.StateBackendTestBase$TestPojo"])

    def test_register_kryo_type(self):

        self.execution_config.register_kryo_type(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

        type_list = self.execution_config.get_registered_kryo_types()

        self.assertEqual(type_list,
                         ["org.apache.flink.runtime.state.StateBackendTestBase$TestPojo"])

    def test_auto_type_registration(self):

        self.assertFalse(self.execution_config.is_auto_type_registration_disabled())

        self.execution_config.disable_auto_type_registration()

        self.assertTrue(self.execution_config.is_auto_type_registration_disabled())

    def test_get_set_use_snapshot_compression(self):

        self.execution_config.set_use_snapshot_compression(False)

        self.assertFalse(self.execution_config.is_use_snapshot_compression())

        self.execution_config.set_use_snapshot_compression(True)

        self.assertTrue(self.execution_config.is_use_snapshot_compression())

    def test_equals_and_hash(self):

        config1 = ExecutionEnvironment.get_execution_environment().get_config()

        config2 = ExecutionEnvironment.get_execution_environment().get_config()

        self.assertEqual(config1, config2)

        self.assertEqual(hash(config1), hash(config2))

        config1.set_parallelism(12)

        self.assertNotEqual(config1, config2)

        self.assertNotEqual(hash(config1), hash(config2))

        config2.set_parallelism(12)

        self.assertEqual(config1, config2)

        self.assertEqual(hash(config1), hash(config2))
