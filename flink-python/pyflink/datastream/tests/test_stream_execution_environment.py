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
import os
import tempfile
import json

import unittest

from pyflink.common import ExecutionConfig, RestartStrategies
from pyflink.datastream import (StreamExecutionEnvironment, CheckpointConfig,
                                CheckpointingMode, MemoryStateBackend, TimeCharacteristic)
from pyflink.table import DataTypes, CsvTableSource, CsvTableSink, StreamTableEnvironment
from pyflink.testing.test_case_utils import PyFlinkTestCase


class StreamExecutionEnvironmentTests(PyFlinkTestCase):

    def setUp(self):
        self.env = StreamExecutionEnvironment.get_execution_environment()

    def test_get_config(self):
        execution_config = self.env.get_config()

        self.assertIsInstance(execution_config, ExecutionConfig)

    def test_get_set_parallelism(self):

        self.env.set_parallelism(10)

        parallelism = self.env.get_parallelism()

        self.assertEqual(parallelism, 10)

    def test_get_set_buffer_timeout(self):

        self.env.set_buffer_timeout(12000)

        timeout = self.env.get_buffer_timeout()

        self.assertEqual(timeout, 12000)

    def test_get_set_default_local_parallelism(self):

        self.env.set_default_local_parallelism(8)

        parallelism = self.env.get_default_local_parallelism()

        self.assertEqual(parallelism, 8)

    def test_set_get_restart_strategy(self):

        self.env.set_restart_strategy(RestartStrategies.no_restart())

        restart_strategy = self.env.get_restart_strategy()

        self.assertEqual(restart_strategy, RestartStrategies.no_restart())

    def test_add_default_kryo_serializer(self):

        self.env.add_default_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.env.get_config().get_default_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_type_with_kryo_serializer(self):

        self.env.register_type_with_kryo_serializer(
            "org.apache.flink.runtime.state.StateBackendTestBase$TestPojo",
            "org.apache.flink.runtime.state.StateBackendTestBase$CustomKryoTestSerializer")

        class_dict = self.env.get_config().get_registered_types_with_kryo_serializer_classes()

        self.assertEqual(class_dict,
                         {'org.apache.flink.runtime.state.StateBackendTestBase$TestPojo':
                          'org.apache.flink.runtime.state'
                          '.StateBackendTestBase$CustomKryoTestSerializer'})

    def test_register_type(self):

        self.env.register_type("org.apache.flink.runtime.state.StateBackendTestBase$TestPojo")

        type_list = self.env.get_config().get_registered_pojo_types()

        self.assertEqual(type_list,
                         ['org.apache.flink.runtime.state.StateBackendTestBase$TestPojo'])

    def test_get_set_max_parallelism(self):

        self.env.set_max_parallelism(12)

        parallelism = self.env.get_max_parallelism()

        self.assertEqual(parallelism, 12)

    def test_operation_chaining(self):

        self.assertTrue(self.env.is_chaining_enabled())

        self.env.disable_operator_chaining()

        self.assertFalse(self.env.is_chaining_enabled())

    def test_get_checkpoint_config(self):

        checkpoint_config = self.env.get_checkpoint_config()

        self.assertIsInstance(checkpoint_config, CheckpointConfig)

    def test_get_set_checkpoint_interval(self):

        self.env.enable_checkpointing(30000)

        interval = self.env.get_checkpoint_interval()

        self.assertEqual(interval, 30000)

    def test_get_set_checkpointing_mode(self):
        mode = self.env.get_checkpointing_mode()
        self.assertEqual(mode, CheckpointingMode.EXACTLY_ONCE)

        self.env.enable_checkpointing(30000, CheckpointingMode.AT_LEAST_ONCE)

        mode = self.env.get_checkpointing_mode()

        self.assertEqual(mode, CheckpointingMode.AT_LEAST_ONCE)

    def test_get_state_backend(self):

        state_backend = self.env.get_state_backend()

        self.assertIsNone(state_backend)

    def test_set_state_backend(self):

        input_backend = MemoryStateBackend()

        self.env.set_state_backend(input_backend)

        output_backend = self.env.get_state_backend()

        self.assertEqual(output_backend._j_memory_state_backend,
                         input_backend._j_memory_state_backend)

    def test_get_set_stream_time_characteristic(self):

        default_time_characteristic = self.env.get_stream_time_characteristic()

        self.assertEqual(default_time_characteristic, TimeCharacteristic.ProcessingTime)

        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        time_characteristic = self.env.get_stream_time_characteristic()

        self.assertEqual(time_characteristic, TimeCharacteristic.EventTime)

    @unittest.skip("Python API does not support DataStream now. refactor this test later")
    def test_get_execution_plan(self):
        tmp_dir = tempfile.gettempdir()
        source_path = os.path.join(tmp_dir + '/streaming.csv')
        tmp_csv = os.path.join(tmp_dir + '/streaming2.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        t_env = StreamTableEnvironment.create(self.env)
        csv_source = CsvTableSource(source_path, field_names, field_types)
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Results",
            CsvTableSink(field_names, field_types, tmp_csv))
        t_env.scan("Orders").insert_into("Results")

        plan = self.env.get_execution_plan()

        json.loads(plan)
