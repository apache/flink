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
import os
import tempfile
import time

from pyflink.common import ExecutionConfig, RestartStrategies
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import DataTypes, BatchTableEnvironment, CsvTableSource, CsvTableSink
from pyflink.testing.test_case_utils import PyFlinkTestCase


class ExecutionEnvironmentTests(PyFlinkTestCase):

    def setUp(self):
        self.env = ExecutionEnvironment.get_execution_environment()

    def test_get_set_parallelism(self):

        self.env.set_parallelism(10)

        parallelism = self.env.get_parallelism()

        self.assertEqual(parallelism, 10)

    def test_get_set_default_local_parallelism(self):

        self.env.set_default_local_parallelism(8)

        parallelism = self.env.get_default_local_parallelism()

        self.assertEqual(parallelism, 8)

    def test_get_config(self):

        execution_config = self.env.get_config()

        self.assertIsInstance(execution_config, ExecutionConfig)

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
                         ["org.apache.flink.runtime.state.StateBackendTestBase$TestPojo"])

    def test_get_execution_plan(self):
        tmp_dir = tempfile.gettempdir()
        source_path = os.path.join(tmp_dir + '/streaming.csv')
        tmp_csv = os.path.join(tmp_dir + '/streaming2.csv')
        field_names = ["a", "b", "c"]
        field_types = [DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        t_env = BatchTableEnvironment.create(self.env)
        csv_source = CsvTableSource(source_path, field_names, field_types)
        t_env.register_table_source("Orders", csv_source)
        t_env.register_table_sink(
            "Results",
            CsvTableSink(field_names, field_types, tmp_csv))
        t_env.scan("Orders").insert_into("Results")

        plan = self.env.get_execution_plan()

        json.loads(plan)

    def test_execute(self):
        tmp_dir = tempfile.gettempdir()
        field_names = ['a', 'b', 'c']
        field_types = [DataTypes.BIGINT(), DataTypes.STRING(), DataTypes.STRING()]
        t_env = BatchTableEnvironment.create(self.env)
        t_env.register_table_sink(
            'Results',
            CsvTableSink(field_names, field_types,
                         os.path.join('{}/{}.csv'.format(tmp_dir, round(time.time())))))
        t_env.insert_into('Results', t_env.from_elements([(1, 'Hi', 'Hello')], ['a', 'b', 'c']))
        execution_result = self.env.execute('test_batch_execute')
        self.assertIsNotNone(execution_result.get_job_id())
        self.assertTrue(execution_result.is_job_execution_result())
        self.assertIsNotNone(execution_result.get_job_execution_result().get_job_id())
        self.assertIsNotNone(execution_result.get_net_runtime())
        self.assertEqual(len(execution_result.get_all_accumulator_results()), 0)
        self.assertIsNone(execution_result.get_accumulator_result('accumulator'))
        self.assertIsNotNone(execution_result.to_string())
