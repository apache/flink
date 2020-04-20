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

from pyflink.ml.api.ml_environment_factory import MLEnvironment
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import BatchTableEnvironment, StreamTableEnvironment


class MLEnvironmentTest(unittest.TestCase):

    def test_default_constructor(self):
        ml_environment = MLEnvironment()
        self.assertIsNotNone(ml_environment.get_execution_environment())
        self.assertIsNotNone(ml_environment.get_stream_execution_environment())
        self.assertIsNotNone(ml_environment.get_batch_table_environment())
        self.assertIsNotNone(ml_environment.get_stream_table_environment())

    def test_construct_with_batch_env(self):
        execution_environment = ExecutionEnvironment.get_execution_environment()
        batch_table_environment = BatchTableEnvironment.create(execution_environment)

        ml_environment = MLEnvironment(
            exe_env=execution_environment,
            batch_tab_env=batch_table_environment)
        self.assertEqual(ml_environment.get_execution_environment(), execution_environment)
        self.assertEqual(ml_environment.get_batch_table_environment(), batch_table_environment)

    def test_construct_with_stream_env(self):
        stream_execution_environment = StreamExecutionEnvironment.get_execution_environment()
        stream_table_environment = StreamTableEnvironment.create(stream_execution_environment)

        ml_environment = MLEnvironment(
            stream_exe_env=stream_execution_environment,
            stream_tab_env=stream_table_environment)
        self.assertEqual(
            ml_environment.get_stream_execution_environment(),
            stream_execution_environment)
        self.assertEqual(ml_environment.get_stream_table_environment(), stream_table_environment)
