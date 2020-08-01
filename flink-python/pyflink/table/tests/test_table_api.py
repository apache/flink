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
# #  distributed under the License is distributed on an "AS IS" BASIS,
# #  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# #  See the License for the specific language governing permissions and
# # limitations under the License.
################################################################################

import unittest

from pyflink.testing.test_case_utils import PyFlinkStreamTableTestCase, PyFlinkBatchTableTestCase, \
    PyFlinkBlinkBatchTableTestCase


class StreamTableTest(PyFlinkStreamTableTestCase):

    @unittest.skip("Open this test case once FLINK-17303 is finished")
    def test_execute(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        source.execute()
        # TODO check result


class BatchTableTest(PyFlinkBatchTableTestCase):

    @unittest.skip("Open this test case once FLINK-17303 is finished")
    def test_execute(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        source.execute()
        # TODO check result


class BlinkBatchTableTest(PyFlinkBlinkBatchTableTestCase):

    @unittest.skip("Open this test case once FLINK-17303 is finished")
    def test_execute(self):
        t_env = self.t_env
        source = t_env.from_elements([(1, "Hi", "Hello"), (2, "Hello", "Hello")], ["a", "b", "c"])
        source.execute()
        # TODO check result
