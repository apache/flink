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
#################################################################################

import logging
import os
import shutil
import sys
import tempfile
import unittest

from pyflink.find_flink_home import _find_flink_home
from pyflink.table import TableEnvironment, TableConfig

if sys.version_info[0] >= 3:
    xrange = range

if os.getenv("VERBOSE"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


class PyFlinkTestCase(unittest.TestCase):
    """
    Base class for unit tests.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        os.environ["FLINK_TESTING"] = "1"
        _find_flink_home()

        logging.info("Using %s as FLINK_HOME...", os.environ["FLINK_HOME"])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    @classmethod
    def assert_equals(cls, actual, expected):
        actual_py_list = cls.to_py_list(actual)
        actual_py_list.sort()
        expected.sort()
        assert all(x == y for x, y in zip(actual_py_list, expected))

    @classmethod
    def to_py_list(cls, actual):
        py_list = []
        for i in xrange(0, actual.length()):
            py_list.append(actual.apply(i))
        return py_list


class PyFlinkStreamTableTestCase(PyFlinkTestCase):
    """
    Base class for stream unit tests.
    """

    def setUp(self):
        super(PyFlinkStreamTableTestCase, self).setUp()
        self.t_config = TableConfig.Builder().as_streaming_execution().set_parallelism(4).build()
        self.t_env = TableEnvironment.get_table_environment(self.t_config)


class PyFlinkBatchTableTestCase(PyFlinkTestCase):
    """
    Base class for batch unit tests.
    """

    def setUp(self):
        super(PyFlinkBatchTableTestCase, self).setUp()
        self.t_config = TableConfig.Builder().as_batch_execution().set_parallelism(4).build()
        self.t_env = TableEnvironment.get_table_environment(self.t_config)
