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
from abc import abstractmethod

from pyflink.find_flink_home import _find_flink_home
from pyflink.table import TableEnvironment, TableConfig
from pyflink.java_gateway import get_gateway

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


class PythonAPICompletenessTestCase(unittest.TestCase):
    """
    Base class for Python API completeness tests, i.e.,
    Python API should be aligned with the Java API as much as possible.
    """

    @classmethod
    def get_python_class_methods(cls, python_class):
        return {cls.snake_to_camel(cls.java_method_name(method_name))
                for method_name in dir(python_class) if not method_name.startswith('_')}

    @staticmethod
    def snake_to_camel(method_name):
        output = ''.join(x.capitalize() or '_' for x in method_name.split('_'))
        return output[0].lower() + output[1:]

    @staticmethod
    def get_java_class_methods(java_class):
        gateway = get_gateway()
        s = set()
        method_arr = gateway.jvm.Class.forName(java_class).getDeclaredMethods()
        for i in range(0, len(method_arr)):
            s.add(method_arr[i].getName())
        return s

    @classmethod
    def check_methods(cls):
        java_methods = PythonAPICompletenessTestCase.get_java_class_methods(cls.java_class())
        python_methods = cls.get_python_class_methods(cls.python_class())
        missing_methods = java_methods - python_methods - cls.excluded_methods()
        if len(missing_methods) > 0:
            print(missing_methods)
            print('The Exception should be raised after FLINK-12407 is merged.')
            # raise Exception('Methods: %s in Java class %s have not been added in Python class %s.'
            #                % (missing_methods, cls.java_class(), cls.python_class()))

    @classmethod
    def java_method_name(cls, python_method_name):
        """
        This method should be overwritten when the method name of the Python API cannot be
        consistent with the Java API method name. e.g.: 'as' is python
        keyword, so we use 'alias' in Python API corresponding 'as' in Java API.

        :param python_method_name: Method name of Python API.
        :return: The corresponding method name of Java API.
        """
        return python_method_name

    @classmethod
    @abstractmethod
    def python_class(cls):
        """
        Return the Python class that needs to be compared. such as :class:`Table`.
        """
        pass

    @classmethod
    @abstractmethod
    def java_class(cls):
        """
        Return the Java class that needs to be compared. such as `org.apache.flink.table.api.Table`.
        """
        pass

    @classmethod
    def excluded_methods(cls):
        """
        Exclude method names that do not need to be checked. When adding excluded methods
        to the lists you should give a good reason in a comment.
        :return:
        """
        return {}

    def test_completeness(self):
        if self.python_class() is not None and self.java_class() is not None:
            self.check_methods()
