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

from py4j.java_gateway import JavaObject
from py4j.protocol import Py4JJavaError

from pyflink.table.sources import CsvTableSource
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.find_flink_home import _find_flink_home
from pyflink.table import BatchTableEnvironment, StreamTableEnvironment, EnvironmentSettings
from pyflink.java_gateway import get_gateway


if os.getenv("VERBOSE"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def get_private_field(java_obj, field_name):
    try:
        field = java_obj.getClass().getDeclaredField(field_name)
        field.setAccessible(True)
        return field.get(java_obj)
    except Py4JJavaError:
        cls = java_obj.getClass()
        while cls.getSuperclass() is not None:
            cls = cls.getSuperclass()
            try:
                field = cls.getDeclaredField(field_name)
                field.setAccessible(True)
                return field.get(java_obj)
            except Py4JJavaError:
                pass


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
        if isinstance(actual, JavaObject):
            actual_py_list = cls.to_py_list(actual)
        else:
            actual_py_list = actual
        actual_py_list.sort()
        expected.sort()
        assert len(actual_py_list) == len(expected)
        assert all(x == y for x, y in zip(actual_py_list, expected))

    @classmethod
    def to_py_list(cls, actual):
        py_list = []
        for i in range(0, actual.length()):
            py_list.append(actual.apply(i))
        return py_list

    @classmethod
    def prepare_csv_source(cls, path, data, data_types, fields):
        if os.path.isfile(path):
            os.remove(path)
        csv_data = ""
        for item in data:
            if isinstance(item, list) or isinstance(item, tuple):
                csv_data += ",".join([str(element) for element in item]) + "\n"
            else:
                csv_data += str(item) + "\n"
        with open(path, 'w') as f:
            f.write(csv_data)
            f.close()
        return CsvTableSource(path, fields, data_types)


class PyFlinkStreamTableTestCase(PyFlinkTestCase):
    """
    Base class for stream tests.
    """

    def setUp(self):
        super(PyFlinkStreamTableTestCase, self).setUp()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(2)
        self.t_env = StreamTableEnvironment.create(self.env)


class PyFlinkBatchTableTestCase(PyFlinkTestCase):
    """
    Base class for batch tests.
    """

    def setUp(self):
        super(PyFlinkBatchTableTestCase, self).setUp()
        self.env = ExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(2)
        self.t_env = BatchTableEnvironment.create(self.env)

    def collect(self, table):
        j_table = table._j_table
        gateway = get_gateway()
        row_result = self.t_env._j_tenv\
            .toDataSet(j_table, gateway.jvm.Class.forName("org.apache.flink.types.Row")).collect()
        string_result = [java_row.toString() for java_row in row_result]
        return string_result


class PyFlinkBlinkStreamTableTestCase(PyFlinkTestCase):
    """
    Base class for stream tests of blink planner.
    """

    def setUp(self):
        super(PyFlinkBlinkStreamTableTestCase, self).setUp()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(2)
        self.t_env = StreamTableEnvironment.create(
            self.env, environment_settings=EnvironmentSettings.new_instance()
                .in_streaming_mode().use_blink_planner().build())


class PyFlinkBlinkBatchTableTestCase(PyFlinkTestCase):
    """
    Base class for batch tests of blink planner.
    """

    def setUp(self):
        super(PyFlinkBlinkBatchTableTestCase, self).setUp()
        self.t_env = BatchTableEnvironment.create(
            environment_settings=EnvironmentSettings.new_instance()
            .in_batch_mode().use_blink_planner().build())
        self.t_env._j_tenv.getPlanner().getExecEnv().setParallelism(2)


class PythonAPICompletenessTestCase(object):
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
        method_arr = gateway.jvm.Class.forName(java_class).getMethods()
        for i in range(0, len(method_arr)):
            s.add(method_arr[i].getName())
        return s

    @classmethod
    def check_methods(cls):
        java_primary_methods = {'getClass', 'notifyAll', 'equals', 'hashCode', 'toString',
                                'notify', 'wait'}
        java_methods = PythonAPICompletenessTestCase.get_java_class_methods(cls.java_class())
        python_methods = cls.get_python_class_methods(cls.python_class())
        missing_methods = java_methods - python_methods - cls.excluded_methods() \
            - java_primary_methods
        if len(missing_methods) > 0:
            raise Exception('Methods: %s in Java class %s have not been added in Python class %s.'
                            % (missing_methods, cls.java_class(), cls.python_class()))

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
        return {"equals", "hashCode", "toString"}

    def test_completeness(self):
        self.check_methods()
