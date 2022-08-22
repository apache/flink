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
import calendar
import datetime
import glob
import logging
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
from abc import abstractmethod
from decimal import Decimal

from py4j.java_gateway import JavaObject

from pyflink.common import JobExecutionResult, Time, Instant, Row
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.find_flink_home import _find_flink_home, _find_flink_source_root
from pyflink.java_gateway import get_gateway
from pyflink.table.environment_settings import EnvironmentSettings
from pyflink.table.table_environment import TableEnvironment
from pyflink.util.java_utils import add_jars_to_context_class_loader, to_jarray

if os.getenv("VERBOSE"):
    log_level = logging.DEBUG
else:
    log_level = logging.INFO
logging.basicConfig(stream=sys.stdout, level=log_level,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def exec_insert_table(table, table_path) -> JobExecutionResult:
    return table.execute_insert(table_path).get_job_client().get_job_execution_result().result()


def _load_specific_flink_module_jars(jars_relative_path):
    flink_source_root = _find_flink_source_root()
    jars_abs_path = flink_source_root + jars_relative_path
    specific_jars = glob.glob(jars_abs_path + '/target/flink*.jar')
    specific_jars = ['file://' + specific_jar for specific_jar in specific_jars]
    add_jars_to_context_class_loader(specific_jars)


def invoke_java_object_method(obj, method_name):
    clz = obj.getClass()
    j_method = None
    while clz is not None:
        try:
            j_method = clz.getDeclaredMethod(method_name, None)
            if j_method is not None:
                break
        except:
            clz = clz.getSuperclass()
    if j_method is None:
        raise Exception("No such method: " + method_name)
    j_method.setAccessible(True)
    return j_method.invoke(obj, to_jarray(get_gateway().jvm.Object, []))


class PyFlinkTestCase(unittest.TestCase):
    """
    Base class for unit tests.
    """

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()

        os.environ["FLINK_TESTING"] = "1"
        os.environ['_python_worker_execution_mode'] = "process"
        _find_flink_home()

        logging.info("Using %s as FLINK_HOME...", os.environ["FLINK_HOME"])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)
        del os.environ['_python_worker_execution_mode']

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
        for i in range(0, actual.size()):
            py_list.append(actual.get(i))
        return py_list


class PyFlinkStreamTableTestCase(PyFlinkTestCase):
    """
    Base class for table stream tests.
    """

    def setUp(self):
        super(PyFlinkStreamTableTestCase, self).setUp()
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        self.t_env.get_config().set("parallelism.default", "2")
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "1")


class PyFlinkBatchTableTestCase(PyFlinkTestCase):
    """
    Base class for table batch tests.
    """

    def setUp(self):
        super(PyFlinkBatchTableTestCase, self).setUp()
        self.t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
        self.t_env.get_config().set("parallelism.default", "2")
        self.t_env.get_config().set(
            "python.fn-execution.bundle.size", "1")


class PyFlinkStreamingTestCase(PyFlinkTestCase):
    """
    Base class for streaming tests.
    """

    def setUp(self):
        super(PyFlinkStreamingTestCase, self).setUp()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(2)
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)


class PyFlinkBatchTestCase(PyFlinkTestCase):
    """
    Base class for batch tests.
    """

    def setUp(self):
        super(PyFlinkBatchTestCase, self).setUp()
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_parallelism(2)
        self.env.set_runtime_mode(RuntimeExecutionMode.BATCH)


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


def replace_uuid(input_obj):
    if isinstance(input_obj, str):
        return re.sub(r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
                      '{uuid}', input_obj)
    elif isinstance(input_obj, dict):
        input_obj_copy = dict()
        for key in input_obj:
            input_obj_copy[replace_uuid(key)] = replace_uuid(input_obj[key])
        return input_obj_copy


class Tuple2(object):

    def __init__(self, f0, f1):
        self.f0 = f0
        self.f1 = f1
        self.field = [f0, f1]

    def getField(self, index):
        return self.field[index]


class TestEnv(object):

    def __init__(self):
        self.result = []

    def registerCachedFile(self, file_path, key):
        self.result.append(Tuple2(key, file_path))

    def getCachedFiles(self):
        return self.result

    def to_dict(self):
        result = dict()
        for item in self.result:
            result[item.f0] = item.f1
        return result


DATE_EPOCH_ORDINAL = datetime.datetime(1970, 1, 1).toordinal()
TIME_EPOCH_ORDINAL = calendar.timegm(time.localtime(0)) * 10 ** 3


def _date_to_millis(d: datetime.date):
    return (d.toordinal() - DATE_EPOCH_ORDINAL) * 86400 * 1000


def _time_to_millis(t: datetime.time):
    if t.tzinfo is not None:
        offset = t.utcoffset()
        offset = offset if offset else datetime.timedelta()
        offset_millis = \
            (offset.days * 86400 + offset.seconds) * 10 ** 3 + offset.microseconds // 1000
    else:
        offset_millis = TIME_EPOCH_ORDINAL
    minutes = t.hour * 60 + t.minute
    seconds = minutes * 60 + t.second
    return seconds * 10 ** 3 + t.microsecond // 1000 - offset_millis


def to_java_data_structure(value):
    jvm = get_gateway().jvm
    if isinstance(value, (int, float, str, bytes)):
        return value
    elif isinstance(value, Decimal):
        return jvm.java.math.BigDecimal.valueOf(float(value))
    elif isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return jvm.java.sql.Timestamp(
                _date_to_millis(value.date()) + _time_to_millis(value.time())
            )
        return jvm.java.time.Instant.ofEpochMilli(
            (
                calendar.timegm(value.utctimetuple()) +
                calendar.timegm(time.localtime(0))
            ) * 1000 +
            value.microsecond // 1000
        )
    elif isinstance(value, datetime.date):
        return jvm.java.sql.Date(_date_to_millis(value))
    elif isinstance(value, datetime.time):
        return jvm.java.sql.Time(_time_to_millis(value))
    elif isinstance(value, Time):
        return jvm.java.sql.Time(value.to_milliseconds())
    elif isinstance(value, Instant):
        return jvm.java.time.Instant.ofEpochMilli(value.to_epoch_milli())
    elif isinstance(value, (list, tuple)):
        j_list = jvm.java.util.ArrayList()
        for i in value:
            j_list.add(to_java_data_structure(i))
        return j_list
    elif isinstance(value, dict):
        j_map = jvm.java.util.HashMap()
        for k, v in value.items():
            j_map.put(to_java_data_structure(k), to_java_data_structure(v))
        return j_map
    elif isinstance(value, Row):
        if hasattr(value, '_fields'):
            j_row = jvm.org.apache.flink.types.Row.withNames(value.get_row_kind().to_j_row_kind())
            for field_name, value in zip(value._fields, value._values):
                j_row.setField(field_name, to_java_data_structure(value))
        else:
            j_row = jvm.org.apache.flink.types.Row.withPositions(
                value.get_row_kind().to_j_row_kind(), len(value)
            )
            for idx, value in enumerate(value._values):
                j_row.setField(idx, to_java_data_structure(value))
        return j_row
    else:
        raise TypeError('unsupported value type {}'.format(str(type(value))))
