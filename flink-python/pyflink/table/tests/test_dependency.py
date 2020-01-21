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
import shutil
import sys
import unittest
import uuid

from pyflink.table import DataTypes
from pyflink.table.udf import udf
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import (PyFlinkBlinkStreamTableTestCase,
                                             PyFlinkBlinkBatchTableTestCase,
                                             PyFlinkStreamTableTestCase,
                                             PyFlinkBatchTableTestCase)


class DependencyTests(object):

    def test_add_python_file(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")
        self.t_env.add_python_file(python_file_path)

        def plus_two(i):
            from test_dependency_manage_lib import add_two
            return add_two(i)

        self.t_env.register_function("add_two", udf(plus_two, DataTypes.BIGINT(),
                                                    DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("add_two(a), a").insert_into("Results")
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3,1", "4,2", "5,3"])


class FlinkStreamDependencyTests(DependencyTests, PyFlinkStreamTableTestCase):

    pass


class FlinkBatchDependencyTests(PyFlinkBatchTableTestCase):

    def test_add_python_file(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")
        self.t_env.add_python_file(python_file_path)

        def plus_two(i):
            from test_dependency_manage_lib import add_two
            return add_two(i)

        self.t_env.register_function("add_two", udf(plus_two, DataTypes.BIGINT(),
                                                    DataTypes.BIGINT()))

        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])\
            .select("add_two(a), a")

        result = self.collect(t)
        self.assertEqual(result, ["3,1", "4,2", "5,3"])


class BlinkBatchDependencyTests(DependencyTests, PyFlinkBlinkBatchTableTestCase):

    pass


class BlinkStreamDependencyTests(DependencyTests, PyFlinkBlinkStreamTableTestCase):

    def test_set_requirements_without_cached_directory(self):
        requirements_txt_path = os.path.join(self.tempdir, str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("cloudpickle==1.2.2")
        self.t_env.set_python_requirements(requirements_txt_path)

        def check_requirements(i):
            import cloudpickle
            assert os.path.abspath(cloudpickle.__file__).startswith(
                os.environ['_PYTHON_REQUIREMENTS_INSTALL_DIR'])
            return i

        self.t_env.register_function("check_requirements",
                                     udf(check_requirements, DataTypes.BIGINT(),
                                         DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("check_requirements(a), a").insert_into("Results")
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1", "2,2", "3,3"])

    def test_set_requirements_with_cached_directory(self):
        tmp_dir = self.tempdir
        requirements_txt_path = os.path.join(tmp_dir, "requirements_txt_" + str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("python-package1==0.0.0")

        requirements_dir_path = os.path.join(tmp_dir, "requirements_dir_" + str(uuid.uuid4()))
        os.mkdir(requirements_dir_path)
        package_file_name = "python-package1-0.0.0.tar.gz"
        with open(os.path.join(requirements_dir_path, package_file_name), 'wb') as f:
            from pyflink.fn_execution.tests.process_mode_test_data import file_data
            import base64
            f.write(base64.b64decode(json.loads(file_data[package_file_name])["data"]))
        self.t_env.set_python_requirements(requirements_txt_path, requirements_dir_path)

        def add_one(i):
            from python_package1 import plus
            return plus(i, 1)

        self.t_env.register_function("add_one",
                                     udf(add_one, DataTypes.BIGINT(),
                                         DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("add_one(a), a").insert_into("Results")
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["2,1", "3,2", "4,3"])

    def test_add_python_archive(self):
        tmp_dir = self.tempdir
        archive_dir_path = os.path.join(tmp_dir, "archive_" + str(uuid.uuid4()))
        os.mkdir(archive_dir_path)
        with open(os.path.join(archive_dir_path, "data.txt"), 'w') as f:
            f.write("2")
        archive_file_path = \
            shutil.make_archive(os.path.dirname(archive_dir_path), 'zip', archive_dir_path)
        self.t_env.add_python_archive(archive_file_path, "data")

        def add_from_file(i):
            with open("data/data.txt", 'r') as f:
                return i + int(f.read())

        self.t_env.register_function("add_from_file",
                                     udf(add_from_file, DataTypes.BIGINT(),
                                         DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("add_from_file(a), a").insert_into("Results")
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["3,1", "4,2", "5,3"])

    def test_set_python_exec(self):
        if getattr(os, "symlink", None) is None:
            self.skipTest("Symbolic link is not supported, skip testing 'test_set_python_exec'...")

        python_exec = sys.executable
        tmp_dir = self.tempdir
        python_exec_link_path = os.path.join(tmp_dir, "py_exec")
        os.symlink(python_exec, python_exec_link_path)
        self.t_env.get_config().set_python_executable(python_exec_link_path)

        def check_python_exec(i):
            import os
            assert os.environ["python"] == python_exec_link_path
            return i

        self.t_env.register_function("check_python_exec",
                                     udf(check_python_exec, DataTypes.BIGINT(),
                                         DataTypes.BIGINT()))
        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        self.t_env.register_table_sink("Results", table_sink)
        t = self.t_env.from_elements([(1, 2), (2, 5), (3, 1)], ['a', 'b'])
        t.select("check_python_exec(a), a").insert_into("Results")
        self.t_env.execute("test")

        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,1", "2,2", "3,3"])


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
