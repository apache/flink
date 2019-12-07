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
import re
import unittest

from pyflink.common import Configuration
from pyflink.common.dependency_manager import DependencyManager
from pyflink.java_gateway import get_gateway
from pyflink.table import TableConfig
from pyflink.testing.test_case_utils import PyFlinkTestCase


def replace_uuid(input_obj):
    if isinstance(input_obj, str):
        return re.sub(r'[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}',
                      '{uuid}', input_obj)
    elif isinstance(input_obj, dict):
        input_obj_copy = dict()
        for key in input_obj:
            input_obj_copy[replace_uuid(key)] = replace_uuid(input_obj[key])
        return input_obj_copy


class DependencyManagerTests(PyFlinkTestCase):

    def setUp(self):
        self.j_env = MockedJavaEnv()
        self.config = Configuration()
        self.dependency_manager = DependencyManager(self.config, self.j_env)

    def test_add_python_file(self):
        self.dependency_manager.add_python_file("tmp_dir/test_file1.py")
        self.dependency_manager.add_python_file("tmp_dir/test_file2.py")
        self.dependency_manager.add_python_file("tmp_dir/test_dir")

        self.assertEqual(
            {"python_file_0_{uuid}": "test_file1.py",
             "python_file_1_{uuid}": "test_file2.py",
             "python_file_2_{uuid}": "test_dir"},
            json.loads(replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_FILES])))
        self.assertEqual(
            {"python_file_0_{uuid}": "tmp_dir/test_file1.py",
             "python_file_1_{uuid}": "tmp_dir/test_file2.py",
             "python_file_2_{uuid}": "tmp_dir/test_dir"},
            replace_uuid(self.j_env.to_dict()))

    def test_set_python_requirements(self):
        self.dependency_manager.set_python_requirements("tmp_dir/requirements.txt",
                                                        "tmp_dir/cache_dir")

        self.assertEqual(
            "python_requirements_file_0_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS_FILE]))
        self.assertEqual(
            "python_requirements_cache_1_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS_CACHE]))
        self.assertEqual(
            {"python_requirements_file_0_{uuid}": "tmp_dir/requirements.txt",
             "python_requirements_cache_1_{uuid}": "tmp_dir/cache_dir"},
            replace_uuid(self.j_env.to_dict()))

        # test single parameter and remove old requirements_dir setting
        self.dependency_manager.set_python_requirements("tmp_dir/requirements.txt")

        self.assertEqual(
            "python_requirements_file_2_{uuid}",
            replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_REQUIREMENTS_FILE]))
        self.assertNotIn(DependencyManager.PYTHON_REQUIREMENTS_CACHE, self.config.to_dict())
        self.assertEqual(
            {"python_requirements_file_2_{uuid}": "tmp_dir/requirements.txt"},
            replace_uuid(self.j_env.to_dict()))

    def test_add_python_archive(self):
        self.dependency_manager.add_python_archive("tmp_dir/py27.zip")
        self.dependency_manager.add_python_archive("tmp_dir/venv2.zip", "py37")

        self.assertEqual(
            {"python_archive_0_{uuid}": "py27.zip", "python_archive_1_{uuid}": "py37"},
            json.loads(replace_uuid(self.config.to_dict()[DependencyManager.PYTHON_ARCHIVES])))
        self.assertEqual(
            {"python_archive_0_{uuid}": "tmp_dir/py27.zip",
             "python_archive_1_{uuid}": "tmp_dir/venv2.zip"},
            replace_uuid(self.j_env.to_dict()))

    def test_set_get_python_executable(self):
        table_config = TableConfig()
        table_config.set_python_executable("/usr/bin/python3")

        self.assertEqual(
            "/usr/bin/python3",
            table_config.get_python_executable())

    def test_constant_consistency(self):
        JDependencyInfo = get_gateway().jvm.org.apache.flink.python.env.PythonDependencyInfo
        self.assertEqual(DependencyManager.PYTHON_REQUIREMENTS_CACHE,
                         JDependencyInfo.PYTHON_REQUIREMENTS_CACHE)
        self.assertEqual(DependencyManager.PYTHON_REQUIREMENTS_FILE,
                         JDependencyInfo.PYTHON_REQUIREMENTS_FILE)
        self.assertEqual(DependencyManager.PYTHON_ARCHIVES,
                         JDependencyInfo.PYTHON_ARCHIVES)
        self.assertEqual(DependencyManager.PYTHON_FILES, JDependencyInfo.PYTHON_FILES)
        self.assertEqual(DependencyManager.PYTHON_EXEC, JDependencyInfo.PYTHON_EXEC)


class Tuple2(object):

    def __init__(self, f0, f1):
        self.f0 = f0
        self.f1 = f1


class MockedJavaEnv(object):

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


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
