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
import unittest

from pyflink.common import Configuration
from pyflink.common.dependency_manager import DependencyManager
from pyflink.java_gateway import get_gateway
from pyflink.table import TableConfig
from pyflink.testing.test_case_utils import PyFlinkTestCase, TestEnv, replace_uuid


class DependencyManagerTests(PyFlinkTestCase):

    def setUp(self):
        self.j_env = TestEnv()
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
        JPythonConfig = get_gateway().jvm.org.apache.flink.python.PythonConfig
        self.assertEqual(DependencyManager.PYTHON_REQUIREMENTS_CACHE,
                         JPythonConfig.PYTHON_REQUIREMENTS_CACHE)
        self.assertEqual(DependencyManager.PYTHON_REQUIREMENTS_FILE,
                         JPythonConfig.PYTHON_REQUIREMENTS_FILE)
        self.assertEqual(DependencyManager.PYTHON_ARCHIVES,
                         JPythonConfig.PYTHON_ARCHIVES)
        self.assertEqual(DependencyManager.PYTHON_FILES, JPythonConfig.PYTHON_FILES)
        self.assertEqual(DependencyManager.PYTHON_EXEC, JPythonConfig.PYTHON_EXEC)

        JPythonDriverEnvUtils = \
            get_gateway().jvm.org.apache.flink.client.python.PythonDriverEnvUtils
        self.assertEqual(DependencyManager.PYFLINK_PY_REQUIREMENTS,
                         JPythonDriverEnvUtils.PYFLINK_PY_REQUIREMENTS)
        self.assertEqual(DependencyManager.PYFLINK_PY_ARCHIVES,
                         JPythonDriverEnvUtils.PYFLINK_PY_ARCHIVES)
        self.assertEqual(DependencyManager.PYFLINK_PY_FILES,
                         JPythonDriverEnvUtils.PYFLINK_PY_FILES)
        self.assertEqual(DependencyManager.PYFLINK_PY_EXECUTABLE,
                         JPythonDriverEnvUtils.PYFLINK_PY_EXECUTABLE)

    def test_load_from_env(self):
        dm = DependencyManager

        system_env = dict()
        system_env[dm.PYFLINK_PY_FILES] = "/file1.py\nhdfs://file2.zip\nfile3.egg"
        system_env[dm.PYFLINK_PY_REQUIREMENTS] = "a.txt\nb_dir"
        system_env[dm.PYFLINK_PY_EXECUTABLE] = "/usr/local/bin/python"
        system_env[dm.PYFLINK_PY_ARCHIVES] = "/py3.zip\nvenv\n/py3.zip\n\ndata.zip\ndata"

        self.dependency_manager.load_from_env(system_env)

        configs = self.config.to_dict()
        python_files = replace_uuid(json.loads(configs[dm.PYTHON_FILES]))
        python_requirements_file = replace_uuid(configs[dm.PYTHON_REQUIREMENTS_FILE])
        python_requirements_cache = replace_uuid(configs[dm.PYTHON_REQUIREMENTS_CACHE])
        python_archives = replace_uuid(json.loads(configs[dm.PYTHON_ARCHIVES]))
        python_exec = configs[dm.PYTHON_EXEC]
        registered_files = replace_uuid(self.j_env.to_dict())

        self.assertEqual(
            {"python_file_0_{uuid}": "file1.py",
             "python_file_1_{uuid}": "file2.zip",
             "python_file_2_{uuid}": "file3.egg"}, python_files)
        self.assertEqual(
            {"python_archive_3_{uuid}": "venv",
             "python_archive_4_{uuid}": "py3.zip",
             "python_archive_5_{uuid}": "data"}, python_archives)
        self.assertEqual("python_requirements_file_6_{uuid}", python_requirements_file)
        self.assertEqual("python_requirements_cache_7_{uuid}", python_requirements_cache)
        self.assertEqual("/usr/local/bin/python", python_exec)
        self.assertEqual(
            {"python_file_0_{uuid}": "/file1.py",
             "python_file_1_{uuid}": "hdfs://file2.zip",
             "python_file_2_{uuid}": "file3.egg",
             "python_archive_3_{uuid}": "/py3.zip",
             "python_archive_4_{uuid}": "/py3.zip",
             "python_archive_5_{uuid}": "data.zip",
             "python_requirements_file_6_{uuid}": "a.txt",
             "python_requirements_cache_7_{uuid}": "b_dir"}, registered_files)


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
