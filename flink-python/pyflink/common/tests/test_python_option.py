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

from py4j.protocol import Py4JError
from pyflink.util.utils import to_jarray

from pyflink.common import Configuration
from pyflink.java_gateway import launch_gateway

from pyflink.common.dependency_manager import DependencyManager

from pyflink.testing.test_case_utils import (PyFlinkTestCase, replace_uuid,
                                             TestEnv, encode_to_base64,
                                             create_empty_file)


class PythonOptionTests(PyFlinkTestCase):

    def setUp(self):
        self.j_env = TestEnv()
        self.config = Configuration()
        self.dependency_manager = DependencyManager(self.config, self.j_env)

    def test_python_options(self):
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

    def test_python_options_integrated(self):
        python_module = tempfile.mkdtemp(dir=self.tempdir)
        python_file1 = create_empty_file(".py", self.tempdir)
        python_file2 = create_empty_file(".zip", self.tempdir)
        python_file3 = create_empty_file(".egg", self.tempdir)
        py_files = ",".join([python_module, python_file1, python_file2, python_file3])

        self.dependency_manager.add_python_file(python_module)
        self.dependency_manager.add_python_file(python_file1)
        self.dependency_manager.add_python_file(python_file2)
        self.dependency_manager.add_python_file(python_file3)
        self.dependency_manager.add_python_archive("c.zip", "venv")
        self.dependency_manager.add_python_archive("d.zip")
        self.dependency_manager.add_python_archive("e.zip", "data")
        self.dependency_manager.set_python_requirements("a.txt", "b_dir")
        self.config.set_string(DependencyManager.PYTHON_EXEC, "venv/bin/python")

        expected_parameter = encode_to_base64(self.config.to_dict())
        expected_files = encode_to_base64(self.j_env.to_dict())

        result_file = tempfile.mktemp(dir=self.tempdir)
        from pyflink.testing import python_option_verifier
        args = ["run",
                "--pyFiles", py_files,
                "--pyExecutable", "venv/bin/python",
                "--pyRequirements", "a.txt#b_dir",
                "--pyArchives", "c.zip#venv,d.zip,e.zip#data",
                "--python", os.path.abspath(python_option_verifier.__file__),
                "--expected_parameter", expected_parameter.decode("utf8"),
                "--expected_files", expected_files.decode("utf8"),
                "--result_file", result_file]
        # CliFrontend will call System.exit(), do not use current gateway to run it.
        gateway = launch_gateway()
        JString = gateway.jvm.java.lang.String
        JCliFrontend = gateway.jvm.org.apache.flink.client.cli.CliFrontend
        j_args = to_jarray(JString, args, gateway)

        try:
            JCliFrontend.main(j_args)
        except Py4JError:
            if os.path.exists(result_file):
                with open(result_file, "r") as f:
                    msg = f.read()
                    if msg == "Verify passed.":
                        pass
                    else:
                        self.fail(msg)
            else:
                raise
