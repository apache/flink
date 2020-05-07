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
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from stat import ST_MODE

import grpc
from apache_beam.portability.api.beam_artifact_api_pb2 import GetManifestResponse, ArtifactChunk
from apache_beam.portability.api.beam_artifact_api_pb2_grpc import (
    ArtifactRetrievalServiceServicer, add_ArtifactRetrievalServiceServicer_to_server)
from apache_beam.portability.api.beam_provision_api_pb2 import (ProvisionInfo,
                                                                GetProvisionInfoResponse)
from apache_beam.portability.api.beam_provision_api_pb2_grpc import (
    ProvisionServiceServicer, add_ProvisionServiceServicer_to_server)
from concurrent import futures
from google.protobuf import json_format

from pyflink.fn_execution.boot import (PYTHON_REQUIREMENTS_FILE,
                                       PYTHON_REQUIREMENTS_CACHE,
                                       PYTHON_REQUIREMENTS_INSTALL_DIR)
from pyflink.fn_execution.tests.process_mode_test_data import (manifest, file_data,
                                                               test_provision_info_json)
from pyflink.java_gateway import get_gateway
from pyflink.pyflink_gateway_server import on_windows
from pyflink.testing.test_case_utils import PyFlinkTestCase


class PythonBootTests(PyFlinkTestCase):

    def setUp(self):
        manifest_response = json_format.Parse(manifest, GetManifestResponse())
        artifact_chunks = dict()
        for file_name in file_data:
            artifact_chunks[file_name] = json_format.Parse(file_data[file_name], ArtifactChunk())
        provision_info = json_format.Parse(test_provision_info_json, ProvisionInfo())
        response = GetProvisionInfoResponse(info=provision_info)

        def get_unused_port():
            sock = socket.socket()
            sock.bind(('', 0))
            port = sock.getsockname()[1]
            sock.close()
            return port

        class ArtifactService(ArtifactRetrievalServiceServicer):
            def GetManifest(self, request, context):
                return manifest_response

            def GetArtifact(self, request, context):
                yield artifact_chunks[request.name]

        def start_test_artifact_server():
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
            add_ArtifactRetrievalServiceServicer_to_server(ArtifactService(), server)
            port = get_unused_port()
            server.add_insecure_port('[::]:' + str(port))
            server.start()
            return server, port

        class ProvisionService(ProvisionServiceServicer):
            def GetProvisionInfo(self, request, context):
                return response

        def start_test_provision_server():
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
            add_ProvisionServiceServicer_to_server(ProvisionService(), server)
            port = get_unused_port()
            server.add_insecure_port('[::]:' + str(port))
            server.start()
            return server, port

        self.artifact_server, self.artifact_port = start_test_artifact_server()
        self.provision_server, self.provision_port = start_test_provision_server()

        self.env = dict(os.environ)
        self.env["python"] = sys.executable
        self.env["FLINK_BOOT_TESTING"] = "1"
        self.env["BOOT_LOG_DIR"] = os.path.join(self.env["FLINK_HOME"], "log")

        self.tmp_dir = tempfile.mkdtemp(str(time.time()), dir=self.tempdir)
        # assume that this file is in flink-python source code directory.
        flink_python_source_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        runner_script = "pyflink-udf-runner.bat" if on_windows() else \
            "pyflink-udf-runner.sh"
        self.runner_path = os.path.join(
            flink_python_source_root, "src", "main", "resources", runner_script)

    def run_boot_py(self):
        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port,
                "--provision_endpoint", "localhost:%d" % self.provision_port,
                "--control_endpoint", "localhost:0000",
                "--semi_persist_dir", self.tmp_dir]

        return subprocess.call(args, env=self.env)

    def check_downloaded_files(self, staged_dir, manifest):
        expected_files_info = json.loads(manifest)["manifest"]["artifact"]
        files = os.listdir(staged_dir)
        self.assertEqual(len(expected_files_info), len(files))
        checked = 0
        for file_name in files:
            for file_info in expected_files_info:
                if file_name == file_info["name"]:
                    self.assertEqual(
                        oct(os.stat(os.path.join(staged_dir, file_name))[ST_MODE])[-3:],
                        str(file_info["permissions"]))
                    with open(os.path.join(staged_dir, file_name), "rb") as f:
                        sha256obj = hashlib.sha256()
                        sha256obj.update(f.read())
                        hash_value = sha256obj.hexdigest()
                    self.assertEqual(hash_value, file_info["sha256"])
                    checked += 1
                    break
        self.assertEqual(checked, len(files))

    def check_installed_files(self, prefix_dir, package_list):
        from distutils.dist import Distribution
        install_obj = Distribution().get_command_obj('install', create=True)
        install_obj.prefix = prefix_dir
        install_obj.finalize_options()
        installed_dir = [install_obj.install_purelib]
        if install_obj.install_purelib != install_obj.install_platlib:
            installed_dir.append(install_obj.install_platlib)
        for package_name in package_list:
            self.assertTrue(any([os.path.exists(os.path.join(package_dir, package_name))
                                 for package_dir in installed_dir]))

    def test_python_boot(self):

        exit_code = self.run_boot_py()
        self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")
        self.check_downloaded_files(os.path.join(self.tmp_dir, "staged"), manifest)

    @unittest.skipIf(on_windows(), "'subprocess.check_output' in Windows always return empty "
                                   "string, skip this test.")
    def test_param_validation(self):
        args = [self.runner_path]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No id provided.", exit_message)

        args = [self.runner_path, "--id", "1"]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No logging endpoint provided.", exit_message)

        args = [self.runner_path, "--id", "1", "--logging_endpoint", "localhost:0000"]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No artifact endpoint provided.", exit_message)

        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No provision endpoint provided.", exit_message)

        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port,
                "--provision_endpoint", "localhost:%d" % self.provision_port]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No control endpoint provided.", exit_message)

    def test_constant_consistency(self):
        JProcessPythonEnvironmentManager = \
            get_gateway().jvm.org.apache.flink.python.env.ProcessPythonEnvironmentManager
        self.assertEqual(PYTHON_REQUIREMENTS_FILE,
                         JProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_FILE)
        self.assertEqual(PYTHON_REQUIREMENTS_CACHE,
                         JProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_CACHE)
        self.assertEqual(PYTHON_REQUIREMENTS_INSTALL_DIR,
                         JProcessPythonEnvironmentManager.PYTHON_REQUIREMENTS_INSTALL_DIR)

    def test_set_working_directory(self):
        JProcessPythonEnvironmentManager = \
            get_gateway().jvm.org.apache.flink.python.env.ProcessPythonEnvironmentManager

        output_file = os.path.join(self.tmp_dir, "output.txt")
        pyflink_dir = os.path.join(self.tmp_dir, "pyflink")
        os.mkdir(pyflink_dir)
        # just create an empty file
        open(os.path.join(pyflink_dir, "__init__.py"), 'a').close()
        fn_execution_dir = os.path.join(pyflink_dir, "fn_execution")
        os.mkdir(fn_execution_dir)
        open(os.path.join(fn_execution_dir, "__init__.py"), 'a').close()
        with open(os.path.join(fn_execution_dir, "boot.py"), "w") as f:
            f.write("import os\nwith open(r'%s', 'w') as f:\n    f.write(os.getcwd())" %
                    output_file)

        # test if the name of working directory variable of udf runner is consist with
        # ProcessPythonEnvironmentManager.
        self.env[JProcessPythonEnvironmentManager.PYTHON_WORKING_DIR] = self.tmp_dir
        self.env["python"] = sys.executable
        args = [self.runner_path]
        subprocess.check_output(args, env=self.env)
        process_cwd = None
        if os.path.exists(output_file):
            with open(output_file, 'r') as f:
                process_cwd = f.read()

        self.assertEqual(os.path.realpath(self.tmp_dir),
                         process_cwd,
                         "setting working directory variable is not work!")

    def test_install_requirements_without_cached_dir(self):
        requirements_txt_path = os.path.join(self.tmp_dir, "requirements_txt_" + str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("#test line continuation\ncloudpickle\\\n==1.2.2\npy4j==0.10.8.1")

        self.env[PYTHON_REQUIREMENTS_FILE] = requirements_txt_path
        requirements_target_dir_path = \
            os.path.join(self.tmp_dir, "requirements_target_dir_" + str(uuid.uuid4()))
        self.env[PYTHON_REQUIREMENTS_INSTALL_DIR] = requirements_target_dir_path

        exit_code = self.run_boot_py()
        self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")
        self.check_installed_files(requirements_target_dir_path, ["cloudpickle", "py4j"])

    def test_install_requirements_with_cached_dir(self):
        requirements_txt_path = os.path.join(self.tmp_dir, "requirements_txt_" + str(uuid.uuid4()))
        with open(requirements_txt_path, 'w') as f:
            f.write("python-package1==0.0.0")

        self.env[PYTHON_REQUIREMENTS_FILE] = requirements_txt_path
        self.env[PYTHON_REQUIREMENTS_CACHE] = os.path.join(self.tmp_dir, "staged")
        requirements_target_dir_path = \
            os.path.join(self.tmp_dir, "requirements_target_dir_" + str(uuid.uuid4()))
        self.env[PYTHON_REQUIREMENTS_INSTALL_DIR] = requirements_target_dir_path

        exit_code = self.run_boot_py()
        self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")
        self.check_installed_files(requirements_target_dir_path, ["python_package1"])

    def tearDown(self):
        self.artifact_server.stop(0)
        self.provision_server.stop(0)
        try:
            if self.tmp_dir is not None:
                shutil.rmtree(self.tmp_dir)
        except:
            pass
