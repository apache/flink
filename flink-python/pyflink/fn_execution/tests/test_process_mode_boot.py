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
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

import grpc
from apache_beam.portability.api.beam_provision_api_pb2 import (ProvisionInfo,
                                                                GetProvisionInfoResponse)
from apache_beam.portability.api.beam_provision_api_pb2_grpc import (
    ProvisionServiceServicer, add_ProvisionServiceServicer_to_server)
from concurrent import futures
from google.protobuf import json_format

from pyflink.java_gateway import get_gateway
from pyflink.pyflink_gateway_server import on_windows
from pyflink.testing.test_case_utils import PyFlinkTestCase


class PythonBootTests(PyFlinkTestCase):

    def setUp(self):
        provision_info = json_format.Parse('{"retrievalToken": "test_token"}', ProvisionInfo())
        response = GetProvisionInfoResponse(info=provision_info)

        def get_unused_port():
            sock = socket.socket()
            sock.bind(('', 0))
            port = sock.getsockname()[1]
            sock.close()
            return port

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

        self.provision_server, self.provision_port = start_test_provision_server()

        self.env = dict(os.environ)
        self.env["python"] = sys.executable
        self.env["FLINK_BOOT_TESTING"] = "1"
        self.env["BOOT_LOG_DIR"] = os.path.join(self.env["FLINK_HOME"], "log")

        self.tmp_dir = tempfile.mkdtemp(str(time.time()), dir=self.tempdir)
        # assume that this file is in flink-python source code directory.
        pyflink_package_dir = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        runner_script = "pyflink-udf-runner.bat" if on_windows() else \
            "pyflink-udf-runner.sh"
        self.runner_path = os.path.join(
            pyflink_package_dir, "bin", runner_script)

    def run_boot_py(self):
        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "whatever",
                "--provision_endpoint", "localhost:%d" % self.provision_port,
                "--control_endpoint", "localhost:0000",
                "--semi_persist_dir", self.tmp_dir]

        return subprocess.call(args, env=self.env)

    def test_python_boot(self):
        exit_code = self.run_boot_py()
        self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")

    @unittest.skipIf(on_windows(), "'subprocess.check_output' in Windows always return empty "
                                   "string, skip this test.")
    def test_param_validation(self):
        args = [self.runner_path]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No id provided.", exit_message)

        args = [self.runner_path, "--id", "1"]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertIn("No provision endpoint provided.", exit_message)

    def test_set_working_directory(self):
        JProcessPythonEnvironmentManager = \
            get_gateway().jvm.org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager

        output_file = os.path.join(self.tmp_dir, "output.txt")
        pyflink_dir = os.path.join(self.tmp_dir, "pyflink")
        os.mkdir(pyflink_dir)
        # just create an empty file
        open(os.path.join(pyflink_dir, "__init__.py"), 'a').close()
        fn_execution_dir = os.path.join(pyflink_dir, "fn_execution")
        os.mkdir(fn_execution_dir)
        open(os.path.join(fn_execution_dir, "__init__.py"), 'a').close()
        beam_dir = os.path.join(fn_execution_dir, "beam")
        os.mkdir(beam_dir)
        open(os.path.join(beam_dir, "__init__.py"), 'a').close()
        with open(os.path.join(beam_dir, "beam_boot.py"), "w") as f:
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

    def tearDown(self):
        self.provision_server.stop(0)
        try:
            if self.tmp_dir is not None:
                shutil.rmtree(self.tmp_dir)
        except:
            pass
