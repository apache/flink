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

from pyflink.fn_execution.tests.process_mode_test_data import (manifest, file_data,
                                                               test_provision_info_json)
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
        self.env["FLINK_LOG_DIR"] = os.path.join(self.env["FLINK_HOME"], "log")

        self.tmp_dir = None
        # assume that this file is in flink-python source code directory.
        flink_python_source_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        self.runner_path = os.path.join(flink_python_source_root, "bin", "pyflink-udf-runner.sh")

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

    def test_python_boot(self):
        self.tmp_dir = tempfile.mkdtemp(str(time.time()))
        print("Using %s as the semi_persist_dir." % self.tmp_dir)

        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port,
                "--provision_endpoint", "localhost:%d" % self.provision_port,
                "--control_endpoint", "localhost:0000",
                "--semi_persist_dir", self.tmp_dir]

        exit_code = subprocess.call(args, stdout=sys.stdout, stderr=sys.stderr, env=self.env)
        self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")
        self.check_downloaded_files(os.path.join(self.tmp_dir, "staged"), manifest)

    def test_param_validation(self):
        args = [self.runner_path]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertTrue(exit_message.endswith("No id provided.\n"))

        args = [self.runner_path, "--id", "1"]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertTrue(exit_message.endswith("No logging endpoint provided.\n"))

        args = [self.runner_path, "--id", "1", "--logging_endpoint", "localhost:0000"]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertTrue(exit_message.endswith("No artifact endpoint provided.\n"))

        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertTrue(exit_message.endswith("No provision endpoint provided.\n"))

        args = [self.runner_path, "--id", "1",
                "--logging_endpoint", "localhost:0000",
                "--artifact_endpoint", "localhost:%d" % self.artifact_port,
                "--provision_endpoint", "localhost:%d" % self.provision_port]
        exit_message = subprocess.check_output(args, env=self.env).decode("utf-8")
        self.assertTrue(exit_message.endswith("No control endpoint provided.\n"))

    def tearDown(self):
        self.artifact_server.stop(0)
        self.provision_server.stop(0)
        try:
            if self.tmp_dir is not None:
                shutil.rmtree(self.tmp_dir)
        except:
            pass
