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
import glob
import os
import socket
import subprocess
import sys
import tempfile
import time

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

    def check_installed_package(self, prefix_dir, package_names):
        packages_dir = [path for path
                        in glob.glob(os.path.join(prefix_dir, "lib", "*", "site-packages", "*"))]
        for package_name in package_names:
            self.assertTrue(
                any([os.path.basename(dir_name).startswith(package_name)
                     for dir_name in packages_dir]),
                "%s not in the install packages list!" % package_name)

    def test_python_boot(self):
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

        artifact_server, artifact_port = start_test_artifact_server()
        provision_server, provision_port = start_test_provision_server()

        env = dict(os.environ)
        env["python"] = sys.executable
        env["FLINK_BOOT_TESTING"] = "1"

        tmp_dir = tempfile.mkdtemp(str(time.time()))
        print("Using %s as the semi_persist_dir." % tmp_dir)

        args = [os.path.join(os.environ["FLINK_HOME"], "bin", "pyflink-udf-runner.sh"),
                "--id",
                "1",
                "--logging_endpoint",
                "localhost:0000",
                "--artifact_endpoint",
                "localhost:%d" % artifact_port,
                "--provision_endpoint",
                "localhost:%d" % provision_port,
                "--control_endpoint",
                "localhost:0000",
                "--semi_persist_dir",
                tmp_dir]
        try:
            exit_code = subprocess.call(args, stdout=sys.stdout, stderr=sys.stderr, env=env)
            self.assertTrue(exit_code == 0, "the boot.py exited with non-zero code.")
            self.check_installed_package(tmp_dir, ["python_package1",  # in requirements.txt
                                                   "python_package2",  # in requirements.txt
                                                   "python_package3",  # in extra_packages.txt
                                                   "apache_beam",  # mocked beam sdk
                                                   "python_package5",  # mocked data flow sdk
                                                   "UNKNOWN"])  # users code
        finally:
            artifact_server.stop(0)
            provision_server.stop(0)
