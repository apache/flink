#!/usr/bin/env python
#################################################################################
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
"""
This script is a python implementation of the "boot.go" script in "beam-sdks-python-container"
project of Apache Beam, see in:

https://github.com/apache/beam/blob/release-2.14.0/sdks/python/container/boot.go

It is implemented in golang and will introduce unnecessary dependencies if used in pure python
project. So we add a python implementation which will be used when the python worker runs in
process mode. It downloads and installs users' python artifacts, then launches the python SDK
harness of Apache Beam.
"""
import argparse
import hashlib
import os
from subprocess import call

import grpc
import logging
import sys

from apache_beam.portability.api.beam_provision_api_pb2_grpc import ProvisionServiceStub
from apache_beam.portability.api.beam_provision_api_pb2 import GetProvisionInfoRequest
from apache_beam.portability.api.beam_artifact_api_pb2_grpc import ArtifactRetrievalServiceStub
from apache_beam.portability.api.beam_artifact_api_pb2 import (GetManifestRequest,
                                                               GetArtifactRequest)
from apache_beam.portability.api.endpoints_pb2 import ApiServiceDescriptor

from google.protobuf import json_format, text_format

parser = argparse.ArgumentParser()

parser.add_argument("--id", default="", help="Local identifier (required).")
parser.add_argument("--logging_endpoint", default="",
                    help="Logging endpoint (required).")
parser.add_argument("--artifact_endpoint", default="",
                    help="Artifact endpoint (required).")
parser.add_argument("--provision_endpoint", default="",
                    help="Provision endpoint (required).")
parser.add_argument("--control_endpoint", default="",
                    help="Control endpoint (required).")
parser.add_argument("--semi_persist_dir", default="/tmp",
                    help="Local semi-persistent directory (optional).")

args = parser.parse_args()

worker_id = args.id
logging_endpoint = args.logging_endpoint
artifact_endpoint = args.artifact_endpoint
provision_endpoint = args.provision_endpoint
control_endpoint = args.control_endpoint
semi_persist_dir = args.semi_persist_dir


def check_not_empty(check_str, error_message):
    if check_str == "":
        logging.fatal(error_message)
        exit(1)


check_not_empty(worker_id, "No id provided.")
check_not_empty(logging_endpoint, "No logging endpoint provided.")
check_not_empty(artifact_endpoint, "No artifact endpoint provided.")
check_not_empty(provision_endpoint, "No provision endpoint provided.")
check_not_empty(control_endpoint, "No control endpoint provided.")

logging.info("Initializing python harness: %s" % " ".join(sys.argv))

metadata = [("worker_id", worker_id)]

# read job information from provision stub
with grpc.insecure_channel(provision_endpoint) as channel:
    client = ProvisionServiceStub(channel=channel)
    info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
    options = json_format.MessageToJson(info.pipeline_options)

staged_dir = os.path.join(semi_persist_dir, "staged")

# download files
with grpc.insecure_channel(artifact_endpoint) as channel:
    client = ArtifactRetrievalServiceStub(channel=channel)
    # get file list via retrieval token
    response = client.GetManifest(GetManifestRequest(retrieval_token=info.retrieval_token),
                                  metadata=metadata)
    artifacts = response.manifest.artifact
    # download files and check hash values
    for artifact in artifacts:
        name = artifact.name
        permissions = artifact.permissions
        sha256 = artifact.sha256
        file_path = os.path.join(staged_dir, name)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                sha256obj = hashlib.sha256()
                sha256obj.update(f.read())
                hash_value = sha256obj.hexdigest()
            if hash_value == sha256:
                logging.info("The file: %s already exists and its sha256 hash value: %s is the "
                             "same as the expected hash value, skipped." % (file_path, sha256))
                continue
            else:
                os.remove(file_path)
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), 0o755)
        stream = client.GetArtifact(
            GetArtifactRequest(name=name, retrieval_token=info.retrieval_token), metadata=metadata)
        with open(file_path, "wb") as f:
            sha256obj = hashlib.sha256()
            for artifact_chunk in stream:
                sha256obj.update(artifact_chunk.data)
                f.write(artifact_chunk.data)
            hash_value = sha256obj.hexdigest()
        if hash_value != sha256:
            raise Exception("The sha256 hash value: %s of the downloaded file: %s is not the same"
                            " as the expected hash value: %s" % (hash_value, file_path, sha256))
        os.chmod(file_path, int(str(permissions), 8))

os.environ["WORKER_ID"] = worker_id
os.environ["PIPELINE_OPTIONS"] = options
os.environ["SEMI_PERSISTENT_DIRECTORY"] = semi_persist_dir
os.environ["LOGGING_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
    ApiServiceDescriptor(url=logging_endpoint))
os.environ["CONTROL_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
    ApiServiceDescriptor(url=control_endpoint))

env = dict(os.environ)

if "FLINK_BOOT_TESTING" in os.environ and os.environ["FLINK_BOOT_TESTING"] == "1":
    exit(0)

call([sys.executable, "-m", "pyflink.fn_execution.sdk_worker_main"],
     stdout=sys.stdout, stderr=sys.stderr, env=env)
