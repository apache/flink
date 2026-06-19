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
import logging
import os
import sys

import grpc
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_fn_api_pb2 import \
    StartWorkerRequest
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_fn_api_pb2_grpc import (
    BeamFnExternalWorkerPoolStub)
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_provision_api_pb2 \
    import GetProvisionInfoRequest
from apache_beam.portability.api.org.apache.beam.model.fn_execution.v1.beam_provision_api_pb2_grpc \
    import ProvisionServiceStub
from apache_beam.portability.api.org.apache.beam.model.pipeline.v1.endpoints_pb2 import (
    ApiServiceDescriptor)
from google.protobuf import json_format, text_format


def check_not_empty(check_str, error_message):
    if check_str == "":
        logging.fatal(error_message)
        exit(1)


python_exec = sys.executable


if __name__ == "__main__":
    # print INFO and higher level messages
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument("--id", default="", help="Local identifier (required).")
    parser.add_argument("--provision_endpoint", default="",
                        help="Provision endpoint (required).")
    parser.add_argument("--semi_persist_dir", default="/tmp",
                        help="Local semi-persistent directory (optional).")

    args = parser.parse_known_args()[0]

    worker_id = args.id
    provision_endpoint = args.provision_endpoint
    semi_persist_dir = args.semi_persist_dir

    check_not_empty(worker_id, "No id provided.")
    check_not_empty(provision_endpoint, "No provision endpoint provided.")

    logging.info("Initializing Python harness: %s" % " ".join(sys.argv))

    if 'PYTHON_LOOPBACK_SERVER_ADDRESS' in os.environ:
        logging.info("Starting up Python harness in loopback mode.")

        params = dict(os.environ)
        params.update({'SEMI_PERSISTENT_DIRECTORY': semi_persist_dir})
        with grpc.insecure_channel(os.environ['PYTHON_LOOPBACK_SERVER_ADDRESS']) as channel:
            client = BeamFnExternalWorkerPoolStub(channel=channel)
            request = StartWorkerRequest(
                worker_id=worker_id,
                provision_endpoint=ApiServiceDescriptor(url=provision_endpoint),
                params=params)
            response = client.StartWorker(request)
            if response.error:
                raise RuntimeError("Error starting worker: %s" % response.error)
    else:
        logging.info("Starting up Python harness in a standalone process.")
        metadata = [("worker_id", worker_id)]

        # read job information from provision stub
        with grpc.insecure_channel(provision_endpoint) as channel:
            client = ProvisionServiceStub(channel=channel)
            info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
            options = json_format.MessageToJson(info.pipeline_options)
            logging_endpoint = info.logging_endpoint.url
            control_endpoint = info.control_endpoint.url

        os.environ["WORKER_ID"] = worker_id
        os.environ["PIPELINE_OPTIONS"] = options
        os.environ["SEMI_PERSISTENT_DIRECTORY"] = semi_persist_dir
        os.environ["LOGGING_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
            ApiServiceDescriptor(url=logging_endpoint))
        os.environ["CONTROL_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
            ApiServiceDescriptor(url=control_endpoint))

        env = dict(os.environ)

        if "FLINK_BOOT_TESTING" in os.environ and os.environ["FLINK_BOOT_TESTING"] == "1":
            logging.info("Shut down Python harness due to FLINK_BOOT_TESTING is set.")
            exit(0)

        from pyflink.fn_execution.beam import beam_sdk_worker_main

        beam_sdk_worker_main.main()
