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

Its original version is implemented by golang and involves unnecessary dependencies, so we
re-implemented and modified it in python to support running in process mode and
custom operators.

It downloads and installs users' python artifacts, then launches the python SDK harness of
Apache Beam.

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

if worker_id == "":
    logging.fatal("No id provided.")

if logging_endpoint == "":
    logging.fatal("No logging endpoint provided.")

if artifact_endpoint == "":
    logging.fatal("No artifact endpoint provided.")

if provision_endpoint == "":
    logging.fatal("No provision endpoint provided.")

if control_endpoint == "":
    logging.fatal("No control endpoint provided.")

logging.info("Initializing python harness: %s" % " ".join(sys.argv))

metadata = [("worker_id", worker_id)]

# read job information from provision stub
with grpc.insecure_channel(provision_endpoint) as channel:
    client = ProvisionServiceStub(channel=channel)
    info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
    options = json_format.MessageToJson(info.pipeline_options)

staged_dir = os.path.join(semi_persist_dir, "staged")
files = []

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
        files.append(name)
        permissions = artifact.permissions
        sha256 = artifact.sha256
        file_path = os.path.join(staged_dir, name)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                sha256obj = hashlib.sha256()
                sha256obj.update(f.read())
                hash_value = sha256obj.hexdigest()
            if hash_value == sha256:
                logging.info("file exist and has same sha256 hash value, skip...")
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
            raise Exception("sha256 not consist!")
        os.chmod(file_path, permissions)

acceptable_whl_specs = [".whl"]


def find_beam_sdk_whl():
    """
    Find apache beam sdk wheel package in downloaded files. If found, it will be installed.

    :return: the file name of apache beam sdk wheel package.
    """
    for filename in files:
        if filename.startswith("apache_beam"):
            for acceptable_whl_spec in acceptable_whl_specs:
                if filename.endswith(acceptable_whl_spec):
                    logging.info("Found Apache Beam SDK wheel: %s" % filename)
                    return filename
    return ""


def python_location():
    """
    Returns the python executable file location.
    If the environment variable "python" exists, using its value.

    :return: The python executable file location. Default value is "python".
    """
    if "python" in os.environ:
        return os.environ["python"]
    else:
        return "python"


def pip_command():
    """
    Returns the pip command.

    If the environment variable "pip" exists, using its value.

    :return: The pip executable file location. Default value is "/usr/local/bin/pip".
    """
    if "pip" in os.environ:
        return [os.environ["pip"]]
    else:
        return [python_location(), "-m", "pip"]


pip = pip_command()
python_path = python_location()


def add_test_params(args):
    if "FLINK_BOOT_TESTING" in os.environ and os.environ["FLINK_BOOT_TESTING"] == "1":
        # do not install those test packages in users' site-packages dir when testing
        args.append("--prefix")
        args.append(semi_persist_dir)
        args.append("-I")
    return args


def pip_install_package(files, dir, name, force, optional, extras):
    """
    Pip install the specified python package.

    :param files: The list of the downloaded file names.
    :type files: list[str]
    :param dir:  The path of the python package directory.
    :type dir: str
    :param name:  The file name of the python package.
    :type name: str
    :param force: Force install regardless of whether the package already installed.
    :type force: bool
    :param optional: If the specified python package not exists and this param is True,
                     return 0. If the specified python package not exists and this param
                     is False, raise an Exception.
    :type optional: bool
    :param extras: Specifies the extras_requires param of pip.
    :type extras: list[str]
    :return: 0 if install success or no need to install.
    """
    for filename in files:
        if filename == name:
            package = name
            if extras is not None and len(extras) > 0:
                package += "[" + ",".join(extras) + "]"
            if force:
                args = ["install", "--upgrade", "--force-reinstall", "--no-deps",
                        os.path.join(dir, package)]
                args[0:0] = pip
                args = add_test_params(args)
                exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
                if exit_code > 0:
                    raise Exception("execute %s error! exit code: %d" % (" ".join(args),
                                                                         exit_code))
                args = ["install", os.path.join(dir, package)]
                args[0:0] = pip
                args = add_test_params(args)
                exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
                if exit_code > 0:
                    raise Exception("execute %s error! exit code: %d" % (" ".join(args),
                                                                         exit_code))
            else:
                args = ["install", os.path.join(dir, package)]
                args[0:0] = pip
                args = add_test_params(args)
                exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
                if exit_code > 0:
                    raise Exception("execute %s error! exit code: %d" % (" ".join(args),
                                                                         exit_code))
            return exit_code
    if optional:
        return 0
    raise Exception("package '" + name + "' not found")


# try to install user-provided apache beam sdk
sdk_whl_file = find_beam_sdk_whl()
if sdk_whl_file != "":
    try:
        exit_code = pip_install_package(files, staged_dir, sdk_whl_file, False, False, ["gcp"])
    except Exception as e:
        print(e)

# try to install the user-provided dataflow python sdk
sdk_src_file = "dataflow_python_sdk.tar"
if os.path.exists(os.path.join(staged_dir, sdk_src_file)):
    pip_install_package(files, staged_dir, sdk_src_file, False, False, ["gcp"])


def pip_install_requirements(files, dir, name):
    """
    Install the libraries listed in the specified requirement file.
    Note that it will not install those libraries from central repositories.
    The artifacts of the libraries must exist in the downloaded files,
    but their dependencies can be installed from central repositories.
    More detail see :class:`apache_beam.options.pipeline_options.SetupOptions`.

    :param files: The list of the downloaded file names.
    :type files: list[str]
    :param dir: The path of the requirement file directory.
    :type dir: str
    :param name: The requirement file name.
    :return: 0 if install success.
    """
    for filename in files:
        if filename == name:
            args = ["install", "-r", os.path.join(dir, name), "--no-index", "--no-deps",
                    "--find-links", dir]
            args[0:0] = pip
            args = add_test_params(args)
            exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
            if exit_code > 0:
                raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
            args = ["install", "-r", os.path.join(dir, name), "--find-links", dir]
            args[0:0] = pip
            args = add_test_params(args)
            exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
            if exit_code > 0:
                raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
            return 0
    return 0


# install all the libraries listed in the requirements.txt
pip_install_requirements(files, staged_dir, "requirements.txt")


def install_extra_packages(files, extra_packages_file, dir):
    """
    Force install extra packages listed in the specified file.
    Each line in the extra_packages_file is a file name(*.tar.gz, *.tar, *.whl and *.zip).
    More detail see :class:`apache_beam.options.pipeline_options.SetupOptions`.

    :param files: The dict of the names of downloaded files.
    :type files: list[str]
    :param extra_packages_file: The file name of the extra_packages_file.
    :type extra_packages_file: str
    :param dir: The path of the extra_packages_file directory.
    :type dir: str
    :return: 0 if install success.
    """
    for filename in files:
        if filename == extra_packages_file:
            with open(os.path.join(dir, extra_packages_file), "r") as f:
                while True:
                    text_line = f.readline().strip()
                    if text_line:
                        pip_install_package(files, dir, text_line, True, False, None)
                    else:
                        break
            return 0
    return 0


# install extra packages listed in the extra_packages.txt
install_extra_packages(files, "extra_packages.txt", staged_dir)

# install the packages created from user-provided setup.py
pip_install_package(files, staged_dir, "workflow.tar.gz", False, True, None)

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

call([python_path, "-m", "apache_beam.runners.worker.sdk_worker_main"],
     stdout=sys.stdout, stderr=sys.stderr, env=env)
