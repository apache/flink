#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
set -o pipefail

source "$(dirname "$0")"/common.sh

docker --version
docker-compose --version

function containers_health_check() {
  local container_names=${@:1}
  for container in ${container_names}; do
    if ! [ $(docker inspect -f '{{.State.Running}}' ${container} 2>&1) = 'true' ];
    then
      return 1;
    fi
  done
}

function build_image() {
    local image_name=${1:-flink-job}
    local default_file_server_address="localhost"
    [[ "${OS_TYPE}" != "linux" ]] && default_file_server_address="host.docker.internal"
    local file_server_address=${2:-${default_file_server_address}}

    echo "Starting fileserver for Flink distribution"
    pushd ${FLINK_DIR}/..
    tar -czf "${TEST_DATA_DIR}/flink.tgz" flink-*
    popd
    pushd ${TEST_DATA_DIR}
    start_file_server
    local server_pid=$!

    echo "Preparing Dockeriles"
    git clone https://github.com/apache/flink-docker.git --branch dev-master --single-branch
    cd flink-docker
    ./add-custom.sh -u ${file_server_address}:9999/flink.tgz -n ${image_name}

    echo "Building images"
    run_with_timeout 600 docker build --no-cache --network="host" -t ${image_name} dev/${image_name}-debian
    popd
}

function start_file_server() {
    command -v python >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      python ${TEST_INFRA_DIR}/python2_fileserver.py &
      return
    fi

    command -v python3 >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      python3 ${TEST_INFRA_DIR}/python3_fileserver.py &
      return
    fi

    echo "Could not find python(3) installation for starting fileserver."
    exit 1
}
