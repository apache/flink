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

function containers_health_check() {
  local container_names=${@:1}
  for container in ${container_names}; do
    if ! [ $(docker inspect -f '{{.State.Running}}' ${container} 2>&1) = 'true' ];
    then
      return 1;
    fi
  done
}

# builds a base image once per job that routes the in-container apt through the same mirror list the host CI step uses
function prepare_fast_base_image() {
    local dockerfile="$1"
    local base_tag="flink-e2e-base:local"
    local mirror_list="${END_TO_END_DIR}/../tools/ci/ubuntu-mirror-list.txt"
    [[ -f "$mirror_list" ]] || return 0
    if ! docker image inspect "$base_tag" >/dev/null 2>&1; then
        local upstream_from ctx
        upstream_from=$(awk '/^FROM /{print $2; exit}' "$dockerfile")
        [[ -z "$upstream_from" ]] && return 0
        ctx=$(mktemp -d)
        cp "$mirror_list" "${ctx}/mirrors.txt"
        cat > "${ctx}/Dockerfile" <<EOF
FROM ${upstream_from}
COPY mirrors.txt /etc/apt/mirrors.txt
RUN sed -i "s|http://archive.ubuntu.com/ubuntu/|mirror+file:/etc/apt/mirrors.txt|g" /etc/apt/sources.list.d/ubuntu.sources /etc/apt/sources.list 2>/dev/null || true
EOF
        docker build -t "$base_tag" --network=host "$ctx"
        rm -rf "$ctx"
    fi
    sed -i.bak "0,/^FROM /s#^FROM .*#FROM ${base_tag}#" "$dockerfile" && rm -f "${dockerfile}.bak"
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

    echo "Preparing Dockerfiles"
    retry_times_with_exponential_backoff 5 git clone https://github.com/apache/flink-docker.git --branch dev-master --single-branch

    local java_version=17
    if [[ ${PROFILE} == *"jdk17"* ]]; then
        java_version=17
    fi
    if [[ ${PROFILE} == *"jdk21"* ]]; then
        java_version=21
    fi
    if [[ ${PROFILE} == *"jdk25"* ]]; then
        java_version=25
    fi

    cd flink-docker
    ./add-custom.sh -u ${file_server_address}:9999/flink.tgz -n ${image_name} -j ${java_version}

    # eclipse-temurin:25+ images don't include wget; add it to the install step
    sed -i 's/apt-get -y install gpg/apt-get -y install gpg wget/' dev/${image_name}-ubuntu/Dockerfile

    prepare_fast_base_image dev/${image_name}-ubuntu/Dockerfile

    echo "Building images"
    run_with_timeout 600 docker build --no-cache --network="host" -t ${image_name} dev/${image_name}-ubuntu
    local build_image_result=$?
    popd
    return $build_image_result
}

function start_file_server() {
    command -v python3 >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      python3 ${TEST_INFRA_DIR}/python3_fileserver.py &
      return
    fi

    command -v python >/dev/null 2>&1
    if [[ $? -eq 0 ]]; then
      python ${TEST_INFRA_DIR}/python2_fileserver.py &
      return
    fi

    echo "Could not find python(3) installation for starting fileserver."
    exit 1
}
