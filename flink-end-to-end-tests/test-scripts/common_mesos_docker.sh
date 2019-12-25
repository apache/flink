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
source "$(dirname "$0")"/common_docker.sh

MAX_RETRY_SECONDS=120
IMAGE_BUILD_RETRIES=5
NODENAME=${NODENAME:-`hostname -f`}

echo "End-to-end directory $END_TO_END_DIR"

start_time=$(date +%s)

# make sure we stop our cluster at the end
function cluster_shutdown {
  docker-compose -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/docker-compose.yml down
}
on_exit cluster_shutdown

function start_flink_cluster_with_mesos() {
    echo "Starting Flink on Mesos cluster"
    build_image

    docker-compose -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/docker-compose.yml up -d

    # wait for the Mesos master and slave set up
    start_time=$(date +%s)
    wait_rest_endpoint_up "http://${NODENAME}:5050/slaves" "Mesos" "\{\"slaves\":\[.+\].*\}"

    # perform health checks
    containers_health_check "mesos-master" "mesos-slave"

    set_config_key "jobmanager.rpc.address" "mesos-master"
    set_config_key "rest.address" "mesos-master"

    docker exec -it mesos-master nohup bash -c "${FLINK_DIR}/bin/mesos-appmaster.sh -Dmesos.master=mesos-master:5050 &"
    wait_rest_endpoint_up "http://${NODENAME}:8081/taskmanagers" "Dispatcher" "\{\"taskmanagers\":\[.*\]\}"
    return 0
}

function build_image() {
    echo "Building Mesos Docker container"
    if ! retry_times $IMAGE_BUILD_RETRIES 0 docker build -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/Dockerfile \
        -t flink/docker-mesos-cluster:latest \
        $END_TO_END_DIR/test-scripts/docker-mesos-cluster/; then
        echo "ERROR: Could not build mesos image. Aborting..."
        exit 1
    fi
}
