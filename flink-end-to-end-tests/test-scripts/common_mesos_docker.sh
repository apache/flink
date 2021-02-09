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
export MESOS_AGENT_CPU=1

echo "End-to-end directory $END_TO_END_DIR"

start_time=$(date +%s)

# make sure we stop our cluster at the end
function cluster_shutdown {
  docker exec mesos-master bash -c "chmod -R ogu+rw $FLINK_LOG_DIR/ ${TEST_DATA_DIR}"
  docker-compose -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/docker-compose.yml down
}
on_exit cluster_shutdown

function start_flink_cluster_with_mesos() {
    echo "Starting Flink on Mesos cluster"
    if ! retry_times $IMAGE_BUILD_RETRIES 0 build_image; then
        echo "ERROR: Could not build mesos image. Aborting..."
        exit 1
    fi
    # build docker image with java and mesos
    build_image

    # we need to export the MVN_REPO location so that mesos can access the files referenced in HADOOP_CLASSPATH
    export MVN_REPO=`mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`

    # start mesos cluster
    docker-compose -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/docker-compose.yml up -d

    # wait for the Mesos master and slave set up
    start_time=$(date +%s)
    wait_rest_endpoint_up "http://${NODENAME}:5050/slaves" "Mesos" "\{\"slaves\":\[.+\].*\}"

    # perform health checks
    containers_health_check "mesos-master" "mesos-slave"

    set_config_key "jobmanager.rpc.address" "mesos-master"
    set_config_key "rest.address" "mesos-master"

    docker exec --env HADOOP_CLASSPATH=$HADOOP_CLASSPATH -itd mesos-master bash -c "${FLINK_DIR}/bin/mesos-appmaster.sh -Dmesos.master=mesos-master:5050"

    wait_rest_endpoint_up "http://${NODENAME}:8081/taskmanagers" "Dispatcher" "\{\"taskmanagers\":\[.*\]\}"
    return 0
}

function build_image() {
    echo "Building Mesos Docker container"
    docker build -f $END_TO_END_DIR/test-scripts/docker-mesos-cluster/Dockerfile \
        -t flink/docker-mesos-cluster:latest \
        $END_TO_END_DIR/test-scripts/docker-mesos-cluster/
}

function wait_job_terminal_state_mesos {
  local job=$1
  local expected_terminal_state=$2
  wait_job_terminal_state $1 $2 "mesos-appmaster"
}

function wait_num_of_occurence_in_logs_mesos() {
    local text=$1
    local number=$2
    wait_num_of_occurence_in_logs $1 $2 "mesos-appmaster"
}
