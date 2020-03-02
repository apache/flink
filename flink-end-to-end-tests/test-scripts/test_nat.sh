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

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_docker.sh

DOCKER_MODULE_DIR=${END_TO_END_DIR}/../flink-container/docker
DOCKER_SCRIPTS=${END_TO_END_DIR}/test-scripts/container-scripts
DOCKER_IMAGE_BUILD_RETRIES=3
BUILD_BACKOFF_TIME=5

export FLINK_JOB=org.apache.flink.examples.java.wordcount.WordCount
export FLINK_DOCKER_IMAGE_NAME=test_nat
export INPUT_VOLUME=${END_TO_END_DIR}/test-scripts/test-data
export OUTPUT_VOLUME=${TEST_DATA_DIR}/out
export INPUT_PATH=/data/test/input
export OUTPUT_PATH=/data/test/output

export HOST_IP=$(get_node_ip | awk '{print $1}')
export JM_EX_HOSTNAME=jm.flink.test
export TM_1_EX_HOSTNAME=tm1.flink.test
export TM_2_EX_HOSTNAME=tm2.flink.test

export JM_RPC_EX_PORT=10000
export JM_RPC_IN_PORT=10000

export TM_1_RPC_EX_PORT=10001
export TM_2_RPC_EX_PORT=10002
export TM_RPC_IN_PORT=10000

export TM_1_DATA_EX_PORT=11001
export TM_2_DATA_EX_PORT=11002
export TM_DATA_IN_PORT=11000

RESULT_HASH="72a690412be8928ba239c2da967328a5"
INPUT_ARGS="--input ${INPUT_PATH}/words"
OUTPUT_PREFIX="docker_wc_out"

export FLINK_JOB_ARGUMENTS="${INPUT_ARGS} --output ${OUTPUT_PATH}/${OUTPUT_PREFIX}"

build_image() {
    build_image_with_jar ${FLINK_DIR}/examples/batch/WordCount.jar ${FLINK_DOCKER_IMAGE_NAME}
}

# user inside the container must be able to create files, this is a workaround in-container permissions
mkdir -p $OUTPUT_VOLUME
chmod 777 $OUTPUT_VOLUME

pushd "$DOCKER_MODULE_DIR"
if ! retry_times $DOCKER_IMAGE_BUILD_RETRIES ${BUILD_BACKOFF_TIME} build_image; then
    echo "Failed to build docker image. Aborting..."
    exit 1
fi
popd

docker-compose -f ${DOCKER_SCRIPTS}/docker-compose.nat.yml up --abort-on-container-exit --exit-code-from job-cluster &> /dev/null
docker-compose -f ${DOCKER_SCRIPTS}/docker-compose.nat.yml logs job-cluster > ${FLINK_DIR}/log/jobmanager.log
docker-compose -f ${DOCKER_SCRIPTS}/docker-compose.nat.yml logs taskmanager1 > ${FLINK_DIR}/log/taskmanager1.log
docker-compose -f ${DOCKER_SCRIPTS}/docker-compose.nat.yml logs taskmanager2 > ${FLINK_DIR}/log/taskmanager2.log

check_result_hash "WordCount" ${OUTPUT_VOLUME}/${OUTPUT_PREFIX}/ "${RESULT_HASH}"
