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
source "$(dirname "$0")"/common_ha.sh

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-dataset-allround-test/target/DataSetAllroundTestProgram.jar

function ha_cleanup() {
  # kill the cluster and zookeeper
  stop_watchdogs
}

on_exit ha_cleanup

function run_ha_test() {
    local PARALLELISM=$1

    local JM_KILLS=3

    CLEARED=0
    mkdir -p ${TEST_DATA_DIR}/control
    touch ${TEST_DATA_DIR}/control/test.txt

    # start the cluster on HA mode
    start_ha_cluster

    echo "Running on HA mode: parallelism=${PARALLELISM}."

    # submit a job in detached mode and let it run
    local JOB_ID=$($FLINK_DIR/bin/flink run -d -p ${PARALLELISM} \
        $TEST_PROGRAM_JAR \
        --loadFactor 4 \
        --outputPath $TEST_DATA_DIR/out/dataset_allround \
        --source true \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}

    # start the watchdog that keeps the number of JMs stable
    start_ha_jm_watchdog 1 "StandaloneSessionClusterEntrypoint" start_jm_cmd "8081"

    for (( c=0; c<${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_single 'StandaloneSessionClusterEntrypoint'
        wait_job_running ${JOB_ID}
    done

    cancel_job ${JOB_ID}

    # do not verify checkpoints in the logs
    verify_logs ${JM_KILLS} false
}

run_ha_test 4
