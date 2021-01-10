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

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar

function ha_cleanup() {
  # kill the cluster and zookeeper
  stop_watchdogs
}

on_exit ha_cleanup

function run_ha_test() {
    local PARALLELISM=$1
    local BACKEND=$2
    local ASYNC=$3
    local INCREM=$4
    local ZOOKEEPER_VERSION=$5

    local JM_KILLS=3
    local CHECKPOINT_DIR="${TEST_DATA_DIR}/checkpoints/"

    CLEARED=0

    # start the cluster on HA mode
    create_ha_config
    # change the pid dir to start log files always from 0, this is important for checks in the
    # jm killing loop
    set_config_key "env.pid.dir" "${TEST_DATA_DIR}"
    set_config_key "env.java.opts" "-ea"
    setup_flink_shaded_zookeeper ${ZOOKEEPER_VERSION}
    start_local_zk
    start_cluster

    echo "Running on HA mode: parallelism=${PARALLELISM}, backend=${BACKEND}, asyncSnapshots=${ASYNC}, incremSnapshots=${INCREM} and zk=${ZOOKEEPER_VERSION}."

    # submit a job in detached mode and let it run
    local JOB_ID=$($FLINK_DIR/bin/flink run -d -p ${PARALLELISM} \
     $TEST_PROGRAM_JAR \
        --environment.parallelism ${PARALLELISM} \
        --test.semantics exactly-once \
        --test.simulate_failure true \
        --test.simulate_failure.num_records 200 \
        --test.simulate_failure.num_checkpoints 1 \
        --test.simulate_failure.max_failures 20 \
        --state_backend ${BACKEND} \
        --state_backend.checkpoint_directory "file://${CHECKPOINT_DIR}" \
        --state_backend.file.async ${ASYNC} \
        --state_backend.rocks.incremental ${INCREM} \
        --sequence_generator_source.sleep_time 15 \
        --sequence_generator_source.sleep_after_elements 1 \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}

    # start the watchdog that keeps the number of JMs stable
    start_ha_jm_watchdog 1 "StandaloneSessionClusterEntrypoint" start_jm_cmd "8081"

    sleep 5

    # start the watchdog that keeps the number of TMs stable
    start_ha_tm_watchdog ${JOB_ID} 1

    # let the job run for a while to take some checkpoints
    wait_num_of_occurence_in_logs "Completed checkpoint [1-9]* for job ${JOB_ID}" 2 "standalonesession"

    for (( c=1; c<=${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_single 'StandaloneSessionClusterEntrypoint'
        # let the job start and take some checkpoints
        wait_num_of_occurence_in_logs "Completed checkpoint [1-9]* for job ${JOB_ID}" 2 "standalonesession-${c}"
    done

    # verify checkpoints in the logs
    verify_logs ${JM_KILLS} true
}

STATE_BACKEND_TYPE=${1:-file}
STATE_BACKEND_FILE_ASYNC=${2:-true}
STATE_BACKEND_ROCKS_INCREMENTAL=${3:-false}
ZOOKEEPER_VERSION=${4:-3.4}

run_test_with_timeout 900 run_ha_test 4 ${STATE_BACKEND_TYPE} ${STATE_BACKEND_FILE_ASYNC} ${STATE_BACKEND_ROCKS_INCREMENTAL} ${ZOOKEEPER_VERSION}
