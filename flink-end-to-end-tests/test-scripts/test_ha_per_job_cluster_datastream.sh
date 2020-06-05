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

TEST_PROGRAM_JAR_NAME=DataStreamAllroundTestProgram.jar
TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-datastream-allround-test/target/${TEST_PROGRAM_JAR_NAME}
FLINK_LIB_DIR=${FLINK_DIR}/lib
JOB_ID="00000000000000000000000000000000"

#
# NOTE: This script requires at least Bash version >= 4. Mac OS in 2020 still ships 3.x
#

function ha_cleanup() {
  stop_watchdogs
  kill_all 'StandaloneApplicationClusterEntryPoint'
}

on_exit ha_cleanup

function run_job() {
    local PARALLELISM=$1
    local BACKEND=$2
    local ASYNC=$3
    local INCREM=$4

    local CHECKPOINT_DIR="${TEST_DATA_DIR}/checkpoints/"

    ${FLINK_DIR}/bin/standalone-job.sh start \
        --job-classname org.apache.flink.streaming.tests.DataStreamAllroundTestProgram \
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
        --sequence_generator_source.sleep_after_elements 1
}

function verify_logs_per_job() {
    local JM_FAILURES=$1
    local EXIT_CODE=0

    # verify that we have no alerts
    if ! check_logs_for_non_empty_out_files; then
        echo "FAILURE: Alerts found at the general purpose job."
        EXIT_CODE=1
    fi

    # checks that all apart from the first JM recover the failed jobgraph.
    if ! verify_num_occurences_in_logs 'standalonejob' 'Found 0 checkpoints in ZooKeeper' 1; then
        echo "FAILURE: A JM did not take over, but started new job."
        EXIT_CODE=1
    fi

    if ! verify_num_occurences_in_logs 'standalonejob' 'Found [[:digit:]]\+ checkpoints in ZooKeeper' $((JM_FAILURES + 1)); then
        echo "FAILURE: A JM did not take over."
        EXIT_CODE=1
    fi

    # search the logs for JMs that log completed checkpoints
    if ! verify_num_occurences_in_logs 'standalonejob' 'Completed checkpoint' $((JM_FAILURES + 1)); then
        echo "FAILURE: A JM did not execute the job."
        EXIT_CODE=1
    fi

    if [[ $EXIT_CODE != 0 ]]; then
        echo "One or more tests FAILED."
        exit $EXIT_CODE
    fi
}

function run_ha_test() {
    local PARALLELISM=$1
    local BACKEND=$2
    local ASYNC=$3
    local INCREM=$4
    local ZOOKEEPER_VERSION=$5

    local JM_KILLS=3

    CLEARED=0

    # add job jar to cluster classpath
    cp ${TEST_PROGRAM_JAR} ${FLINK_LIB_DIR}

    # start the cluster on HA mode
    create_ha_config

    # change the pid dir to start log files always from 0, this is important for checks in the
    # jm killing loop
    set_config_key "env.pid.dir" "${TEST_DATA_DIR}"

    setup_flink_shaded_zookeeper ${ZOOKEEPER_VERSION}
    start_local_zk

    echo "Running on HA mode: parallelism=${PARALLELISM}, backend=${BACKEND}, asyncSnapshots=${ASYNC}, incremSnapshots=${INCREM} and zk=${ZOOKEEPER_VERSION}."

    # submit a job in detached mode and let it run
    run_job ${PARALLELISM} ${BACKEND} ${ASYNC} ${INCREM}

    # divide parallelism by slots per tm with rounding up
    local neededTaskmanagers=$(( (${PARALLELISM} + ${TASK_SLOTS_PER_TM_HA} - 1)  / ${TASK_SLOTS_PER_TM_HA} ))
    start_taskmanagers ${neededTaskmanagers}

    wait_job_running ${JOB_ID}

    # start the watchdog that keeps the number of JMs stable
    start_ha_jm_watchdog 1 "StandaloneApplicationClusterEntryPoint" run_job ${PARALLELISM} ${BACKEND} ${ASYNC} ${INCREM}

    # start the watchdog that keeps the number of TMs stable
    start_ha_tm_watchdog ${JOB_ID} ${neededTaskmanagers}

    # let the job run for a while to take some checkpoints
    wait_num_of_occurence_in_logs "Completed checkpoint [1-9]* for job ${JOB_ID}" 2 "standalonejob"

    for (( c=1; c<=${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_single 'StandaloneApplicationClusterEntryPoint'
        # let the job start and take some checkpoints
        wait_num_of_occurence_in_logs "Completed checkpoint [1-9]* for job ${JOB_ID}" 2 "standalonejob-${c}"
    done

    # verify checkpoints in the logs
    verify_logs_per_job ${JM_KILLS}
}


STATE_BACKEND_TYPE=${1:-file}
STATE_BACKEND_FILE_ASYNC=${2:-true}
STATE_BACKEND_ROCKS_INCREMENTAL=${3:-false}
ZOOKEEPER_VERSION=${4:-3.4}

run_test_with_timeout 900 run_ha_test 4 ${STATE_BACKEND_TYPE} ${STATE_BACKEND_FILE_ASYNC} ${STATE_BACKEND_ROCKS_INCREMENTAL} ${ZOOKEEPER_VERSION}
