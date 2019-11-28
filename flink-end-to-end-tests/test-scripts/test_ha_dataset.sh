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

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-dataset-fine-grained-recovery-test/target/DataSetFineGrainedRecoveryTestProgram.jar

function ha_cleanup() {
  # kill the cluster and zookeeper
  stop_watchdogs
}

on_exit ha_cleanup

function run_ha_test() {
    local PARALLELISM=$1

    local JM_KILLS=2
    local TM_KILLS=2

    local LATCH_FILE_PATH=$TEST_DATA_DIR/latchFile

    CLEARED=0

    setup_and_start_cluster ${PARALLELISM}
    echo "Running on HA mode: parallelism=${PARALLELISM}."

    # submit a job in detached mode and let it run
    local JOB_ID=$($FLINK_DIR/bin/flink run -d -p ${PARALLELISM} \
        $TEST_PROGRAM_JAR \
        --latchFilePath $LATCH_FILE_PATH \
        --outputPath $TEST_DATA_DIR/out/dataset_allround \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}

    local c
    for (( c=0; c<${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_single 'StandaloneSessionClusterEntrypoint'
        wait_job_running ${JOB_ID}
    done

    for (( c=0; c<${TM_KILLS}; c++ )); do
        kill_and_replace_random_task_manager
    done

    touch ${LATCH_FILE_PATH}

    wait_job_terminal_state ${JOB_ID} "FINISHED"
    check_result_hash "DataSet-FineGrainedRecovery-Test" $TEST_DATA_DIR/out/dataset_allround "ac3d26e1afce19aa657527f000acb88b"
}

function setup_and_start_cluster() {
    local NUM_TASK_MANAGERS=$1

    create_ha_config

    set_config_key "jobmanager.execution.failover-strategy" "region"
    set_config_key "taskmanager.numberOfTaskSlots" "1"

    set_config_key "restart-strategy" "fixed-delay"
    set_config_key "restart-strategy.fixed-delay.attempts" "2147483647"

    set_config_key "heartbeat.interval" "2000"
    set_config_key "heartbeat.timeout" "10000"

    start_local_zk
    start_ha_jm_watchdog 1 "StandaloneSessionClusterEntrypoint" start_jm_cmd "8081"
    start_taskmanagers ${NUM_TASK_MANAGERS}
}

function kill_and_replace_random_task_manager() {
    local NUM_TASK_MANAGERS=$(query_number_of_running_tms)

    kill_random_taskmanager
    wait_for_number_of_running_tms $(( ${NUM_TASK_MANAGERS} - 1 ))
    start_taskmanagers 1
    wait_for_number_of_running_tms ${NUM_TASK_MANAGERS}
}

run_ha_test 4
