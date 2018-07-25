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

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-dataset-allround-test/target/DataSetAllroundTestProgram.jar

JM_WATCHDOG_PID=0

# flag indicating if we have already cleared up things after a test
CLEARED=0

function stop_cluster_and_watchdog() {
    if [ ${CLEARED} -eq 0 ]; then

        if ! [ ${JM_WATCHDOG_PID} -eq 0 ]; then
            echo "Killing JM watchdog @ ${JM_WATCHDOG_PID}"
            kill ${JM_WATCHDOG_PID} 2> /dev/null
            wait ${JM_WATCHDOG_PID} 2> /dev/null
        fi

        CLEARED=1
    fi
}

function verify_logs() {
    local OUTPUT=$FLINK_DIR/log/*.out
    local JM_FAILURES=$1
    local EXIT_CODE=0

    # verify that we have no alerts
    if ! [ `cat ${OUTPUT} | wc -l` -eq 0 ]; then
        echo "FAILURE: Alerts found at the general purpose DataSet job."
        EXIT_CODE=1
    fi

    # checks that all apart from the first JM recover the failed jobgraph.
    if ! [ `grep -r --include '*standalonesession*.log' 'Recovered SubmittedJobGraph' "${FLINK_DIR}/log/" | cut -d ":" -f 1 | uniq | wc -l` -eq ${JM_FAILURES} ]; then
        echo "FAILURE: A JM did not take over."
        EXIT_CODE=1
    fi

    if [[ $EXIT_CODE != 0 ]]; then
        echo "One or more tests FAILED."
        exit $EXIT_CODE
    fi
}

function jm_watchdog() {
    local EXPECTED_JMS=$1
    local IP_PORT=$2

    while true; do
        local RUNNING_JMS=`jps | grep 'StandaloneSessionClusterEntrypoint' | wc -l`;
        local MISSING_JMS=$((EXPECTED_JMS-RUNNING_JMS))
        for (( c=0; c<MISSING_JMS; c++ )); do
            "$FLINK_DIR"/bin/jobmanager.sh start "localhost" ${IP_PORT}
        done
        sleep 1;
    done
}

function kill_jm {
    local JM_PIDS=`jps | grep 'StandaloneSessionClusterEntrypoint' | cut -d " " -f 1`
    local JM_PIDS=(${JM_PIDS[@]})
    local PID=${JM_PIDS[0]}
    kill -9 ${PID}

    echo "Killed JM @ ${PID}"
}

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
        --source ${TEST_DATA_DIR}/control/test.txt \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}

    # start the watchdog that keeps the number of JMs stable
    jm_watchdog 1 "8081" &
    JM_WATCHDOG_PID=$!
    echo "Running JM watchdog @ ${JM_WATCHDOG_PID}"

    for (( c=0; c<${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_jm
        sleep 20
        wait_job_running ${JOB_ID}
    done

    printf "STOP" >> ${TEST_DATA_DIR}/control/test.txt

    wait_job_terminal_state "${JOB_ID}" "FINISHED"

    verify_logs ${JM_KILLS}

    check_result_hash "DataSet-Allround-Test HA" $TEST_DATA_DIR/out/dataset_allround "d3cf2aeaa9320c772304cba42649eb47"
    # kill the cluster and zookeeper
    stop_cluster_and_watchdog
}

trap stop_cluster_and_watchdog INT
trap stop_cluster_and_watchdog EXIT

run_ha_test 4
