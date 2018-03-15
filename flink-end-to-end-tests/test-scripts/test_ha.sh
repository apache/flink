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

TEST_PROGRAM_JAR=$FLINK_DIR/examples/streaming/StateMachineExample.jar\ --error-rate\ 0.0\ --sleep\ 2

JM_WATCHDOG_PID=0
TM_WATCHDOG_PID=0

# flag indicating if we have already cleared up things after a test
CLEARED=0

function stop_cluster_and_watchdog() {
    if [ ${CLEARED} -eq 0 ]; then

        if ! [ ${JM_WATCHDOG_PID} -eq 0 ]; then
            echo "Killing JM watchdog @ ${JM_WATCHDOG_PID}"
            kill ${JM_WATCHDOG_PID} 2> /dev/null
            wait ${JM_WATCHDOG_PID} 2> /dev/null
        fi

        if ! [ ${TM_WATCHDOG_PID} -eq 0 ]; then
            echo "Killing TM watchdog @ ${TM_WATCHDOG_PID}"
            kill ${TM_WATCHDOG_PID} 2> /dev/null
            wait ${TM_WATCHDOG_PID} 2> /dev/null
        fi

        cleanup
        CLEARED=1
    fi
}

function verify_logs() {
    local OUTPUT=$1
    local JM_FAILURES=$2

    # verify that we have no alerts
    if ! [ `cat ${OUTPUT} | wc -l` -eq 0 ]; then
        echo "FAILURE: Alerts found at the StateMachineExample with 0.0 error rate."
        PASS=""
    fi

    # checks that all apart from the first JM recover the failed jobgraph.
    if ! [ `grep -r --include '*standalonesession*.log' Recovered SubmittedJobGraph "${FLINK_DIR}/log/" | cut -d ":" -f 1 | uniq | wc -l` -eq ${JM_FAILURES} ]; then
        echo "FAILURE: A JM did not take over."
        PASS=""
    fi

    # search the logs for JMs that log completed checkpoints
    if ! [ `grep -r --include '*standalonesession*.log' Completed checkpoint "${FLINK_DIR}/log/" | cut -d ":" -f 1 | uniq | wc -l` -eq $((JM_FAILURES + 1)) ]; then
        echo "FAILURE: A JM did not execute the job."
        PASS=""
    fi

    if [[ ! "$PASS" ]]; then
        echo "One or more tests FAILED."
        exit 1
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
        sleep 5;
    done
}

function kill_jm {
    local JM_PIDS=`jps | grep 'StandaloneSessionClusterEntrypoint' | cut -d " " -f 1`
    local JM_PIDS=(${JM_PIDS[@]})
    local PID=${JM_PIDS[0]}
    kill -9 ${PID}

    echo "Killed JM @ ${PID}"
}

function tm_watchdog() {
    local JOB_ID=$1
    local EXPECTED_TMS=$2

    # the number of already seen successful checkpoints
    local SUCCESSFUL_CHCKP=0

    while true; do

        # check how many successful checkpoints we have
        # and kill a TM only if the previous one already had some

        local CHECKPOINTS=`curl -s "http://localhost:8081/jobs/${JOB_ID}/checkpoints" | cut -d ":" -f 6 | sed 's/,.*//'`

        if [[ ${CHECKPOINTS} =~ '^[0-9]+$' ]] || [[ ${CHECKPOINTS} == "" ]]; then

            # this may be the case during leader election.
            # in this case we retry later with a smaller interval
            sleep 5; continue

        elif [ "${CHECKPOINTS}" -ne "${SUCCESSFUL_CHCKP}" ]; then

            # we are not only searching for > because when the JM goes down,
            # the job starts with reporting 0 successful checkpoints

            local RUNNING_TMS=`jps | grep 'TaskManager' | wc -l`
            local TM_PIDS=`jps | grep 'TaskManager' | cut -d " " -f 1`

            local MISSING_TMS=$((EXPECTED_TMS-RUNNING_TMS))
            if [ ${MISSING_TMS} -eq 0 ]; then
                # start a new TM only if we have exactly the expected number
                "$FLINK_DIR"/bin/taskmanager.sh start > /dev/null
            fi

            # kill an existing one
            local TM_PIDS=(${TM_PIDS[@]})
            local PID=${TM_PIDS[0]}
            kill -9 ${PID}

            echo "Killed TM @ ${PID}"

            SUCCESSFUL_CHCKP=${CHECKPOINTS}
        fi

        sleep 11;
    done
}

function run_ha_test() {
    local PARALLELISM=$1
    local BACKEND=$2
    local ASYNC=$3
    local INCREM=$4
    local OUTPUT=$5

    local JM_KILLS=3
    local CHECKPOINT_DIR="${TEST_DATA_DIR}/checkpoints/"

    CLEARED=0

    # start the cluster on HA mode
    start_ha_cluster

    echo "Running on HA mode: parallelism=${PARALLELISM}, backend=${BACKEND}, asyncSnapshots=${ASYNC}, and incremSnapshots=${INCREM}."

    # submit a job in detached mode and let it run
    local JOB_ID=$($FLINK_DIR/bin/flink run -d -p ${PARALLELISM} \
     $TEST_PROGRAM_JAR \
        --backend ${BACKEND} \
        --checkpoint-dir "file://${CHECKPOINT_DIR}" \
        --async-checkpoints ${ASYNC} \
        --incremental-checkpoints ${INCREM} \
        --output ${OUTPUT} | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}

    # start the watchdog that keeps the number of JMs stable
    jm_watchdog 1 "8081" &
    JM_WATCHDOG_PID=$!
    echo "Running JM watchdog @ ${JM_WATCHDOG_PID}"

    sleep 5

    # start the watchdog that keeps the number of TMs stable
    tm_watchdog ${JOB_ID} 1 &
    TM_WATCHDOG_PID=$!
    echo "Running TM watchdog @ ${TM_WATCHDOG_PID}"

    # let the job run for a while to take some checkpoints
    sleep 20

    for (( c=0; c<${JM_KILLS}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new one which will take over
        kill_jm
        sleep 60
    done

    verify_logs ${OUTPUT} ${JM_KILLS}

    # kill the cluster and zookeeper
    stop_cluster_and_watchdog
}

trap stop_cluster_and_watchdog EXIT
run_ha_test 4 "file" "false" "false" "${TEST_DATA_DIR}/output.txt"
run_ha_test 4 "rocks" "false" "false" "${TEST_DATA_DIR}/output.txt"
run_ha_test 4 "file" "true" "false" "${TEST_DATA_DIR}/output.txt"
run_ha_test 4 "rocks" "false" "true" "${TEST_DATA_DIR}/output.txt"
