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

#FLINK_DIR=/Users/kkloudas/repos/dataartisans/flink/build-target flink-end-to-end-tests/test-scripts/test_ha.sh
TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-examples/flink-examples-streaming/target/StateMachineExample.jar\ --error-rate\ 0.0\ --sleep\ 2

stop_cluster_and_watchdog() {
    kill ${watchdogPid} 2> /dev/null
    wait ${watchdogPid} 2> /dev/null

    stop_ha_cluster
}

verify_logs() {
    expectedRetries=$1

    # verify that we have no alerts
    if ! [ `cat ${output} | wc -l` -eq 0 ]; then
        echo "FAILURE: Alerts found at the StateMachineExample with 0.0 error rate."
        PASS=""
    fi

    # checks that all apart from the first JM recover the failes jobgraph.
    if ! [ `grep -r --include '*standalonesession*.log' Recovered SubmittedJobGraph "${FLINK_DIR}/log/" | cut -d ":" -f 1 | uniq | wc -l` -eq ${expectedRetries} ]; then
        echo "FAILURE: A JM did not take over."
        PASS=""
    fi

    # search the logs for JMs that log completed checkpoints
    if ! [ `grep -r --include '*standalonesession*.log' Completed checkpoint "${FLINK_DIR}/log/" | cut -d ":" -f 1 | uniq | wc -l` -eq $((expectedRetries + 1)) ]; then
        echo "FAILURE: A JM did not execute the job."
        PASS=""
    fi
}

run_ha_test() {
    parallelism=$1
    backend=$2
    async=$3
    incremental=$4
    maxAttempts=$5
    rstrtInterval=$6
    output=$7

    jmKillAndRetries=2
    checkpointDir="${TEST_DATA_DIR}/checkpoints/"

    # start the cluster on HA mode and
    # verify that all JMs are running
    start_ha_cluster

    echo "Running on HA mode: parallelism=${parallelism}, backend=${backend}, asyncSnapshots=${async}, and incremSnapshots=${incremental}."

    # submit a job in detached mode and let it run
    $FLINK_DIR/bin/flink run -d -p ${parallelism} \
     $TEST_PROGRAM_JAR \
        --stateBackend ${backend} \
        --checkpointDir "file://${checkpointDir}" \
        --asyncCheckpoints ${async} \
        --incrementalCheckpoints ${incremental} \
        --restartAttempts ${maxAttempts} \
        --restartDelay ${rstrtInterval} \
        --output ${output} > /dev/null

    # start the watchdog that keeps the number of JMs stable
    jm_watchdog 1 "8081" &
    watchdogPid=$!

    # let the job run for a while to take some checkpoints
    sleep 50

    for (( c=0; c<${jmKillAndRetries}; c++ )); do
        # kill the JM and wait for watchdog to
        # create a new JM which will take over
        kill_jm 0
        sleep 50
    done

    verify_logs ${jmKillAndRetries}

    # kill the cluster and zookeeper
    stop_cluster_and_watchdog
}

run_ha_test 1 "file" "false" "false" 3 100 "${TEST_DATA_DIR}/output.txt"
run_ha_test 1 "rocks" "false" "false" 3 100 "${TEST_DATA_DIR}/output.txt"
run_ha_test 1 "file" "true" "false" 3 100 "${TEST_DATA_DIR}/output.txt"
run_ha_test 1 "rocks" "false" "true" 3 100 "${TEST_DATA_DIR}/output.txt"
trap stop_cluster_and_watchdog EXIT
