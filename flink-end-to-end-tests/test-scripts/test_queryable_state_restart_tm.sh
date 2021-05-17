#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/queryable_state_base.sh

QUERYABLE_STATE_SERVER_JAR=${END_TO_END_DIR}/flink-queryable-state-test/target/QsStateProducer.jar
QUERYABLE_STATE_CLIENT_JAR=${END_TO_END_DIR}/flink-queryable-state-test/target/QsStateClient.jar

#####################
# Test that queryable state works as expected with HA mode when restarting a taskmanager
#
# The general outline is like this:
# 1. start cluster in HA mode with 1 TM
# 2. start a job that exposes queryable state from a mapstate with increasing num. of keys
# 3. query the state with a queryable state client and expect no error to occur
# 4. stop the TM
# 5. check how many keys were in our mapstate at the time of the latest snapshot
# 6. start a new TM
# 7. query the state with a queryable state client and retrieve the number of elements
#    in the mapstate
# 8. expect the number of elements in the mapstate after restart of TM to be > number of elements
#    at last snapshot
#
# Globals:
#   QUERYABLE_STATE_SERVER_JAR
#   QUERYABLE_STATE_CLIENT_JAR
# Arguments:
#   None
# Returns:
#   None
#####################
function run_test() {
    local EXIT_CODE=0
    local PARALLELISM=1 # parallelism of queryable state app
    local PORT="9069" # port of queryable state server

    # speeds up TM loss detection
    set_config_key "heartbeat.interval" "2000"
    set_config_key "heartbeat.timeout" "10000"

    link_queryable_state_lib
    start_cluster

    local JOB_ID=$(${FLINK_DIR}/bin/flink run \
        -p ${PARALLELISM} \
        -d ${QUERYABLE_STATE_SERVER_JAR} \
        --state-backend "rocksdb" \
        --tmp-dir file://${TEST_DATA_DIR} \
        | awk '{print $NF}' | tail -n 1)

    wait_job_running ${JOB_ID}
    wait_for_number_of_checkpoints ${JOB_ID} 10 60

    SERVER=$(get_queryable_state_server_ip)
    PORT=$(get_queryable_state_proxy_port)

    echo SERVER: ${SERVER}
    echo PORT: ${PORT}

    java -jar ${QUERYABLE_STATE_CLIENT_JAR} \
        --host ${SERVER} \
        --port ${PORT} \
        --iterations 1 \
        --job-id ${JOB_ID}

    if [ $? != 0 ]; then
        echo "An error occurred when executing queryable state client"
        exit 1
    fi

    kill_random_taskmanager
    wait_for_number_of_running_tms 0

    latest_snapshot_count=$(cat $FLINK_LOG_DIR/*out* | grep "on snapshot" | tail -n 1 | awk '{print $4}')
    echo "Latest snapshot count was ${latest_snapshot_count}"

    start_and_wait_for_tm

    wait_job_running ${JOB_ID}

    local current_num_checkpoints="$(get_completed_number_of_checkpoints ${JOB_ID})"
    # wait for some more checkpoint to have happened
    local expected_num_checkpoints=$((current_num_checkpoints + 5))

    wait_for_number_of_checkpoints ${JOB_ID} ${expected_num_checkpoints} 60

    local num_entries_in_map_state_after=$(java -jar ${QUERYABLE_STATE_CLIENT_JAR} \
        --host ${SERVER} \
        --port ${PORT} \
        --iterations 1 \
        --job-id ${JOB_ID} | grep "MapState has" | awk '{print $3}')

    echo "after: $num_entries_in_map_state_after"

    if ((latest_snapshot_count > num_entries_in_map_state_after)); then
        echo "An error occurred"
        EXIT_CODE=1
    fi

    exit ${EXIT_CODE}
}

###################################
# Wait a specific number of successful checkpoints
# to have happened
#
# Globals:
#   None
# Arguments:
#   $1: the job id
#   $2: the number of expected successful checkpoints
#   $3: timeout in seconds
# Returns:
#   None
###################################
function wait_for_number_of_checkpoints {
    local job_id=$1
    local expected_num_checkpoints=$2
    local timeout=$3
    local count=0

    echo "Starting to wait for completion of ${expected_num_checkpoints} checkpoints"
    while (($(get_completed_number_of_checkpoints ${job_id}) < ${expected_num_checkpoints})); do

        if [[ ${count} -gt ${timeout} ]]; then
            echo "A timeout occurred waiting for successful checkpoints"
            exit 1
        else
            ((count+=2))
        fi

        local current_num_checkpoints=$(get_completed_number_of_checkpoints ${job_id})
        echo "${current_num_checkpoints}/${expected_num_checkpoints} completed checkpoints"
        sleep 2
    done
}

function get_completed_number_of_checkpoints {
    local job_id=$1
    local json_res=$(curl -s http://localhost:8081/jobs/${job_id}/checkpoints)

    echo ${json_res}    | # {"counts":{"restored":0,"total":25,"in_progress":1,"completed":24,"failed":0} ...
        cut -d ":" -f 6 | # 24,"failed"
        sed 's/,.*//'     # 24
}

run_test
