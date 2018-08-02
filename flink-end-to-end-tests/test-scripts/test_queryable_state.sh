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

function run_test {
    link_queryable_state_lib
    start_cluster

    QUERYABLE_STATE_PRODUCER_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QsStateProducer.jar
    QUERYABLE_STATE_CONSUMER_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-queryable-state-test/target/QsStateClient.jar

    # start app with queryable state and wait for it to be available
    JOB_ID=$(${FLINK_DIR}/bin/flink run \
        -p 1 \
        -d ${QUERYABLE_STATE_PRODUCER_JAR} \
        --state-backend $1 \
        --tmp-dir file://${TEST_DATA_DIR} \
        | awk '{print $NF}' | tail -n 1)

    wait_job_running ${JOB_ID}

    # run the client and query state the first time
    first_result=$(java -jar ${QUERYABLE_STATE_CONSUMER_JAR} \
        --host $(get_queryable_state_server_ip) \
        --port $(get_queryable_state_proxy_port) \
        --job-id ${JOB_ID})

    EXIT_CODE=$?

    # Exit
    exit ${EXIT_CODE}
}

function test_cleanup {
    unlink_queryable_state_lib
    clean_stdout_files
}

trap test_cleanup EXIT
run_test $1
