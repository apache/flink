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

# This function checks the logs for entries that indicate problems with local recovery
function check_logs {
    local parallelism=$1
    local attempts=$2
    (( expected_count=parallelism * (attempts + 1) ))

    # Search for the log message that indicates restore problem from existing local state for the keyed backend.
    local failed_local_recovery=$(grep '^.*Creating keyed state backend.* from alternative (2/2)\.$' $FLINK_LOG_DIR/* | wc -l | tr -d ' ')

    # Search for attempts to recover locally.
    local attempt_local_recovery=$(grep '^.*Creating keyed state backend.* from alternative (1/2)\.$' $FLINK_LOG_DIR/* | wc -l | tr -d ' ')

    if [ ${failed_local_recovery} -ne 0 ]
    then
        echo "FAILURE: Found ${failed_local_recovery} failed attempt(s) for local recovery of correctly scheduled task(s)."
        exit 1
    fi

    if [ ${attempt_local_recovery} -eq 0 ]
    then
        echo "FAILURE: Found no attempt for local recovery. Configuration problem?"
        exit 1
    fi
}

# This function does a cleanup after the test. The watchdog is terminated and temporary
# files and folders are deleted.
function cleanup_after_test {
    kill ${watchdog_pid} 2> /dev/null || true
    wait ${watchdog_pid} 2> /dev/null || true
}
on_exit cleanup_after_test

## This function executes one run for a certain configuration
function run_local_recovery_test {
    local parallelism=$1
    local max_attempts=$2
    local backend=$3
    local incremental=$4
    local kill_jvm=$5
    local delay=$6

    echo "Running local recovery test with configuration:
        parallelism: ${parallelism}
        max attempts: ${max_attempts}
        backend: ${backend}
        incremental checkpoints: ${incremental}
        kill JVM: ${kill_jvm}
        delay: ${delay}ms"

    TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-local-recovery-and-allocation-test/target/StickyAllocationAndLocalRecoveryTestJob.jar
    # configure for HA
    create_ha_config
    # required for PID business in StickyAllocationAndLocalRecoveryTestJob
    set_config_key env.java.opts.taskmanager "--add-opens=java.management/sun.management=ALL-UNNAMED"

    # Enable debug logging
    sed -i -e 's/rootLogger.level = .*/rootLogger.level = DEBUG/' "$FLINK_DIR/conf/log4j.properties"

    # Enable local recovery
    set_config_key "state.backend.local-recovery" "true"
    # Ensure that each TM only has one operator(chain)
    set_config_key "taskmanager.numberOfTaskSlots" "1"

    rm $FLINK_LOG_DIR/* 2> /dev/null

    # Start HA server
    start_local_zk
    start_cluster

    tm_watchdog ${parallelism} &
    watchdog_pid=$!

    echo "Started TM watchdog with PID ${watchdog_pid}."

    $FLINK_DIR/bin/flink run -c org.apache.flink.streaming.tests.StickyAllocationAndLocalRecoveryTestJob \
    -p ${parallelism} $TEST_PROGRAM_JAR \
    -D state.backend.local-recovery=ENABLE_FILE_BASED \
    --checkpointDir file://$TEST_DATA_DIR/local_recovery_test/checkpoints \
    --output $TEST_DATA_DIR/out/local_recovery_test/out --killJvmOnFail ${kill_jvm} --checkpointInterval 1000 \
    --maxAttempts ${max_attempts} --parallelism ${parallelism} --stateBackend ${backend} \
    --incrementalCheckpoints ${incremental} --delay ${delay}

    check_logs ${parallelism} ${max_attempts}
    cleanup_after_test
}

## MAIN
run_test_with_timeout 900 run_local_recovery_test "$@"
