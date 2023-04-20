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

set -Eeuo pipefail

SCALE="1"
USE_TABLE_STATS=true

source "$(dirname "$0")"/common.sh

function run_test() {
    ################################################################################
    # Generate test data
    ################################################################################
    TPCDS_TOOL_DIR="$END_TO_END_DIR/flink-tpcds-test/tpcds-tool"
    ORGIN_ANSWER_DIR="$END_TO_END_DIR/flink-tpcds-test/tpcds-tool/answer_set"
    TPCDS_QUERY_DIR="$END_TO_END_DIR/flink-tpcds-test/tpcds-tool/query"

    TARGET_DIR="$END_TO_END_DIR/flink-tpcds-test/target"

    TPCDS_GENERATOR_DIR_DIR="$TARGET_DIR/generator"
    TPCDS_DATA_DIR="$TARGET_DIR/table"

    mkdir -p "$TPCDS_GENERATOR_DIR_DIR"
    mkdir -p "$TPCDS_DATA_DIR"

    cd "$TPCDS_TOOL_DIR"
    # use relative path, because tpcds gennerator cannot recognize path which is too long.
    TPCDS_GENERATOR_RELATIVE_DIR="../target/generator"
    TPCDS_DATA_RELATIVE_DIR="../table"

    ${TPCDS_TOOL_DIR}/data_generator.sh "$TPCDS_GENERATOR_RELATIVE_DIR" "$SCALE" "$TPCDS_DATA_RELATIVE_DIR" "$END_TO_END_DIR/test-scripts"

    cd "$END_TO_END_DIR"

    ################################################################################
    # Prepare Flink
    ################################################################################

    echo "[INFO]Preparing Flink cluster..."

    local scheduler="$1"

    set_config_key "jobmanager.scheduler" "${scheduler}"
    set_config_key "taskmanager.memory.process.size" "4096m"
    set_config_key "taskmanager.memory.network.fraction" "0.2"
    set_config_key "parallelism.default" "4"

    if [ "${scheduler}" == "Default" ]; then
        set_config_key "taskmanager.numberOfTaskSlots" "4"
    elif [ "${scheduler}" == "AdaptiveBatch" ]; then
        set_config_key "taskmanager.numberOfTaskSlots" "8"
        set_config_key "execution.batch.adaptive.auto-parallelism.max-parallelism" "8"
        set_config_key "execution.batch.adaptive.auto-parallelism.avg-data-volume-per-task" "6m"
        set_config_key "execution.batch.speculative.enabled" "true"
        set_config_key "execution.batch.speculative.block-slow-node-duration" "0s"
        set_config_key "slow-task-detector.execution-time.baseline-ratio" "0.0"
        set_config_key "slow-task-detector.execution-time.baseline-lower-bound" "0s"
    else
        echo "ERROR: Scheduler ${scheduler} is unsupported for tpcds test. Aborting..."
        exit 1
    fi

    start_cluster


    ################################################################################
    # Run TPC-DS SQL
    ################################################################################

    echo "[INFO] Runing TPC-DS queries..."

    RESULT_DIR="$TARGET_DIR/result"
    mkdir -p "$RESULT_DIR"

    $FLINK_DIR/bin/flink run -c org.apache.flink.table.tpcds.TpcdsTestProgram "$TARGET_DIR/TpcdsTestProgram.jar" -sourceTablePath "$TPCDS_DATA_DIR" -queryPath "$TPCDS_QUERY_DIR" -sinkTablePath "$RESULT_DIR" -useTableStats "$USE_TABLE_STATS"

    ################################################################################
    # validate result
    ################################################################################
    QUALIFIED_ANSWER_DIR="$TARGET_DIR/answer_set_qualified"
    mkdir -p "$QUALIFIED_ANSWER_DIR"

    java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.utils.AnswerFormatter -originDir "$ORGIN_ANSWER_DIR" -destDir "$QUALIFIED_ANSWER_DIR"

    java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.utils.TpcdsResultComparator -expectedDir "$QUALIFIED_ANSWER_DIR" -actualDir "$RESULT_DIR"

    ################################################################################
    # Clean-up generated data folder
    ################################################################################

    rm -rf "${TPCDS_DATA_DIR}"
    echo "Deleted all files under $TPCDS_DATA_DIR"
}

function check_logs_for_exceptions_for_adaptive_batch_scheduler {
    local additional_allowed_exceptions=("ExecutionGraphException: The execution attempt" \
    "Cannot find task to fail for execution" \
    "ExceptionInChainedOperatorException: Could not forward element to next operator" \
    "CancelTaskException: Buffer pool has already been destroyed" \
    "java.nio.channels.ClosedChannelException" \
    "java.lang.IllegalStateException: File writer is already closed")

    internal_check_logs_for_exceptions "${additional_allowed_exceptions[@]}"
}

function check_logs_for_errors_for_adaptive_batch_scheduler {
    local additional_allowed_errors=("The handler of the request-complete-callback threw an exception: java.nio.channels.ClosedChannelException")

    internal_check_logs_for_errors "${additional_allowed_errors[@]}"
}

SCHEDULER="${1:-Default}"
ACTION="${2:-run_test}"

if [ "${ACTION}" == "run_test" ]; then
    run_test "${SCHEDULER}"
elif [ "${ACTION}" == "check_exceptions" ]; then
    if [[ "${SCHEDULER}" != "AdaptiveBatch" ]]; then
        echo "Only supports checking exceptions for adaptive batch scheduler."
        exit 1
    fi

    check_logs_for_errors_for_adaptive_batch_scheduler
    check_logs_for_exceptions_for_adaptive_batch_scheduler
    check_logs_for_non_empty_out_files
else
    echo "ERROR: Action ${ACTION} is unsupported for tpcds test. Aborting..."
    exit 1
fi

exit $EXIT_CODE
