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

set_config_key "taskmanager.memory.process.size" "4096m"
set_config_key "taskmanager.numberOfTaskSlots" "4"
set_config_key "parallelism.default" "4"
start_cluster


################################################################################
# Run TPC-DS SQL
################################################################################

echo "[INFO] Runing TPC-DS queries..."

RESULT_DIR="$TARGET_DIR/result"
mkdir -p "$RESULT_DIR"

$FLINK_DIR/bin/flink run -c org.apache.flink.table.tpcds.TpcdsTestProgram "$TARGET_DIR/TpcdsTestProgram.jar" -sourceTablePath "$TPCDS_DATA_DIR" -queryPath "$TPCDS_QUERY_DIR" -sinkTablePath "$RESULT_DIR" -useTableStats "$USE_TABLE_STATS"

function sql_cleanup() {
  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all
}
on_exit sql_cleanup

################################################################################
# validate result
################################################################################
QUALIFIED_ANSWER_DIR="$TARGET_DIR/answer_set_qualified"
mkdir -p "$QUALIFIED_ANSWER_DIR"

java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.utils.AnswerFormatter -originDir "$ORGIN_ANSWER_DIR" -destDir "$QUALIFIED_ANSWER_DIR"

java -cp "$TARGET_DIR/TpcdsTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpcds.utils.TpcdsResultComparator -expectedDir "$QUALIFIED_ANSWER_DIR" -actualDir "$RESULT_DIR"
