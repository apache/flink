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

PLANNER="${1:-old}"

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-stream-sql-test/target/StreamSQLTestProgram.jar

start_cluster
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start
$FLINK_DIR/bin/taskmanager.sh start

$FLINK_DIR/bin/flink run -p 4 $TEST_PROGRAM_JAR -outputPath file://${TEST_DATA_DIR}/out/result -planner ${PLANNER}

# collect results from files
cat $TEST_DATA_DIR/out/result/20/.part-* $TEST_DATA_DIR/out/result/20/part-* | sort > $TEST_DATA_DIR/out/result-complete

# check result:
# 20,1970-01-01 00:00:00.0
# 20,1970-01-01 00:00:20.0
# 20,1970-01-01 00:00:40.0
check_result_hash "StreamSQL" $TEST_DATA_DIR/out/result-complete "b29f14ed221a936211202ff65b51ee26"
