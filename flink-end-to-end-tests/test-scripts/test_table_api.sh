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

# End to end test for join with custom type examples. It only verifies that the job can be submitted
# and run correctly.
#
# Usage:
# FLINK_DIR=<flink dir> flink-end-to-end-tests/test-scripts/test_table_api.sh

source "$(dirname "$0")"/common.sh

start_cluster

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-end-to-end-tests-table-api/target/JoinWithCustomTypeExample.jar

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR
EXIT_CODE=$?

stop_cluster

exit $EXIT_CODE
