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

set -Eexuo pipefail

source "$(dirname "$0")"/common.sh

PARALLELISM="${1:-25}"
TM_NUM=2
let "SLOTS_PER_TM = (PARALLELISM + TM_NUM - 1) / TM_NUM"

TEST=flink-high-parallelism-iterations-test
TEST_PROGRAM_NAME=HighParallelismIterationsTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

set_config_key "taskmanager.numberOfTaskSlots" "$SLOTS_PER_TM"
set_config_key "taskmanager.memory.network.min" "160m"
set_config_key "taskmanager.memory.network.max" "160m"
set_config_key "taskmanager.memory.framework.off-heap.size" "300m"

print_mem_use
start_cluster
print_mem_use

let "TM_NUM -= 1"
start_taskmanagers ${TM_NUM}
print_mem_use

$FLINK_DIR/bin/flink run -p $PARALLELISM $TEST_PROGRAM_JAR
print_mem_use
