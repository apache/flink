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

PARALLELISM="${1:-25}"

TEST=flink-high-parallelism-iterations-test
TEST_PROGRAM_NAME=HighParallelismIterationsTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

backup_config

set_conf "taskmanager.heap.mb" "52" # 52Mb x 100 TMs = 5Gb total heap

set_conf "taskmanager.memory.size" "8" # 8Mb
set_conf "taskmanager.network.memory.min" "8388608" # 8Mb
set_conf "taskmanager.network.memory.max" "8388608" # 8Mb
set_conf "taskmanager.memory.segment-size" "8192" # 8Kb

set_conf "taskmanager.network.netty.server.numThreads" "1"
set_conf "taskmanager.network.netty.client.numThreads" "1"

set_conf "taskmanager.numberOfTaskSlots" "1"

print_mem_use
start_cluster
print_mem_use

let TMNUM=$PARALLELISM-1
echo "Start $TMNUM more task managers"
for i in `seq 1 $TMNUM`; do
    $FLINK_DIR/bin/taskmanager.sh start
    print_mem_use
done

$FLINK_DIR/bin/flink run -p $PARALLELISM $TEST_PROGRAM_JAR
print_mem_use
