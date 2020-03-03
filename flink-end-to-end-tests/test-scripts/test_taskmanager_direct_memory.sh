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

TEST=flink-taskmanager-direct-memory-test
TEST_PROGRAM_NAME=TaskManagerDirectMemoryTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

set_config_key "akka.ask.timeout" "60 s"
set_config_key "web.timeout" "60000"

set_config_key "taskmanager.memory.process.size" "1536m"

set_config_key "taskmanager.memory.managed.size" "8" # 8Mb
set_config_key "taskmanager.memory.network.min" "256mb"
set_config_key "taskmanager.memory.network.max" "256mb"
set_config_key "taskmanager.memory.jvm-metaspace.size" "64m"

set_config_key "taskmanager.numberOfTaskSlots" "20" # 20 slots per TM
set_config_key "taskmanager.network.netty.num-arenas" "1" # Use only one arena for each TM
set_config_key "taskmanager.memory.framework.off-heap.size" "20m" # One chunk (16M) and some additional consumption

start_cluster # this also starts 1TM
start_taskmanagers 4 # 1TM + 4TM = 5TM a 20 slots = 100 slots

$FLINK_DIR/bin/flink run ${TEST_PROGRAM_JAR} \
--test.map_parallelism 80 \
--test.reduce_parallelism 20 \
--test.record_length 4096 \
--test.running_time_in_seconds 120
