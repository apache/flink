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

TEST=flink-netty-shuffle-memory-control-test
TEST_PROGRAM_NAME=NettyShuffleMemoryControlTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

set_config_key "taskmanager.memory.flink.size" "512m"
set_config_key "taskmanager.memory.network.min" "128m"
set_config_key "taskmanager.memory.network.max" "128m"

# 20 slots per task manager.
set_config_key "taskmanager.numberOfTaskSlots" "20"

# Sets only one arena per TM for boosting the netty internal memory overhead.
set_config_key "taskmanager.network.netty.num-arenas" "1"

# Limits the direct memory to be one chunk (4M) plus some margins.
set_config_key "taskmanager.memory.framework.off-heap.size" "7m"

# Starts the cluster which includes one TaskManager.
start_cluster

# Starts 4 more TaskManagers. Then we will have 5 TaskManagers and 100 slots in total.
start_taskmanagers 4

# Starts a job with 80 map tasks and 20 reduce tasks.
$FLINK_DIR/bin/flink run ${TEST_PROGRAM_JAR} \
--test.map_parallelism 80 \
--test.reduce_parallelism 20 \
--test.running_time_in_seconds 120
