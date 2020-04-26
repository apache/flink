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

CHECKPOINT_DIR="file://$TEST_DATA_DIR/savepoint-e2e-test-chckpt-dir"

TEST=flink-heavy-deployment-stress-test
TEST_PROGRAM_NAME=HeavyDeploymentStressTestProgram
TEST_PROGRAM_JAR=${END_TO_END_DIR}/$TEST/target/$TEST_PROGRAM_NAME.jar

set_config_key "akka.ask.timeout" "60 s"
set_config_key "web.timeout" "60000"

set_config_key "taskmanager.memory.process.size" "1024m" # 1024Mb x 5TMs = 5Gb total heap

set_config_key "taskmanager.memory.managed.size" "8" # 8Mb
set_config_key "taskmanager.memory.network.min" "128mb"
set_config_key "taskmanager.memory.network.max" "128mb"
set_config_key "taskmanager.network.request-backoff.max" "60000"
set_config_key "taskmanager.memory.segment-size" "8kb"
set_config_key "taskmanager.memory.jvm-metaspace.size" "64m"
set_config_key "taskmanager.memory.framework.off-heap.size" "200m"

set_config_key "taskmanager.numberOfTaskSlots" "20" # 20 slots per TM

start_cluster # this also starts 1TM
start_taskmanagers 4 # 1TM + 4TM = 5TM a 20 slots = 100 slots

# This call will result in a deployment with state meta data of 100 x 100 x 40 union states x each 40 entries.
# We can scale up the numbers to make the test even heavier.
$FLINK_DIR/bin/flink run ${TEST_PROGRAM_JAR} \
--environment.max_parallelism 1024 --environment.parallelism 100 \
--environment.restart_strategy fixed_delay --environment.restart_strategy.fixed_delay.attempts 3 \
--state_backend.checkpoint_directory ${CHECKPOINT_DIR} \
--heavy_deployment_test.num_list_states_per_op 40 --heavy_deployment_test.num_partitions_per_list_state 40
