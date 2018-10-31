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

set_conf "taskmanager.numberOfTaskSlots" "13"

start_cluster

NUM_TMS=20

#20 x 13 slots to support a parallelism of 256
echo "Start $NUM_TMS more task managers"
for i in `seq 1 $NUM_TMS`; do
    $FLINK_DIR/bin/taskmanager.sh start
done

$FLINK_DIR/bin/flink run -p 256 ${TEST_PROGRAM_JAR} \
--environment.max_parallelism 1024 --environment.parallelism 256 \
--state_backend.checkpoint_directory ${CHECKPOINT_DIR} \
--heavy_deployment_test.num_list_states_per_op 50 --heavy_deployment_test.num_partitions_per_list_state 75
