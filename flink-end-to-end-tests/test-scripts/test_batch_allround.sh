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

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-dataset-allround-test/target/DataSetAllroundTestProgram.jar

echo "Run DataSet-Allround-Test Program"

# modify configuration to include spilling to disk
set_config_key "taskmanager.network.memory.min" "10485760"
set_config_key "taskmanager.network.memory.max" "10485760"

set_conf_ssl "server"
start_cluster
start_taskmanagers 3

$FLINK_DIR/bin/flink run -p 4 $TEST_PROGRAM_JAR --loadFactor 4 --outputPath $TEST_DATA_DIR/out/dataset_allround

check_result_hash "DataSet-Allround-Test" $TEST_DATA_DIR/out/dataset_allround "d3cf2aeaa9320c772304cba42649eb47"
