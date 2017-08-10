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


set -e
set -o pipefail

# Convert relative path to absolute path
TEST_ROOT=`pwd`
TEST_INFRA_DIR="$0"
TEST_INFRA_DIR=`dirname "$TEST_INFRA_DIR"`
cd $TEST_INFRA_DIR
TEST_INFRA_DIR=`pwd`
cd $TEST_ROOT

. "$TEST_INFRA_DIR"/common.sh

start_cluster

$FLINK_DIR/bin/flink run -p 1 $FLINK_DIR/examples/batch/WordCount.jar --input $TEST_INFRA_DIR/test-data/words --output $TEST_DATA_DIR/out/wc_out
check_result_hash "WordCount" $TEST_DATA_DIR/out/wc_out "72a690412be8928ba239c2da967328a5"

stop_cluster
clean_data_dir
check_all_pass