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

TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/target/ClassLoaderTestProgram.jar

# kill any remaining JobManagers/TaskManagers at the end
trap 'pkill -f "JobManager|TaskManager"' EXIT

echo "Testing parent-first class loading"

# remove any leftover classloader settings
sed -i -e 's/classloader.resolve-order: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
echo "classloader.resolve-order: parent-first" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order parent-first --output $TEST_DATA_DIR/out/cl_out_pf

stop_cluster

# remove classloader settings again
sed -i -e 's/classloader.resolve-order: .*//' $FLINK_DIR/conf/flink-conf.yaml

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_pf`
EXPECTED="NoSuchMethodError"
if [[ "$OUTPUT" != "$EXPECTED" ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  PASS=""
fi

echo "Testing child-first class loading"

# remove any leftover classloader settings
sed -i -e 's/classloader.resolve-order: .*//' "$FLINK_DIR/conf/flink-conf.yaml"
echo "classloader.resolve-order: child-first" >> "$FLINK_DIR/conf/flink-conf.yaml"

start_cluster

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --resolve-order child-first --output $TEST_DATA_DIR/out/cl_out_cf

stop_cluster

# remove classloader settings again
sed -i -e 's/classloader.resolve-order: .*//' $FLINK_DIR/conf/flink-conf.yaml

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_cf`
EXPECTED="Hello, World!"
if [[ "$OUTPUT" != "$EXPECTED" ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  PASS=""
fi

clean_data_dir
check_all_pass
