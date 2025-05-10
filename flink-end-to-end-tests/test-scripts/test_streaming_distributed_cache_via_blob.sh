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

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-distributed-cache-via-blob-test/target/DistributedCacheViaBlobTestProgram.jar

echo "Testing distributing files via DistributedCache & BlobServer"

start_cluster

mkdir -p $TEST_DATA_DIR

$FLINK_DIR/bin/flink run -p 1 $TEST_PROGRAM_JAR --inputFile $TEST_INFRA_DIR/test-data/words --inputDir $TEST_INFRA_DIR/test-data --tempDir $TEST_DATA_DIR/ --output $TEST_DATA_DIR/out/cl_out_pf

OUTPUT=`cat $TEST_DATA_DIR/out/cl_out_pf`

EXPECTED="Hello World how are you, my dear dear world"
if [[ "$OUTPUT" != "$EXPECTED" ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: $EXPECTED"
  echo -e "ACTUAL: $OUTPUT"
  exit 1
fi
