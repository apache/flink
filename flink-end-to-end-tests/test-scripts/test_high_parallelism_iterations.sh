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

PARALLELISM="${PARALLELISM:-100}"

TEST=flink-high-parallelism-iterations-test
TEST_PROGRAM_NAME=HighParallelismIterationsTestProgram
TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/$TEST/target/$TEST_PROGRAM_NAME.jar

echo "Run Not So MiniCluster Iterations Graph Connected Components Program"

cp $FLINK_DIR/conf/flink-conf.yaml $FLINK_DIR/conf/flink-conf.yaml.bak

echo "taskmanager.heap.mb: 52" >> $FLINK_DIR/conf/flink-conf.yaml # 52Mb x 100 TMs = 5Gb total heap

echo "taskmanager.memory.size: 8" >> $FLINK_DIR/conf/flink-conf.yaml # 8Mb
echo "taskmanager.network.memory.min: 8388608" >> $FLINK_DIR/conf/flink-conf.yaml # 8Mb
echo "taskmanager.network.memory.max: 8388608" >> $FLINK_DIR/conf/flink-conf.yaml # 8Mb
echo "taskmanager.memory.segment-size: 8192" >> $FLINK_DIR/conf/flink-conf.yaml # 8Kb

echo "taskmanager.network.netty.server.numThreads: 1" >> $FLINK_DIR/conf/flink-conf.yaml
echo "taskmanager.network.netty.client.numThreads: 1" >> $FLINK_DIR/conf/flink-conf.yaml

echo "taskmanager.numberOfTaskSlots: 1" >> $FLINK_DIR/conf/flink-conf.yaml

start_cluster

let TMNUM=$PARALLELISM-1
echo "Start $TMNUM more task managers"
for i in `seq 1 $TMNUM`; do
    $FLINK_DIR/bin/taskmanager.sh start
done

function test_cleanup {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_cluster
  $FLINK_DIR/bin/taskmanager.sh stop-all

  # revert our modifications to the Flink distribution
  mv -f $FLINK_DIR/conf/flink-conf.yaml.bak $FLINK_DIR/conf/flink-conf.yaml

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

$FLINK_DIR/bin/flink run -p $PARALLELISM $TEST_PROGRAM_JAR
