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

#if [ -z $1 ] || [ -z $2 ]; then
# echo "Usage: ./test_rocksdb_state_memory_control.sh "
# exit 1
#fi

source "$(dirname "$0")"/common.sh

PARALLELISM=2
CHECKPOINT_DIR="$TEST_DATA_DIR/test_rocksdb_state_memory_control-dir"
mkdir -p $CHECKPOINT_DIR
CHECKPOINT_DIR_URI="file://$CHECKPOINT_DIR"

# The managed memory is fixed at 300m which gives an allowed cache size of about 157,000,000b,
# via the compensation logic for RocksDBs memory exceeding the cache.
#
# Due to RocksDB's lenient (non-strict) memory accounting, we add num-states * arena-size MBs in extra tolerance
# (because in corner cases, especially with slow I/O, cache footprint can temporarily rise)
# which brings this to about 190,000,000 bytes, rounded up to 200,000,000 for a safety/stability margin.
#
# With unrestricted memory use, this test would use more than 400m of RocksDB memory, so we are
# well below this limit, thus testing that the memory limiting is actually active.
EXPECTED_MAX_MEMORY_USAGE=200000000

set_config_key "taskmanager.numberOfTaskSlots" "$PARALLELISM"
set_config_key "taskmanager.memory.process.size" "1184m"
set_config_key "taskmanager.memory.managed.size" "300m"
set_config_key "state.backend.rocksdb.memory.managed" "true"
set_config_key "state.backend.rocksdb.memory.write-buffer-ratio" "0.8"
set_config_key "state.backend.rocksdb.metrics.size-all-mem-tables" "true"
set_config_key "state.backend.rocksdb.metrics.cur-size-active-mem-table" "true"
set_config_key "state.backend.rocksdb.metrics.num-immutable-mem-table" "true"
set_config_key "state.backend.rocksdb.metrics.block-cache-usage" "true"
set_config_key "state.backend.rocksdb.metrics.estimate-table-readers-mem" "true"
set_config_key "metrics.fetcher.update-interval" "1000"
setup_flink_slf4j_metric_reporter "numRecordsIn,block-cache-usage"
start_cluster

echo "Running RocksDB state backend memory control test"

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-rocksdb-state-memory-control-test/target/RocksDBStateMemoryControlTestProgram.jar

function buildBaseJobCmd {
  local flink_args="$1"

  echo "$FLINK_DIR/bin/flink run -d ${flink_args} -p $PARALLELISM $TEST_PROGRAM_JAR \
    --environment.parallelism $PARALLELISM \
    --environment.checkpoint_interval 600000 \
    --state_backend rocks \
    --state_backend.checkpoint_directory $CHECKPOINT_DIR_URI \
    --state_backend.rocks.incremental true \
    --sequence_generator_source.sleep_time 1 \
    --sequence_generator_source.keyspace 1000000 \
    --sequence_generator_source.payload_size 50000 \
    --useValueState true \
    --useListState true \
    --useMapState true \
    --useWindow true \
    --sequence_generator_source.sleep_after_elements 1"
}

function find_max_block_cache_usage() {
  OPERATOR=$1
  JOB_NAME="${2:-General purpose test job}"
  N=$(grep ".${JOB_NAME}.$OPERATOR.rocksdb.block-cache-usage:" $FLINK_LOG_DIR/*taskexecutor*.log* | sed 's/.* //g' | sort -rn | head -n 1)
  if [ -z $N ]; then
    N=0
  fi
  echo $N
}

function memory_under_limit() {
    local MAX_BLOCK_CACHE_0_USAGE=$1
    local MAX_BLOCK_CACHE_1_USAGE=$2
    local EXPECTED_MAX_MEMORY_USAGE=$3

    if [ "$MAX_BLOCK_CACHE_0_USAGE" -gt "$MAX_BLOCK_CACHE_1_USAGE" ]; then
      MAX_BLOCK_CACHE_USAGE=$MAX_BLOCK_CACHE_0_USAGE
    else
      MAX_BLOCK_CACHE_USAGE=$MAX_BLOCK_CACHE_1_USAGE
    fi
    echo "[INFO] Current max block cache usage of RocksDB instance in slot was $MAX_BLOCK_CACHE_USAGE"

    if [ "$MAX_BLOCK_CACHE_USAGE" -gt "$EXPECTED_MAX_MEMORY_USAGE" ]; then
      echo "[WARN] Current max block cache usage $MAX_BLOCK_CACHE_USAGE larger than expected memory limit $EXPECTED_MAX_MEMORY_USAGE"
      return 1
    fi
}

JOB_CMD=`buildBaseJobCmd `

MAX_ERROR_TIMES=3
for ((i=1;i<=$MAX_ERROR_TIMES;i++)); do
  DATASTREAM_JOB=$($JOB_CMD | grep "Job has been submitted with JobID" | sed 's/.* //g')

  wait_job_running $DATASTREAM_JOB
  wait_oper_metric_num_in_records TumblingWindowOperator.0 10000 'RocksDB test job'
  cancel_job $DATASTREAM_JOB

  MAX_BLOCK_CACHE_0_USAGE=$(find_max_block_cache_usage 'TumblingWindowOperator.0.window-contents' 'RocksDB test job')
  MAX_BLOCK_CACHE_1_USAGE=$(find_max_block_cache_usage 'TumblingWindowOperator.1.window-contents' 'RocksDB test job')
  memory_under_limit $MAX_BLOCK_CACHE_0_USAGE $MAX_BLOCK_CACHE_1_USAGE $EXPECTED_MAX_MEMORY_USAGE
  if [[ $? -ne 0 ]]; then
    if [[ $i -eq $MAX_ERROR_TIMES ]]; then
      echo "[ERROR] Max block cache usage has exceeded expected memory usage $EXPECTED_MAX_MEMORY_USAGE with $MAX_ERROR_TIMES times."
      exit 1
    else
      echo "[WARN] Max block cache usage exceed expected memory usage $EXPECTED_MAX_MEMORY_USAGE once, try to run end-to-end test again."
      continue
    fi
  else
    echo "[INFO] Max block cache usage under expected memory usage $EXPECTED_MAX_MEMORY_USAGE."
    break
  fi
done



