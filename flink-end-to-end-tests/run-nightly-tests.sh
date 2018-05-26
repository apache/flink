#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd )`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

source "$(dirname "$0")"/test-scripts/common.sh

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd )`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

EXIT_CODE=0

# Template for adding a test:

# if [ $EXIT_CODE == 0 ]; then
#    run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>"
#    EXIT_CODE=$?
# fi


if [ $EXIT_CODE == 0 ]; then
  run_test "Running HA (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha.sh file true false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Running HA (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha.sh file false false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Running HA (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha.sh rocks true false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Running HA (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha.sh rocks true true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (file, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (rocks, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (rocks, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Savepoint (rocks, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh file true false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh file false false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint (rocks) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh rocks false"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint after terminal failure (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh file true true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint after terminal failure (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh file false true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Resuming Externalized Checkpoint after terminal failure (rocks) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh rocks true"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "DataSet allround end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_allround.sh"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Streaming SQL end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_sql.sh"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Streaming bucketing end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_bucketing.sh"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Stateful stream job upgrade end-to-end test" "$END_TO_END_DIR/test-scripts/test_stateful_stream_job_upgrade.sh 2 4"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test \
    "Elasticsearch (v1.7.1) sink end-to-end test" \
    "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 1 https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.1.tar.gz"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test \
    "Elasticsearch (v2.3.5) sink end-to-end test" \
    "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 2 https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.5/elasticsearch-2.3.5.tar.gz"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test \
    "Elasticsearch (v5.1.2) sink end-to-end test" \
    "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 5 https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.2.tar.gz"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Local recovery and sticky scheduling nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh"
  EXIT_CODE=$?
fi

if [ $EXIT_CODE == 0 ]; then
  run_test "Quickstarts nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh"
  EXIT_CODE=$?
fi

# Exit code for Travis build success/failure
exit $EXIT_CODE
