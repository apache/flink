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
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd -P)`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

export END_TO_END_DIR

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd -P)`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>"

run_test "Flink CLI end-to-end test" "$END_TO_END_DIR/test-scripts/test_cli.sh"

run_test "Queryable state (rocksdb) end-to-end test" "$END_TO_END_DIR/test-scripts/test_queryable_state.sh rocksdb"
run_test "Queryable state (rocksdb) with TM restart end-to-end test" "$END_TO_END_DIR/test-scripts/test_queryable_state_restart_tm.sh" "skip_check_exceptions"

run_test "DataSet allround end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_allround.sh"
run_test "Batch SQL end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_sql.sh"
run_test "Streaming SQL end-to-end test (Old planner)" "$END_TO_END_DIR/test-scripts/test_streaming_sql.sh old" "skip_check_exceptions"
run_test "Streaming SQL end-to-end test (Blink planner)" "$END_TO_END_DIR/test-scripts/test_streaming_sql.sh blink" "skip_check_exceptions"
run_test "Streaming bucketing end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_bucketing.sh" "skip_check_exceptions"
run_test "Streaming File Sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_file_sink.sh" "skip_check_exceptions"
run_test "Streaming File Sink s3 end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_file_sink.sh s3" "skip_check_exceptions"
run_test "Stateful stream job upgrade end-to-end test" "$END_TO_END_DIR/test-scripts/test_stateful_stream_job_upgrade.sh 2 4"

run_test "Elasticsearch (v2.3.5) sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 2 https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.5/elasticsearch-2.3.5.tar.gz"
run_test "Elasticsearch (v5.1.2) sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 5 https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.2.tar.gz"
run_test "Elasticsearch (v6.3.1) sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_elasticsearch.sh 6 https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.3.1.tar.gz"

run_test "Quickstarts Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh java"
run_test "Quickstarts Scala nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh scala"

run_test "Avro Confluent Schema Registry nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_confluent_schema_registry.sh"

run_test "State TTL Heap backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh file"
run_test "State TTL RocksDb backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh rocks"

run_test "SQL Client end-to-end test (Old planner)" "$END_TO_END_DIR/test-scripts/test_sql_client.sh old"
run_test "SQL Client end-to-end test (Blink planner)" "$END_TO_END_DIR/test-scripts/test_sql_client.sh blink"
run_test "SQL Client end-to-end test for Kafka 0.10" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka010.sh"
run_test "SQL Client end-to-end test for Kafka 0.11" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka011.sh"
run_test "SQL Client end-to-end test for modern Kafka" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka.sh"

run_test "Dependency shading of table modules test" "$END_TO_END_DIR/test-scripts/test_table_shaded_dependencies.sh"

run_test "Shaded Hadoop S3A with credentials provider end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_wordcount.sh hadoop_with_provider"

printf "\n[PASS] All tests passed\n"
exit 0
