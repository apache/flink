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

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>" ["skip_check_exceptions"]

# IMPORTANT:
# With the "skip_check_exceptions" flag one can disable default exceptions and errors checking in log files. This should be done
# carefully though. A valid reasons for doing so could be e.g killing TMs randomly as we cannot predict what exception could be thrown. Whenever
# those checks are disabled, one should take care that a proper checks are performed in the tests itself that ensure that the test finished
# in an expected state.

################################################################################
# Checkpointing tests
################################################################################

run_test "Resuming Savepoint (file, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file true"
run_test "Resuming Savepoint (file, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 file false"
run_test "Resuming Savepoint (file, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file true"
run_test "Resuming Savepoint (file, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 file false"
run_test "Resuming Savepoint (file, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file true"
run_test "Resuming Savepoint (file, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 file false"
run_test "Resuming Savepoint (rocks, no parallelism change, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false heap"
run_test "Resuming Savepoint (rocks, scale up, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false heap"
run_test "Resuming Savepoint (rocks, scale down, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false heap"
run_test "Resuming Savepoint (rocks, no parallelism change, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false rocks"
run_test "Resuming Savepoint (rocks, scale up, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false rocks"
run_test "Resuming Savepoint (rocks, scale down, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false rocks"

run_test "Resuming Externalized Checkpoint (file, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 file true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (file, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 file false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true false" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint (rocks, incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true true" "skip_check_exceptions"

run_test "Resuming Externalized Checkpoint after terminal failure (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file true false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 file false false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false true" "skip_check_exceptions"
run_test "Resuming Externalized Checkpoint after terminal failure (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true true" "skip_check_exceptions"

################################################################################
# Docker
################################################################################

run_test "Running Kerberized YARN on Docker test (default input)" "$END_TO_END_DIR/test-scripts/test_yarn_kerberos_docker.sh"
run_test "Running Kerberized YARN on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_yarn_kerberos_docker.sh dummy-fs"

################################################################################
# High Availability
################################################################################

run_test "Running HA dataset end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_dataset.sh" "skip_check_exceptions"

run_test "Running HA (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh file true false" "skip_check_exceptions"
run_test "Running HA (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh file false false" "skip_check_exceptions"
run_test "Running HA (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh rocks true false" "skip_check_exceptions"
run_test "Running HA (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh rocks true true" "skip_check_exceptions"

run_test "Running HA per-job cluster (file, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh file true false" "skip_check_exceptions"
run_test "Running HA per-job cluster (file, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh file false false" "skip_check_exceptions"
run_test "Running HA per-job cluster (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh rocks true false" "skip_check_exceptions"
run_test "Running HA per-job cluster (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh rocks true true" "skip_check_exceptions"

################################################################################
# Kubernetes
################################################################################

run_test "Run Kubernetes test" "$END_TO_END_DIR/test-scripts/test_kubernetes_embedded_job.sh"
run_test "Run kubernetes session test" "$END_TO_END_DIR/test-scripts/test_kubernetes_session.sh"

################################################################################
# Mesos
################################################################################

run_test "Run Mesos WordCount test" "$END_TO_END_DIR/test-scripts/test_mesos_wordcount.sh"
run_test "Run Mesos multiple submission test" "$END_TO_END_DIR/test-scripts/test_mesos_multiple_submissions.sh"

################################################################################
# Miscellaneous
################################################################################

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

run_test "Walkthrough Table Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_table_walkthroughs.sh java"
run_test "Walkthrough Table Scala nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_table_walkthroughs.sh scala"
run_test "Walkthrough DataStream Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_datastream_walkthroughs.sh java"
run_test "Walkthrough DataStream Scala nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_datastream_walkthroughs.sh scala"

run_test "Test PubSub connector with Docker based Google PubSub Emulator" "$END_TO_END_DIR/test-scripts/test_streaming_gcp_pubsub.sh"

run_test "Avro Confluent Schema Registry nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_confluent_schema_registry.sh"

run_test "State TTL Heap backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh file" "skip_check_exceptions"
run_test "State TTL RocksDb backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh rocks" "skip_check_exceptions"

run_test "SQL Client end-to-end test (Old planner)" "$END_TO_END_DIR/test-scripts/test_sql_client.sh old"
run_test "SQL Client end-to-end test (Blink planner)" "$END_TO_END_DIR/test-scripts/test_sql_client.sh blink"
run_test "SQL Client end-to-end test for Kafka 0.10" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka010.sh"
run_test "SQL Client end-to-end test for Kafka 0.11" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka011.sh"
run_test "SQL Client end-to-end test for modern Kafka" "$END_TO_END_DIR/test-scripts/test_sql_client_kafka.sh"

run_test "TPC-H end-to-end test (Blink planner)" "$END_TO_END_DIR/test-scripts/test_tpch.sh"
run_test "TPC-DS end-to-end test (Blink planner)" "$END_TO_END_DIR/test-scripts/test_tpcds.sh"


run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"

run_test "ConnectedComponents iterations with high parallelism end-to-end test" "$END_TO_END_DIR/test-scripts/test_high_parallelism_iterations.sh 25"

run_test "Dependency shading of table modules test" "$END_TO_END_DIR/test-scripts/test_table_shaded_dependencies.sh"

################################################################################
# Sticky Scheduling
################################################################################

run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 3 file false false" "skip_check_exceptions"
run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 3 file false true" "skip_check_exceptions"
run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks false false" "skip_check_exceptions"
run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks true false" "skip_check_exceptions"
run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks false true" "skip_check_exceptions"
run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks true true" "skip_check_exceptions"

printf "\n[PASS] All bash e2e-tests passed\n"
printf "\n Please remember to run java e2e-tests by running mvn verify -Dcategories=\"\" -DdistDir=<path to flink-dist> in flink-end-to-end-tests\n"
exit 0
