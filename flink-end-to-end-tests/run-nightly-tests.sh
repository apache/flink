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

if [ -z "${FLINK_DIR:-}" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

if [ -z "$FLINK_LOG_DIR" ] ; then
    export FLINK_LOG_DIR="$FLINK_DIR/log"
fi

# On Azure CI, use artifacts dir
if [ -z "$DEBUG_FILES_OUTPUT_DIR" ] ; then
    export DEBUG_FILES_OUTPUT_DIR="$FLINK_LOG_DIR"
fi

source "${END_TO_END_DIR}/../tools/ci/maven-utils.sh"
source "${END_TO_END_DIR}/test-scripts/test-runner-common.sh"

function run_on_exit {
    collect_coredumps $(pwd) $DEBUG_FILES_OUTPUT_DIR
}

on_exit run_on_exit

if [[ ${PROFILE} == *"enable-adaptive-scheduler"* ]]; then
	echo "Enabling adaptive scheduler properties"
	export JVM_ARGS="-Dflink.tests.enable-adaptive-scheduler=true"
fi

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd -P)`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

echo "Java and Maven version"
java -version
run_mvn -version

echo "Free disk space"
df -h

echo "Running with profile '$PROFILE'"

# Template for adding a test:

# run_test "<description>" "$END_TO_END_DIR/test-scripts/<script_name>" ["skip_check_exceptions"]

# IMPORTANT:
# With the "skip_check_exceptions" flag one can disable default exceptions and errors checking in log files. This should be done
# carefully though. A valid reasons for doing so could be e.g killing TMs randomly as we cannot predict what exception could be thrown. Whenever
# those checks are disabled, one should take care that a proper checks are performed in the tests itself that ensure that the test finished
# in an expected state.

printf "\n\n==============================================================================\n"
printf "Running bash end-to-end tests\n"
printf "==============================================================================\n"

function run_group_1 {
    ################################################################################
    # Checkpointing tests
    ################################################################################

    run_test "Resuming Savepoint (hashmap, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 hashmap false"
    run_test "Resuming Savepoint (hashmap, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 hashmap false"
    run_test "Resuming Savepoint (hashmap, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 hashmap true"
    run_test "Resuming Savepoint (hashmap, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 hashmap false"
    run_test "Resuming Savepoint (rocks, no parallelism change, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false heap"
    run_test "Resuming Savepoint (rocks, scale up, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false heap"
    run_test "Resuming Savepoint (rocks, scale down, heap timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false heap"
    run_test "Resuming Savepoint (rocks, no parallelism change, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2 rocks false rocks"
    run_test "Resuming Savepoint (rocks, scale up, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4 rocks false rocks"
    run_test "Resuming Savepoint (rocks, scale down, rocks timers) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2 rocks false rocks"

    run_test "Resuming Externalized Checkpoint (hashmap, async, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, async, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 5 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, async, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 5 2 hashmap true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (hashmap, sync, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 hashmap false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, no parallelism change) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 3 4 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, scale up) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 4 rocks true true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, non-incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 5 3 rocks true false" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint (rocks, incremental, scale down) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 4 2 rocks true true" "skip_check_exceptions"

    run_test "Resuming Externalized Checkpoint after terminal failure (hashmap, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap true false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (hashmap, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 hashmap false false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true false true" "skip_check_exceptions"
    run_test "Resuming Externalized Checkpoint after terminal failure (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh 2 2 rocks true true true" "skip_check_exceptions"

    run_test "RocksDB Memory Management end-to-end test" "$END_TO_END_DIR/test-scripts/test_rocksdb_state_memory_control.sh"

    ################################################################################
    # Docker / Container / Kubernetes tests
    ################################################################################

    if [[ ${PROFILE} != *"enable-adaptive-scheduler"* ]]; then
        run_test "Wordcount on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_docker_embedded_job.sh dummy-fs"

        run_test "Run Kubernetes test" "$END_TO_END_DIR/test-scripts/test_kubernetes_embedded_job.sh"
        run_test "Run kubernetes session test (default input)" "$END_TO_END_DIR/test-scripts/test_kubernetes_session.sh"
        run_test "Run kubernetes session test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_kubernetes_session.sh dummy-fs"
        run_test "Run kubernetes application test" "$END_TO_END_DIR/test-scripts/test_kubernetes_application.sh"
        run_test "Run kubernetes application HA test" "$END_TO_END_DIR/test-scripts/test_kubernetes_application_ha.sh"
        run_test "Run Kubernetes IT test" "$END_TO_END_DIR/test-scripts/test_kubernetes_itcases.sh"

        run_test "Running Flink over NAT end-to-end test" "$END_TO_END_DIR/test-scripts/test_nat.sh" "skip_check_exceptions"

        if [[ `uname -i` != 'aarch64' ]]; then
            # Skip PyFlink e2e test, because MiniConda and Pyarrow which Pyflink depends doesn't support aarch64 currently.
            run_test "Run kubernetes pyflink application test" "$END_TO_END_DIR/test-scripts/test_kubernetes_pyflink_application.sh"

            # Hadoop YARN deosn't support aarch64 at this moment. See: https://issues.apache.org/jira/browse/HADOOP-16723
            # These tests are known to fail on JDK11. See FLINK-13719
            if [[ ${PROFILE} != *"jdk11"* ]]; then
                run_test "Running Kerberized YARN per-job on Docker test (default input)" "$END_TO_END_DIR/test-scripts/test_yarn_job_kerberos_docker.sh"
                run_test "Running Kerberized YARN per-job on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_yarn_job_kerberos_docker.sh dummy-fs"
                run_test "Running Kerberized YARN application on Docker test (default input)" "$END_TO_END_DIR/test-scripts/test_yarn_application_kerberos_docker.sh"
                run_test "Running Kerberized YARN application on Docker test (custom fs plugin)" "$END_TO_END_DIR/test-scripts/test_yarn_application_kerberos_docker.sh dummy-fs"
            fi
        fi
    fi

    ################################################################################
    # High Availability
    ################################################################################

    run_test "Running HA dataset end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_dataset.sh" "skip_check_exceptions"

    run_test "Running HA (hashmap, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh hashmap true false" "skip_check_exceptions"
    run_test "Running HA (hashmap, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh hashmap false false" "skip_check_exceptions"
    run_test "Running HA (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh rocks true false" "skip_check_exceptions"
    run_test "Running HA (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_datastream.sh rocks true true" "skip_check_exceptions"

    run_test "Running HA per-job cluster (hashmap, async) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh hashmap true false" "skip_check_exceptions"
    run_test "Running HA per-job cluster (hashmap, sync) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh hashmap false false" "skip_check_exceptions"
    run_test "Running HA per-job cluster (rocks, non-incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh rocks true false" "skip_check_exceptions"
    run_test "Running HA per-job cluster (rocks, incremental) end-to-end test" "$END_TO_END_DIR/test-scripts/test_ha_per_job_cluster_datastream.sh rocks true true" "skip_check_exceptions"
}

function run_group_2 {
    ################################################################################
    # Miscellaneous
    ################################################################################

    run_test "Flink CLI end-to-end test" "$END_TO_END_DIR/test-scripts/test_cli.sh"

    run_test "Flink streaming examples end-to-end test" "$END_TO_END_DIR/test-scripts/test_streaming_examples.sh"

    run_test "Queryable state (rocksdb) end-to-end test" "$END_TO_END_DIR/test-scripts/test_queryable_state.sh rocksdb"
    run_test "Queryable state (rocksdb) with TM restart end-to-end test" "$END_TO_END_DIR/test-scripts/test_queryable_state_restart_tm.sh" "skip_check_exceptions"

    run_test "DataSet allround end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_allround.sh"
    run_test "Batch SQL end-to-end test using blocking shuffle" "$END_TO_END_DIR/test-scripts/test_batch_sql.sh blocking"
    run_test "Batch SQL end-to-end test using hybrid full shuffle" "$END_TO_END_DIR/test-scripts/test_batch_sql.sh hybrid_full"
    run_test "Batch SQL end-to-end test using hybrid selective shuffle" "$END_TO_END_DIR/test-scripts/test_batch_sql.sh hybrid_selective"
    run_test "Streaming SQL end-to-end test using planner loader" "$END_TO_END_DIR/test-scripts/test_streaming_sql.sh" "skip_check_exceptions"
    run_test "Streaming SQL end-to-end test using planner with Scala version" "$END_TO_END_DIR/test-scripts/test_streaming_sql.sh scala-planner" "skip_check_exceptions"
    run_test "Sql Jdbc Driver end-to-end test" "$END_TO_END_DIR/test-scripts/test_sql_jdbc_driver.sh" "skip_check_exceptions"

    if [[ ${PROFILE} != *"enable-adaptive-scheduler"* ]]; then # FLINK-21400
      run_test "Streaming File Sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_file_sink.sh local StreamingFileSink" "skip_check_exceptions"
      run_test "Streaming File Sink s3 end-to-end test" "$END_TO_END_DIR/test-scripts/test_file_sink.sh s3 StreamingFileSink" "skip_check_exceptions"
      run_test "New File Sink end-to-end test" "$END_TO_END_DIR/test-scripts/test_file_sink.sh local FileSink" "skip_check_exceptions"
      run_test "New File Sink s3 end-to-end test" "$END_TO_END_DIR/test-scripts/test_file_sink.sh s3 FileSink" "skip_check_exceptions"

      run_test "Stateful stream job upgrade end-to-end test" "$END_TO_END_DIR/test-scripts/test_stateful_stream_job_upgrade.sh 2 4"
    fi

    run_test "Netty shuffle direct memory consumption end-to-end test" "$END_TO_END_DIR/test-scripts/test_netty_shuffle_memory_control.sh"

    run_test "Quickstarts Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_quickstarts.sh java"

    run_test "Walkthrough DataStream Java nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_datastream_walkthroughs.sh java"

    run_test "Avro Confluent Schema Registry nightly end-to-end test" "$END_TO_END_DIR/test-scripts/test_confluent_schema_registry.sh"

    run_test "State TTL Heap backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh hashmap" "skip_check_exceptions"
    run_test "State TTL RocksDb backend end-to-end test" "$END_TO_END_DIR/test-scripts/test_stream_state_ttl.sh rocks" "skip_check_exceptions"

    run_test "TPC-H end-to-end test" "$END_TO_END_DIR/test-scripts/test_tpch.sh"
    run_test "TPC-DS end-to-end test" "$END_TO_END_DIR/test-scripts/test_tpcds.sh"
    run_test "TPC-DS end-to-end test with adaptive batch scheduler" "$END_TO_END_DIR/test-scripts/test_tpcds.sh AdaptiveBatch run_test" "custom_check_exceptions" "$END_TO_END_DIR/test-scripts/test_tpcds.sh AdaptiveBatch check_exceptions"

    run_test "Heavy deployment end-to-end test" "$END_TO_END_DIR/test-scripts/test_heavy_deployment.sh" "skip_check_exceptions"

    run_test "ConnectedComponents iterations with high parallelism end-to-end test" "$END_TO_END_DIR/test-scripts/test_high_parallelism_iterations.sh 25"

    run_test "Dependency shading of table modules test" "$END_TO_END_DIR/test-scripts/test_table_shaded_dependencies.sh"

    run_test "Shaded Hadoop S3A with credentials provider end-to-end test" "$END_TO_END_DIR/test-scripts/test_batch_wordcount.sh hadoop_with_provider"

    run_test "Failure Enricher end-to-end test" "$END_TO_END_DIR/test-scripts/test_failure_enricher.sh" "skip_check_exceptions"

    if [[ `uname -i` != 'aarch64' ]]; then
        run_test "PyFlink end-to-end test" "$END_TO_END_DIR/test-scripts/test_pyflink.sh" "skip_check_exceptions"
    fi
    # These tests are known to fail on JDK11. See FLINK-13719
    if [[ ${PROFILE} != *"jdk11"* ]] && [[ `uname -i` != 'aarch64' ]]; then
        run_test "PyFlink YARN per-job on Docker test" "$END_TO_END_DIR/test-scripts/test_pyflink_yarn.sh" "skip_check_exceptions"
    fi

    ################################################################################
    # Sticky Scheduling
    ################################################################################

    if [[ ${PROFILE} != *"enable-adaptive-scheduler"* ]]; then #FLINK-21450
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 3 hashmap false false 100" "skip_check_exceptions"
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 3 hashmap false true 100" "skip_check_exceptions"
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks false false 100" "skip_check_exceptions"
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks true false 100" "skip_check_exceptions"
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks false true 100" "skip_check_exceptions"
        run_test "Local recovery and sticky scheduling end-to-end test" "$END_TO_END_DIR/test-scripts/test_local_recovery_and_scheduling.sh 4 10 rocks true true 100" "skip_check_exceptions"
    fi

    printf "\n[PASS] All bash e2e-tests passed\n"

    printf "\n\n==============================================================================\n"
    printf "Running Java end-to-end tests\n"
    printf "==============================================================================\n"


    LOG4J_PROPERTIES=${END_TO_END_DIR}/../tools/ci/log4j.properties

    MVN_LOGGING_OPTIONS="-Dlog.dir=${DEBUG_FILES_OUTPUT_DIR} -DlogBackupDir=${DEBUG_FILES_OUTPUT_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"
    MVN_COMMON_OPTIONS="-Dfast -Pskip-webui-build"
    e2e_modules=$(find flink-end-to-end-tests -mindepth 2 -maxdepth 5 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')
    e2e_modules="${e2e_modules},$(find flink-walkthroughs -mindepth 2 -maxdepth 2 -name 'pom.xml' -printf '%h\n' | sort -u | tr '\n' ',')"

    PROFILE="$PROFILE -Prun-end-to-end-tests"
    run_mvn ${MVN_COMMON_OPTIONS} ${MVN_LOGGING_OPTIONS} ${PROFILE} verify -pl ${e2e_modules} -DdistDir=$(readlink -e build-target) -Dcache-dir=$E2E_CACHE_FOLDER -Dcache-download-attempt-timeout=4min -Dcache-download-global-timeout=10min

    EXIT_CODE=$?
}

if [ "$1" == "1" ]; then
    run_group_1
elif [ "$1" == "2" ]; then
    run_group_2
else
    run_group_1
    run_group_2
fi



exit $EXIT_CODE
