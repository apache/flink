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

function run_test {
    PARALLELISM=$1
    TEST_JOB_JAR="${END_TO_END_DIR}/flink-state-evolution-test/target/StatefulStreamingJob.jar"
    BASE_DIR="${TEST_DATA_DIR}/flink-state-evolution-test"
    CHECKPOINT_DIR="file://${BASE_DIR}/checkpoints/"
    SAVEPOINT_PARENT_DIR="file://${BASE_DIR}/savepoints/"

    SAVEPOINT_DIR="file:///${END_TO_END_DIR}/flink-state-evolution-test/savepoints/1.6/"

    set_conf "state.backend.fs.memory-threshold" 1048576
    start_cluster

    # restart the job from the savepoint from Flink 1.6
    JOB_ID=$($FLINK_DIR/bin/flink run -s $SAVEPOINT_DIR -p $PARALLELISM -d $TEST_JOB_JAR --checkpoint.dir $CHECKPOINT_DIR \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}
    wait_num_checkpoints ${JOB_ID} 3

    SECOND_SAVEPOINT_DIR=$($FLINK_DIR/bin/flink cancel -s $SAVEPOINT_PARENT_DIR $JOB_ID | grep "Savepoint stored in file" | sed 's/.* //g' | sed 's/\.$//')

    # restart the job from the savepoint from Flink 1.7
    FINAL_JOB_ID=$($FLINK_DIR/bin/flink run -s $SECOND_SAVEPOINT_DIR -p $PARALLELISM -d $TEST_JOB_JAR --checkpoint.dir $CHECKPOINT_DIR \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${FINAL_JOB_ID}
    wait_num_checkpoints ${FINAL_JOB_ID} 3

    cancel_job ${FINAL_JOB_ID}
}

run_test 1
