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

# For the case of evolution, this test will try to start from a savepoint taken
# with 1.7 (same version) but in which the schema of the (old) avro type differs
# from the current one. The new schema adds an optional field with a default value
# to the old one and verifies that it can see the old value.
#
# The old avro schema of the Address data type was:
#
# {"namespace": "org.apache.flink.formats.parquet.generated",
# "type": "record",
# "name": "Address",
# "fields": [
#     {"name": "num", "type": "int"},
#     {"name": "street", "type": "string"},
#     {"name": "city", "type": "string"},
#     {"name": "state", "type": "string"},
#     {"name": "zip", "type": "string"}
#  ]
# }
#
# while the new is:
#
# {"namespace": "org.apache.flink.avro.generated",
# "type": "record",
# "name": "Address",
# "fields": [
#     {"name": "num", "type": "int"},
#     {"name": "street", "type": "string"},
#     {"name": "city", "type": "string"},
#     {"name": "state", "type": "string"},
#     {"name": "zip", "type": "string"},
#     {"name": "appno", "type": [ "string", "null" ], "default": "123"}
#  ]
# }
function run_test {
    PARALLELISM=$1
    TEST_JOB_JAR="${END_TO_END_DIR}/flink-state-evolution-test/target/StatefulStreamingJob.jar"
    BASE_DIR="${TEST_DATA_DIR}/flink-state-evolution-test"
    CHECKPOINT_DIR="file://${BASE_DIR}/checkpoints/"
    SAVEPOINT_PARENT_DIR="file://${BASE_DIR}/savepoints/"

    SAVEPOINT_DIR="file:///${END_TO_END_DIR}/flink-state-evolution-test/savepoints/1.7/"

    set_config_key "state.backend.fs.memory-threshold" 1048576
    start_cluster

    # restart the job from the savepoint from Flink 1.7 with the old Avro Schema
    JOB_ID=$($FLINK_DIR/bin/flink run -s $SAVEPOINT_DIR -p $PARALLELISM -d $TEST_JOB_JAR --checkpoint.dir $CHECKPOINT_DIR \
        | grep "Job has been submitted with JobID" | sed 's/.* //g')

    wait_job_running ${JOB_ID}
    wait_num_checkpoints ${JOB_ID} 3

    SECOND_SAVEPOINT_DIR=$($FLINK_DIR/bin/flink cancel -s $SAVEPOINT_PARENT_DIR $JOB_ID | grep "Savepoint stored in file" | sed 's/.* //g' | sed 's/\.$//')
    echo "Took Savepoint @ ${SECOND_SAVEPOINT_DIR}"
}

run_test 1
