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

if [ -z $1 ] || [ -z $2 ]; then
  echo "Usage: ./test_resume_savepoint.sh <original_dop> <new_dop>"
  exit 1
fi

source "$(dirname "$0")"/common.sh

ORIGINAL_DOP=$1
NEW_DOP=$2

if (( $ORIGINAL_DOP >= $NEW_DOP )); then
  NUM_SLOTS=$ORIGINAL_DOP
else
  NUM_SLOTS=$NEW_DOP
fi

# modify configuration to have enough slots
cp $FLINK_DIR/conf/flink-conf.yaml $FLINK_DIR/conf/flink-conf.yaml.bak
sed -i -e "s/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: $NUM_SLOTS/" $FLINK_DIR/conf/flink-conf.yaml

# modify configuration to use SLF4J reporter; we will be using this to monitor the state machine progress
cp $FLINK_DIR/opt/flink-metrics-slf4j-*.jar $FLINK_DIR/lib/
echo "metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter" >> $FLINK_DIR/conf/flink-conf.yaml
echo "metrics.reporter.slf4j.interval: 1 SECONDS" >> $FLINK_DIR/conf/flink-conf.yaml

start_cluster

# make sure to stop Kafka and ZooKeeper at the end, as well as cleaning up the Flink cluster and our moodifications
function test_cleanup {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  # revert our modifications to the Flink distribution
  mv -f $FLINK_DIR/conf/flink-conf.yaml.bak $FLINK_DIR/conf/flink-conf.yaml
  rm $FLINK_DIR/lib/flink-metrics-slf4j-*.jar

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

CHECKPOINT_DIR="file://$TEST_DATA_DIR/savepoint-e2e-test-chckpt-dir"

# run the DataStream allroundjob
TEST_PROGRAM_JAR=$TEST_INFRA_DIR/../../flink-end-to-end-tests/flink-datastream-allround-test/target/DataStreamAllroundTestProgram.jar
DATASTREAM_JOB=$($FLINK_DIR/bin/flink run -d -p $ORIGINAL_DOP $TEST_PROGRAM_JAR \
  --test.semantics exactly-once \
  --environment.parallelism $ORIGINAL_DOP \
  --state_backend.checkpoint_directory $CHECKPOINT_DIR \
  --sequence_generator_source.sleep_time 15 \
  --sequence_generator_source.sleep_after_elements 1 \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $DATASTREAM_JOB

function get_metric_processed_records {
  grep ".General purpose test job.ArtificalKeyedStateMapper.0.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | sed 's/.* //g' | tail -1
}

function get_num_metric_samples {
  grep ".General purpose test job.ArtificalKeyedStateMapper.0.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | wc -l
}

# monitor the numRecordsIn metric of the state machine operator;
# only proceed to savepoint when the operator has processed 200 records
while : ; do
  NUM_RECORDS=$(get_metric_processed_records)

  if [ -z $NUM_RECORDS ]; then
    NUM_RECORDS=0
  fi

  if (( $NUM_RECORDS < 200 )); then
    echo "Waiting for job to process up to 200 records, current progress: $NUM_RECORDS records ..."
    sleep 1
  else
    break
  fi
done

# take a savepoint of the state machine job
SAVEPOINT_PATH=$(take_savepoint $DATASTREAM_JOB $TEST_DATA_DIR \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

cancel_job $DATASTREAM_JOB

# Since it is not possible to differentiate reporter output between the first and second execution,
# we remember the number of metrics sampled in the first execution so that they can be ignored in the following monitorings
OLD_NUM_METRICS=$(get_num_metric_samples)

# resume state machine job with savepoint
DATASTREAM_JOB=$($FLINK_DIR/bin/flink run -s $SAVEPOINT_PATH -p $NEW_DOP -d $TEST_PROGRAM_JAR \
  --test.semantics exactly-once \
  --environment.parallelism $NEW_DOP \
  --state_backend.checkpoint_directory $CHECKPOINT_DIR \
  --sequence_generator_source.sleep_time 15 \
  --sequence_generator_source.sleep_after_elements 1 \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $DATASTREAM_JOB

# monitor the numRecordsIn metric of the state machine operator in the second execution
# we let the test finish once the second restore execution has processed 200 records
while : ; do
  NUM_METRICS=$(get_num_metric_samples)
  NUM_RECORDS=$(get_metric_processed_records)

  # only account for metrics that appeared in the second execution
  if (( $OLD_NUM_METRICS >= $NUM_METRICS )) ; then
    NUM_RECORDS=0
  fi

  if (( $NUM_RECORDS < 200 )); then
    echo "Waiting for job to process up to 200 records, current progress: $NUM_RECORDS records ..."
    sleep 1
  else
    break
  fi
done

# if state is errorneous and the state machine job produces alerting state transitions,
# output would be non-empty and the test will not pass
