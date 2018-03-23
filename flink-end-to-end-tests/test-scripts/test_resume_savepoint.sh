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

# modify configuration to have 2 slots
cp $FLINK_DIR/conf/flink-conf.yaml $FLINK_DIR/conf/flink-conf.yaml.bak
sed -i -e 's/taskmanager.numberOfTaskSlots: 1/taskmanager.numberOfTaskSlots: 2/' $FLINK_DIR/conf/flink-conf.yaml

# modify configuration to use SLF4J reporter; we will be using this to monitor the state machine progress
cp $FLINK_DIR/opt/flink-metrics-slf4j-1.6-SNAPSHOT.jar $FLINK_DIR/lib/
echo "metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter" >> $FLINK_DIR/conf/flink-conf.yaml
echo "metrics.reporter.slf4j.interval: 5 SECONDS" >> $FLINK_DIR/conf/flink-conf.yaml

start_cluster

# get Kafka 0.10.0
mkdir -p $TEST_DATA_DIR
if [ -z "$3" ]; then
  # need to download Kafka because no Kafka was specified on the invocation
  KAFKA_URL="https://archive.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz"
  echo "Downloading Kafka from $KAFKA_URL"
  curl "$KAFKA_URL" > $TEST_DATA_DIR/kafka.tgz
else
  echo "Using specified Kafka from $3"
  cp $3 $TEST_DATA_DIR/kafka.tgz
fi

tar xzf $TEST_DATA_DIR/kafka.tgz -C $TEST_DATA_DIR/
KAFKA_DIR=$TEST_DATA_DIR/kafka_2.11-0.10.2.0

# fix kafka config
sed -i -e "s+^\(dataDir\s*=\s*\).*$+\1$TEST_DATA_DIR/zookeeper+" $KAFKA_DIR/config/zookeeper.properties
sed -i -e "s+^\(log\.dirs\s*=\s*\).*$+\1$TEST_DATA_DIR/kafka+" $KAFKA_DIR/config/server.properties
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties

# make sure to stop Kafka and ZooKeeper at the end, as well as cleaning up the Flink cluster and our moodifications
function test_cleanup {
  $KAFKA_DIR/bin/kafka-server-stop.sh
  $KAFKA_DIR/bin/zookeeper-server-stop.sh

  # revert our modifications to the Flink distribution
  rm $FLINK_DIR/conf/flink-conf.yaml
  mv $FLINK_DIR/conf/flink-conf.yaml.bak $FLINK_DIR/conf/flink-conf.yaml
  rm $FLINK_DIR/lib/flink-metrics-slf4j-1.6-SNAPSHOT.jar

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

# zookeeper outputs the "Node does not exist" bit to stderr
while [[ $($KAFKA_DIR/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/0 2>&1) =~ .*Node\ does\ not\ exist.* ]]; do
  echo "Waiting for broker..."
  sleep 1
done

# create the required topic
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-input

# run the state machine example job
STATE_MACHINE_JOB=$($FLINK_DIR/bin/flink run -d $FLINK_DIR/examples/streaming/StateMachineExample.jar \
  --kafka-topic test-input \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $STATE_MACHINE_JOB

# then, run the events generator
EVENTS_GEN_JOB=$($FLINK_DIR/bin/flink run -d -c org.apache.flink.streaming.examples.statemachine.KafkaEventsGeneratorJob $FLINK_DIR/examples/streaming/StateMachineExample.jar \
  --kafka-topic test-input --sleep 30 \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $EVENTS_GEN_JOB

function get_metric_state_machine_processed_records {
  grep ".State machine job.Flat Map -> Sink: Print to Std. Out.0.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | sed 's/.* //g' | tail -1
}

function get_num_metric_samples {
  grep ".State machine job.Flat Map -> Sink: Print to Std. Out.0.numRecordsIn:" $FLINK_DIR/log/*taskexecutor*.log | wc -l
}

# monitor the numRecordsIn metric of the state machine operator;
# only proceed to savepoint when the operator has processed 200 records
while : ; do
  NUM_RECORDS=$(get_metric_state_machine_processed_records)

  if [ -z $NUM_RECORDS ]; then
    NUM_RECORDS=0
  fi

  if (( $NUM_RECORDS < 200 )); then
    echo "Waiting for state machine job to process up to 200 records, current progress: $NUM_RECORDS records ..."
    sleep 5
  else
    break
  fi
done

# take a savepoint of the state machine job
SAVEPOINT_PATH=$(take_savepoint $STATE_MACHINE_JOB $TEST_DATA_DIR \
  | grep "Savepoint completed. Path:" | sed 's/.* //g')

cancel_job $STATE_MACHINE_JOB

# Since it is not possible to differentiate reporter output between the first and second exuection,
# we remember the number of metrics sampled in the first execution so that they can be ignored in the following monitorings
OLD_NUM_METRICS=$(get_num_metric_samples)

# resume state machine job with savepoint
STATE_MACHINE_JOB=$($FLINK_DIR/bin/flink run -s $SAVEPOINT_PATH -d $FLINK_DIR/examples/streaming/StateMachineExample.jar \
  --kafka-topic test-input \
  | grep "Job has been submitted with JobID" | sed 's/.* //g')

wait_job_running $STATE_MACHINE_JOB

# monitor the numRecordsIn metric of the state machine operator in the second execution
# we let the test finish once the second restore execution has processed 200 records
while : ; do
  NUM_METRICS=$(get_num_metric_samples)
  NUM_RECORDS=$(get_metric_state_machine_processed_records)

  # only account for metrics that appeared in the second execution
  if (( $OLD_NUM_METRICS >= $NUM_METRICS )) ; then
    NUM_RECORDS=0
  fi

  if (( $NUM_RECORDS < 200 )); then
    echo "Waiting for state machine job to process up to 200 records, current progress: $NUM_RECORDS records ..."
    sleep 5
  else
    break
  fi
done

# if state is errorneous and the state machine job produces alerting state transitions,
# output would be non-empty and the test will not pass
