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

set -o pipefail

if [[ -z $TEST_DATA_DIR ]]; then
  echo "Must run common.sh before kafka-common.sh."
  exit 1
fi

KAFKA_DIR=$TEST_DATA_DIR/kafka_2.11-0.10.2.0

function setup_kafka_dist {
  # download Kafka
  mkdir -p $TEST_DATA_DIR
  KAFKA_URL="https://archive.apache.org/dist/kafka/0.10.2.0/kafka_2.11-0.10.2.0.tgz"
  echo "Downloading Kafka from $KAFKA_URL"
  curl "$KAFKA_URL" > $TEST_DATA_DIR/kafka.tgz

  tar xzf $TEST_DATA_DIR/kafka.tgz -C $TEST_DATA_DIR/

  # fix kafka config
  sed -i -e "s+^\(dataDir\s*=\s*\).*$+\1$TEST_DATA_DIR/zookeeper+" $KAFKA_DIR/config/zookeeper.properties
  sed -i -e "s+^\(log\.dirs\s*=\s*\).*$+\1$TEST_DATA_DIR/kafka+" $KAFKA_DIR/config/server.properties
}

function start_kafka_cluster {
  if [[ -z $KAFKA_DIR ]]; then
    echo "Must run 'setup_kafka_dist' before attempting to start Kafka cluster"
    exit 1
  fi

  $KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
  $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties

  # zookeeper outputs the "Node does not exist" bit to stderr
  while [[ $($KAFKA_DIR/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/0 2>&1) =~ .*Node\ does\ not\ exist.* ]]; do
    echo "Waiting for broker..."
    sleep 1
  done
}

function stop_kafka_cluster {
  $KAFKA_DIR/bin/kafka-server-stop.sh
  $KAFKA_DIR/bin/zookeeper-server-stop.sh
}

function create_kafka_topic {
  $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor $1 --partitions $2 --topic $3
}

function send_messages_to_kafka {
  echo -e $1 | $KAFKA_DIR/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $2
}

function read_messages_from_kafka {
  $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning \
    --max-messages $1 \
    --topic $2 \
    --consumer-property group.id=$3 2> /dev/null
}

function modify_num_partitions {
  $KAFKA_DIR/bin/kafka-topics.sh --alter --topic $1 --partitions $2 --zookeeper localhost:2181
}

function get_num_partitions {
  $KAFKA_DIR/bin/kafka-topics.sh --describe --topic $1 --zookeeper localhost:2181 | grep -Eo "PartitionCount:[0-9]+" | cut -d ":" -f 2
}

function get_partition_end_offset {
  local topic=$1
  local partition=$2

  # first, use the console consumer to produce a dummy consumer group
  read_messages_from_kafka 0 $topic dummy-consumer

  # then use the consumer offset utility to get the LOG_END_OFFSET value for the specified partition
  $KAFKA_DIR/bin/kafka-consumer-groups.sh --describe --group dummy-consumer --bootstrap-server localhost:9092 2> /dev/null \
    | grep "$topic \+$partition" \
    | tr -s " " \
    | cut -d " " -f 4
}
