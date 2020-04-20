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

if [[ -z $TEST_DATA_DIR ]]; then
  echo "Must run common.sh before kafka-common.sh."
  exit 1
fi

KAFKA_VERSION="$1"
CONFLUENT_VERSION="$2"
CONFLUENT_MAJOR_VERSION="$3"

KAFKA_DIR=$TEST_DATA_DIR/kafka_2.11-$KAFKA_VERSION
CONFLUENT_DIR=$TEST_DATA_DIR/confluent-$CONFLUENT_VERSION
SCHEMA_REGISTRY_PORT=8082
SCHEMA_REGISTRY_URL=http://localhost:${SCHEMA_REGISTRY_PORT}
MAX_RETRY_SECONDS=120

function setup_kafka_dist {
  # download Kafka
  mkdir -p $TEST_DATA_DIR
  KAFKA_URL="https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_2.11-$KAFKA_VERSION.tgz"
  echo "Downloading Kafka from $KAFKA_URL"
  curl ${KAFKA_URL} --retry 10 --retry-max-time 120 --output ${TEST_DATA_DIR}/kafka.tgz

  tar xzf $TEST_DATA_DIR/kafka.tgz -C $TEST_DATA_DIR/

  # fix kafka config
  sed -i -e "s+^\(dataDir\s*=\s*\).*$+\1$TEST_DATA_DIR/zookeeper+" $KAFKA_DIR/config/zookeeper.properties
  sed -i -e "s+^\(log\.dirs\s*=\s*\).*$+\1$TEST_DATA_DIR/kafka+" $KAFKA_DIR/config/server.properties
}

function setup_confluent_dist {
  # download confluent
  mkdir -p $TEST_DATA_DIR
  CONFLUENT_URL="http://packages.confluent.io/archive/$CONFLUENT_MAJOR_VERSION/confluent-oss-$CONFLUENT_VERSION-2.11.tar.gz"
  echo "Downloading confluent from $CONFLUENT_URL"
  curl ${CONFLUENT_URL} --retry 10 --retry-max-time 120 --output ${TEST_DATA_DIR}/confluent.tgz

  tar xzf $TEST_DATA_DIR/confluent.tgz -C $TEST_DATA_DIR/

  # fix confluent config
  sed -i -e "s#listeners=http://0.0.0.0:8081#listeners=http://0.0.0.0:${SCHEMA_REGISTRY_PORT}#" $CONFLUENT_DIR/etc/schema-registry/schema-registry.properties
}

function start_kafka_cluster {
  if [[ -z $KAFKA_DIR ]]; then
    echo "Must run 'setup_kafka_dist' before attempting to start Kafka cluster"
    exit 1
  fi

  $KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
  $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties

  start_time=$(date +%s)
  #
  # Wait for the broker info to appear in ZK. We assume propery registration once an entry
  # similar to this is in ZK: {"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://my-host:9092"],"jmx_port":-1,"host":"honorary-pig","timestamp":"1583157804932","port":9092,"version":4}
  #
  while ! [[ $($KAFKA_DIR/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/0 2>&1) =~ .*listener_security_protocol_map.* ]]; do
    current_time=$(date +%s)
    time_diff=$((current_time - start_time))

    if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
        echo "Kafka cluster did not start after $MAX_RETRY_SECONDS seconds. Printing Kafka logs:"
        debug_error
        exit 1
    else
        echo "Waiting for broker..."
        sleep 1
    fi
  done
}

function stop_kafka_cluster {
  if ! [[ -z $($KAFKA_DIR/bin/kafka-server-stop.sh) ]]; then
    echo "Kafka server was already shut down; dumping logs:"
    cat ${KAFKA_DIR}/logs/server.out
  fi
  $KAFKA_DIR/bin/zookeeper-server-stop.sh

  # Terminate Kafka process if it still exists
  PIDS=$(jps -vl | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}'|| echo "")

  if [ ! -z "$PIDS" ]; then
    kill -s TERM $PIDS || true
  fi

  # Terminate QuorumPeerMain process if it still exists
  PIDS=$(jps -vl | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}'|| echo "")

  if [ ! -z "$PIDS" ]; then
    kill -s TERM $PIDS || true
  fi
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

function send_messages_to_kafka_avro {
  echo -e $1 | $CONFLUENT_DIR/bin/kafka-avro-console-producer --broker-list localhost:9092 --topic $2 --property value.schema=$3 --property schema.registry.url=${SCHEMA_REGISTRY_URL}
}

function read_messages_from_kafka_avro {
  $CONFLUENT_DIR/bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --from-beginning \
    --max-messages $1 \
    --topic $2 \
    --property value.schema=$3 \
    --property schema.registry.url=${SCHEMA_REGISTRY_URL} \
    --consumer-property group.id=$4
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

  $KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic --partitions $partition --time -1 | cut -d ":" -f 3
}

function start_confluent_schema_registry {
  $CONFLUENT_DIR/bin/schema-registry-start -daemon $CONFLUENT_DIR/etc/schema-registry/schema-registry.properties

  # wait until the schema registry REST endpoint is up
  for i in {1..30}; do
    if get_and_verify_schema_subjects_exist; then
        echo "Schema registry is up."
        return 0
    fi

    echo "Waiting for schema registry..."
    sleep 1
  done

  if ! get_and_verify_schema_subjects_exist; then
      echo "Could not start confluent schema registry"
      debug_error
      return 1
  fi
}

function get_schema_subjects {
    curl "${SCHEMA_REGISTRY_URL}/subjects" 2> /dev/null || true
}

function get_and_verify_schema_subjects_exist {
    QUERY_RESULT=$(get_schema_subjects)

    [[ ${QUERY_RESULT} =~ \[.*\] ]]
}

function stop_confluent_schema_registry {
    $CONFLUENT_DIR/bin/schema-registry-stop
}

function debug_error {
    echo "Debugging test failure. Currently running JVMs:"
    jps -v
    echo "Kafka logs:"
    find $KAFKA_DIR/logs/ -type f -exec printf "\n===\ncontents of {}:\n===\n" \; -exec cat {} \;

    echo "Kafka config:"
    find $KAFKA_DIR/config/ -type f -exec printf "\n===\ncontents of {}:\n===\n" \; -exec cat {} \;
}
