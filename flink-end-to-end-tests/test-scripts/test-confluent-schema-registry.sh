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
source "$(dirname "$0")"/kafka-common.sh

function verify_output {
  local expected=$(printf $1)
  local result=$(echo $2 | sed 's/ //g')

  if [[ "$result" != "$expected" ]]; then
    echo "Output from Flink program does not match expected output."
    echo -e "EXPECTED FOR KEY: --$expected--"
    echo -e "ACTUAL: --$result--"
    PASS=""
    exit 1
  fi
}

function test_cleanup {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_kafka_cluster
  stop_confluent_schema_registry

  # revert our modifications to the Flink distribution
  mv -f $FLINK_DIR/conf/flink-conf.yaml.bak $FLINK_DIR/conf/flink-conf.yaml

  # make sure to run regular cleanup as well
  cleanup
}

trap test_cleanup INT
trap test_cleanup EXIT

setup_kafka_dist
setup_confluent_dist

cd flink-end-to-end-tests/flink-confluent-schema-registry
mvn clean package -nsu

start_kafka_cluster
start_confluent_schema_registry
sleep 5

# modify configuration to use port 8082 for Flink
cp $FLINK_DIR/conf/flink-conf.yaml $FLINK_DIR/conf/flink-conf.yaml.bak
sed -i -e "s/web.port: 8081/web.port: 8082/" $FLINK_DIR/conf/flink-conf.yaml

TEST_PROGRAM_JAR=target/TestAvroConsumerConfluent.jar

INPUT_MESSAGE_1='{"name":"Alyssa","favoriteNumber":"250","favoriteColor":"green","eventType":"meeting"}'
INPUT_MESSAGE_2='{"name":"Charlie","favoriteNumber":"10","favoriteColor":"blue","eventType":"meeting"}'
INPUT_MESSAGE_3='{"name":"Ben","favoriteNumber":"7","favoriteColor":"red","eventType":"meeting"}'
USER_SCHEMA='{"namespace":"example.avro","type":"record","name":"User","fields":[{"name":"name","type":"string","default":""},{"name":"favoriteNumber","type":"string","default":""},{"name":"favoriteColor","type":"string","default":""},{"name":"eventType","type":{"name":"EventType","type":"enum","symbols":["meeting"]}}]}'

curl -X POST \
  http://localhost:8081/subjects/users-value/versions \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema": "{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"User\",\"fields\": [{\"name\": \"name\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteNumber\",  \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteColor\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"eventType\",\"type\": {\"name\": \"EventType\",\"type\": \"enum\", \"symbols\": [\"meeting\"] }}]}"}'

echo "Sending messages to Kafka topic [test-avro-input] ..."

send_messages_to_kafka_avro $INPUT_MESSAGE_1 test-avro-input $USER_SCHEMA
send_messages_to_kafka_avro $INPUT_MESSAGE_2 test-avro-input $USER_SCHEMA
send_messages_to_kafka_avro $INPUT_MESSAGE_3 test-avro-input $USER_SCHEMA

start_cluster

# Read Avro message from [test-avro-input], check the schema and send message to [test-avro-out]
$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR \
  --input-topic test-avro-input --output-topic test-avro-out \
  --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer --auto.offset.reset earliest \
  --schema-registry-url http://localhost:8081

#echo "Reading messages from Kafka topic [test-avro-out] ..."

KEY_1_MSGS=$(read_messages_from_kafka 3 test-avro-out  Alyssa_consumer | grep Alyssa)
KEY_2_MSGS=$(read_messages_from_kafka 3 test-avro-out Charlie_consumer | grep Charlie)
KEY_3_MSGS=$(read_messages_from_kafka 3 test-avro-out Ben_consumer | grep Ben)

## Verifying AVRO output with actual message
verify_output $INPUT_MESSAGE_1 "$KEY_1_MSGS"
verify_output $INPUT_MESSAGE_2 "$KEY_2_MSGS"
verify_output $INPUT_MESSAGE_3 "$KEY_3_MSGS"

