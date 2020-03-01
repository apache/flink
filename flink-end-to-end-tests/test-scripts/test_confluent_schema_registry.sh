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

set -Eeuo pipefail

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka-common.sh 0.10.2.0 3.2.0 3.2

function verify_output {
  local expected=$(printf $1)
  local result=$(echo $2 | sed 's/ //g')

  if [[ "$result" != "$expected" ]]; then
    echo "Output from Flink program does not match expected output."
    echo -e "EXPECTED FOR KEY: --$expected--"
    echo -e "ACTUAL: --$result--"
    exit 1
  fi
}

function test_setup {
  start_kafka_cluster
  start_confluent_schema_registry
}

function test_cleanup {
  stop_confluent_schema_registry
  stop_kafka_cluster
}

on_exit test_cleanup

setup_kafka_dist
setup_confluent_dist

retry_times_with_backoff_and_cleanup 3 5 test_setup test_cleanup

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-confluent-schema-registry/target/TestAvroConsumerConfluent.jar

INPUT_MESSAGE_1='{"name":"Alyssa","favoriteNumber":"250","favoriteColor":"green","eventType":"meeting"}'
INPUT_MESSAGE_2='{"name":"Charlie","favoriteNumber":"10","favoriteColor":"blue","eventType":"meeting"}'
INPUT_MESSAGE_3='{"name":"Ben","favoriteNumber":"7","favoriteColor":"red","eventType":"meeting"}'
USER_SCHEMA='{"namespace":"example.avro","type":"record","name":"User","fields":[{"name":"name","type":"string","default":""},{"name":"favoriteNumber","type":"string","default":""},{"name":"favoriteColor","type":"string","default":""},{"name":"eventType","type":{"name":"EventType","type":"enum","symbols":["meeting"]}}]}'

curl -X POST \
  ${SCHEMA_REGISTRY_URL}/subjects/users-value/versions \
  -H 'cache-control: no-cache' \
  -H 'content-type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema": "{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"User\",\"fields\": [{\"name\": \"name\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteNumber\",  \"type\": \"string\", \"default\": \"\"},{\"name\": \"favoriteColor\", \"type\": \"string\", \"default\": \"\"},{\"name\": \"eventType\",\"type\": {\"name\": \"EventType\",\"type\": \"enum\", \"symbols\": [\"meeting\"] }}]}"}'

echo "Sending messages to Kafka topic [test-avro-input] ..."

send_messages_to_kafka_avro $INPUT_MESSAGE_1 test-avro-input $USER_SCHEMA
send_messages_to_kafka_avro $INPUT_MESSAGE_2 test-avro-input $USER_SCHEMA
send_messages_to_kafka_avro $INPUT_MESSAGE_3 test-avro-input $USER_SCHEMA

start_cluster

create_kafka_topic 1 1 test-string-out
create_kafka_topic 1 1 test-avro-out

# Read Avro message from [test-avro-input], check the schema and send message to [test-string-ou]
$FLINK_DIR/bin/flink run -d $TEST_PROGRAM_JAR \
  --input-topic test-avro-input --output-string-topic test-string-out --output-avro-topic test-avro-out --output-subject test-output-subject \
  --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer --auto.offset.reset earliest \
  --schema-registry-url ${SCHEMA_REGISTRY_URL}

#echo "Reading messages from Kafka topic [test-string-ou] ..."

KEY_1_STRING_MSGS=$(read_messages_from_kafka 3 test-string-out Alyssa_consumer | grep Alyssa)
KEY_2_STRING_MSGS=$(read_messages_from_kafka 3 test-string-out Charlie_consumer | grep Charlie)
KEY_3_STRING_MSGS=$(read_messages_from_kafka 3 test-string-out Ben_consumer | grep Ben)

## Verifying STRING output with actual message
verify_output $INPUT_MESSAGE_1 "$KEY_1_STRING_MSGS"
verify_output $INPUT_MESSAGE_2 "$KEY_2_STRING_MSGS"
verify_output $INPUT_MESSAGE_3 "$KEY_3_STRING_MSGS"

KEY_1_AVRO_MSGS=$(read_messages_from_kafka_avro 3 test-avro-out $USER_SCHEMA Alyssa_consumer_1 | grep Alyssa)
KEY_2_AVRO_MSGS=$(read_messages_from_kafka_avro 3 test-avro-out $USER_SCHEMA Charlie_consumer_1 | grep Charlie)
KEY_3_AVRO_MSGS=$(read_messages_from_kafka_avro 3 test-avro-out $USER_SCHEMA Ben_consumer_1 | grep Ben)

## Verifying AVRO output with actual message
verify_output $INPUT_MESSAGE_1 "$KEY_1_AVRO_MSGS"
verify_output $INPUT_MESSAGE_2 "$KEY_2_AVRO_MSGS"
verify_output $INPUT_MESSAGE_3 "$KEY_3_AVRO_MSGS"

