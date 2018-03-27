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

setup_kafka_dist
start_kafka_cluster

start_cluster

function test_cleanup {
  stop_kafka_cluster

  # make sure to run regular cleanup as well
  cleanup
}
trap test_cleanup INT
trap test_cleanup EXIT

# create the required topics
create_kafka_topic 1 1 test-input
create_kafka_topic 1 1 test-output

# run the Flink job (detached mode)
$FLINK_DIR/bin/flink run -d $FLINK_DIR/examples/streaming/Kafka010Example.jar \
  --input-topic test-input --output-topic test-output \
  --prefix=PREFIX \
  --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer --auto.offset.reset earliest

# send some data to Kafka
send_messages_to_kafka "hello,45218\nwhats,46213\nup,51348" test-input
DATA_FROM_KAFKA=$(read_messages_from_kafka 3 test-output)

# make sure we have actual newlines in the string, not "\n"
EXPECTED=$(printf "PREFIX:hello,45218\nPREFIX:whats,46213\nPREFIX:up,51348")
if [[ "$DATA_FROM_KAFKA" != "$EXPECTED" ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: --$EXPECTED--"
  echo -e "ACTUAL: --$DATA_FROM_KAFKA--"
  PASS=""
fi
