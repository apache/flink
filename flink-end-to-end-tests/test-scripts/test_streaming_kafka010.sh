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

# make sure to stop Kafka and ZooKeeper at the end

function kafka_cleanup {
  $KAFKA_DIR/bin/kafka-server-stop.sh
  $KAFKA_DIR/bin/zookeeper-server-stop.sh

  # make sure to run regular cleanup as well
  cleanup
}
trap kafka_cleanup INT
trap kafka_cleanup EXIT

# zookeeper outputs the "Node does not exist" bit to stderr
while [[ $($KAFKA_DIR/bin/zookeeper-shell.sh localhost:2181 get /brokers/ids/0 2>&1) =~ .*Node\ does\ not\ exist.* ]]; do
  echo "Waiting for broker..."
  sleep 1
done

# create the required topics
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-input
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-output

# run the Flink job (detached mode)
$FLINK_DIR/bin/flink run -d $FLINK_DIR/examples/streaming/Kafka010Example.jar \
  --input-topic test-input --output-topic test-output \
  --prefix=PREFIX \
  --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer --auto.offset.reset earliest

# send some data to Kafka
echo -e "hello,45218\nwhats,46213\nup,51348" | $KAFKA_DIR/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-input

DATA_FROM_KAFKA=$($KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-output --from-beginning --max-messages 3 2> /dev/null)

# make sure we have actual newlines in the string, not "\n"
EXPECTED=$(printf "PREFIX:hello,45218\nPREFIX:whats,46213\nPREFIX:up,51348")
if [[ "$DATA_FROM_KAFKA" != "$EXPECTED" ]]; then
  echo "Output from Flink program does not match expected output."
  echo -e "EXPECTED: --$EXPECTED--"
  echo -e "ACTUAL: --$DATA_FROM_KAFKA--"
  PASS=""
fi
