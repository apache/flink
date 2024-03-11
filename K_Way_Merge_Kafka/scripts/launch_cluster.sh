#!/bin/bash

# Path to Kafka server directory
KAFKA_SERVER_DIR="./kafka-3.7.0-src"
cd "$KAFKA_SERVER_DIR" || exit
PID_FILE="kafka_pids.txt"

rm -rf /tmp/kafka-logs /tmp/kafka-logs1 /tmp/zookeeper /tmp/kraft-combined-logs
# Function to start Kafka Zookeeper
start_zookeeper() {
    echo "Starting Kafka Zookeeper..."
    bin/zookeeper-server-start.sh config/zookeeper.properties
}

# Function to create Kafka broker
create_broker() {
    local config_file="$1"
    echo "Creating Kafka broker with config $config_file "
    bin/kafka-server-start.sh "$config_file"
}

make_topic() {
    local topic_name="$1"
    local replication_factor="$2"
    local num_partitions="$3"

    bin/kafka-topics.sh --create --topic "$topic_name" --bootstrap-server localhost:9092 --replication-factor $replication_factor --partitions $num_partitions
}

start_zookeeper &
sleep 1 # give it a second
echo $! >> "$PID_FILE"


# Start first broker
create_broker "config/server.properties" &
echo $! >> "$PID_FILE"
sleep 2 # let it finish initializing

# Start the second broker
create_broker "config/server1.properties" &
echo $! >> "$PID_FILE"
sleep 2

make_topic "test_topic" 2 3 2 > make_topic_error.log &
echo $! >> "$PID_FILE"

