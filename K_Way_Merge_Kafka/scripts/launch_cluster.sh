#!/bin/bash

# Path to Kafka server directory
KAFKA_SERVER_DIR="./kafka-3.7.0-src"
cd "$KAFKA_SERVER_DIR" || exit 

rm -rf /tmp/kafka-logs /tmp/zookeeper /tmp/kraft-combined-logs
# Function to start Kafka Zookeeper
start_zookeeper() {
    echo "Starting Kafka Zookeeper..."
    # Insert command to start Kafka Zookeeper here
    # Example: ./start_zookeeper_command.sh
}

# Function to create a new terminal window or tab
open_new_terminal() {
    echo "Opening new terminal..."
    # Insert command to open new terminal here
    # Example for Linux:
    # gnome-terminal --working-directory="$PWD"
    # Example for macOS:
    # osascript -e 'tell app "Terminal" to do script "cd '$PWD'"'
}

# Function to create Kafka broker
create_broker() {
    echo "Creating Kafka broker..."
    # Insert command to create Kafka broker here
    # Example: ./create_broker_command.sh
}