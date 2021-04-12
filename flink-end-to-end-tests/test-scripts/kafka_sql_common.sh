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

KAFKA_VERSION="$1"
CONFLUENT_VERSION="$2"
CONFLUENT_MAJOR_VERSION="$3"
KAFKA_SQL_VERSION="$4"

source "$(dirname "$0")"/kafka-common.sh $1 $2 $3

function create_kafka_json_source {
    topicName="$1"
    create_kafka_topic 1 1 $topicName

    # put JSON data into Kafka
    echo "Sending messages to Kafka..."

    send_messages_to_kafka '{"timestamp": "2018-03-12T08:00:00Z", "user": "Alice", "event": { "type": "WARNING", "message": "This is a warning."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T08:10:00Z", "user": "Alice", "event": { "type": "WARNING", "message": "This is a warning."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T09:00:00Z", "user": "Bob", "event": { "type": "WARNING", "message": "This is another warning."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T09:10:00Z", "user": "Alice", "event": { "type": "INFO", "message": "This is a info."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T09:20:00Z", "user": "Steve", "event": { "type": "INFO", "message": "This is another info."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T09:30:00Z", "user": "Steve", "event": { "type": "INFO", "message": "This is another info."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T09:30:00Z", "user": null, "event": { "type": "WARNING", "message": "This is a bad message because the user is missing."}}' $topicName
    send_messages_to_kafka '{"timestamp": "2018-03-12T10:40:00Z", "user": "Bob", "event": { "type": "ERROR", "message": "This is an error."}}' $topicName
}

function get_kafka_json_source_schema {
    topicName="$1"
    tableName="$2"
    cat << EOF
  - name: $tableName
    type: source-table
    update-mode: append
    schema:
      - name: rowtime
        data-type: TIMESTAMP(3)
        rowtime:
          timestamps:
            type: from-field
            from: timestamp
          watermarks:
            type: periodic-bounded
            delay: 2000
      - name: user
        data-type: STRING
      - name: event
        data-type: ROW<type STRING, message STRING>
    connector:
      type: kafka
      version: "$KAFKA_SQL_VERSION"
      topic: $topicName
      startup-mode: earliest-offset
      properties:
        bootstrap.servers: localhost:9092
    format:
      type: json
      json-schema: >
        {
          "type": "object",
          "properties": {
            "timestamp": {
              "type": "string",
              "format": "date-time"
            },
            "user": {
              "type": ["string", "null"]
            },
            "event": {
              "type": "object",
              "properties": {
                "type": {
                  "type": "string"
                },
                "message": {
                  "type": "string"
                }
              }
            }
          }
        }
EOF
}
