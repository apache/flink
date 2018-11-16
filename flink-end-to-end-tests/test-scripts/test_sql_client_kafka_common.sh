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

KAFKA_CONNECTOR_VERSION="$1"
KAFKA_VERSION="$2"
CONFLUENT_VERSION="$3"
CONFLUENT_MAJOR_VERSION="$4"
KAFKA_SQL_JAR="$5"
KAFKA_SQL_VERSION="$6"

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka_sql_common.sh \
  $KAFKA_CONNECTOR_VERSION \
  $KAFKA_VERSION \
  $CONFLUENT_VERSION \
  $CONFLUENT_MAJOR_VERSION \
  $KAFKA_SQL_VERSION

################################################################################
# Prepare connectors
################################################################################

function sql_cleanup() {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_kafka_cluster
}
trap sql_cleanup INT
trap sql_cleanup EXIT

# prepare Kafka
echo "Preparing Kafka $KAFKA_VERSION..."

setup_kafka_dist

start_kafka_cluster

create_kafka_json_source test-json
create_kafka_topic 1 1 test-avro

################################################################################
# Prepare Flink
################################################################################

echo "Preparing Flink..."

start_cluster
start_taskmanagers 2

################################################################################
# Run SQL statements
################################################################################

echo "Testing SQL statements..."

SQL_TOOLBOX_JAR=$END_TO_END_DIR/flink-sql-client-test/target/SqlToolbox.jar
SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars

AVRO_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "avro" )
JSON_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "json" )
KAFKA_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "${KAFKA_SQL_JAR}_" )

# create session environment file
RESULT=$TEST_DATA_DIR/result
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

cat >> $SQL_CONF << EOF
tables:
EOF

get_kafka_json_source_schema test-json JsonSourceTable >> $SQL_CONF

cat >> $SQL_CONF << EOF
  - name: AvroBothTable
    type: source-sink-table
    update-mode: append
    schema:
      - name: event_timestamp
        type: VARCHAR
      - name: user
        type: VARCHAR
      - name: message
        type: VARCHAR
      - name: duplicate_count
        type: BIGINT
    connector:
      type: kafka
      version: "$KAFKA_SQL_VERSION"
      topic: test-avro
      startup-mode: earliest-offset
      properties:
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092
    format:
      type: avro
      avro-schema: >
        {
          "namespace": "org.apache.flink.table.tests",
          "type": "record",
          "name": "NormalizedEvent",
            "fields": [
              {"name": "event_timestamp", "type": "string"},
              {"name": "user", "type": ["string", "null"]},
              {"name": "message", "type": "string"},
              {"name": "duplicate_count", "type": "long"}
            ]
        }
  - name: CsvSinkTable
    type: sink-table
    update-mode: append
    schema:
      - name: event_timestamp
        type: VARCHAR
      - name: user
        type: VARCHAR
      - name: message
        type: VARCHAR
      - name: duplicate_count
        type: BIGINT
      - name: constant
        type: VARCHAR
    connector:
      type: filesystem
      path: $RESULT
    format:
      type: csv
      fields:
        - name: event_timestamp
          type: VARCHAR
        - name: user
          type: VARCHAR
        - name: message
          type: VARCHAR
        - name: duplicate_count
          type: BIGINT
        - name: constant
          type: VARCHAR

functions:
  - name: RegReplace
    from: class
    class: org.apache.flink.table.toolbox.StringRegexReplaceFunction
EOF

# submit SQL statements

echo "Executing SQL: Kafka $KAFKA_VERSION JSON -> Kafka $KAFKA_VERSION Avro"

SQL_STATEMENT_1=$(cat << EOF
INSERT INTO AvroBothTable
  SELECT
    CAST(TUMBLE_START(rowtime, INTERVAL '1' HOUR) AS VARCHAR) AS event_timestamp,
    user,
    RegReplace(event.message, ' is ', ' was ') AS message,
    COUNT(*) AS duplicate_count
  FROM JsonSourceTable
  WHERE user IS NOT NULL
  GROUP BY
    user,
    event.message,
    TUMBLE(rowtime, INTERVAL '1' HOUR)
EOF
)

echo "$SQL_STATEMENT_1"

$FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $JSON_SQL_JAR \
  --jar $AVRO_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1"

echo "Executing SQL: Kafka $KAFKA_VERSION Avro -> Filesystem CSV"

SQL_STATEMENT_2=$(cat << EOF
INSERT INTO CsvSinkTable
  SELECT AvroBothTable.*, RegReplace('Test constant folding.', 'Test', 'Success') AS constant
  FROM AvroBothTable
EOF
)

echo "$SQL_STATEMENT_2"

$FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $JSON_SQL_JAR \
  --jar $AVRO_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_2"

echo "Waiting for CSV results..."
for i in {1..10}; do
  if [ -e $RESULT ]; then
    CSV_LINE_COUNT=`cat $RESULT | wc -l`
    if [ $((CSV_LINE_COUNT)) -eq 4 ]; then
      break
    fi
  fi
  sleep 5
done

check_result_hash "SQL Client Kafka $KAFKA_VERSION" $RESULT "0a1bf8bf716069b7269f575f87a802c0"
