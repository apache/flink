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

PLANNER="${1:-old}"

KAFKA_VERSION="2.2.2"
CONFLUENT_VERSION="5.0.0"
CONFLUENT_MAJOR_VERSION="5.0"
KAFKA_SQL_VERSION="universal"
ELASTICSEARCH_VERSION=7
# we use the smallest version possible
ELASTICSEARCH_MAC_DOWNLOAD_URL='https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-7.5.1-no-jdk-darwin-x86_64.tar.gz'
ELASTICSEARCH_LINUX_DOWNLOAD_URL='https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-oss-7.5.1-no-jdk-linux-x86_64.tar.gz'

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka_sql_common.sh \
  $KAFKA_VERSION \
  $CONFLUENT_VERSION \
  $CONFLUENT_MAJOR_VERSION \
  $KAFKA_SQL_VERSION
source "$(dirname "$0")"/elasticsearch-common.sh

SQL_TOOLBOX_JAR=$END_TO_END_DIR/flink-sql-client-test/target/SqlToolbox.jar
SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars

################################################################################
# Verify existing SQL jars
################################################################################

EXTRACTED_JAR=$TEST_DATA_DIR/extracted

mkdir -p $EXTRACTED_JAR

for SQL_JAR in $SQL_JARS_DIR/*.jar; do
  echo "Checking SQL JAR: $SQL_JAR"
  (cd $EXTRACTED_JAR && jar xf $SQL_JAR)

  # check for proper shading
  for EXTRACTED_FILE in $(find $EXTRACTED_JAR -type f); do

    if ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/org/apache/flink"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/META-INF"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/LICENSE"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/NOTICE"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/org/apache/avro"* ]] && \
        # Following required by amazon-kinesis-producer in flink-connector-kinesis
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/amazon-kinesis-producer-native-binaries"* ]] && \
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/cacerts"* ]] ; then
      echo "Bad file in JAR: $EXTRACTED_FILE"
      exit 1
    fi
  done

  # check for proper legacy table factory
  # Kinesis connector does not support legacy Table API
  if [[ $SQL_JAR == *"flink-sql-connector-kinesis"* ]]; then
    echo "Skipping Legacy Table API for: $SQL_JAR"
  elif [ ! -f $EXTRACTED_JAR/META-INF/services/org.apache.flink.table.factories.TableFactory ]; then
    echo "No legacy table factory found in JAR: $SQL_JAR"
    exit 1
  fi

  # check for table factory
  if [ ! -f $EXTRACTED_JAR/META-INF/services/org.apache.flink.table.factories.Factory ]; then
    echo "No table factory found in JAR: $SQL_JAR"
    exit 1
  fi

  # clean up
  rm -r $EXTRACTED_JAR/*
done

rm -r $EXTRACTED_JAR

################################################################################
# Prepare connectors
################################################################################

ELASTICSEARCH_INDEX='my_users'

function sql_cleanup() {
  stop_kafka_cluster
  shutdown_elasticsearch_cluster "$ELASTICSEARCH_INDEX"
}
on_exit sql_cleanup

function prepare_elasticsearch {
  echo "Preparing Elasticsearch (version=$ELASTICSEARCH_VERSION)..."
  # elastcisearch offers different release binary file for corresponding system since version 7.
  case "$(uname -s)" in
      Linux*)     OS_TYPE=linux;;
      Darwin*)    OS_TYPE=mac;;
      *)          OS_TYPE="UNKNOWN:${unameOut}"
  esac

  if [[ "$OS_TYPE" == "mac" ]]; then
    DOWNLOAD_URL=$ELASTICSEARCH_MAC_DOWNLOAD_URL
  elif [[ "$OS_TYPE" == "linux" ]]; then
    DOWNLOAD_URL=$ELASTICSEARCH_LINUX_DOWNLOAD_URL
  else
    echo "[ERROR] Unsupported OS for Elasticsearch: $OS_TYPE"
    exit 1
  fi

  setup_elasticsearch $DOWNLOAD_URL $ELASTICSEARCH_VERSION
  wait_elasticsearch_working
}

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist

start_kafka_cluster

create_kafka_json_source test-json
create_kafka_topic 1 1 test-avro

# prepare Elasticsearch
prepare_elasticsearch

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

KAFKA_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "kafka_" )
ELASTICSEARCH_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "elasticsearch$ELASTICSEARCH_VERSION" )

# create session environment file
RESULT=$TEST_DATA_DIR/result
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

cat >> $SQL_CONF << EOF
tables:
EOF

get_kafka_json_source_schema test-json JsonSourceTable >> $SQL_CONF

cat >> $SQL_CONF << EOF
  - name: ElasticsearchUpsertSinkTable
    type: sink-table
    update-mode: upsert
    schema:
      - name: user_id
        data-type: INT
      - name: user_name
        data-type: STRING
      - name: user_count
        data-type: BIGINT
    connector:
      type: elasticsearch
      version: "$ELASTICSEARCH_VERSION"
      hosts: "http://localhost:9200"
      index: "$ELASTICSEARCH_INDEX"
      document-type: "user"
      bulk-flush:
        max-actions: 1
    format:
      type: json
      derive-schema: true
  - name: ElasticsearchAppendSinkTable
    type: sink-table
    update-mode: append
    schema:
      - name: user_id
        data-type: INT
      - name: user_name
        data-type: STRING
      - name: user_count
        data-type: BIGINT
    connector:
      type: elasticsearch
      version: "$ELASTICSEARCH_VERSION"
      hosts: "http://localhost:9200"
      index: "$ELASTICSEARCH_INDEX"
      document-type: "user"
      bulk-flush:
        max-actions: 1
    format:
      type: json
      derive-schema: true

functions:
  - name: RegReplace
    from: class
    class: org.apache.flink.table.toolbox.StringRegexReplaceFunction

execution:
  planner: "$PLANNER"
EOF

# submit SQL statements

echo "Executing SQL: Values -> Elasticsearch (upsert)"

SQL_STATEMENT_1=$(cat << EOF
INSERT INTO ElasticsearchUpsertSinkTable
  SELECT user_id, user_name, COUNT(*) AS user_count
  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))
    AS UserCountTable(user_id, user_name)
  GROUP BY user_id, user_name
EOF
)

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $ELASTICSEARCH_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

verify_result_line_number 3 "$ELASTICSEARCH_INDEX"

echo "Executing SQL: Values -> Elasticsearch (append, no key)"

SQL_STATEMENT_2=$(cat << EOF
INSERT INTO ElasticsearchAppendSinkTable
  SELECT *
  FROM (
    VALUES
      (1, 'Bob', CAST(0 AS BIGINT)),
      (22, 'Tom', CAST(0 AS BIGINT)),
      (42, 'Kim', CAST(0 AS BIGINT)),
      (42, 'Kim', CAST(0 AS BIGINT)),
      (42, 'Kim', CAST(0 AS BIGINT)),
      (1, 'Bob', CAST(0 AS BIGINT)))
    AS UserCountTable(user_id, user_name, user_count)
EOF
)

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $ELASTICSEARCH_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_2" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

# 3 upsert results and 6 append results
verify_result_line_number 9 "$ELASTICSEARCH_INDEX"

echo "Executing SQL: Match recognize -> Elasticsearch"

SQL_STATEMENT_3=$(cat << EOF
INSERT INTO ElasticsearchAppendSinkTable
  SELECT 1 as user_id, T.userName as user_name, cast(1 as BIGINT) as user_count
  FROM (
    SELECT user, rowtime
    FROM JsonSourceTable
    WHERE user IS NOT NULL)
  MATCH_RECOGNIZE (
    ORDER BY rowtime
    MEASURES
        user as userName
    PATTERN (A)
    DEFINE
        A as user = 'Alice'
  ) T
EOF
)

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $ELASTICSEARCH_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_3" | grep "Job ID:" | sed 's/.* //g')

# 3 upsert results and 6 append results and 3 match_recognize results
verify_result_line_number 12 "$ELASTICSEARCH_INDEX"
