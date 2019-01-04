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

KAFKA_CONNECTOR_VERSION="2.0"
KAFKA_VERSION="2.0.1"
CONFLUENT_VERSION="5.0.0"
CONFLUENT_MAJOR_VERSION="5.0"
KAFKA_SQL_VERSION="universal"

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka_sql_common.sh \
  $KAFKA_CONNECTOR_VERSION \
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
        ! [[ $EXTRACTED_FILE = "$EXTRACTED_JAR/NOTICE"* ]] ; then
      echo "Bad file in JAR: $EXTRACTED_FILE"
      exit 1
    fi
  done

  # check for proper factory
  if [ ! -f $EXTRACTED_JAR/META-INF/services/org.apache.flink.table.factories.TableFactory ]; then
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
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  stop_kafka_cluster
  shutdown_elasticsearch_cluster "$ELASTICSEARCH_INDEX"
}
trap sql_cleanup INT
trap sql_cleanup EXIT

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist

start_kafka_cluster

create_kafka_json_source test-json
create_kafka_topic 1 1 test-avro

# prepare Elasticsearch
echo "Preparing Elasticsearch..."

ELASTICSEARCH_VERSION=6
DOWNLOAD_URL='https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.3.1.tar.gz'

setup_elasticsearch $DOWNLOAD_URL
wait_elasticsearch_working

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

JSON_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "json" )
KAFKA_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "kafka_" )
ELASTICSEARCH_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "elasticsearch6" )

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
        type: INT
      - name: user_name
        type: VARCHAR
      - name: user_count
        type: BIGINT
    connector:
      type: elasticsearch
      version: 6
      hosts:
        - hostname: "localhost"
          port: 9200
          protocol: "http"
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
        type: INT
      - name: user_name
        type: VARCHAR
      - name: user_count
        type: BIGINT
    connector:
      type: elasticsearch
      version: 6
      hosts:
        - hostname: "localhost"
          port: 9200
          protocol: "http"
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
EOF

# submit SQL statements

echo "Executing SQL: Values -> Elasticsearch (upsert)"

SQL_STATEMENT_3=$(cat << EOF
INSERT INTO ElasticsearchUpsertSinkTable
  SELECT user_id, user_name, COUNT(*) AS user_count
  FROM (VALUES (1, 'Bob'), (22, 'Alice'), (42, 'Greg'), (42, 'Greg'), (42, 'Greg'), (1, 'Bob'))
    AS UserCountTable(user_id, user_name)
  GROUP BY user_id, user_name
EOF
)

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --library $SQL_JARS_DIR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_3" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

verify_result_hash "SQL Client Elasticsearch Upsert" "$ELASTICSEARCH_INDEX" 3 "982cb32908def9801e781381c1b8a8db"

echo "Executing SQL: Values -> Elasticsearch (append, no key)"

SQL_STATEMENT_4=$(cat << EOF
INSERT INTO ElasticsearchAppendSinkTable
  SELECT *
  FROM (
    VALUES
      (1, 'Bob', CAST(0 AS BIGINT)),
      (22, 'Alice', CAST(0 AS BIGINT)),
      (42, 'Greg', CAST(0 AS BIGINT)),
      (42, 'Greg', CAST(0 AS BIGINT)),
      (42, 'Greg', CAST(0 AS BIGINT)),
      (1, 'Bob', CAST(0 AS BIGINT)))
    AS UserCountTable(user_id, user_name, user_count)
EOF
)

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --jar $KAFKA_SQL_JAR \
  --jar $JSON_SQL_JAR \
  --jar $ELASTICSEARCH_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_4" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

# 3 upsert results and 6 append results
verify_result_line_number 9 "$ELASTICSEARCH_INDEX"

echo "Executing SQL: Match recognize -> Elasticsearch"

SQL_STATEMENT_5=$(cat << EOF
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
  --jar $JSON_SQL_JAR \
  --jar $ELASTICSEARCH_SQL_JAR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_5" | grep "Job ID:" | sed 's/.* //g')

# 3 upsert results and 6 append results and 3 match_recognize results
verify_result_line_number 12 "$ELASTICSEARCH_INDEX"
