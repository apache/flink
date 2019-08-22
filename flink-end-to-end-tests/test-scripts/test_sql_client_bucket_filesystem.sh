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

################################################################################
# Prepare Flink
################################################################################

echo "Preparing Flink..."

start_cluster
start_taskmanagers 1

################################################################################
# Run SQL statements
################################################################################

echo "Testing SQL statements..."

SQL_TOOLBOX_JAR=$END_TO_END_DIR/flink-sql-client-test/target/SqlToolbox.jar
SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars

CSV_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "csv" )
JSON_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "json" )
FILESYSTEM_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "bucket" )

# create session environment file
RESULT=$TEST_DATA_DIR/result
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

echo "SQL_CONF is $RESULT"

cat >> $SQL_CONF << EOF
tables:
EOF


cat >> $SQL_CONF << EOF
  - name: JsonFileSinkTable
    type: sink-table
    update-mode: append
    schema:
      - name: user_id
        type: INT
      - name: user_name
        type: VARCHAR
    connector:
      type: bucket
      basepath: $RESULT/json
      format.type: row
      date.format: yyyyMMddHH
    format:
      type: json
      derive-schema: true
  - name: CsvFileSinkTable
    type: sink-table
    update-mode: append
    schema:
      - name: user_id
        type: INT
      - name: user_name
        type: VARCHAR
    connector:
      type: bucket
      basepath: $RESULT/csv
      format.type: row
    format:
      type: csv
      derive-schema: true


EOF

# submit SQL statements


################################################################################
# write data to file with json format
################################################################################

echo "Executing SQL: write data to file with json format"

SQL_STATEMENT_1=$(cat << EOF
INSERT INTO JsonFileSinkTable
  SELECT *
  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))
    AS UserInfoTable(user_id, user_name)
EOF
)

echo "$SQL_STATEMENT_1"

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --library $SQL_JARS_DIR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_1" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

cat $RESULT/json/*/.part-* | sort > $RESULT/json/result-complete

check_result_hash "Sql2FileJson" $RESULT/json/result-complete "1db0810e81484f1a870554feccdca9df"


###############################################################################
# write data to file with csv format
###############################################################################

echo "Executing SQL: write data to file with csv format"

SQL_STATEMENT_2=$(cat << EOF
INSERT INTO CsvFileSinkTable
  SELECT *
  FROM (VALUES (1, 'Bob'), (22, 'Tom'), (42, 'Kim'), (42, 'Kim'), (42, 'Kim'), (1, 'Bob'))
    AS UserInfoTable(user_id, user_name)
EOF
)

echo "$SQL_STATEMENT_2"

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --library $SQL_JARS_DIR \
  --jar $SQL_TOOLBOX_JAR \
  --environment $SQL_CONF \
  --update "$SQL_STATEMENT_2" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

cat $RESULT/csv/*/.part-* | sort > $RESULT/csv/result-complete

check_result_hash "Sql2FileCsv" $RESULT/csv/result-complete "33cbc445948549d75bfa50ae5482779d"




