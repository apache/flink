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

# Test for SQL (batch mode) job that runs successfully on a Flink cluster with fewer slots (1) than job's total slots (9).
set -Eeuo pipefail

source "$(dirname "$0")"/common.sh

TEST_PROGRAM_JAR=${END_TO_END_DIR}/flink-batch-sql-test/target/BatchSQLTestProgram.jar

OUTPUT_FILE_PATH="${TEST_DATA_DIR}/out/result/results.csv"

function sqlJobQuery() {
    local tumbleWindowSizeSeconds=10

    overQuery=$(cat <<SQL
SELECT key, rowtime, 42 AS cnt FROM table1
SQL
)

    tumbleQuery=$(cat <<SQL
SELECT
    key,
    CASE SUM(cnt) / COUNT(*) WHEN 101 THEN 1 WHEN -1 THEN NULL ELSE 99 END AS correct,
    TUMBLE_START(rowtime, INTERVAL '${tumbleWindowSizeSeconds}' SECOND) AS wStart,
    TUMBLE_ROWTIME(rowtime, INTERVAL '${tumbleWindowSizeSeconds}' SECOND) AS rowtime
FROM (${overQuery})
WHERE rowtime > TIMESTAMP '1970-01-01 00:00:01'
GROUP BY key, TUMBLE(rowtime, INTERVAL '${tumbleWindowSizeSeconds}' SECOND)
SQL
)

    joinQuery=$(cat <<SQL
SELECT
    t1.key,
    t2.rowtime AS rowtime,
    t2.correct,
    t2.wStart
FROM table2 t1, (${tumbleQuery}) t2
WHERE
    t1.key = t2.key AND
    t1.rowtime BETWEEN t2.rowtime AND t2.rowtime + INTERVAL '${tumbleWindowSizeSeconds}' SECOND
SQL
)

    echo "
SELECT
    SUM(correct) AS correct,
    TUMBLE_START(rowtime, INTERVAL '20' SECOND) AS rowtime
FROM (${joinQuery})
GROUP BY TUMBLE(rowtime, INTERVAL '20' SECOND)"
}

set_config_key "taskmanager.numberOfTaskSlots" "1"

start_cluster

# The task has total 2 x (1 + 1 + 1 + 1) + 1 = 9 slots
$FLINK_DIR/bin/flink run -p 2 $TEST_PROGRAM_JAR -outputPath "file://${OUTPUT_FILE_PATH}" -sqlStatement \
    "INSERT INTO sinkTable $(sqlJobQuery)"

# check result:
#1980,1970-01-01 00:00:00.0
#1980,1970-01-01 00:00:20.0
#1980,1970-01-01 00:00:40.0
check_result_hash "BatchSQL" "${OUTPUT_FILE_PATH}" "c7ccd2c3a25c3e06616806cf6aecaa66"
