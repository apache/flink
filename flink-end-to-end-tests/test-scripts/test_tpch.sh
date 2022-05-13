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

SCALE="0.01"

source "$(dirname "$0")"/common.sh

################################################################################
# Generate test data
################################################################################

echo "Generating test data..."

TARGET_DIR="$END_TO_END_DIR/flink-tpch-test/target"
TPCH_DATA_DIR="$END_TO_END_DIR/test-scripts/test-data/tpch"
java -cp "$TARGET_DIR/TpchTestProgram.jar:$TARGET_DIR/lib/*" org.apache.flink.table.tpch.TpchDataGenerator "$SCALE" "$TARGET_DIR"

################################################################################
# Prepare Flink
################################################################################

echo "Preparing Flink..."

set_config_key "taskmanager.memory.managed.fraction" "0.55f"
start_cluster

################################################################################
# Run SQL statements
################################################################################

TABLE_DIR="$TARGET_DIR/table"
ORIGIN_QUERY_DIR="$TARGET_DIR/query"
MODIFIED_QUERY_DIR="$TPCH_DATA_DIR/modified-query"
EXPECTED_DIR="$TARGET_DIR/expected"
RESULT_DIR="$TEST_DATA_DIR/result"
INIT_SQL="$TEST_DATA_DIR/init_table.sql"

mkdir "$RESULT_DIR"

SOURCES_SQL=$(cat "$TPCH_DATA_DIR/source.sql")
SOURCES_SQL=${SOURCES_SQL//\$TABLE_DIR/"$TABLE_DIR"}

for i in {1..22}
do
    echo "Running query #$i..."

    SINK_SQL=$(cat "$TPCH_DATA_DIR/sink/q${i}.sql")
    SINK_SQL=${SINK_SQL//\$RESULT_DIR/"$RESULT_DIR"}

    cat > "$INIT_SQL" << EOF
${SOURCES_SQL}
${SINK_SQL}
SET execution.runtime-mode=batch;
SET parallelism.default=2;
EOF

    if [[ -e "$MODIFIED_QUERY_DIR/q$i.sql" ]]
    then
        SQL_STATEMENT="INSERT INTO q$i $(cat "$MODIFIED_QUERY_DIR/q$i.sql")"
    else
        SQL_STATEMENT="INSERT INTO q$i $(cat "$ORIGIN_QUERY_DIR/q$i.sql")"
    fi

    JOB_ID=$("$FLINK_DIR/bin/sql-client.sh" \
        -i "$INIT_SQL" \
        --update "$SQL_STATEMENT" | grep "Job ID:" | sed 's/.* //g')

    wait_job_terminal_state "$JOB_ID" "FINISHED"

    java -cp "$TARGET_DIR/TpchTestProgram.jar" org.apache.flink.table.tpch.TpchResultComparator "$EXPECTED_DIR/q$i.csv" "$RESULT_DIR/q$i"
done
