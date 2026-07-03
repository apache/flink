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

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
source "${CURRENT_DIR}"/common.sh

function test_clean_up {
    stop_cluster
}
on_exit test_clean_up

cp -r "${FLINK_DIR}/conf" "${TEST_DATA_DIR}/conf"

# standard yaml do not allow duplicate keys
sed -i -e "/^taskmanager.memory.task.off-heap.size: /d" "${TEST_DATA_DIR}/conf/config.yaml"
sed -i -e "/^taskmanager.memory.process.size: /d" "${TEST_DATA_DIR}/conf/config.yaml"
sed -i -e "/^taskmanager.numberOfTaskSlots: /d" "${TEST_DATA_DIR}/conf/config.yaml"
echo "taskmanager.memory.task.off-heap.size: 768m" >> "${TEST_DATA_DIR}/conf/config.yaml"
echo "taskmanager.memory.process.size: 3172m" >> "${TEST_DATA_DIR}/conf/config.yaml"
echo "taskmanager.numberOfTaskSlots: 5" >> "${TEST_DATA_DIR}/conf/config.yaml"
export FLINK_CONF_DIR="${TEST_DATA_DIR}/conf"

FLINK_PYTHON_DIR=`cd "${CURRENT_DIR}/../../flink-python" && pwd -P`

UV_HOME="${FLINK_PYTHON_DIR}/dev/.uv"

"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -s uv

PYTHON_EXEC="${UV_HOME}/bin/python"

source "${UV_HOME}/bin/activate"

cd "${FLINK_PYTHON_DIR}"

rm -rf dist

uv pip install --group dev

python setup.py sdist

pushd apache-flink-libraries

python setup.py sdist

uv pip install dist/*

popd

uv pip install dist/*

cd dev

zip -q -r "${TEST_DATA_DIR}/venv.zip" .uv

deactivate

cd "${CURRENT_DIR}"

start_cluster

echo "Test PyFlink Table job:"

FLINK_PYTHON_TEST_DIR=`cd "${CURRENT_DIR}/../flink-python-test" && pwd -P`
REQUIREMENTS_PATH="${TEST_DATA_DIR}/requirements.txt"

echo "pytest==8.3.5" > "${REQUIREMENTS_PATH}"

echo "Test submitting python job with 'pipeline.jars':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.uv/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.jars "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test submitting python job with 'pipeline.classpaths':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.uv/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.classpaths "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.uv/bin/python" \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.uv/bin/python" \
    -c org.apache.flink.python.tests.BatchPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test using python udf in sql client:\n"
SUBMITTED_SQL=$TEST_DATA_DIR/submit-sql-client.sql

cat >> $SUBMITTED_SQL << EOF
CREATE TABLE sink (
  a BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$TEST_DATA_DIR/sql-client-test',
  'format' = 'csv'
);

SET 'python.logging.level.overrides' = 'add_one: DEBUG';

CREATE FUNCTION add_one AS 'add_one.add_one' LANGUAGE PYTHON;

SET 'python.client.executable'='$PYTHON_EXEC';

INSERT INTO sink SELECT add_one(a) FROM (VALUES (1), (2), (3)) as source (a);
EOF

JOB_ID=$($FLINK_DIR/bin/sql-client.sh \
  -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
  -pyreq "${REQUIREMENTS_PATH}" \
  -pyarch "${TEST_DATA_DIR}/venv.zip" \
  -pyexec "venv.zip/.uv/bin/python" \
  -f "$SUBMITTED_SQL" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

# check logs
captured_log="DEBUG: input is"
grep_debug_cmd="grep -c '$captured_log' $FLINK_LOG_DIR/*taskexecutor*.log"
debug_line_cnt=$(eval "$grep_debug_cmd")
if [[ ${debug_line_cnt} -lt 0 ]]; then
  echo "Don't find any PythonFunction debug lines."
  EXIT_CODE=1
fi

echo "Test PyFlink DataStream job:"

DATA_STREAM_INPUT_DIR="${TEST_DATA_DIR}/timer-stream-input"
DATA_STREAM_OUTPUT_DIR="${TEST_DATA_DIR}/timer-stream-output"
mkdir -p "${DATA_STREAM_INPUT_DIR}" "${DATA_STREAM_OUTPUT_DIR}"

cat > "${DATA_STREAM_INPUT_DIR}/payment_msgs.jsonl" << 'EOF'
{"createTime": 1603679413000, "orderId": 1603679414, "payAmount": 83685.44904332698, "payPlatform": 0, "provinceId": 3}
{"createTime": 1603679426000, "orderId": 1603679427, "payAmount": 30092.50657757042, "payPlatform": 0, "provinceId": 1}
{"createTime": 1603679427000, "orderId": 1603679428, "payAmount": 62644.01719293056, "payPlatform": 0, "provinceId": 6}
{"createTime": 1603679428000, "orderId": 1603679429, "payAmount": 6449.806795118451, "payPlatform": 0, "provinceId": 2}
{"createTime": 1603679491000, "orderId": 1603679492, "payAmount": 41108.36128417494, "payPlatform": 0, "provinceId": 0}
{"createTime": 1603679492000, "orderId": 1603679493, "payAmount": 64882.44233197067, "payPlatform": 0, "provinceId": 4}
{"createTime": 1603679521000, "orderId": 1603679522, "payAmount": 81648.80712644062, "payPlatform": 0, "provinceId": 3}
{"createTime": 1603679522000, "orderId": 1603679523, "payAmount": 81861.73063103345, "payPlatform": 0, "provinceId": 4}
EOF

PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/datastream" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.uv/bin/python" \
    -pym "data_stream_job" \
    "${DATA_STREAM_INPUT_DIR}/payment_msgs.jsonl" "${DATA_STREAM_OUTPUT_DIR}"

sort > "${TEST_DATA_DIR}/expected_data_stream_result" << 'EOF'
Current key: 1603679414, orderId: 1603679414, payAmount: 83685.44904332698, timestamp: 1603679413000
Current key: 1603679427, orderId: 1603679427, payAmount: 30092.50657757042, timestamp: 1603679426000
Current key: 1603679428, orderId: 1603679428, payAmount: 62644.01719293056, timestamp: 1603679427000
Current key: 1603679429, orderId: 1603679429, payAmount: 6449.806795118451, timestamp: 1603679428000
Current key: 1603679492, orderId: 1603679492, payAmount: 41108.36128417494, timestamp: 1603679491000
Current key: 1603679493, orderId: 1603679493, payAmount: 64882.44233197067, timestamp: 1603679492000
Current key: 1603679522, orderId: 1603679522, payAmount: 81648.80712644062, timestamp: 1603679521000
Current key: 1603679523, orderId: 1603679523, payAmount: 81861.73063103345, timestamp: 1603679522000
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
On timer timestamp: -9223372036854774308
EOF

find "${DATA_STREAM_OUTPUT_DIR}" -type f -name "part-*" -exec cat {} + | sort \
    > "${TEST_DATA_DIR}/actual_data_stream_result"

if ! diff "${TEST_DATA_DIR}/expected_data_stream_result" "${TEST_DATA_DIR}/actual_data_stream_result"; then
    echo "Output from PyFlink DataStream job does not match expected output."
    exit 1
fi

# clean up python env
"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -r
