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

KAFKA_VERSION="2.8.1"
CONFLUENT_VERSION="6.2.2"
CONFLUENT_MAJOR_VERSION="6.2"
# Check the Confluent Platform <> Apache Kafka compatibility matrix when updating KAFKA_VERSION
KAFKA_SQL_VERSION="universal"
SQL_JARS_DIR=${END_TO_END_DIR}/flink-sql-client-test/target/sql-jars
KAFKA_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "kafka" )

function create_data_stream_kafka_source {
    topicName="test-python-data-stream-source"
    create_kafka_topic 1 1 $topicName

    echo "Sending messages to Kafka..."

    send_messages_to_kafka '{"f0": "a", "f1": 1}' $topicName
    send_messages_to_kafka '{"f0": "ab", "f1": 2}' $topicName
    send_messages_to_kafka '{"f0": "abc", "f1": 3}' $topicName
    send_messages_to_kafka '{"f0": "abcd", "f1": 4}' $topicName
    send_messages_to_kafka '{"f0": "abcde", "f1": 5}' $topicName
}

function sort_msg {
    arr=()
    while read line
    do
        value=$line
        arr+=("$value")
    done <<< "$1"
    IFS=$'\n' sorted=($(sort <<< "${arr[*]}")); unset IFS
    echo "${sorted[*]}"
}

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
source "${CURRENT_DIR}"/common.sh
source "${CURRENT_DIR}"/kafka_sql_common.sh \
  ${KAFKA_VERSION} \
  ${CONFLUENT_VERSION} \
  ${CONFLUENT_MAJOR_VERSION} \
  ${KAFKA_SQL_VERSION}

function test_clean_up {
    stop_cluster
    stop_kafka_cluster
}
on_exit test_clean_up

cp -r "${FLINK_DIR}/conf" "${TEST_DATA_DIR}/conf"

echo "taskmanager.memory.task.off-heap.size: 768m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
echo "taskmanager.memory.process.size: 3172m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
echo "taskmanager.numberOfTaskSlots: 5" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
export FLINK_CONF_DIR="${TEST_DATA_DIR}/conf"

FLINK_PYTHON_DIR=`cd "${CURRENT_DIR}/../../flink-python" && pwd -P`

CONDA_HOME="${FLINK_PYTHON_DIR}/dev/.conda"

"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -s miniconda

PYTHON_EXEC="${CONDA_HOME}/bin/python"

source "${CONDA_HOME}/bin/activate"

cd "${FLINK_PYTHON_DIR}"

rm -rf dist

pip install -r dev/dev-requirements.txt

python setup.py sdist

pushd apache-flink-libraries

python setup.py sdist

pip install dist/*

popd

pip install dist/*

cd dev

rm -rf .conda/pkgs

zip -q -r "${TEST_DATA_DIR}/venv.zip" .conda

deactivate

cd "${CURRENT_DIR}"

start_cluster

echo "Test PyFlink Table job:"

FLINK_PYTHON_TEST_DIR=`cd "${CURRENT_DIR}/../flink-python-test" && pwd -P`
REQUIREMENTS_PATH="${TEST_DATA_DIR}/requirements.txt"

echo "pytest==4.4.1" > "${REQUIREMENTS_PATH}"

echo "Test submitting python job with 'pipeline.jars':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.jars "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test submitting python job with 'pipeline.classpaths':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.classpaths "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.BatchPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test using python udf in sql client:\n"
INIT_SQL=$TEST_DATA_DIR/sql-client-init.sql

cat >> $INIT_SQL << EOF
CREATE TABLE sink (
  a BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '$TEST_DATA_DIR/sql-client-test',
  'format' = 'csv'
);

CREATE FUNCTION add_one AS 'add_one.add_one' LANGUAGE PYTHON;

SET 'python.client.executable'='$PYTHON_EXEC';
EOF

SQL_STATEMENT="insert into sink select add_one(a) from (VALUES (1), (2), (3)) as source (a)"

JOB_ID=$($FLINK_DIR/bin/sql-client.sh \
  --init $INIT_SQL \
  -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
  -pyreq "${REQUIREMENTS_PATH}" \
  -pyarch "${TEST_DATA_DIR}/venv.zip" \
  -pyexec "venv.zip/.conda/bin/python" \
  --update "$SQL_STATEMENT" | grep "Job ID:" | sed 's/.* //g')

wait_job_terminal_state "$JOB_ID" "FINISHED"

echo "Test PyFlink DataStream job:"

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist

start_kafka_cluster

# End to end test for DataStream ProcessFunction with timer
create_kafka_topic 1 1 timer-stream-source
create_kafka_topic 1 1 timer-stream-sink

PAYMENT_MSGS='{"createTime": 1603679413000, "orderId": 1603679414, "payAmount": 83685.44904332698, "payPlatform": 0, "provinceId": 3}
{"createTime": 1603679426000, "orderId": 1603679427, "payAmount": 30092.50657757042, "payPlatform": 0, "provinceId": 1}
{"createTime": 1603679427000, "orderId": 1603679428, "payAmount": 62644.01719293056, "payPlatform": 0, "provinceId": 6}
{"createTime": 1603679428000, "orderId": 1603679429, "payAmount": 6449.806795118451, "payPlatform": 0, "provinceId": 2}
{"createTime": 1603679491000, "orderId": 1603679492, "payAmount": 41108.36128417494, "payPlatform": 0, "provinceId": 0}
{"createTime": 1603679492000, "orderId": 1603679493, "payAmount": 64882.44233197067, "payPlatform": 0, "provinceId": 4}
{"createTime": 1603679521000, "orderId": 1603679522, "payAmount": 81648.80712644062, "payPlatform": 0, "provinceId": 3}
{"createTime": 1603679522000, "orderId": 1603679523, "payAmount": 81861.73063103345, "payPlatform": 0, "provinceId": 4}'

function send_msg_to_kafka {
    while read line
    do
	    send_messages_to_kafka "$line" "timer-stream-source"
        sleep 1
    done <<< "$1"
}

function read_msg_from_kafka {
    $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --from-beginning \
    --max-messages $1 \
    --topic $2 \
    --consumer-property group.id=$3 --timeout-ms 90000 2> /dev/null
}

send_msg_to_kafka "${PAYMENT_MSGS[*]}"

JOB_ID=$(${FLINK_DIR}/bin/flink run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/datastream" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -pym "data_stream_job" \
    -j "${KAFKA_SQL_JAR}")

echo "${JOB_ID}"
JOB_ID=`echo "${JOB_ID}" | sed 's/.* //g'`

wait_job_running ${JOB_ID}

echo "Reading kafka messages..."
READ_MSG=$(read_msg_from_kafka 16 timer-stream-sink pyflink-e2e-test-timer)

# We use env.execute_async() to submit the job, cancel it after fetched results.
cancel_job "${JOB_ID}"

EXPECTED_MSG='Current key: 1603679414, orderId: 1603679414, payAmount: 83685.44904332698, timestamp: 1603679413000
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
On timer timestamp: -9223372036854774308'

EXPECTED_MSG=$(sort_msg "${EXPECTED_MSG[*]}")
SORTED_READ_MSG=$(sort_msg "${READ_MSG[*]}")

if [[ "${EXPECTED_MSG[*]}" != "${SORTED_READ_MSG[*]}" ]]; then
    echo "Output from Flink program does not match expected output."
    echo -e "EXPECTED Output: --${EXPECTED_MSG[*]}--"
    echo -e "ACTUAL: --${SORTED_READ_MSG[*]}--"
    exit 1
fi

# clean up python env
"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -r
