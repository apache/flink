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

KAFKA_VERSION="2.2.0"
CONFLUENT_VERSION="5.0.0"
CONFLUENT_MAJOR_VERSION="5.0"
KAFKA_SQL_VERSION="universal"
SQL_JARS_DIR=$END_TO_END_DIR/flink-sql-client-test/target/sql-jars
KAFKA_SQL_JAR=$(find "$SQL_JARS_DIR" | grep "kafka_" )

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

function test_clean_up {
    stop_cluster
    stop_kafka_cluster
}

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/kafka_sql_common.sh \
  $KAFKA_VERSION \
  $CONFLUENT_VERSION \
  $CONFLUENT_MAJOR_VERSION \
  $KAFKA_SQL_VERSION


echo "Preparing Flink..."

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
source "${CURRENT_DIR}"/common.sh

cp -r "${FLINK_DIR}/conf" "${TEST_DATA_DIR}/conf"

echo "taskmanager.memory.task.off-heap.size: 768m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
echo "taskmanager.memory.process.size: 3172m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
echo "taskmanager.numberOfTaskSlots: 5" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"

export FLINK_CONF_DIR="${TEST_DATA_DIR}/conf"

FLINK_PYTHON_DIR=`cd "${CURRENT_DIR}/../../flink-python" && pwd -P`

CONDA_HOME="${FLINK_PYTHON_DIR}/dev/.conda"

"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -s basic

PYTHON_EXEC="${CONDA_HOME}/bin/python"

source "${CONDA_HOME}/bin/activate"

cd "${FLINK_PYTHON_DIR}"

rm -rf dist

python setup.py sdist

pip install dist/*

cd dev

conda install -y -q zip=3.0

rm -rf .conda/pkgs

zip -q -r "${TEST_DATA_DIR}/venv.zip" .conda

deactivate

cd "${CURRENT_DIR}"

start_cluster

on_exit test_clean_up

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist

start_kafka_cluster

create_data_stream_kafka_source

create_kafka_topic 1 1 test-python-data-stream-sink

FLINK_PYTHON_TEST_DIR=`cd "${CURRENT_DIR}/../flink-python-test" && pwd -P`
REQUIREMENTS_PATH="${TEST_DATA_DIR}/requirements.txt"

# Never used but added to test the '-pyreq' option.
echo "scipy==1.4.1" > "${REQUIREMENTS_PATH}"

echo "Test PyFlink DataStream job:"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC}

JOB_ID=$(${FLINK_DIR}/bin/flink run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/datastream" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -pym "data_stream_job" \
    -j "${KAFKA_SQL_JAR}")

echo "${JOB_ID}"
JOB_ID=`echo "${JOB_ID}" | sed 's/.* //g'`

echo "Reading kafka messages..."
READ_MSG=$(read_messages_from_kafka 20 test-python-data-stream-sink pyflink-e2e-test)

# We use env.execute_async() to submit the job, cancel it after fetched results.
cancel_job "${JOB_ID}"

EXPECTED_MSG='{"f0":"a","f1":0,"f2":1}
{"f0":"a","f1":1,"f2":1}
{"f0":"ab","f1":0,"f2":2}
{"f0":"ab","f1":1,"f2":2}
{"f0":"ab","f1":2,"f2":2}
{"f0":"abc","f1":0,"f2":3}
{"f0":"abc","f1":1,"f2":3}
{"f0":"abc","f1":2,"f2":3}
{"f0":"abc","f1":3,"f2":3}
{"f0":"abcde","f1":0,"f2":5}
{"f0":"abcde","f1":1,"f2":5}
{"f0":"abcde","f1":2,"f2":5}
{"f0":"abcde","f1":3,"f2":5}
{"f0":"abcde","f1":4,"f2":5}
{"f0":"abcde","f1":5,"f2":5}
{"f0":"abcd","f1":0,"f2":4}
{"f0":"abcd","f1":1,"f2":4}
{"f0":"abcd","f1":2,"f2":4}
{"f0":"abcd","f1":3,"f2":4}
{"f0":"abcd","f1":4,"f2":4}'

EXPECTED_MSG=$(sort_msg "${EXPECTED_MSG[*]}")
SORTED_READ_MSG=$(sort_msg "${READ_MSG[*]}")

if [[ "${EXPECTED_MSG[*]}" != "${SORTED_READ_MSG[*]}" ]]; then
    echo "Output from Flink program does not match expected output."
    echo -e "EXPECTED Output: --${EXPECTED_MSG[*]}--"
    echo -e "ACTUAL: --${SORTED_READ_MSG[*]}--"
    exit 1
fi
