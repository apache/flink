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
SQL_JARS_DIR=${END_TO_END_DIR}/flink-sql-client-test/target/sql-jars
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

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
source "${CURRENT_DIR}"/common.sh
source "${CURRENT_DIR}"/kafka_sql_common.sh \
  ${KAFKA_VERSION} \
  ${CONFLUENT_VERSION} \
  ${CONFLUENT_MAJOR_VERSION} \
  ${KAFKA_SQL_VERSION}

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

python setup.py sdist

pip install dist/*

cd dev

rm -rf .conda/pkgs

zip -q -r "${TEST_DATA_DIR}/venv.zip" .conda

deactivate

cd "${CURRENT_DIR}"

start_cluster
on_exit test_clean_up

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

echo "Test blink stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test blink batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.BlinkBatchPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test flink stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.FlinkStreamPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test flink batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.FlinkBatchPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test using python udf in sql client:\n"
SQL_CONF=$TEST_DATA_DIR/sql-client-session.conf

cat >> $SQL_CONF << EOF
tables:
- name: sink
  type: sink-table
  update-mode: append
  schema:
  - name: a
    type: BIGINT
  connector:
    type: filesystem
    path: "$TEST_DATA_DIR/sql-client-test.csv"
  format:
    type: csv
    fields:
    - name: a
      type: BIGINT

functions:
- name: add_one
  from: python
  fully-qualified-name: add_one.add_one

configuration:
  python.client.executable: "$PYTHON_EXEC"
EOF

SQL_STATEMENT="insert into sink select add_one(a) from (VALUES (1), (2), (3)) as source (a)"

JOB_ID=$($FLINK_DIR/bin/sql-client.sh embedded \
  --environment $SQL_CONF \
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

PAYMENT_MSGS='{"createTime": "2020-10-26 10:30:13", "orderId": 1603679414, "payAmount": 83685.44904332698, "payPlatform": 0, "provinceId": 3}
{"createTime": "2020-10-26 10:30:26", "orderId": 1603679427, "payAmount": 30092.50657757042, "payPlatform": 0, "provinceId": 1}
{"createTime": "2020-10-26 10:30:27", "orderId": 1603679428, "payAmount": 62644.01719293056, "payPlatform": 0, "provinceId": 6}
{"createTime": "2020-10-26 10:30:28", "orderId": 1603679429, "payAmount": 6449.806795118451, "payPlatform": 0, "provinceId": 2}
{"createTime": "2020-10-26 10:31:31", "orderId": 1603679492, "payAmount": 41108.36128417494, "payPlatform": 0, "provinceId": 0}
{"createTime": "2020-10-26 10:31:32", "orderId": 1603679493, "payAmount": 64882.44233197067, "payPlatform": 0, "provinceId": 4}
{"createTime": "2020-10-26 10:32:01", "orderId": 1603679522, "payAmount": 81648.80712644062, "payPlatform": 0, "provinceId": 3}
{"createTime": "2020-10-26 10:32:02", "orderId": 1603679523, "payAmount": 81861.73063103345, "payPlatform": 0, "provinceId": 4}'

function send_msg_to_kafka {

    while read line
    do
	 	send_messages_to_kafka "$line" "timer-stream-source"
        sleep 3
    done <<< "$1"
}

JOB_ID=$(${FLINK_DIR}/bin/flink run \
    -p 2 \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/datastream" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -pym "data_stream_timer_job" \
    -j "${KAFKA_SQL_JAR}")

echo "${JOB_ID}"
JOB_ID=`echo "${JOB_ID}" | sed 's/.* //g'`

wait_job_running ${JOB_ID}

# wait 10s to ensure all tasks are up.
sleep 10

send_msg_to_kafka "${PAYMENT_MSGS[*]}"

echo "Reading kafka messages..."
READ_MSG=$(read_messages_from_kafka 15 timer-stream-sink pyflink-e2e-test)

# We use env.execute_async() to submit the job, cancel it after fetched results.
cancel_job "${JOB_ID}"

EXPECTED_MSG='Current orderId: 1603679414 payAmount: 83685.44904332698
On timer Current timestamp: -9223372036854774308, watermark: 1603708211000
Current orderId: 1603679427 payAmount: 30092.50657757042
On timer Current timestamp: 1603708212500, watermark: 1603708224000
Current orderId: 1603679428 payAmount: 62644.01719293056
Current orderId: 1603679429 payAmount: 6449.806795118451
On timer Current timestamp: 1603708225500, watermark: 1603708226000
Current orderId: 1603679492 payAmount: 41108.36128417494
On timer Current timestamp: 1603708226500, watermark: 1603708289000
On timer Current timestamp: 1603708227500, watermark: 1603708289000
Current orderId: 1603679493 payAmount: 64882.44233197067
Current orderId: 1603679522 payAmount: 81648.80712644062
On timer Current timestamp: 1603708290500, watermark: 1603708319000
On timer Current timestamp: 1603708291500, watermark: 1603708319000
Current orderId: 1603679523 payAmount: 81861.73063103345'

if [[ "${EXPECTED_MSG[*]}" != "${READ_MSG[*]}" ]]; then
    echo "Output from Flink program does not match expected output."
    echo -e "EXPECTED Output: --${EXPECTED_MSG[*]}--"
    echo -e "ACTUAL: --${READ_MSG[*]}--"
    exit 1
fi

stop_cluster

# These tests are known to fail on JDK11. See FLINK-13719
if [[ ${PROFILE} != *"jdk11"* ]]; then
    cd "${CURRENT_DIR}/../"
    source "${CURRENT_DIR}"/common_yarn_docker.sh
    # test submitting on yarn
    start_hadoop_cluster_and_prepare_flink

    # copy test files
    docker cp "${FLINK_PYTHON_DIR}/dev/lint-python.sh" master:/tmp/
    docker cp "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar" master:/tmp/
    docker cp "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" master:/tmp/
    docker cp "${REQUIREMENTS_PATH}" master:/tmp/
    docker cp "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" master:/tmp/
    PYFLINK_PACKAGE_FILE=$(basename "${FLINK_PYTHON_DIR}"/dist/apache-flink-*.tar.gz)
    docker cp "${FLINK_PYTHON_DIR}/dist/${PYFLINK_PACKAGE_FILE}" master:/tmp/

    # prepare environment
    docker exec master bash -c "
    /tmp/lint-python.sh -s miniconda
    source /tmp/.conda/bin/activate
    pip install /tmp/${PYFLINK_PACKAGE_FILE}
    conda install -y -q zip=3.0
    rm -rf /tmp/.conda/pkgs
    cd /tmp
    zip -q -r /tmp/venv.zip .conda
    echo \"taskmanager.memory.task.off-heap.size: 100m\" >> \"/home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml\"
    "

    docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
        export PYFLINK_CLIENT_EXECUTABLE=/tmp/.conda/bin/python && \
        /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ytm 1500 -yjm 1000 \
        -pyfs /tmp/add_one.py \
        -pyreq /tmp/requirements.txt \
        -pyarch /tmp/venv.zip \
        -pyexec venv.zip/.conda/bin/python \
        /tmp/PythonUdfSqlJobExample.jar"

    docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
        export PYFLINK_CLIENT_EXECUTABLE=/tmp/.conda/bin/python && \
        /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ytm 1500 -yjm 1000 \
        -pyfs /tmp/add_one.py \
        -pyreq /tmp/requirements.txt \
        -pyarch /tmp/venv.zip \
        -pyexec venv.zip/.conda/bin/python \
        -py /tmp/python_job.py \
        pipeline.jars file:/tmp/PythonUdfSqlJobExample.jar"
fi
