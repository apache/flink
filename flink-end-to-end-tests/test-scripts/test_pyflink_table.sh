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

cp -r "${FLINK_DIR}/conf" "${TEST_DATA_DIR}/conf"

echo "taskmanager.memory.task.off-heap.size: 768m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
echo "taskmanager.memory.process.size: 3172m" >> "${TEST_DATA_DIR}/conf/flink-conf.yaml"
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

conda install -y -q zip=3.0

rm -rf .conda/pkgs

zip -q -r "${TEST_DATA_DIR}/venv.zip" .conda

deactivate

cd "${CURRENT_DIR}"

start_cluster
on_exit stop_cluster

FLINK_PYTHON_TEST_DIR=`cd "${CURRENT_DIR}/../flink-python-test" && pwd -P`
REQUIREMENTS_PATH="${TEST_DATA_DIR}/requirements.txt"

echo "scipy==1.4.1" > "${REQUIREMENTS_PATH}"

echo "Test submitting python job with 'pipeline.jars':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.jars "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test submitting python job with 'pipeline.classpaths':\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" \
    pipeline.classpaths "file://${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test blink stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test blink batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.BlinkBatchPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test flink stream python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -c org.apache.flink.python.tests.FlinkStreamPythonUdfSqlJob \
    "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar"

echo "Test flink batch python udf sql job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
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
