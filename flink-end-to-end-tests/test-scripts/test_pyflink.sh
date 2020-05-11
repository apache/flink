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

echo "Test submitting python job:\n"
PYFLINK_CLIENT_EXECUTABLE=${PYTHON_EXEC} "${FLINK_DIR}/bin/flink" run \
    -pyfs "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" \
    -pyreq "${REQUIREMENTS_PATH}" \
    -pyarch "${TEST_DATA_DIR}/venv.zip" \
    -pyexec "venv.zip/.conda/bin/python" \
    -py "${FLINK_PYTHON_TEST_DIR}/python/python_job.py"

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
