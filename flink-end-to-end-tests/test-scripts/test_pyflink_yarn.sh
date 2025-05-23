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
FLINK_PYTHON_DIR=`cd "${CURRENT_DIR}/../../flink-python" && pwd -P`
FLINK_PYTHON_TEST_DIR=`cd "${CURRENT_DIR}/../flink-python-test" && pwd -P`
REQUIREMENTS_PATH="${TEST_DATA_DIR}/requirements.txt"

echo "pytest==8.3.5" > "${REQUIREMENTS_PATH}"

# These tests are known to fail on JDK11. See FLINK-13719
cd "${CURRENT_DIR}/../"
source "${CURRENT_DIR}"/common_yarn_docker.sh
# test submitting on yarn
start_hadoop_cluster_and_prepare_flink

# copy test files
docker cp "${FLINK_PYTHON_DIR}/dev/lint-python.sh" master:/tmp/
docker cp "${FLINK_PYTHON_DIR}/dev/dev-requirements.txt" master:/tmp/
docker cp "${FLINK_PYTHON_TEST_DIR}/target/PythonUdfSqlJobExample.jar" master:/tmp/
docker cp "${FLINK_PYTHON_TEST_DIR}/python/add_one.py" master:/tmp/
docker cp "${REQUIREMENTS_PATH}" master:/tmp/
docker cp "${FLINK_PYTHON_TEST_DIR}/python/python_job.py" master:/tmp/
PYFLINK_PACKAGE_FILE=$(basename "${FLINK_PYTHON_DIR}"/dist/apache_flink-*.tar.gz)
PYFLINK_LIBRARIES_PACKAGE_FILE=$(basename "${FLINK_PYTHON_DIR}"/apache-flink-libraries/dist/apache_flink_libraries-*.tar.gz)
docker cp "${FLINK_PYTHON_DIR}/dist/${PYFLINK_PACKAGE_FILE}" master:/tmp/
docker cp "${FLINK_PYTHON_DIR}/apache-flink-libraries/dist/${PYFLINK_LIBRARIES_PACKAGE_FILE}" master:/tmp/

# prepare environment
docker exec master bash -c "
/tmp/lint-python.sh -s uv
source /tmp/.uv/bin/activate
pip install -r /tmp/dev-requirements.txt
pip install /tmp/${PYFLINK_LIBRARIES_PACKAGE_FILE}
pip install /tmp/${PYFLINK_PACKAGE_FILE}
apt-get install -y zip
cd /tmp
zip -q -r /tmp/venv.zip .uv
"

docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
    export PYFLINK_CLIENT_EXECUTABLE=/tmp/.uv/bin/python && \
    /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -t yarn-application \
    -Djobmanager.memory.process.size=1500m \
    -Dtaskmanager.memory.process.size=1000m \
    -pyfs /tmp/add_one.py \
    -pyreq /tmp/requirements.txt \
    -pyarch /tmp/venv.zip \
    -pyexec venv.zip/.uv/bin/python \
    /tmp/PythonUdfSqlJobExample.jar"

docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
    export PYFLINK_CLIENT_EXECUTABLE=/tmp/.uv/bin/python && \
    /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -t yarn-application \
    -Djobmanager.memory.process.size=1500m \
    -Dtaskmanager.memory.process.size=1000m \
    -pyfs /tmp/add_one.py \
    -pyreq /tmp/requirements.txt \
    -pyarch /tmp/venv.zip \
    -pyexec venv.zip/.uv/bin/python \
    -py /tmp/python_job.py \
    pipeline.jars file:/tmp/PythonUdfSqlJobExample.jar"

# clean up python env
"${FLINK_PYTHON_DIR}/dev/lint-python.sh" -r

# clean up apache-flink-libraries
rm -rf "${FLINK_PYTHON_DIR}/apache-flink-libraries/dist/${PYFLINK_LIBRARIES_PACKAGE_FILE}"
