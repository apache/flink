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

set -o pipefail

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/common_mesos_docker.sh

RESULT_HASH="72a690412be8928ba239c2da967328a5"
INPUT_ARGS="--input ${TEST_INFRA_DIR}/test-data/words"
OUTPUT_LOCATION="${TEST_DATA_DIR}/out/wc_out_mesos"
TEST_PROGRAM_JAR=${FLINK_DIR}/examples/batch/WordCount.jar

mkdir -p "${TEST_DATA_DIR}"

set_hadoop_classpath

start_flink_cluster_with_mesos

docker exec --env HADOOP_CLASSPATH=$HADOOP_CLASSPATH mesos-master nohup bash -c "${FLINK_DIR}/bin/flink run -p 1 ${TEST_PROGRAM_JAR} ${INPUT_ARGS} --output ${OUTPUT_LOCATION}"

check_result_hash "Mesos WordCount test" "${OUTPUT_LOCATION}" "${RESULT_HASH}"
