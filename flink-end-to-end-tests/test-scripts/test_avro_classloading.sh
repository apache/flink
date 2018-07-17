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

source "$(dirname "$0")"/common.sh

start_cluster


TEST_PROGRAM_JAR=${TEST_INFRA_DIR}/../../flink-end-to-end-tests/flink-avro-classloading-test/target/AvroClassLoading.jar

JOB_ID=$(${FLINK_DIR}/bin/flink run -d ${TEST_PROGRAM_JAR} | awk '{print $NF}' | tail -n 1)
echo "Running Job ${JOB_ID} from jar ${TEST_PROGRAM_JAR}"

expect_in_taskmanager_logs "switched from DEPLOYING to RUNNING." 20

touch /tmp/die
expect_in_taskmanager_logs "java.lang.RuntimeException: /tmp/die exists!" 20

rm /tmp/die

# Have it run for a while after recovering
sleep 15

fail_if_logs_contain "java.lang.ClassCastException: org.apache.flink.tests.streaming.User cannot be cast to org.apache.flink.tests.streaming.User"

rm ${FLINK_DIR}/log/*.out
stop_cluster

