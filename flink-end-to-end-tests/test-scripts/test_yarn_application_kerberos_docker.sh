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

# shellcheck source=common.sh
source "$(dirname "$0")"/common.sh

# shellcheck source=common_yarn_docker.sh
source "$(dirname "$0")"/common_yarn_docker.sh

# Configure Flink dir before making tarball.
INPUT_TYPE=${1:-default-input}
case $INPUT_TYPE in
    (default-input)
        INPUT_ARGS=""
    ;;
    (dummy-fs)
        # shellcheck source=common_dummy_fs.sh
        source "$(dirname "$0")"/common_dummy_fs.sh
        dummy_fs_setup
        INPUT_ARGS="--input dummy://localhost/words --input anotherDummy://localhost/words"
    ;;
    (*)
        echo "Unknown input type ${INPUT_TYPE}"
        exit 1
    ;;
esac

start_hadoop_cluster_and_prepare_flink

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH=hdfs:///user/hadoop-user/wc-out-$RANDOM

# it's important to run this with higher parallelism, otherwise we might risk that
# JM and TM are on the same YARN node and that we therefore don't test the keytab shipping
if docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   /home/hadoop-user/${FLINK_DIRNAME}/bin/flink run-application \
   -t yarn-application \
   -Dtaskmanager.numberOfTaskSlots=1 \
   -Dtaskmanager.memory.process.size=1000m \
   -Djobmanager.memory.process.size=1000m \
   -Dparallelism.default=3 \
   -Dtaskmanager.memory.jvm-metaspace.size=128m \
   /home/hadoop-user/${FLINK_DIRNAME}/examples/streaming/WordCount.jar ${INPUT_ARGS} --output ${OUTPUT_PATH}";
then
    echo "Flink YARN Application submitted."
else
    echo "Running the job failed."
    exit 1
fi

wait_for_single_yarn_application

# now we should have the application output ready

OUTPUT=$(get_output "${OUTPUT_PATH}/*/*")
echo "==== OUTPUT_BEGIN ===="
echo "${OUTPUT}"
echo "==== OUTPUT_END ===="

YARN_APPLICATION_LOGS=$(get_yarn_application_logs)
if [[ ! "${YARN_APPLICATION_LOGS}" =~ "Receive initial delegation tokens from resource manager" ]]; then
    echo "YARN logs does not contain delegation token usage message as required"
    exit 1
fi

echo "Running Job without configured keytab, the exception you see below is expected"
docker exec master bash -c "echo \"\" > /home/hadoop-user/${FLINK_DIRNAME}/conf/flink-conf.yaml"
# verify that it doesn't work if we don't configure a keytab
docker exec master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
    /home/hadoop-user/${FLINK_DIRNAME}/bin/flink run-application \
    -t yarn-application \
    -Dtaskmanager.numberOfTaskSlots=1 \
    -Dtaskmanager.memory.process.size=1000m \
    -Djobmanager.memory.process.size=1000m \
    -Dparallelism.default=3 \
    -Dtaskmanager.memory.jvm-metaspace.size=128m \
    /home/hadoop-user/${FLINK_DIRNAME}/examples/streaming/WordCount.jar --output ${OUTPUT_PATH}" > stderrAndstdoutFile 2>&1
STD_ERR_AND_STD_OUT=$(cat stderrAndstdoutFile)
rm stderrAndstdoutFile
echo "==== STD_ERR_AND_STD_OUT_BEGIN ===="
echo "${STD_ERR_AND_STD_OUT}"
echo "==== STD_ERR_AND_STD_OUT_END ===="

if [[ ! "${STD_ERR_AND_STD_OUT}" =~ "Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials" ]]; then
    echo "Output does not contain the Kerberos error message as required"
    exit 1
fi
