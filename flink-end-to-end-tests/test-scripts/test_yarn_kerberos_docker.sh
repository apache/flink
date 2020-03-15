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
source "$(dirname "$0")"/common_yarn_docker.sh

# Configure Flink dir before making tarball.
INPUT_TYPE=${1:-default-input}
EXPECTED_RESULT_LOG_CONTAINS=()
case $INPUT_TYPE in
    (default-input)
        INPUT_ARGS=""
        EXPECTED_RESULT_LOG_CONTAINS=("consummation,1" "of,14" "calamity,1")
    ;;
    (dummy-fs)
        source "$(dirname "$0")"/common_dummy_fs.sh
        dummy_fs_setup
        INPUT_ARGS="--input dummy://localhost/words --input anotherDummy://localhost/words"
        EXPECTED_RESULT_LOG_CONTAINS=("my,2" "dear,4" "world,4")
    ;;
    (*)
        echo "Unknown input type $INPUT_TYPE"
        exit 1
    ;;
esac

start_hadoop_cluster_and_prepare_flink

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH=hdfs:///user/hadoop-user/wc-out-$RANDOM

# it's important to run this with higher parallelism, otherwise we might risk that
# JM and TM are on the same YARN node and that we therefore don't test the keytab shipping
if docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ys 1 -ytm 1000 -yjm 1000 -p 3 \
   -yD taskmanager.memory.jvm-metaspace.size=128m \
   /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar $INPUT_ARGS --output $OUTPUT_PATH";
then
    OUTPUT=$(get_output "$OUTPUT_PATH/*")
    echo "$OUTPUT"
else
    echo "Running the job failed."
    copy_and_show_logs
    exit 1
fi

for expected_result in ${EXPECTED_RESULT_LOG_CONTAINS[@]}; do
    if [[ ! "$OUTPUT" =~ $expected_result ]]; then
        echo "Output does not contain '$expected_result' as required"
        copy_and_show_logs
        exit 1
    fi
done

echo "Running Job without configured keytab, the exception you see below is expected"
docker exec -it master bash -c "echo \"\" > /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"
# verify that it doesn't work if we don't configure a keytab
OUTPUT=$(docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
    /home/hadoop-user/$FLINK_DIRNAME/bin/flink run \
    -m yarn-cluster -ys 1 -ytm 1000 -yjm 1000 -p 3 \
    -yD taskmanager.memory.jvm-metaspace.size=128m \
    /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar --output $OUTPUT_PATH")
echo "$OUTPUT"

if [[ ! "$OUTPUT" =~ "Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials" ]]; then
    echo "Output does not contain the Kerberos error message as required"
    exit 1
fi
