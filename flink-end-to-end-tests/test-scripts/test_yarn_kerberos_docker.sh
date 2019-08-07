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

FLINK_TARBALL_DIR=$TEST_DATA_DIR
FLINK_TARBALL=flink.tar.gz
FLINK_DIRNAME=$(basename $FLINK_DIR)

MAX_RETRY_SECONDS=120
CLUSTER_SETUP_RETRIES=3

echo "Flink Tarball directory $FLINK_TARBALL_DIR"
echo "Flink tarball filename $FLINK_TARBALL"
echo "Flink distribution directory name $FLINK_DIRNAME"
echo "End-to-end directory $END_TO_END_DIR"
docker --version
docker-compose --version

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
        INPUT_ARGS="--input dummy://localhost/words"
        EXPECTED_RESULT_LOG_CONTAINS=("my,1" "dear,2" "world,2")
    ;;
    (*)
        echo "Unknown input type $INPUT_TYPE"
        exit 1
    ;;
esac

# make sure we stop our cluster at the end
function cluster_shutdown {
  docker-compose -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml down
  rm $FLINK_TARBALL_DIR/$FLINK_TARBALL
}
on_exit cluster_shutdown

function start_hadoop_cluster() {
    echo "Starting Hadoop cluster"
    docker-compose -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml up -d

    # wait for kerberos to be set up
    start_time=$(date +%s)
    until docker logs master 2>&1 | grep -q "Finished master initialization"; do
        current_time=$(date +%s)
        time_diff=$((current_time - start_time))

        if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
            return 1
        else
            echo "Waiting for hadoop cluster to come up. We have been trying for $time_diff seconds, retrying ..."
            sleep 5
        fi
    done

    # perform health checks
    if ! { [ $(docker inspect -f '{{.State.Running}}' master 2>&1) = 'true' ] &&
           [ $(docker inspect -f '{{.State.Running}}' slave1 2>&1) = 'true' ] &&
           [ $(docker inspect -f '{{.State.Running}}' slave2 2>&1) = 'true' ] &&
           [ $(docker inspect -f '{{.State.Running}}' kdc 2>&1) = 'true' ]; };
    then
        return 1
    fi

    # try and see if NodeManagers are up, otherwise the Flink job will not have enough resources
    # to run
    nm_running="0"
    start_time=$(date +%s)
    while [ "$nm_running" -lt "2" ]; do
        current_time=$(date +%s)
        time_diff=$((current_time - start_time))

        if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
            return 1
        else
            echo "We only have $nm_running NodeManagers up. We have been trying for $time_diff seconds, retrying ..."
            sleep 1
        fi

        docker exec -it master bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
        nm_running=`docker exec -it master bash -c "yarn node -list" | grep RUNNING | wc -l`
        docker exec -it master bash -c "kdestroy"
    done

    return 0
}
echo "Building Hadoop Docker container"
until docker build --build-arg HADOOP_VERSION=2.8.4 \
    -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/Dockerfile \
    -t flink/docker-hadoop-secure-cluster:latest \
    $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/;
do
    # with all the downloading and ubuntu updating a lot of flakiness can happen, make sure
    # we don't immediately fail
    echo "Something went wrong while building the Docker image, retrying ..."
    sleep 2
done

if ! retry_times $CLUSTER_SETUP_RETRIES 0 start_hadoop_cluster; then
    echo "ERROR: Could not start hadoop cluster. Aborting..."
    exit 1
fi

mkdir -p $FLINK_TARBALL_DIR
tar czf $FLINK_TARBALL_DIR/$FLINK_TARBALL -C $(dirname $FLINK_DIR) .

docker cp $FLINK_TARBALL_DIR/$FLINK_TARBALL master:/home/hadoop-user/

# now, at least the container is ready
docker exec -it master bash -c "tar xzf /home/hadoop-user/$FLINK_TARBALL --directory /home/hadoop-user/"

# minimal Flink config, bebe
FLINK_CONFIG=$(cat << END
security.kerberos.login.keytab: /home/hadoop-user/hadoop-user.keytab
security.kerberos.login.principal: hadoop-user
slot.request.timeout: 120000
containerized.heap-cutoff-min: 100
END
)
docker exec -it master bash -c "echo \"$FLINK_CONFIG\" > /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

echo "Flink config:"
docker exec -it master bash -c "cat /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH=hdfs:///user/hadoop-user/wc-out-$RANDOM

function copy_and_show_logs {
    mkdir -p $TEST_DATA_DIR/logs
    echo "Hadoop logs:"
    docker cp master:/var/log/hadoop/* $TEST_DATA_DIR/logs/
    for f in $TEST_DATA_DIR/logs/*; do
        echo "$f:"
        cat $f
    done
    echo "Docker logs:"
    docker logs master

    echo "Flink logs:"
    docker exec -it master bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
    application_id=`docker exec -it master bash -c "yarn application -list -appStates ALL" | grep "Flink session cluster" | awk '{print \$1}'`
    echo "Application ID: $application_id"
    docker exec -it master bash -c "yarn logs -applicationId $application_id"
    docker exec -it master bash -c "kdestroy"
}

start_time=$(date +%s)
# it's important to run this with higher parallelism, otherwise we might risk that
# JM and TM are on the same YARN node and that we therefore don't test the keytab shipping
if docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -yn 3 -ys 1 -ytm 1000 -yjm 1000 \
   -p 3 /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar $INPUT_ARGS --output $OUTPUT_PATH";
then
    docker exec -it master bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
    docker exec -it master bash -c "hdfs dfs -ls $OUTPUT_PATH"
    OUTPUT=$(docker exec -it master bash -c "hdfs dfs -cat $OUTPUT_PATH/*")
    docker exec -it master bash -c "kdestroy"
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
    -m yarn-cluster -yn 3 -ys 1 -ytm 1000 -yjm 1000 -p 3 \
    /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar --output $OUTPUT_PATH")
echo "$OUTPUT"

if [[ ! "$OUTPUT" =~ "Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials" ]]; then
    echo "Output does not contain the Kerberos error message as required"
    exit 1
fi
