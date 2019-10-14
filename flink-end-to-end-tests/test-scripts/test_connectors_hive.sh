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
EXPECTED_RESULT_LOG_CONTAINS=("1	1	a	1000	1.11" \
    "2	2	a	2000	2.22" \
    "3	3	a	3000	3.33" \
    "4	4	a	4000	4.44")

# make sure we stop our cluster at the end
function cluster_shutdown {
  docker-compose -f $END_TO_END_DIR/test-scripts/docker-hive-hadoop-cluster/docker-compose.yml down
  rm $FLINK_TARBALL_DIR/$FLINK_TARBALL
}
on_exit cluster_shutdown

function start_hadoop_cluster_and_hive() {
    echo "Starting Hadoop cluster"
    docker-compose -f $END_TO_END_DIR/test-scripts/docker-hive-hadoop-cluster/docker-compose.yml up -d

    # wait for hadoop cluster to be set up
    start_time=$(date +%s)
    until docker logs hive 2>&1 | grep -q "The hadoop cluster and hive service have been ready.."; do
        current_time=$(date +%s)
        time_diff=$((current_time - start_time))

        if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
            return 1
        else
            echo "Waiting for hadoop cluster and hive service to come up. We have been trying for $time_diff seconds, retrying ..."
            sleep 5
        fi
    done

    # perform health checks
    if ! { [[ $(docker inspect -f '{{.State.Running}}' master 2>&1) = 'true' ]] &&
           [[ $(docker inspect -f '{{.State.Running}}' slave1 2>&1) = 'true' ]] &&
           [[ $(docker inspect -f '{{.State.Running}}' slave2 2>&1) = 'true' ]] &&
           [[ $(docker inspect -f '{{.State.Running}}' mysql 2>&1) = 'true' ]] &&
           [[ $(docker inspect -f '{{.State.Running}}' hive 2>&1) = 'true' ]]; };
    then
        return 1
    fi

    # try and see if NodeManagers are up, otherwise the Flink job will not have enough resources
    # to run
    nm_running=`docker exec -it master bash -c "yarn node -list" | grep RUNNING | wc -l`
    start_time=$(date +%s)
    while [[ "$nm_running" -lt "2" ]]; do
        current_time=$(date +%s)
        time_diff=$((current_time - start_time))

        if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
            return 1
        else
            echo "We only have $nm_running NodeManagers up. We have been trying for $time_diff seconds, retrying ..."
            sleep 1
        fi
        nm_running=`docker exec -it master bash -c "yarn node -list" | grep RUNNING | wc -l`
    done

    return 0
}
echo "Building Hadoop hive Docker container"
until docker build --build-arg HADOOP_VERSION=2.8.4 --build-arg HIVE_VERSION=2.3.4 \
    -f $END_TO_END_DIR/test-scripts/docker-hive-hadoop-cluster/Dockerfile \
    -t flink/flink-hadoop-hive-cluster:latest \
    $END_TO_END_DIR/test-scripts/docker-hive-hadoop-cluster/;
do
    # with all the downloading and ubuntu updating a lot of flakiness can happen, make sure
    # we don't immediately fail
    echo "Something went wrong while building the Docker image, retrying ..."
    sleep 2
done

if ! retry_times $CLUSTER_SETUP_RETRIES 0 start_hadoop_cluster_and_hive; then
    echo "ERROR: Could not start hadoop cluster and hive. Aborting..."
    exit 1
fi

mkdir -p $FLINK_TARBALL_DIR
tar czf $FLINK_TARBALL_DIR/$FLINK_TARBALL -C $(dirname $FLINK_DIR) .

docker cp $FLINK_TARBALL_DIR/$FLINK_TARBALL master:/home/hadoop-user/
docker cp $END_TO_END_DIR/flink-connector-hive-test/target/testHive.jar master:/home/hadoop-user/

# now, at least the container is ready
docker exec -it master bash -c "tar xzf /home/hadoop-user/$FLINK_TARBALL --directory /home/hadoop-user/"

# minimal Flink config, bebe
FLINK_CONFIG=$(cat << END
slot.request.timeout: 120000
containerized.heap-cutoff-min: 100
END
)
docker exec -it master bash -c "echo \"$FLINK_CONFIG\" > /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

echo "Flink config:"
docker exec -it master bash -c "cat /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

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
    application_id=`docker exec -it master bash -c "yarn application -list -appStates ALL" | grep "Flink session cluster" | awk '{print \$1}'`
    echo "Application ID: $application_id"
    docker exec -it master bash -c "yarn logs -applicationId $application_id"
}

start_time=$(date +%s)
if docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && \
   /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -ys 1 -ytm 1000 -yjm 1000 \
   -p 1 -c org.apache.flink.connectors.hive.tests.HiveReadWriteDataTest /home/hadoop-user/testHive.jar";
then
    echo "run e2e test hive job successful"
    OUTPUT=$(docker exec -it master bash -c "hive -e \"select * from dest_non_partition_table\"")
    echo "$OUTPUT"
    for expected_result in ${EXPECTED_RESULT_LOG_CONTAINS[@]}; do
        if [[ ! "$OUTPUT" =~ $expected_result ]]; then
            echo "Output does not contain '$expected_result' as required"
            copy_and_show_logs
            exit 1
        fi
    done
else
    echo "Running the job failed."
    copy_and_show_logs
    exit 1
fi
