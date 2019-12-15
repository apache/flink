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
source "$(dirname "$0")"/common_docker.sh

FLINK_TARBALL_DIR=$TEST_DATA_DIR
FLINK_TARBALL=flink.tar.gz
FLINK_DIRNAME=$(basename $FLINK_DIR)

MAX_RETRY_SECONDS=120
CLUSTER_SETUP_RETRIES=3
IMAGE_BUILD_RETRIES=5

echo "Flink Tarball directory $FLINK_TARBALL_DIR"
echo "Flink tarball filename $FLINK_TARBALL"
echo "Flink distribution directory name $FLINK_DIRNAME"
echo "End-to-end directory $END_TO_END_DIR"

start_time=$(date +%s)

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
    containers_health_check "master" "slave1" "slave2" "kdc"

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

    echo "We now have $nm_running NodeManagers up."

    return 0
}

function build_image() {
    echo "Building Hadoop Docker container"
    if ! retry_times $IMAGE_BUILD_RETRIES 2 docker build --build-arg HADOOP_VERSION=2.8.4 \
        -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/Dockerfile \
        -t flink/docker-hadoop-secure-cluster:latest \
        $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/; then
        echo "ERROR: Could not build hadoop image. Aborting..."
        exit 1
    fi
}

function start_hadoop_cluster_and_prepare_flink() {
    build_image
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
}

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
    docker exec -it master bash -c "yarn application -list -appStates ALL"
    application_id=`docker exec -it master bash -c "yarn application -list -appStates ALL" | grep "Flink" | grep "cluster" | awk '{print \$1}'`
    echo "Application ID: $application_id"
    docker exec -it master bash -c "yarn logs -applicationId $application_id"
    docker exec -it master bash -c "kdestroy"
}

function get_output {
    docker exec -it master bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
        docker exec -it master bash -c "hdfs dfs -ls $1"
        OUTPUT=$(docker exec -it master bash -c "hdfs dfs -cat $1")
        docker exec -it master bash -c "kdestroy"
        echo "$OUTPUT"
}
