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

MAX_RETRY_SECONDS=800

echo "Flink Tarball directory $FLINK_TARBALL_DIR"
echo "Flink tarball filename $FLINK_TARBALL"
echo "Flink distribution directory name $FLINK_DIRNAME"
echo "End-to-end directory $END_TO_END_DIR"
docker --version
docker-compose --version

mkdir -p $FLINK_TARBALL_DIR
tar czf $FLINK_TARBALL_DIR/$FLINK_TARBALL -C $(dirname $FLINK_DIR) .

echo "Building Hadoop Docker container"
until docker build --build-arg HADOOP_VERSION=2.8.4 -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/Dockerfile -t flink/docker-hadoop-secure-cluster:latest $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/; do
    # with all the downloading and ubuntu updating a lot of flakiness can happen, make sure
    # we don't immediately fail
    echo "Something went wrong while building the Docker image, retrying ..."
    sleep 2
done

echo "Starting Hadoop cluster"
docker-compose -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml up -d

# make sure we stop our cluster at the end
function cluster_shutdown {
  # don't call ourselves again for another signal interruption
  trap "exit -1" INT
  # don't call ourselves again for normal exit
  trap "" EXIT

  docker-compose -f $END_TO_END_DIR/test-scripts/docker-hadoop-secure-cluster/docker-compose.yml down
  rm $FLINK_TARBALL_DIR/$FLINK_TARBALL
}
trap cluster_shutdown INT
trap cluster_shutdown EXIT

until docker cp $FLINK_TARBALL_DIR/$FLINK_TARBALL master:/home/hadoop-user/; do
    # we're retrying this one because we don't know yet if the container is ready
    echo "Uploading Flink tarball to docker master failed, retrying ..."
    sleep 5
done

# now, at least the container is ready
docker exec -it master bash -c "tar xzf /home/hadoop-user/$FLINK_TARBALL --directory /home/hadoop-user/"

# minimal Flink config, bebe
docker exec -it master bash -c "echo \"security.kerberos.login.keytab: /home/hadoop-user/hadoop-user.keytab\" > /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"
docker exec -it master bash -c "echo \"security.kerberos.login.principal: hadoop-user\" >> /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"
docker exec -it master bash -c "echo \"slot.request.timeout: 60000\" >> /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

echo "Flink config:"
docker exec -it master bash -c "cat /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"

# make the output path random, just in case it already exists, for example if we
# had cached docker containers
OUTPUT_PATH=hdfs:///user/hadoop-user/wc-out-$RANDOM

start_time=$(date +%s)
# it's important to run this with higher parallelism, otherwise we might risk that
# JM and TM are on the same YARN node and that we therefore don't test the keytab shipping
until docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -yn 3 -ys 1 -ytm 2000 -yjm 2000 -p 3 /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar --output $OUTPUT_PATH"; do
    current_time=$(date +%s)
	time_diff=$((current_time - start_time))

    if [ $time_diff -ge $MAX_RETRY_SECONDS ]; then
        echo "We tried running the job for $time_diff seconds, max is $MAX_RETRY_SECONDS seconds, aborting"
        mkdir -p $TEST_DATA_DIR/logs
        echo "Hadoop logs:"
        docker cp master:/var/log/hadoop/* $TEST_DATA_DIR/logs/
        for f in $TEST_DATA_DIR/logs/*; do
            echo "$f:"
            cat $f
        done
        echo "Docker logs:"
        docker logs master
        exit 1
    else
        echo "Running the Flink job failed, might be that the cluster is not ready yet. We have been trying for $time_diff seconds, retrying ..."
        sleep 5
    fi
done

docker exec -it master bash -c "kinit -kt /home/hadoop-user/hadoop-user.keytab hadoop-user"
docker exec -it master bash -c "hdfs dfs -ls $OUTPUT_PATH"
OUTPUT=$(docker exec -it master bash -c "hdfs dfs -cat $OUTPUT_PATH/*")
docker exec -it master bash -c "kdestroy"
echo "$OUTPUT"

if [[ ! "$OUTPUT" =~ "consummation,1" ]]; then
    echo "Output does not contain (consummation, 1) as required"
    mkdir -p $TEST_DATA_DIR/logs
    echo "Hadoop logs:"
    docker cp master:/var/log/hadoop/* $TEST_DATA_DIR/logs/
    for f in $TEST_DATA_DIR/logs/*; do
        echo "$f:"
        cat $f
    done
    echo "Docker logs:"
    docker logs master
    exit 1
fi

if [[ ! "$OUTPUT" =~ "of,14" ]]; then
    echo "Output does not contain (of, 14) as required"
    exit 1
fi

if [[ ! "$OUTPUT" =~ "calamity,1" ]]; then
    echo "Output does not contain (calamity, 1) as required"
    exit 1
fi

echo "Running Job without configured keytab, the exception you see below is expected"
docker exec -it master bash -c "echo \"\" > /home/hadoop-user/$FLINK_DIRNAME/conf/flink-conf.yaml"
# verify that it doesn't work if we don't configure a keytab
OUTPUT=$(docker exec -it master bash -c "export HADOOP_CLASSPATH=\`hadoop classpath\` && /home/hadoop-user/$FLINK_DIRNAME/bin/flink run -m yarn-cluster -yn 3 -ys 1 -ytm 1200 -yjm 800 -p 3 /home/hadoop-user/$FLINK_DIRNAME/examples/streaming/WordCount.jar --output $OUTPUT_PATH")
echo "$OUTPUT"

if [[ ! "$OUTPUT" =~ "Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials" ]]; then
    echo "Output does not contain the Kerberos error message as required"
    exit 1
fi
