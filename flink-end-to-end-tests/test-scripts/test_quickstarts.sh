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

# End to end test for quick starts test.

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/elasticsearch-common.sh

mkdir -p $TEST_DATA_DIR

cd $TEST_DATA_DIR

mvn archetype:generate                             \
    -DarchetypeGroupId=org.apache.flink            \
    -DarchetypeArtifactId=flink-quickstart-java    \
    -DarchetypeVersion=${FLINK_VERSION}            \
    -DgroupId=org.apache.flink.quickstart          \
    -DartifactId=flink-java-project                \
    -Dversion=0.1                                  \
    -Dpackage=org.apache.flink.quickstart          \
    -DinteractiveMode=false

cd flink-java-project

# use the Flink Elasticsearch sink example job code in flink-elasticsearch5-tests to simulate modifications to contained job
cp ${END_TO_END_DIR}/flink-elasticsearch5-test/src/main/java/org/apache/flink/streaming/tests/Elasticsearch5SinkExample.java $TEST_DATA_DIR/flink-java-project/src/main/java/org/apache/flink/quickstart/
sed -i -e 's/package org.apache.flink.streaming.tests;/package org.apache.flink.quickstart;/' $TEST_DATA_DIR/flink-java-project/src/main/java/org/apache/flink/quickstart/Elasticsearch5SinkExample.java

position=$(awk '/<dependencies>/ {print NR}' pom.xml | head -1)

sed -i -e ''"$(($position + 1))"'i\
<dependency>\
<groupId>org.apache.flink</groupId>\
<artifactId>flink-connector-elasticsearch5_${scala.binary.version}</artifactId>\
<version>${flink.version}</version>\
</dependency>' pom.xml

sed -i -e "s/org.apache.flink.quickstart.StreamingJob/org.apache.flink.streaming.tests.Elasticsearch5SinkExample/" pom.xml

mvn clean package -nsu

cd target
jar tvf flink-java-project-0.1.jar > contentsInJar.txt

if [[ `grep -c "org/apache/flink/api/java" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/api" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/experimental" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/runtime" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/util" contentsInJar.txt` -eq '0' ]]; then

    echo "Success: There are no flink core classes are contained in the jar."
else
    echo "Failure: There are flink core classes are contained in the jar."
    exit 1
fi

if [[ `grep -c "org/apache/flink/quickstart/StreamingJob.class" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/quickstart/Elasticsearch5SinkExample.class" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/connectors/elasticsearch5" contentsInJar.txt` -eq '0' ]]; then

    echo "Failure: Since Elasticsearch5SinkExample.class and other user classes are not included in the jar. "
    exit 1
else
    echo "Success: Elasticsearch5SinkExample.class and other user classes are included in the jar."
fi

setup_elasticsearch "https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.1.2.tar.gz"
verify_elasticsearch_process_exist

function shutdownAndCleanup {
    shutdown_elasticsearch_cluster

    # make sure to run regular cleanup as well
    cleanup
}
trap shutdownAndCleanup INT
trap shutdownAndCleanup EXIT

TEST_PROGRAM_JAR=$TEST_DATA_DIR/flink-java-project/target/flink-java-project-0.1.jar

start_cluster

$FLINK_DIR/bin/flink run -c org.apache.flink.quickstart.Elasticsearch5SinkExample $TEST_PROGRAM_JAR \
  --numRecords 20 \
  --index index \
  --type type

verify_result 20
