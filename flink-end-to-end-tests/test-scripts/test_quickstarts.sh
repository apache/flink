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

mkdir -p $TEST_DATA_DIR

cd $TEST_DATA_DIR

mvn archetype:generate                             \
    -DarchetypeGroupId=org.apache.flink            \
    -DarchetypeArtifactId=flink-quickstart-java    \
    -DarchetypeVersion=1.6-SNAPSHOT                \
    -DgroupId=org.apache.flink.quickstart          \
    -DartifactId=flink-java-project                \
    -Dversion=0.1                                  \
    -Dpackage=org.apache.flink.quickstart          \
    -DinteractiveMode=false

cd flink-java-project

cp $TEST_DATA_DIR/../../flink-quickstart-test/src/main/java/org/apache/flink/quickstart/ElasticsearchStreamingJob.java $TEST_DATA_DIR/flink-java-project/src/main/java/org/apache/flink/quickstart/

position=$(awk '/<dependencies>/ {print NR}' pom.xml | head -1)

sed -i -e ''"$(($position + 1))"'i\
<dependency>\
<groupId>org.apache.flink</groupId>\
<artifactId>flink-connector-elasticsearch2_${scala.binary.version}</artifactId>\
<version>${flink.version}</version>\
</dependency>' pom.xml

sed -i -e "s/org.apache.flink.quickstart.StreamingJob/org.apache.flink.quickstart.ElasticsearchStreamingJob/" pom.xml

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
    PASS=""
    exit 1
fi

if [[ `grep -c "org/apache/flink/quickstart/StreamingJob.class" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/quickstart/ElasticsearchStreamingJob.class" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/streaming/connectors/elasticsearch2" contentsInJar.txt` -eq '0' ]]; then

    echo "Failure: Since ElasticsearchStreamingJob.class and other user classes are not included in the jar. "
    PASS=""
    exit 1
else
    echo "Success: ElasticsearchStreamingJob.class and other user classes are included in the jar."
fi

ELASTICSEARCH_URL="https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/2.3.5/elasticsearch-2.3.5.tar.gz"

curl "$ELASTICSEARCH_URL" > $TEST_DATA_DIR/elasticsearch.tar.gz
tar xzf $TEST_DATA_DIR/elasticsearch.tar.gz -C $TEST_DATA_DIR/
ELASTICSEARCH_DIR=$TEST_DATA_DIR/elasticsearch-2.3.5

nohup $ELASTICSEARCH_DIR/bin/elasticsearch &

ELASTICSEARCH_PROCESS=$(jps | grep Elasticsearch | awk '{print $2}')

# make sure the elasticsearch node is actually running
if [ "$ELASTICSEARCH_PROCESS" != "Elasticsearch" ]; then
  echo "Elasticsearch node is not running."
  PASS=""
  exit 1
else
  echo "Elasticsearch node is running."
fi

TEST_PROGRAM_JAR=$TEST_DATA_DIR/flink-java-project/target/flink-java-project-0.1.jar

start_cluster

# run the Flink job
$FLINK_DIR/bin/flink run -c org.apache.flink.quickstart.ElasticsearchStreamingJob $TEST_PROGRAM_JAR

curl 'localhost:9200/my-index/_search?q=*&pretty&size=21' > $TEST_DATA_DIR/output

if [ -n "$(grep '\"total\" : 21' $TEST_DATA_DIR/output)" ]; then
    echo "Quickstarts end to end test pass."
else
    echo "Quickstarts end to end test failed."
    PASS=""
    exit 1
fi

function shutdownAndCleanup {
    pid=$(jps | grep Elasticsearch | awk '{print $1}')
    kill -SIGTERM $pid

    # make sure to run regular cleanup as well
    cleanup
}

trap shutdownAndCleanup INT
trap shutdownAndCleanup EXIT
