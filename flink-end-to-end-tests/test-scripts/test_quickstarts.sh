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
# Usage:
# FLINK_DIR=<flink dir> flink-end-to-end-tests/test-scripts/test_quickstarts.sh <Type (java or scala)>

source "$(dirname "$0")"/common.sh
source "$(dirname "$0")"/elasticsearch-common.sh

TEST_TYPE=$1
TEST_CLASS_NAME=Elasticsearch5SinkExample
TEST_FILE_PATH=flink-quickstart-test/src/main/${TEST_TYPE}/org/apache/flink/quickstarts/test/${TEST_CLASS_NAME}.${TEST_TYPE}
QUICKSTARTS_FILE_PATH=${TEST_DATA_DIR}/flink-quickstart-${TEST_TYPE}/src/main/${TEST_TYPE}/org/apache/flink/quickstart/${TEST_CLASS_NAME}.${TEST_TYPE}
ES_INDEX=index_${TEST_TYPE}

# get the elasticsearch dependency from flink-quickstart-test
ES_DEPENDENCY="<dependency>\
<groupId>org.apache.flink</groupId>\
$(awk '/flink-connector-elasticsearch/ {print $1}' ${END_TO_END_DIR}/flink-quickstart-test/target/dependency-reduced-pom.xml)\
<version>\${flink.version}</version>\
</dependency>"

mkdir -p "${TEST_DATA_DIR}"
cd "${TEST_DATA_DIR}"

ARTIFACT_ID=flink-quickstart-${TEST_TYPE}
ARTIFACT_VERSION=0.1

mvn archetype:generate                                   \
    -DarchetypeGroupId=org.apache.flink                  \
    -DarchetypeArtifactId=flink-quickstart-${TEST_TYPE}  \
    -DarchetypeVersion=${FLINK_VERSION}                  \
    -DgroupId=org.apache.flink.quickstart                \
    -DartifactId=${ARTIFACT_ID}                          \
    -Dversion=${ARTIFACT_VERSION}                        \
    -Dpackage=org.apache.flink.quickstart                \
    -DinteractiveMode=false

cd "${ARTIFACT_ID}"

# use the Flink Elasticsearch sink example job code in flink-quickstart-test to simulate modifications to contained job
cp ${END_TO_END_DIR}/${TEST_FILE_PATH} "$QUICKSTARTS_FILE_PATH"
sed -i -e 's/package org.apache.flink.quickstarts.test/package org.apache.flink.quickstart/' "${QUICKSTARTS_FILE_PATH}"

position=$(awk '/<dependencies>/ {print NR}' pom.xml | head -1)

# Add ElasticSearch dependency to pom.xml
sed -i -e ''$(($position + 1))'i\
'${ES_DEPENDENCY}'' pom.xml

sed -i -e "s/org.apache.flink.quickstart.StreamingJob/org.apache.flink.quickstart.$TEST_CLASS_NAME/" pom.xml

mvn clean package -nsu

cd target
jar tvf flink-quickstart-${TEST_TYPE}-0.1.jar > contentsInJar.txt

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
wait_elasticsearch_working

function shutdownAndCleanup {
    shutdown_elasticsearch_cluster "$ES_INDEX"
}
on_exit shutdownAndCleanup

TEST_PROGRAM_JAR=${TEST_DATA_DIR}/${ARTIFACT_ID}/target/${ARTIFACT_ID}-${ARTIFACT_VERSION}.jar

start_cluster

${FLINK_DIR}/bin/flink run -c org.apache.flink.quickstart.Elasticsearch5SinkExample "$TEST_PROGRAM_JAR" \
  --numRecords 20 \
  --index "${ES_INDEX}" \
  --type type

verify_result_line_number 20 "${ES_INDEX}"
