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

projectVersion=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep -E '^([0-9]+.[0-9]+(.[0-9]+)?(-[a-zA-Z0-9]+)?)$'`
snapshotPrefix=`echo ${projectVersion} | grep -Eo "\-SNAPSHOT"`

TEST_PROGRAM_JAR=$TEST_DATA_DIR/flink-quickstart/target/flink-quickstart-0.1${snapshotPrefix}.jar
mkdir -p $TEST_DATA_DIR
cd $TEST_DATA_DIR

mvn archetype:generate                             \
    -DarchetypeGroupId=org.apache.flink            \
    -DarchetypeArtifactId=flink-quickstart-java    \
    -DarchetypeVersion=${projectVersion}           \
    -DgroupId=org.apache.flink.quickstart          \
    -DartifactId=flink-quickstart                  \
    -Dversion=0.1${snapshotPrefix}                 \
    -Dpackage=org.apache.flink.quickstart          \
    -DinteractiveMode=false

cd flink-quickstart
mvn clean package -nsu

cd target
jar tvf flink-quickstart-0.1${snapshotPrefix}.jar > contentsInJar.txt

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

if [[ `grep -c "org/apache/flink/quickstart/BatchJob.class" contentsInJar.txt` -eq '0' && \
      `grep -c "org/apache/flink/quickstart/StreamingJob.class" contentsInJar.txt` -eq '0' ]]; then
    echo "Failure: One of quickstart classes [ BatchJob or StreamingJob] are not included in the jar. "
    PASS=""
    exit 1
else
    echo "Success: All quickstart classes [ BatchJob  and StreamingJob] are included in the jar."
fi

cd $TEST_DATA_DIR/flink-quickstart
rm -rf target/
tar -zcvf quickstart-java${snapshotPrefix}.zip .
#[TODO]: ADD right command to upload the .zip file to flink repo
#curl -F 'quickstart-java${snapshotPrefix}=@quickstart-java${snapshotPrefix}.zip' https://flink.apache.org/q/quickstart-java${snapshotPrefix}.zip
