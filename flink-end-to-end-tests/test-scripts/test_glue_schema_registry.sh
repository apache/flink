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

################################################################################
# To run this test locally, AWS credential is required.
# And IAM permission of AWSGlueSchemaRegistryReadonlyAccess,
# AWSGlueSchemaRegistryFullAccess, and AWSGlueSchemaRegistryTagsFullAccess
# needs to be added.
################################################################################
source "$(dirname "$0")"/common.sh

echo "Running streaming kinesis with glue schema registry "

# Kinesalite doesn't support CBOR
export AWS_CBOR_DISABLE=1

# Required by the KPL native process
export AWS_ACCESS_KEY_ID=${IT_CASE_GLUE_SCHEMA_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${IT_CASE_GLUE_SCHEMA_SECRET_KEY}

KINESALITE_PORT=4567

function start_kinesalite {
    #docker run -d --rm --name flink-glue-schema-registry-test -p ${KINESALITE_PORT}:${KINESALITE_PORT} instructure/kinesalite
    # override entrypoint to enable SSL
    docker run -d --rm --entrypoint "/tini" \
        --name flink-glue-schema-registry-test \
        -p ${KINESALITE_PORT}:${KINESALITE_PORT} \
        instructure/kinesalite -- \
        /usr/src/app/node_modules/kinesalite/cli.js --path /var/lib/kinesalite --ssl
}

START_KINESALITE_MAX_RETRIES=50
if ! retry_times ${START_KINESALITE_MAX_RETRIES} 0 start_kinesalite; then
    echo "Failed to run kinesalite docker image"
    exit 1
fi

# reveal potential issues with the container in the CI environment
docker logs flink-glue-schema-registry-test

function test_cleanup {
  # job needs to stop before kinesalite
  stop_cluster
  echo "terminating kinesalite"
  docker kill flink-glue-schema-registry-test
}
on_exit test_cleanup

# prefix com.amazonaws.sdk.disableCertChecking to account for shading
DISABLE_CERT_CHECKING_JAVA_OPTS="-Dorg.apache.flink.kinesis.shaded.com.amazonaws.sdk.disableCertChecking -Dcom.amazonaws.sdk.disableCertChecking"

export FLINK_ENV_JAVA_OPTS=${DISABLE_CERT_CHECKING_JAVA_OPTS}
start_cluster

TEST_JAR="${END_TO_END_DIR}/flink-glue-schema-registry-test/target/GlueSchemaRegistryExample.jar"
JVM_ARGS=${DISABLE_CERT_CHECKING_JAVA_OPTS} \
$FLINK_DIR/bin/flink run -p 1 -c org.apache.flink.glue.schema.registry.test.GlueSchemaRegistryExampleTest $TEST_JAR \
  --input-stream gsr-input-stream --output-stream gsr-output-stream \
  --aws.endpoint https://localhost:${KINESALITE_PORT} --aws.credentials.provider.basic.secretkey fakekey --aws.credentials.provider.basic.accesskeyid fakeid \
  --flink.stream.initpos TRIM_HORIZON \
  --flink.partition-discovery.interval-millis 1000
