#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

STAGE_COMPILE="compile"
STAGE_CORE="core"
STAGE_LIBRARIES="libraries"
STAGE_CONNECTORS="connectors"
STAGE_TESTS="tests"
STAGE_MISC="misc"
STAGE_CLEANUP="cleanup"

MODULES_CORE="\
flink-test-utils-parent/flink-test-utils,\
flink-state-backends/flink-statebackend-rocksdb,\
flink-clients,\
flink-core,\
flink-java,\
flink-optimizer,\
flink-runtime,\
flink-runtime-web,\
flink-scala,\
flink-streaming-java,\
flink-streaming-scala"

MODULES_LIBRARIES="\
flink-contrib/flink-storm,\
flink-contrib/flink-storm-examples,\
flink-libraries/flink-cep,\
flink-libraries/flink-cep-scala,\
flink-libraries/flink-gelly,\
flink-libraries/flink-gelly-scala,\
flink-libraries/flink-gelly-examples,\
flink-libraries/flink-ml,\
flink-libraries/flink-python,\
flink-libraries/flink-streaming-python,\
flink-libraries/flink-table,\
flink-queryable-state/flink-queryable-state-runtime,\
flink-queryable-state/flink-queryable-state-client-java"

MODULES_CONNECTORS="\
flink-contrib/flink-connector-wikiedits,\
flink-filesystems/flink-hadoop-fs,\
flink-filesystems/flink-mapr-fs,\
flink-filesystems/flink-s3-fs-base,\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-formats/flink-avro,\
flink-formats/flink-parquet,\
flink-connectors/flink-hbase,\
flink-connectors/flink-hcatalog,\
flink-connectors/flink-hadoop-compatibility,\
flink-connectors/flink-jdbc,\
flink-connectors/flink-connector-cassandra,\
flink-connectors/flink-connector-elasticsearch,\
flink-connectors/flink-connector-elasticsearch2,\
flink-connectors/flink-connector-elasticsearch5,\
flink-connectors/flink-connector-elasticsearch6,\
flink-connectors/flink-connector-elasticsearch-base,\
flink-connectors/flink-connector-filesystem,\
flink-connectors/flink-connector-kafka-0.9,\
flink-connectors/flink-connector-kafka-0.10,\
flink-connectors/flink-connector-kafka-0.11,\
flink-connectors/flink-connector-kafka-base,\
flink-connectors/flink-connector-nifi,\
flink-connectors/flink-connector-rabbitmq,\
flink-connectors/flink-connector-twitter"

MODULES_TESTS="\
flink-tests"

if [[ ${PROFILE} == *"include-kinesis"* ]]; then
    MODULES_CONNECTORS="$MODULES_CONNECTORS,flink-connectors/flink-connector-kinesis"
fi

# we can only build the Kafka 0.8 connector when building for Scala 2.11
if [[ $PROFILE == *"scala-2.11"* ]]; then
    MODULES_CONNECTORS="$MODULES_CONNECTORS,flink-connectors/flink-connector-kafka-0.8"
fi

# we can only build the Scala Shell when building for Scala 2.11
if [[ $PROFILE == *"scala-2.11"* ]]; then
    MODULES_CORE="$MODULES_CORE,flink-scala-shell"
fi

function get_compile_modules_for_stage() {
    local stage=$1

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $MODULES_CORE -am"
        ;;
        (${STAGE_LIBRARIES})
            echo "-pl $MODULES_LIBRARIES -am"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $MODULES_CONNECTORS -am"
        ;;
        (${STAGE_TESTS})
            echo "-pl $MODULES_TESTS -am"
        ;;
        (${STAGE_MISC})
            # compile everything since dist needs it anyway
            echo ""
        ;;
    esac
}

function get_test_modules_for_stage() {
    local stage=$1

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $MODULES_CORE"
        ;;
        (${STAGE_LIBRARIES})
            echo "-pl $MODULES_LIBRARIES"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $MODULES_CONNECTORS"
        ;;
        (${STAGE_TESTS})
            echo "-pl $MODULES_TESTS"
        ;;
        (${STAGE_MISC})
            NEGATED_CORE=\!${MODULES_CORE//,/,\!}
            NEGATED_LIBRARIES=\!${MODULES_LIBRARIES//,/,\!}
            NEGATED_CONNECTORS=\!${MODULES_CONNECTORS//,/,\!}
            NEGATED_TESTS=\!${MODULES_TESTS//,/,\!}
            echo "-pl $NEGATED_CORE,$NEGATED_LIBRARIES,$NEGATED_CONNECTORS,$NEGATED_TESTS"
        ;;
    esac
}
