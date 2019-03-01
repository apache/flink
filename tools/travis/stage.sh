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

MODULES_CORE_JDK9_EXCLUSIONS="\
!flink-state-backends/flink-statebackend-rocksdb,\
!flink-clients,\
!flink-java,\
!flink-runtime,\
!flink-scala,\
!flink-scala-shell"

MODULES_LIBRARIES="\
flink-libraries/flink-cep,\
flink-libraries/flink-cep-scala,\
flink-libraries/flink-gelly,\
flink-libraries/flink-gelly-scala,\
flink-libraries/flink-gelly-examples,\
flink-libraries/flink-ml,\
flink-libraries/flink-python,\
flink-libraries/flink-streaming-python,\
flink-table/flink-table-common,\
flink-table/flink-table-api-java,\
flink-table/flink-table-api-scala,\
flink-table/flink-table-api-java-bridge,\
flink-table/flink-table-api-scala-bridge,\
flink-table/flink-table-planner,\
flink-table/flink-sql-client,\
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
flink-formats/flink-json,\
flink-formats/flink-csv,\
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

MODULES_CONNECTORS_JDK9_EXCLUSIONS="\
!flink-filesystems/flink-hadoop-fs,\
!flink-filesystems/flink-mapr-fs,\
!flink-filesystems/flink-s3-fs-hadoop,\
!flink-filesystems/flink-s3-fs-presto,\
!flink-formats/flink-avro,\
!flink-connectors/flink-hbase,\
!flink-connectors/flink-connector-cassandra,\
!flink-connectors/flink-connector-elasticsearch,\
!flink-connectors/flink-connector-kafka-0.9,\
!flink-connectors/flink-connector-kafka-0.10,\
!flink-connectors/flink-connector-kafka-0.11"

MODULES_TESTS="\
flink-tests"

MODULES_TESTS_JDK9_EXCLUSIONS="\
!flink-tests"

MODULES_MISC_JDK9_EXCLUSIONS="\
!flink-metrics/flink-metrics-jmx,\
!flink-metrics/flink-metrics-dropwizard,\
!flink-metrics/flink-metrics-prometheus,\
!flink-metrics/flink-metrics-statsd,\
!flink-metrics/flink-metrics-slf4j,\
!flink-yarn-tests"

if [[ ${PROFILE} == *"include-kinesis"* ]]; then
    MODULES_CONNECTORS="$MODULES_CONNECTORS,flink-connectors/flink-connector-kinesis"
fi

# we can only build the Kafka 0.8 connector when building for Scala 2.11
if [[ $PROFILE == *"scala-2.11"* ]]; then
    MODULES_CONNECTORS="$MODULES_CONNECTORS,flink-connectors/flink-connector-kafka-0.8"
    MODULES_CONNECTORS_JDK9_EXCLUSIONS="${MODULES_CONNECTORS_JDK9_EXCLUSIONS},!flink-connectors/flink-connector-kafka-0.8"
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

    local modules_core=$MODULES_CORE
    local modules_libraries=$MODULES_LIBRARIES
    local modules_connectors=$MODULES_CONNECTORS
    local modules_tests=$MODULES_TESTS
    local negated_core=\!${MODULES_CORE//,/,\!}
    local negated_libraries=\!${MODULES_LIBRARIES//,/,\!}
    local negated_connectors=\!${MODULES_CONNECTORS//,/,\!}
    local negated_tests=\!${MODULES_TESTS//,/,\!}
    local modules_misc="$negated_core,$negated_libraries,$negated_connectors,$negated_tests"

    # various modules fail testing on JDK 9; exclude them
    if [[ ${PROFILE} == *"jdk9"* ]]; then
        modules_core="$modules_core,$MODULES_CORE_JDK9_EXCLUSIONS"
        modules_connectors="$modules_connectors,$MODULES_CONNECTORS_JDK9_EXCLUSIONS"
        # add flink-annotations so that at least one module is tested, otherwise maven fails
        modules_tests="$modules_tests,$MODULES_TESTS_JDK9_EXCLUSIONS,flink-annotations"
        modules_misc="$modules_misc,$MODULES_MISC_JDK9_EXCLUSIONS"
    fi

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_LIBRARIES})
            echo "-pl $modules_libraries"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $modules_connectors"
        ;;
        (${STAGE_TESTS})
            echo "-pl $modules_tests"
        ;;
        (${STAGE_MISC})
            echo "-pl $modules_misc"
        ;;
    esac
}
