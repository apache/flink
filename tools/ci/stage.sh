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
STAGE_PYTHON="python"
STAGE_LIBRARIES="libraries"
STAGE_TABLE="table"
STAGE_CONNECTORS="connectors"
STAGE_KAFKA_GELLY="kafka/gelly"
STAGE_TESTS="tests"
STAGE_MISC="misc"
STAGE_CLEANUP="cleanup"
STAGE_FINEGRAINED_RESOURCE_MANAGEMENT="finegrained_resource_management"

MODULES_CORE="\
flink-annotations,\
flink-test-utils-parent/flink-test-utils,\
flink-state-backends/flink-statebackend-rocksdb,\
flink-clients,\
flink-core,\
flink-java,\
flink-optimizer,\
flink-rpc,\
flink-rpc/flink-rpc-core,\
flink-rpc/flink-rpc-akka,\
flink-runtime,\
flink-runtime-web,\
flink-scala,\
flink-streaming-java,\
flink-streaming-scala,\
flink-metrics,\
flink-metrics/flink-metrics-core,\
flink-external-resources,\
flink-external-resources/flink-external-resource-gpu"

MODULES_LIBRARIES="\
flink-libraries/flink-cep,\
flink-libraries/flink-cep-scala,\
flink-libraries/flink-state-processing-api"

MODULES_TABLE="\
flink-table/flink-sql-parser,\
flink-table/flink-sql-parser-hive,\
flink-table/flink-table-common,\
flink-table/flink-table-api-java,\
flink-table/flink-table-api-scala,\
flink-table/flink-table-api-java-bridge,\
flink-table/flink-table-api-scala-bridge,\
flink-table/flink-sql-client,\
flink-table/flink-table-planner,\
flink-table/flink-table-runtime,\
flink-table/flink-table-code-splitter"

MODULES_CONNECTORS="\
flink-contrib/flink-connector-wikiedits,\
flink-filesystems,\
flink-filesystems/flink-fs-hadoop-shaded,\
flink-filesystems/flink-hadoop-fs,\
flink-filesystems/flink-mapr-fs,\
flink-filesystems/flink-oss-fs-hadoop,\
flink-filesystems/flink-s3-fs-base,\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-fs-tests,\
flink-formats,\
flink-formats/flink-avro-confluent-registry,\
flink-formats/flink-avro,\
flink-formats/flink-parquet,\
flink-formats/flink-sequence-file,\
flink-formats/flink-json,\
flink-formats/flink-csv,\
flink-formats/flink-orc,\
flink-formats/flink-orc-nohive,\
flink-connectors/flink-connector-hbase-base,\
flink-connectors/flink-connector-hbase-1.4,\
flink-connectors/flink-connector-hbase-2.2,\
flink-connectors/flink-hcatalog,\
flink-connectors/flink-hadoop-compatibility,\
flink-connectors,\
flink-connectors/flink-connector-jdbc,\
flink-connectors/flink-connector-cassandra,\
flink-connectors/flink-connector-elasticsearch5,\
flink-connectors/flink-connector-elasticsearch6,\
flink-connectors/flink-connector-elasticsearch7,\
flink-connectors/flink-sql-connector-elasticsearch6,\
flink-connectors/flink-sql-connector-elasticsearch7,\
flink-connectors/flink-connector-elasticsearch-base,\
flink-connectors/flink-connector-nifi,\
flink-connectors/flink-connector-rabbitmq,\
flink-connectors/flink-connector-twitter,\
flink-connectors/flink-connector-kinesis,\
flink-metrics/flink-metrics-dropwizard,\
flink-metrics/flink-metrics-graphite,\
flink-metrics/flink-metrics-jmx,\
flink-metrics/flink-metrics-influxdb,\
flink-metrics/flink-metrics-prometheus,\
flink-metrics/flink-metrics-statsd,\
flink-metrics/flink-metrics-datadog,\
flink-metrics/flink-metrics-slf4j,\
flink-queryable-state/flink-queryable-state-runtime,\
flink-queryable-state/flink-queryable-state-client-java"

MODULES_KAFKA_GELLY="\
flink-libraries/flink-gelly,\
flink-libraries/flink-gelly-scala,\
flink-libraries/flink-gelly-examples,\
flink-connectors/flink-connector-kafka,\
flink-connectors/flink-sql-connector-kafka,"

MODULES_TESTS="\
flink-tests"

MODULES_FINEGRAINED_RESOURCE_MANAGEMENT=${MODULES_CORE},${MODULES_TESTS}

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
        (${STAGE_TABLE})
            echo "-pl $MODULES_TABLE -am"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $MODULES_CONNECTORS -am"
        ;;
        (${STAGE_KAFKA_GELLY})
            echo "-pl $MODULES_KAFKA_GELLY -am"
        ;;
        (${STAGE_TESTS})
            echo "-pl $MODULES_TESTS -am"
        ;;
        (${STAGE_MISC})
            # compile everything; using the -am switch does not work with negated module lists!
            # the negation takes precedence, thus not all required modules would be built
            echo ""
        ;;
        (${STAGE_PYTHON})
            # compile everything for PyFlink.
            echo ""
        ;;
        (${STAGE_FINEGRAINED_RESOURCE_MANAGEMENT})
            echo "-pl $MODULES_FINEGRAINED_RESOURCE_MANAGEMENT -am"
        ;;
    esac
}

function get_test_modules_for_stage() {
    local stage=$1

    local modules_core=$MODULES_CORE
    local modules_libraries=$MODULES_LIBRARIES
    local modules_table=$MODULES_TABLE
    local modules_kafka_gelly=$MODULES_KAFKA_GELLY
    local modules_connectors=$MODULES_CONNECTORS
    local modules_tests=$MODULES_TESTS
    local negated_core=\!${MODULES_CORE//,/,\!}
    local negated_libraries=\!${MODULES_LIBRARIES//,/,\!}
    local negated_table=\!${MODULES_TABLE//,/,\!}
    local negated_kafka_gelly=\!${MODULES_KAFKA_GELLY//,/,\!}
    local negated_connectors=\!${MODULES_CONNECTORS//,/,\!}
    local negated_tests=\!${MODULES_TESTS//,/,\!}
    local modules_misc="$negated_core,$negated_libraries,$negated_table,$negated_connectors,$negated_kafka_gelly,$negated_tests"
    local modules_finegrained_resource_management=$MODULES_FINEGRAINED_RESOURCE_MANAGEMENT

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_LIBRARIES})
            echo "-pl $modules_libraries"
        ;;
        (${STAGE_TABLE})
            echo "-pl $modules_table"
        ;;
        (${STAGE_CONNECTORS})
            echo "-pl $modules_connectors"
        ;;
        (${STAGE_KAFKA_GELLY})
            echo "-pl $modules_kafka_gelly"
        ;;
        (${STAGE_TESTS})
            echo "-pl $modules_tests"
        ;;
        (${STAGE_MISC})
            echo "-pl $modules_misc"
        ;;
        (${STAGE_FINEGRAINED_RESOURCE_MANAGEMENT})
            echo "-pl $modules_finegrained_resource_management"
        ;;
    esac
}
