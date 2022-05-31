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
STAGE_TABLE="table"
STAGE_CONNECTORS_1="connect_1"
STAGE_CONNECTORS_2="connect_2"
STAGE_TESTS="tests"
STAGE_MISC="misc"
STAGE_CLEANUP="cleanup"
STAGE_FINEGRAINED_RESOURCE_MANAGEMENT="finegrained_resource_management"

MODULES_CORE="\
flink-annotations,\
flink-test-utils-parent/flink-test-utils,\
flink-state-backends,\
flink-state-backends/flink-statebackend-changelog,\
flink-state-backends/flink-statebackend-heap-spillable,\
flink-state-backends/flink-statebackend-rocksdb,\
flink-clients,\
flink-core,\
flink-java,\
flink-optimizer,\
flink-rpc,\
flink-rpc/flink-rpc-core,\
flink-rpc/flink-rpc-akka,\
flink-rpc/flink-rpc-akka-loader,\
flink-runtime,\
flink-runtime-web,\
flink-scala,\
flink-streaming-java,\
flink-streaming-scala,\
flink-metrics,\
flink-metrics/flink-metrics-core,\
flink-external-resources,\
flink-external-resources/flink-external-resource-gpu,\
flink-libraries,\
flink-libraries/flink-cep,\
flink-libraries/flink-cep-scala,\
flink-libraries/flink-state-processing-api,\
flink-libraries/flink-gelly,\
flink-libraries/flink-gelly-scala,\
flink-libraries/flink-gelly-examples,\
flink-queryable-state,\
flink-queryable-state/flink-queryable-state-runtime,\
flink-queryable-state/flink-queryable-state-client-java,\
flink-container,\
flink-dstl,\
flink-dstl/flink-dstl-dfs,\
"

MODULES_TABLE="\
flink-table,\
flink-table/flink-sql-parser,\
flink-table/flink-sql-parser-hive,\
flink-table/flink-table-common,\
flink-table/flink-table-api-java,\
flink-table/flink-table-api-scala,\
flink-table/flink-table-api-bridge-base,\
flink-table/flink-table-api-java-bridge,\
flink-table/flink-table-api-scala-bridge,\
flink-table/flink-table-api-java-uber,\
flink-table/flink-sql-client,\
flink-table/flink-table-planner,\
flink-table/flink-table-planner-loader,\
flink-table/flink-table-planner-loader-bundle,\
flink-table/flink-table-runtime,\
flink-table/flink-table-code-splitter,\
flink-table/flink-table-test-utils,\
"

MODULES_CONNECTORS_1="\
flink-contrib/flink-connector-wikiedits,\
flink-filesystems,\
flink-filesystems/flink-azure-fs-hadoop,\
flink-filesystems/flink-fs-hadoop-shaded,\
flink-filesystems/flink-gs-fs-hadoop,\
flink-filesystems/flink-hadoop-fs,\
flink-filesystems/flink-oss-fs-hadoop,\
flink-filesystems/flink-s3-fs-base,\
flink-filesystems/flink-s3-fs-hadoop,\
flink-filesystems/flink-s3-fs-presto,\
flink-fs-tests,\
flink-formats,\
flink-formats/flink-format-common,\
flink-formats/flink-avro-confluent-registry,\
flink-formats/flink-sql-avro-confluent-registry,\
flink-formats/flink-avro-glue-schema-registry,\
flink-formats/flink-json-glue-schema-registry,\
flink-formats/flink-avro,\
flink-formats/flink-sql-avro,\
flink-formats/flink-compress,\
flink-formats/flink-hadoop-bulk,\
flink-formats/flink-parquet,\
flink-formats/flink-sql-parquet,\
flink-formats/flink-sequence-file,\
flink-formats/flink-json,\
flink-formats/flink-csv,\
flink-formats/flink-orc,\
flink-formats/flink-sql-orc,\
flink-formats/flink-orc-nohive,\
flink-connectors/flink-file-sink-common,\
flink-connectors/flink-connector-hbase-base,\
flink-connectors/flink-connector-hbase-1.4,\
flink-connectors/flink-sql-connector-hbase-1.4,\
flink-connectors/flink-connector-hbase-2.2,\
flink-connectors/flink-sql-connector-hbase-2.2,\
flink-connectors/flink-hcatalog,\
flink-connectors/flink-hadoop-compatibility,\
flink-connectors,\
flink-connectors/flink-connector-files,\
flink-connectors/flink-connector-jdbc,\
flink-connectors/flink-connector-cassandra,\
flink-connectors/flink-connector-elasticsearch6,\
flink-connectors/flink-connector-elasticsearch7,\
flink-connectors/flink-sql-connector-elasticsearch6,\
flink-connectors/flink-sql-connector-elasticsearch7,\
flink-connectors/flink-connector-elasticsearch-base,\
flink-metrics/flink-metrics-dropwizard,\
flink-metrics/flink-metrics-graphite,\
flink-metrics/flink-metrics-jmx,\
flink-metrics/flink-metrics-influxdb,\
flink-metrics/flink-metrics-prometheus,\
flink-metrics/flink-metrics-statsd,\
flink-metrics/flink-metrics-datadog,\
flink-metrics/flink-metrics-slf4j,\
"

MODULES_CONNECTORS_2="\
flink-connectors/flink-connector-base,\
flink-connectors/flink-connector-kafka,\
flink-connectors/flink-sql-connector-kafka,\
flink-connectors/flink-connector-gcp-pubsub,\
flink-connectors/flink-connector-pulsar,\
flink-connectors/flink-sql-connector-pulsar,\
flink-connectors/flink-connector-rabbitmq,\
flink-connectors/flink-sql-connector-rabbitmq,\
flink-connectors/flink-connector-aws-base,\
flink-connectors/flink-connector-kinesis,\
flink-connectors/flink-sql-connector-kinesis,\
flink-connectors/flink-connector-aws-kinesis-streams,\
flink-connectors/flink-sql-connector-aws-kinesis-streams,\
flink-connectors/flink-connector-aws-kinesis-firehose,\
flink-connectors/flink-sql-connector-aws-kinesis-firehose,\
"

MODULES_TESTS="\
flink-tests,\
"

MODULES_FINEGRAINED_RESOURCE_MANAGEMENT="\
flink-runtime,\
flink-tests,\
"

function get_compile_modules_for_stage() {
    local stage=$1

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $MODULES_CORE -am"
        ;;
        (${STAGE_TABLE})
            echo "-pl $MODULES_TABLE -am"
        ;;
        (${STAGE_CONNECTORS_1})
            echo "-pl $MODULES_CONNECTORS_1 -am"
        ;;
        (${STAGE_CONNECTORS_2})
            echo "-pl $MODULES_CONNECTORS_2 -am"
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
    local modules_table=$MODULES_TABLE
    local modules_connectors_2=$MODULES_CONNECTORS_2
    local modules_connectors_1=$MODULES_CONNECTORS_1
    local modules_tests=$MODULES_TESTS
    local negated_core=\!${MODULES_CORE//,/,\!}
    local negated_table=\!${MODULES_TABLE//,/,\!}
    local negated_connectors_2=\!${MODULES_CONNECTORS_2//,/,\!}
    local negated_connectors_1=\!${MODULES_CONNECTORS_1//,/,\!}
    local negated_tests=\!${MODULES_TESTS//,/,\!}
    local modules_misc="flink-yarn-tests"
    local modules_finegrained_resource_management=$MODULES_FINEGRAINED_RESOURCE_MANAGEMENT

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_TABLE})
            echo "-pl $modules_table"
        ;;
        (${STAGE_CONNECTORS_1})
            echo "-pl $modules_connectors_1"
        ;;
        (${STAGE_CONNECTORS_2})
            echo "-pl $modules_connectors_2"
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
